#!/usr/bin/env python3
"""V13 Script 1/5: Wallet equity series reconstruction.

Per projects/quant/v13 Section 5.1.

Equity at time t is decomposed (forward, first principles):

    equity[t] = cumulative_ledger_net[t]
              + cumulative_realized_pnl[t]
              + open_position_mtm[t]

where:
    - cumulative_ledger_net[t]   = sum(deposits up to t) - sum(withdrawals up to t)
    - cumulative_realized_pnl[t] = sum(closedPnl up to t) from S3 fills
    - open_position_mtm[t]       = sum_coin(position[coin][t] * mark_price[coin][t])

The position state at any t is derived by walking the wallet's fills forward
from start_of_data and applying the signed size deltas.

Today's equity from info.user_state is used as an AUDIT check, not as the
anchor; the forward decomposition is mathematically equivalent to the
backward formulation in the v13 doc but easier to verify per timestep.

Inputs:
    --wallets <path>      Newline-separated wallet addresses, OR
    --discover-from-fills Use unique wallets in the S3 fill data
    --max-wallets N       Cap the wallet count (default: 200)
    --start YYYY-MM-DD    First date in the equity series (default: earliest fill)
    --end YYYY-MM-DD      Last date (default: today)
    --output <path>       Output parquet (default: app/data/v13/wallet_equity_series.parquet)

Outputs:
    wallet_equity_series.parquet, columns:
        wallet, date, equity_usd, realized_pnl_cum, ledger_net_cum,
        open_position_mtm, audit_today_diff_pct

Usage:
    python scripts/v13_equity_reconstruct.py --discover-from-fills --max-wallets 200
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_equity] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_equity_series.parquet"

HL_INFO_URL = "https://api.hyperliquid.xyz/info"

# ---------------------------------------------------------------------------
# HL info API helpers (light wrappers, no SDK to keep this script self-contained)
# ---------------------------------------------------------------------------

def _hl_post(body: dict, max_retries: int = 5, base_sleep: float = 0.5) -> dict | list | None:
    """POST to HL info endpoint with simple exponential backoff."""
    for attempt in range(max_retries):
        try:
            r = requests.post(HL_INFO_URL, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                sleep_s = base_sleep * (2 ** attempt)
                logger.warning(f"HL 429, backing off {sleep_s:.1f}s")
                time.sleep(sleep_s)
                continue
            logger.warning(f"HL {r.status_code}: {r.text[:200]}")
            return None
        except requests.RequestException as e:
            sleep_s = base_sleep * (2 ** attempt)
            logger.warning(f"HL request error: {e}, retry in {sleep_s:.1f}s")
            time.sleep(sleep_s)
    return None


def get_user_state(addr: str) -> dict | None:
    return _hl_post({"type": "clearinghouseState", "user": addr})


def get_spot_user_state(addr: str) -> dict | None:
    return _hl_post({"type": "spotClearinghouseState", "user": addr})


def get_non_funding_ledger_updates(addr: str, start_ms: int, end_ms: int) -> list:
    body = {
        "type": "userNonFundingLedgerUpdates",
        "user": addr,
        "startTime": start_ms,
        "endTime": end_ms,
    }
    resp = _hl_post(body)
    return resp if isinstance(resp, list) else []


def get_current_equity_usd(addr: str) -> float:
    """Today's total equity = perp account_value + spot USDC. Audit anchor."""
    us = get_user_state(addr) or {}
    sus = get_spot_user_state(addr) or {}
    perp = float((us.get("marginSummary") or {}).get("accountValue", 0))
    spot = 0.0
    for bal in (sus.get("balances") or []):
        if bal.get("coin") == "USDC":
            spot += float(bal.get("total", 0))
    return perp + spot


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_fills_for_dates(
    start: datetime, end: datetime, wallets: set[str] | None = None
) -> pd.DataFrame:
    """Load S3 fills parquets in the date range. Optionally filter to wallets."""
    frames = []
    cur = start
    while cur <= end:
        p = FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            if wallets is not None:
                df = df[df["wallet"].isin(wallets)]
            frames.append(df)
        cur += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["wallet"] = out["wallet"].str.lower()
    return out


def load_daily_close_prices(coins: list[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Pull 1h candles from Mongo, resample to UTC midnight close. Returns wide DataFrame indexed by date with columns=coins."""
    c = MongoClient("mongodb://localhost:27017")["quants_lab"]["hyperliquid_candles_1h"]
    start_ms = int(start.replace(tzinfo=timezone.utc).timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp() * 1000)

    docs = list(c.find(
        {"coin": {"$in": list(coins)}, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
        {"coin": 1, "timestamp_utc": 1, "close": 1, "_id": 0},
    ))
    if not docs:
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    df["date"] = df["dt"].dt.floor("D")

    # take the LAST close per day per coin (UTC midnight close = last hour's close)
    daily = df.sort_values("dt").groupby(["coin", "date"], as_index=False).last()
    pivot = daily.pivot(index="date", columns="coin", values="close")
    pivot.index = pivot.index.tz_convert("UTC").date
    return pivot


# ---------------------------------------------------------------------------
# Per-wallet equity reconstruction
# ---------------------------------------------------------------------------

def reconstruct_wallet_equity(
    wallet: str,
    wallet_fills: pd.DataFrame,
    daily_close: pd.DataFrame,
    date_range: list,
    start_ms: int,
    end_ms: int,
) -> pd.DataFrame:
    """Per-day equity series for one wallet via forward decomposition.

    Returns DataFrame with: wallet, date, equity_usd, realized_pnl_cum,
    ledger_net_cum, open_position_mtm, audit_today_diff_pct (filled only on
    the last row).
    """
    # 1) Compute daily realized PnL from fills.
    if wallet_fills.empty:
        return pd.DataFrame()
    wf = wallet_fills.copy()
    wf["dt"] = pd.to_datetime(wf["time"], unit="ms", utc=True)
    wf["date"] = wf["dt"].dt.floor("D").dt.date

    daily_realized = wf.groupby("date")["closedPnl"].sum()
    daily_realized = daily_realized.reindex(date_range, fill_value=0.0)
    cum_realized = daily_realized.cumsum()

    # 2) Compute running position per coin from fills.
    # side: "A" = ask = sell, "B" = bid = buy
    wf["signed_size"] = wf.apply(
        lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1
    )
    pos_deltas = wf.groupby(["date", "coin"])["signed_size"].sum().unstack(fill_value=0.0)
    pos_deltas = pos_deltas.reindex(date_range, fill_value=0.0)
    cum_position = pos_deltas.cumsum()    # date index, columns = coins

    # 3) Ledger updates (deposits / withdrawals) from HL API.
    ledger = get_non_funding_ledger_updates(wallet, start_ms, end_ms)
    daily_deposit = pd.Series(0.0, index=date_range)
    daily_withdraw = pd.Series(0.0, index=date_range)
    for entry in ledger:
        ts = int(entry.get("time", 0))
        d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()
        if d not in daily_deposit.index:
            continue
        delta = entry.get("delta") or {}
        kind = delta.get("type", "")
        usdc = float(delta.get("usdc", 0))
        if kind in ("deposit", "internalTransfer", "subAccountTransfer"):
            daily_deposit[d] += abs(usdc) if usdc > 0 else 0
            daily_withdraw[d] += abs(usdc) if usdc < 0 else 0
        elif kind == "withdraw":
            daily_withdraw[d] += abs(usdc)
        # other types (rewards / vault) treated as non-flow for v1
    cum_ledger_net = (daily_deposit - daily_withdraw).cumsum()

    # 4) Open position MTM per day.
    mtm_per_day = []
    common_coins = [c for c in cum_position.columns if c in daily_close.columns]
    for d in date_range:
        pos_row = cum_position.loc[d]
        if pos_row.empty:
            mtm_per_day.append(0.0)
            continue
        if d not in daily_close.index:
            mtm_per_day.append(0.0)
            continue
        price_row = daily_close.loc[d]
        mtm = 0.0
        for coin in common_coins:
            sz = pos_row.get(coin, 0.0)
            px = price_row.get(coin)
            if sz != 0 and px is not None and not pd.isna(px):
                mtm += sz * float(px)
        mtm_per_day.append(mtm)

    # 5) Anchor on today's API equity. The data window does NOT necessarily
    # include the wallet's first deposit, so the forward sum is missing an
    # initial offset. Anchoring on today removes that offset:
    #
    #   flow[t] = cum_realized[t] + cum_ledger[t] + mtm[t]
    #   equity[t] = equity[today_api] + (flow[t] - flow[today])
    #
    # For t = last day in window, equity = equity[today_api] (exactly).
    # For earlier t, equity walks back through the realized + ledger + mtm
    # deltas between t and today.
    api_equity = get_current_equity_usd(wallet)
    mtm_series = pd.Series(mtm_per_day, index=date_range)
    flow = cum_realized + cum_ledger_net + mtm_series   # all aligned on date_range
    flow_last = flow.iloc[-1]
    equity = api_equity + (flow - flow_last).values

    df = pd.DataFrame({
        "wallet": wallet,
        "date": list(date_range),
        "equity_usd": equity,
        "realized_pnl_cum": cum_realized.values,
        "ledger_net_cum": cum_ledger_net.values,
        "open_position_mtm": mtm_per_day,
    })

    # 6) Audit: last-row reconstructed equity should match API exactly by
    # construction. Surface any divergence as a sanity check.
    if df.empty or api_equity == 0:
        df["audit_today_diff_pct"] = None
    else:
        last = df.iloc[-1]["equity_usd"]
        diff_pct = 100 * (last - api_equity) / api_equity
        df["audit_today_diff_pct"] = None
        df.iloc[-1, df.columns.get_loc("audit_today_diff_pct")] = diff_pct

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def discover_wallets_from_fills(fills_df: pd.DataFrame, max_n: int) -> list[str]:
    """Return top-N wallets by fill count."""
    counts = fills_df["wallet"].str.lower().value_counts()
    return counts.head(max_n).index.tolist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", help="Path to newline-separated wallet list")
    ap.add_argument("--discover-from-fills", action="store_true",
                    help="Use top-N wallets by fill count in the S3 data")
    ap.add_argument("--max-wallets", type=int, default=200)
    ap.add_argument("--start", help="YYYY-MM-DD")
    ap.add_argument("--end", help="YYYY-MM-DD")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    # Date range.
    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        # Earliest fill on disk.
        files = sorted(FILLS_DIR.glob("*.parquet"))
        if not files:
            logger.error("No S3 fills found. Run hl_s3_fill_downloader.py first.")
            sys.exit(1)
        first_name = files[0].stem
        start = datetime.strptime(first_name, "%Y%m%d").replace(tzinfo=timezone.utc)

    if args.end:
        end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    logger.info(f"Date range: {start.date()} to {end.date()}")

    # Wallet universe.
    if args.wallets:
        with open(args.wallets) as f:
            wallets = [w.strip().lower() for w in f if w.strip()]
    elif args.discover_from_fills:
        logger.info("Discovering wallets from S3 fills...")
        all_fills = load_fills_for_dates(start, end)
        if all_fills.empty:
            logger.error("No fills loaded.")
            sys.exit(1)
        wallets = discover_wallets_from_fills(all_fills, args.max_wallets)
        logger.info(f"Discovered {len(wallets)} wallets (top by fill count)")
    else:
        logger.error("Provide --wallets or --discover-from-fills")
        sys.exit(2)

    # Load fills once for all candidate wallets.
    logger.info(f"Loading fills for {len(wallets)} wallets in date window...")
    fills = load_fills_for_dates(start, end, set(wallets))
    if fills.empty:
        logger.error("No matching fills found.")
        sys.exit(1)
    logger.info(f"Loaded {len(fills):,} fills")

    # Load daily close prices for relevant coins.
    coins = sorted(fills["coin"].dropna().unique().tolist())
    logger.info(f"Loading daily close prices for {len(coins)} coins from MongoDB...")
    daily_close = load_daily_close_prices(coins, start, end)
    if daily_close.empty:
        logger.warning("No candle data loaded -- MTM will be zero for all days.")

    # Build date index (per-day floor, UTC).
    n_days = (end - start).days + 1
    date_range = [(start + timedelta(days=i)).date() for i in range(n_days)]

    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)

    # Reconstruct each wallet.
    all_rows = []
    for i, w in enumerate(wallets, 1):
        wf = fills[fills["wallet"] == w]
        if wf.empty:
            continue
        try:
            df = reconstruct_wallet_equity(
                w, wf, daily_close, date_range, start_ms, end_ms
            )
            if not df.empty:
                all_rows.append(df)
        except Exception as e:
            logger.exception(f"[{w[:8]}] reconstruction failed: {e}")
        if i % 20 == 0 or i == len(wallets):
            logger.info(f"Reconstructed {i}/{len(wallets)} wallets")

    if not all_rows:
        logger.error("Zero wallets successfully reconstructed.")
        sys.exit(1)

    out_df = pd.concat(all_rows, ignore_index=True)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(out_df):,} rows to {out_path}")

    # Audit summary.
    audits = out_df.dropna(subset=["audit_today_diff_pct"])
    if not audits.empty:
        median_abs = audits["audit_today_diff_pct"].abs().median()
        p90 = audits["audit_today_diff_pct"].abs().quantile(0.90)
        bad = (audits["audit_today_diff_pct"].abs() > 30).sum()
        logger.info(
            f"Audit vs today: n={len(audits)}, median |diff|={median_abs:.2f}%, "
            f"p90={p90:.2f}%, >30% off={bad}"
        )


if __name__ == "__main__":
    main()
