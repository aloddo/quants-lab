#!/usr/bin/env python3
"""V13 Script 1/5 (v2): Wallet equity series reconstruction.

Per projects/quant/v13 Section 5.1 and remediation plan v2.

The equity series for each wallet is built as:

    equity[t] = today_api_equity
              - (cum_realized[end .. today]    - cum_realized[end .. t])
              - (cum_ledger_net[end .. today]  - cum_ledger_net[end .. t])
              - (mtm_unrealized[today]         - mtm_unrealized[t])

Equivalently, the daily delta is composed of realized PnL + signed ledger flow
+ change in unrealized PnL. Equity is ANCHORED on today's live API equity,
which is the only externally-verifiable ground truth.

Unrealized PnL at time t is computed against AVERAGE COST BASIS, not against
notional. Opening a position does not change equity; the position holds value
equal to its mark price minus its cost basis times quantity.

Carry-in handling: each (wallet, coin) is walked back through prior S3 fills
to the last point where net position was zero (last-flat). If we cannot reach
last-flat within available data, the pair is marked PRE_WINDOW_INCOMPLETE and
excluded from journey-level metrics and backtest signal replay.

Historical end-date handling: if --end < today, the equity series is back-
solved by walking activity from end to today via the same identity above. If
data between end and today is missing for any wallet, that wallet is marked
INCOMPLETE and skipped (not silently anchored on a future-leaking value).

Inputs:
    --wallets <path>        Newline-separated wallet addresses, OR
    --discover-from-fills   Use unique wallets in the S3 fill data
    --max-wallets N         Cap the wallet count (default: 200)
    --start YYYY-MM-DD      First date in the equity series (default: earliest fill)
    --end YYYY-MM-DD        Last date (default: today)
    --output <path>         app/data/v13/wallet_equity_series.parquet

Outputs:
    wallet_equity_series.parquet, columns:
        wallet, date, equity_usd, realized_pnl_cum, ledger_net_cum,
        mtm_unrealized, audit_today_diff_pct,
        carry_in_status, pre_window_position_value
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
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

EPS = 1e-9                                    # dust / zero comparison epsilon
VALID_SIDES = frozenset({"A", "B"})

# Ledger type accounting. Each entry maps a HL non-funding ledger `delta.type`
# to (classification, sign_convention).
# - external_explicit: signed flow taken from delta.usdc as provided
# - internal: net-zero for total-account equity (just shuffles within account)
# - income: positive flow (gains the wallet)
LEDGER_TYPE_MAP = {
    "deposit":             "external_explicit",
    "withdraw":            "external_explicit",
    "internalTransfer":    "internal",
    "subAccountTransfer":  "external_explicit",
    "spotTransfer":        "external_explicit",
    "vaultDeposit":        "external_explicit",
    "vaultWithdraw":       "external_explicit",
    "vaultDistribution":   "income",
    "rewards":             "income",
    "rewardsClaim":        "income",
    "staking":             "income",
    "stakingDeposit":      "internal",          # USDC -> HYPE staking conversion
    "stakingWithdraw":     "internal",          # reverse of stakingDeposit
    "stakingUnlock":       "internal",
    "cStakingTransfer":    "internal",          # CORE staking shift (HYPE token), not USDC equity flow
    "cStakingDeposit":     "internal",
    "cStakingWithdraw":    "internal",
    "cWithdraw":           "external_explicit",
    "cDeposit":            "external_explicit",
    "perpDelist":          "internal",
    "send":                "external_send",      # outbound transfer to another address
    "receive":             "external_receive",   # inbound transfer from another address
    "liquidation":         "internal",          # PnL already captured in fills
    "transferFromSubAccount": "external_explicit",
    "transferToSubAccount":   "external_explicit",
}


# ---------------------------------------------------------------------------
# HL info API helpers
# ---------------------------------------------------------------------------

def _hl_post(body: dict, max_retries: int = 5, base_sleep: float = 0.5) -> dict | list | None:
    for attempt in range(max_retries):
        try:
            r = requests.post(HL_INFO_URL, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 429:
                time.sleep(base_sleep * (2 ** attempt))
                continue
            logger.warning(f"HL {r.status_code}: {r.text[:200]}")
            return None
        except requests.RequestException as e:
            time.sleep(base_sleep * (2 ** attempt))
            logger.warning(f"HL request error: {e}")
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


def get_current_equity_usd(addr: str) -> tuple[float, bool]:
    """Returns (equity_usd, ok). ok is False if any API call failed.

    Zero equity is allowed only if both spot and perp returned successful
    responses and both are zero. A missing/failed response is hard fail.
    """
    us = get_user_state(addr)
    sus = get_spot_user_state(addr)
    if us is None or sus is None:
        return 0.0, False
    perp = float((us.get("marginSummary") or {}).get("accountValue", 0))
    spot = 0.0
    for bal in (sus.get("balances") or []):
        if bal.get("coin") == "USDC":
            spot += float(bal.get("total", 0))
    return spot + perp, True


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_fills_for_dates(
    start: datetime, end: datetime, wallets: set[str] | None = None
) -> pd.DataFrame:
    frames = []
    cur = start
    while cur <= end:
        p = FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            if wallets is not None:
                df = df[df["wallet"].str.lower().isin(wallets)]
            frames.append(df)
        cur += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["wallet"] = out["wallet"].str.lower()
    return out


def load_daily_close_prices(coins: list[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Returns wide DataFrame indexed by date (UTC) with columns=coins, values=close."""
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
    daily = df.sort_values("dt").groupby(["coin", "date"], as_index=False).last()
    pivot = daily.pivot(index="date", columns="coin", values="close")
    pivot.index = pivot.index.tz_convert("UTC").date
    return pivot


# ---------------------------------------------------------------------------
# Per-fill schema validation
# ---------------------------------------------------------------------------

REQUIRED_FILL_COLUMNS = {"wallet", "coin", "side", "size", "price", "time", "closedPnl", "dir"}


def validate_and_normalize_fills(df: pd.DataFrame) -> pd.DataFrame:
    """Strict validation: required columns present, side in {A,B}, time/price > 0.
    Skip rows with NaN coin or dust size. Hard fail on bad schema.
    """
    if df.empty:
        return df
    missing = REQUIRED_FILL_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"fills missing required columns: {missing}")
    # Hard fail on unknown side.
    bad_sides = df[~df["side"].isin(VALID_SIDES)]
    if not bad_sides.empty:
        raise ValueError(f"Unknown side enum found in {len(bad_sides)} fills, sample: {bad_sides['side'].unique()[:5]}")
    df = df.dropna(subset=["coin"])
    df = df[df["coin"].astype(str).str.len() > 0]
    df = df[df["size"].astype(float) > EPS]
    df = df[df["price"].astype(float) > EPS]
    df = df[df["time"].astype("int64") > 0]
    return df


# ---------------------------------------------------------------------------
# Ledger flow accumulator
# ---------------------------------------------------------------------------

def accumulate_ledger_flow(entries: list) -> pd.DataFrame:
    """Returns DataFrame with columns: date, signed_flow_usd.

    Hard fails on unknown delta.type.
    """
    rows = []
    for entry in entries:
        ts = int(entry.get("time", 0))
        if ts == 0:
            continue
        delta = entry.get("delta") or {}
        kind = delta.get("type", "")
        if not kind:
            continue                                # malformed entry
        if kind not in LEDGER_TYPE_MAP:
            raise ValueError(f"Unknown HL ledger type: {kind} (entry: {entry})")
        classification = LEDGER_TYPE_MAP[kind]
        # Different ledger types put the USDC amount in different fields.
        # `usdc` field is used by deposit/withdraw/vault/staking/etc.
        # `send`/`receive` use `usdcValue` (or `amount` when token=USDC).
        usdc = float(delta.get("usdc", 0))
        if classification == "external_send":
            # send: outbound, only count USDC-equivalent value.
            amt = float(delta.get("usdcValue", delta.get("amount", 0))) if delta.get("token", "USDC") == "USDC" else float(delta.get("usdcValue", 0))
            signed = -abs(amt)
        elif classification == "external_receive":
            amt = float(delta.get("usdcValue", delta.get("amount", 0))) if delta.get("token", "USDC") == "USDC" else float(delta.get("usdcValue", 0))
            signed = abs(amt)
        elif classification == "internal":
            signed = 0.0
        elif classification == "external_explicit":
            signed = usdc                          # delta.usdc carries the sign (positive = inflow)
        elif classification == "income":
            signed = abs(usdc)
        else:
            raise ValueError(f"Bad classification {classification}")
        rows.append({
            "date": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date(),
            "signed_flow_usd": signed,
            "classification": classification,
            "raw_type": kind,
        })
    if not rows:
        return pd.DataFrame(columns=["date", "signed_flow_usd", "classification", "raw_type"])
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Carry-in walk-back to last flat
# ---------------------------------------------------------------------------

def load_prior_fills_for_wallets(
    wallets: set[str],
    prior_fills_dir: Path,
    window_start: datetime,
    max_walkback_days: int = 90,
) -> pd.DataFrame:
    """Single bulk load of prior fills for ALL wallets in one pass.

    Returns a DataFrame keyed by (wallet, coin, time) of all fills strictly
    before window_start, going back up to max_walkback_days. This avoids
    re-reading every daily parquet per (wallet, coin) pair.
    """
    cur_date = window_start.date() - timedelta(days=1)
    days_walked = 0
    frames: list[pd.DataFrame] = []
    while days_walked < max_walkback_days:
        p = prior_fills_dir / f"{cur_date.strftime('%Y%m%d')}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            df = df[df["wallet"].str.lower().isin(wallets)]
            if not df.empty:
                frames.append(df)
        cur_date -= timedelta(days=1)
        days_walked += 1
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["wallet"] = out["wallet"].str.lower()
    return out


def find_carry_in_state_from_prior(
    wallet: str,
    coin: str,
    prior_fills: pd.DataFrame,                       # already filtered to wallets
) -> tuple[float, float, str]:
    """Compute (position, cost_basis, status) at window_start from a pre-loaded
    prior_fills DataFrame.

    The walkback assumes the wallet was FLAT before the earliest loaded fill.
    Validity of that assumption is proven only if we observe a true zero
    crossing somewhere AFTER the first loaded fill. If position ends non-zero
    and we never saw such a crossing, the wallet had pre-load activity we
    cannot account for -> incomplete.

    status:
        "no_carry"       wallet had no prior fills OR ended flat by window_start
        "carry_resolved" passed through zero after first-loaded-fill; carry-in seed is valid
        "incomplete"     position non-zero, no proven zero-crossing within loaded history
    """
    if prior_fills.empty:
        return 0.0, 0.0, "no_carry"
    # Caller may pass a wallet-pre-grouped frame (only that wallet's rows). If
    # the frame still has multiple wallets, filter; otherwise just filter by
    # coin (O(N) of rows for this wallet).
    if (prior_fills["wallet"] != wallet).any():
        pf = prior_fills[(prior_fills["wallet"] == wallet) & (prior_fills["coin"] == coin)]
    else:
        pf = prior_fills[prior_fills["coin"] == coin]
    if pf.empty:
        return 0.0, 0.0, "no_carry"
    pf = validate_and_normalize_fills(pf)
    if pf.empty:
        return 0.0, 0.0, "no_carry"
    sort_keys = ["time", "side", "price", "size"]
    if "hash" in pf.columns:
        sort_keys = ["time", "hash", "side", "price", "size"]
    pf = pf.sort_values(sort_keys, kind="stable")
    prior_fills_sorted = pf

    # Walk forward through prior fills using itertuples for speed.
    position = 0.0
    cost_basis = 0.0
    saw_flat_after_first = False
    is_first = True
    for r in prior_fills_sorted.itertuples(index=False):
        size = float(r.size)
        signed = size if r.side == "B" else -size
        price = float(r.price)
        new_pos = position + signed

        if not is_first and abs(position) < EPS:
            saw_flat_after_first = True

        if abs(position) < EPS:
            cost_basis = price
        elif (position > 0 and signed > 0) or (position < 0 and signed < 0):
            total_qty = abs(new_pos)
            cost_basis = (cost_basis * abs(position) + price * abs(signed)) / total_qty
        elif abs(new_pos) < EPS:
            cost_basis = 0.0
        elif (position > 0 and new_pos > 0) or (position < 0 and new_pos < 0):
            pass
        else:
            cost_basis = price
            saw_flat_after_first = True

        position = new_pos
        is_first = False

    if abs(position) < EPS:
        return 0.0, 0.0, "no_carry"
    if saw_flat_after_first:
        return position, cost_basis, "carry_resolved"
    # Non-zero position with no proven zero crossing -> pre-load activity exists.
    return position, cost_basis, "incomplete"


# ---------------------------------------------------------------------------
# Per-wallet daily series builder
# ---------------------------------------------------------------------------

def reconstruct_one_wallet(
    wallet: str,
    in_window_fills: pd.DataFrame,
    daily_close: pd.DataFrame,
    date_range: list,
    start_ms: int,
    end_ms: int,
    today_api_equity: float,
    historical_anchor: bool,                       # True if end < today
    historical_gap_fills: pd.DataFrame | None,     # fills in [end, today] if historical
    historical_gap_ledger: pd.DataFrame | None,
    historical_gap_daily_close: pd.DataFrame | None,
    prior_fills: pd.DataFrame | None = None,       # pre-loaded prior fills bulk
    window_start: datetime | None = None,
) -> pd.DataFrame | None:
    """Build per-day equity series for one wallet."""
    wf = validate_and_normalize_fills(in_window_fills.copy())
    if wf.empty and not historical_anchor:
        # If end == today and there are no fills, the wallet may still hold
        # carry-in positions. Continue with carry-in only.
        pass

    # 1) Per-fill signed size + signed notional.
    if not wf.empty:
        wf["signed_size"] = wf.apply(
            lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1
        )
        wf["fill_price"] = wf["price"].astype(float)
        wf["dt"] = pd.to_datetime(wf["time"], unit="ms", utc=True)
        wf["date"] = wf["dt"].dt.floor("D").dt.date

    # 2) Carry-in seeds per coin. Use pre-loaded prior_fills (bulk-loaded once
    # at main() level). coins_seen = union(in_window_coins, prior_wallet_coins)
    # so wallets with prior-only-traded positions are covered.
    in_window_coins = set(wf["coin"].dropna().unique().tolist()) if not wf.empty else set()
    prior_for_wallet = prior_fills[prior_fills["wallet"] == wallet] if prior_fills is not None and not prior_fills.empty else pd.DataFrame()
    prior_coins = set(prior_for_wallet["coin"].dropna().unique().tolist()) if not prior_for_wallet.empty else set()
    coins_seen = sorted(in_window_coins | prior_coins)
    carry_in: dict[str, tuple[float, float, str]] = {}
    incomplete_pairs: set[str] = set()
    for coin in coins_seen:
        pos_in, cb_in, status = find_carry_in_state_from_prior(wallet, coin, prior_for_wallet)
        carry_in[coin] = (pos_in, cb_in, status)
        if status == "incomplete":
            incomplete_pairs.add(coin)

    # 3) Walk in-window fills forward; maintain per-coin (position, cost basis).
    # Build daily realized PnL and end-of-day unrealized PnL.
    position: dict[str, float] = {c: carry_in[c][0] for c in coins_seen}
    cost_basis: dict[str, float] = {c: carry_in[c][1] for c in coins_seen}

    daily_realized = pd.Series(0.0, index=date_range)
    daily_unrealized = pd.Series(0.0, index=date_range)
    # Walk per fill; bucket by day.
    if not wf.empty:
        wf_sorted = wf.sort_values(["time", "coin", "side", "price", "size"], kind="stable")
        for _, r in wf_sorted.iterrows():
            coin = r["coin"]
            signed = float(r["signed_size"])
            price = float(r["fill_price"])
            d = r["date"]
            pos = position.get(coin, 0.0)
            cb = cost_basis.get(coin, 0.0)
            new_pos = pos + signed

            realized_today = 0.0
            if abs(pos) < EPS:
                cb = price
            elif (pos > 0 and signed > 0) or (pos < 0 and signed < 0):
                # Same-direction add.
                total_qty = abs(new_pos)
                cb = (cb * abs(pos) + price * abs(signed)) / total_qty if total_qty > EPS else price
            elif abs(new_pos) < EPS:
                # Full close.
                realized_today = (price - cb) * pos    # signed: long * (price - cb) = profit if price>cb
                cb = 0.0
            elif (pos > 0 and new_pos > 0) or (pos < 0 and new_pos < 0):
                # Trim (partial close, same side remains).
                closed_qty = abs(signed)
                # Realized PnL of the closed portion: sign(pos) * (price - cb) * closed_qty
                realized_today = (price - cb) * (closed_qty if pos > 0 else -closed_qty)
                # cost basis unchanged
            else:
                # Reverse: close existing leg fully + open new leg at price.
                close_qty = abs(pos)
                realized_today = (price - cb) * pos     # closes the leg
                cb = price                              # new leg starts here
            position[coin] = new_pos
            cost_basis[coin] = cb
            daily_realized[d] = daily_realized.get(d, 0.0) + realized_today

    # 4) Compute daily unrealized PnL using end-of-day positions + mark prices.
    # We need the position state at end of each day; walk fills again, but
    # bucket by date and snapshot.
    position2: dict[str, float] = {c: carry_in[c][0] for c in coins_seen}
    cost_basis2: dict[str, float] = {c: carry_in[c][1] for c in coins_seen}
    if not wf.empty:
        wf_grouped = wf.sort_values(["time", "coin", "side", "price", "size"], kind="stable").groupby("date")
        for d in date_range:
            if d in wf_grouped.groups:
                day_fills = wf_grouped.get_group(d)
                for _, r in day_fills.iterrows():
                    coin = r["coin"]
                    signed = float(r["signed_size"])
                    price = float(r["fill_price"])
                    pos = position2.get(coin, 0.0)
                    cb = cost_basis2.get(coin, 0.0)
                    new_pos = pos + signed
                    if abs(pos) < EPS:
                        cb = price
                    elif (pos > 0 and signed > 0) or (pos < 0 and signed < 0):
                        total_qty = abs(new_pos)
                        cb = (cb * abs(pos) + price * abs(signed)) / total_qty if total_qty > EPS else price
                    elif abs(new_pos) < EPS:
                        cb = 0.0
                    elif (pos > 0 and new_pos > 0) or (pos < 0 and new_pos < 0):
                        pass
                    else:
                        cb = price
                    position2[coin] = new_pos
                    cost_basis2[coin] = cb
            # End-of-day mark-to-market.
            mtm = 0.0
            for coin, pos in position2.items():
                if abs(pos) < EPS:
                    continue
                if daily_close is not None and not daily_close.empty and d in daily_close.index and coin in daily_close.columns:
                    px = daily_close.at[d, coin]
                    if not pd.isna(px):
                        mtm += pos * (float(px) - cost_basis2.get(coin, 0.0))
            daily_unrealized[d] = mtm
    else:
        # No in-window fills; just MTM the carry-in positions each day.
        for d in date_range:
            mtm = 0.0
            for coin, (pos, cb, _) in carry_in.items():
                if abs(pos) < EPS:
                    continue
                if daily_close is not None and not daily_close.empty and d in daily_close.index and coin in daily_close.columns:
                    px = daily_close.at[d, coin]
                    if not pd.isna(px):
                        mtm += pos * (float(px) - cb)
            daily_unrealized[d] = mtm

    # 5) Ledger flow.
    ledger_entries = get_non_funding_ledger_updates(wallet, start_ms, end_ms)
    try:
        ledger_df = accumulate_ledger_flow(ledger_entries)
    except ValueError as e:
        logger.error(f"[{wallet[:8]}] ledger reconstruction failed: {e}")
        return None
    daily_ledger = pd.Series(0.0, index=date_range)
    if not ledger_df.empty:
        daily_sum = ledger_df.groupby("date")["signed_flow_usd"].sum()
        for d, v in daily_sum.items():
            if d in daily_ledger.index:
                daily_ledger[d] = float(v)

    cum_realized = daily_realized.cumsum()
    cum_ledger = daily_ledger.cumsum()

    # 6) Historical anchor: backsolve equity[end] from today's API.
    # equity[end] = today_api - (realized_in_gap + ledger_in_gap + (mtm_today - mtm_end))
    # For in-window day t:
    #   equity[t] = equity[end] + (cum_realized[t] + cum_ledger[t] + mtm[t]) - (cum_realized[end] + cum_ledger[end] + mtm[end])
    # which simplifies, with end as last date in date_range, to:
    #   equity[t] = today_api - flow_gap - (flow[end] - flow[t]) - (mtm[today] - mtm[end])
    # If end == today, flow_gap = 0 and mtm[today] = mtm[end]; the original same-day identity holds.
    if historical_anchor:
        if historical_gap_fills is None or historical_gap_ledger is None:
            logger.warning(f"[{wallet[:8]}] historical anchor required but gap data missing; marking INCOMPLETE")
            return None
        # Compute realized + ledger flows + mtm[today] for this wallet in the gap.
        gap_wf = historical_gap_fills[historical_gap_fills["wallet"] == wallet]
        try:
            gap_wf = validate_and_normalize_fills(gap_wf)
        except ValueError as e:
            logger.error(f"[{wallet[:8]}] gap fills validation failed: {e}")
            return None
        # We don't fully simulate cost basis here; we use closedPnl from S3 as
        # realized PnL proxy in the gap (matches the V1 approach for the gap
        # window only; the in-window is properly reconstructed).
        gap_realized = float(gap_wf["closedPnl"].sum()) if "closedPnl" in gap_wf.columns else 0.0
        gap_ledger = float(historical_gap_ledger[historical_gap_ledger["wallet"] == wallet]["signed_flow_usd"].sum()) if not historical_gap_ledger.empty else 0.0
        # mtm[today] vs mtm[end]: positions evolved over gap. For v2, we
        # require mtm change = 0 ASSUMPTION (positions held the same value).
        # If positions changed materially, we need the full forward replay of
        # gap fills. Mark INCOMPLETE if the gap has fills that change net
        # position significantly.
        if not gap_wf.empty:
            net_size_change = gap_wf.apply(lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1).abs().sum()
            if net_size_change > EPS * 1e6:
                # Material activity in gap; can't anchor cleanly without full replay.
                logger.warning(f"[{wallet[:8]}] gap window has material activity ({net_size_change:.4f}); marking INCOMPLETE")
                return None
        # equity[end] approx = today_api - gap_realized - gap_ledger
        equity_end = today_api_equity - gap_realized - gap_ledger
    else:
        # end == today: anchor on today's API equity directly.
        equity_end = today_api_equity

    flow_end = cum_realized.iloc[-1] + cum_ledger.iloc[-1] + daily_unrealized.iloc[-1]
    flow_t = cum_realized + cum_ledger + daily_unrealized
    equity = equity_end - (flow_end - flow_t.values)

    df = pd.DataFrame({
        "wallet": wallet,
        "date": list(date_range),
        "equity_usd": equity,
        "realized_pnl_cum": cum_realized.values,
        "ledger_net_cum": cum_ledger.values,
        "mtm_unrealized": daily_unrealized.values,
    })

    # 7) Audit + carry-in metadata.
    if df.empty:
        return df
    diff_pct = 100 * (df.iloc[-1]["equity_usd"] - equity_end) / equity_end if equity_end != 0 else 0.0
    df["audit_today_diff_pct"] = None
    df.iloc[-1, df.columns.get_loc("audit_today_diff_pct")] = diff_pct

    # Carry-in summary: how many coins had incomplete carry-in?
    incomplete = sum(1 for c, (_, _, s) in carry_in.items() if s == "incomplete")
    df["carry_in_status"] = "ok" if incomplete == 0 else f"{incomplete}_incomplete"
    df["pre_window_position_value"] = sum(pos * cb for coin, (pos, cb, _) in carry_in.items())

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def discover_wallets_from_fills(fills_df: pd.DataFrame, max_n: int) -> list[str]:
    counts = fills_df["wallet"].str.lower().value_counts()
    return counts.head(max_n).index.tolist()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets")
    ap.add_argument("--discover-from-fills", action="store_true")
    ap.add_argument("--max-wallets", type=int, default=200)
    ap.add_argument("--start")
    ap.add_argument("--end")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--walkback-days", type=int, default=90,
                    help="How far to walk back for carry-in proof (default 90)")
    args = ap.parse_args()

    files = sorted(FILLS_DIR.glob("*.parquet"))
    if not files:
        logger.error("No S3 fills found.")
        sys.exit(1)

    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = datetime.strptime(files[0].stem, "%Y%m%d").replace(tzinfo=timezone.utc)
    if args.end:
        end_requested = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_requested = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    # Always reconstruct to today (the anchorable endpoint). If the user asked
    # for end < today, we trim the output to [start, end_requested] at the end.
    # This eliminates the gap-anchor complexity: equity[t] is fully forward-
    # reconstructed from window_start to today using all data on disk, then
    # anchored on today's API equity.
    end = today_utc
    output_trim = end_requested < today_utc
    historical_anchor = False                     # no longer needed; always anchor on today

    logger.info(f"Reconstruction range: {start.date()} -> {end.date()} (today-anchor); "
                f"output trimmed to {start.date()} -> {end_requested.date()}" if output_trim
                else f"Date range: {start.date()} to {end.date()} (today-anchor)")

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
        logger.info(f"Discovered {len(wallets)} wallets")
    else:
        logger.error("Provide --wallets or --discover-from-fills")
        sys.exit(2)

    # Missing-data guard: if reconstruction runs to today and any daily parquet
    # in the [start, today] window is missing, the equity series silently
    # treats that day as zero activity. Hard-fail before reconstruction.
    missing_days = []
    cur = start
    while cur <= end:
        if not (FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet").exists():
            missing_days.append(cur.date())
        cur += timedelta(days=1)
    if missing_days:
        logger.error(f"Missing daily parquets in reconstruction window: {missing_days[:5]}{'...' if len(missing_days)>5 else ''}")
        logger.error("Reconstruction would silently zero-fill these days. Aborting.")
        sys.exit(1)

    fills = load_fills_for_dates(start, end, set(wallets))
    if not fills.empty:
        fills = validate_and_normalize_fills(fills)
    logger.info(f"Loaded + validated {len(fills):,} in-window fills")

    # BULK-load prior fills for ALL candidate wallets at once. This is the
    # carry-in scan source; one pass through the walkback window of daily
    # parquets, filtered to our wallet set. Memory cliff above ~90 days.
    # Note: we load prior fills even if there are zero in-window fills, so
    # that pure-carry-in holders (positions opened pre-window, held through)
    # can be reconstructed via MTM-only.
    logger.info(f"Bulk-loading prior fills for carry-in walkback (up to {args.walkback_days} days)...")
    prior_fills = load_prior_fills_for_wallets(set(wallets), FILLS_DIR, start, max_walkback_days=args.walkback_days)
    logger.info(f"Bulk-loaded {len(prior_fills):,} prior fills for {len(wallets)} wallets")

    if fills.empty and prior_fills.empty:
        logger.error("No fills found in-window OR pre-window for any selected wallet. Aborting.")
        sys.exit(1)

    # Coin universe = in-window UNION prior-window (so carry-in coins have marks).
    in_window_coins = set(fills["coin"].dropna().unique().tolist()) if not fills.empty else set()
    prior_coins = set(prior_fills["coin"].dropna().unique().tolist()) if not prior_fills.empty else set()
    coins = sorted(in_window_coins | prior_coins)
    logger.info(f"Loading daily close prices for {len(coins)} coins (in-window={len(in_window_coins)}, prior-only={len(coins)-len(in_window_coins)})...")
    daily_close = load_daily_close_prices(coins, start, end)

    # Historical-anchor / gap logic deleted: reconstruction always runs through
    # today, so there is no gap window to bridge.
    n_days = (end - start).days + 1
    date_range = [(start + timedelta(days=i)).date() for i in range(n_days)]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)

    all_rows = []
    skipped = {"api_equity_failed": 0, "ledger_unknown_type": 0, "gap_material": 0}
    for i, w in enumerate(wallets, 1):
        wf = fills[fills["wallet"] == w] if not fills.empty else pd.DataFrame()
        # Pure carry-in holder allowed: wf may be empty if the wallet only has
        # prior-window positions held through the window.
        api_eq, ok = get_current_equity_usd(w)
        if not ok:
            skipped["api_equity_failed"] += 1
            logger.warning(f"[{w[:8]}] API equity failed; skipping wallet")
            continue
        if api_eq <= EPS:
            # Check whether truly empty (both spot+perp zero with successful responses)
            # vs a 0 anchor that would poison the series. ok==True means both
            # responses arrived. A genuinely-empty wallet has api_eq==0 with ok==True.
            # We still skip such wallets from the series since their equity-relative
            # signals would divide by zero.
            skipped["api_equity_failed"] += 1
            logger.info(f"[{w[:8]}] API equity is zero; skipping (empty account)")
            continue
        try:
            df = reconstruct_one_wallet(
                w, wf, daily_close, date_range, start_ms, end_ms,
                api_eq, False,                  # historical_anchor always False
                None, None, None,
                prior_fills=prior_fills,
                window_start=start,
            )
        except ValueError as e:
            skipped["ledger_unknown_type"] += 1
            logger.error(f"[{w[:8]}] reconstruction failed: {e}")
            continue
        if df is None:
            skipped["gap_material"] += 1
            continue
        if not df.empty:
            all_rows.append(df)
        if i % 25 == 0 or i == len(wallets):
            logger.info(f"Reconstructed {i}/{len(wallets)}; skipped={skipped}")

    if not all_rows:
        logger.error("Zero wallets reconstructed.")
        sys.exit(1)

    out = pd.concat(all_rows, ignore_index=True)
    # Audit summary BEFORE trimming (the audit row is on `today`, which gets
    # trimmed if --end < today).
    audits = out.dropna(subset=["audit_today_diff_pct"])
    if not audits.empty:
        median_abs = audits["audit_today_diff_pct"].abs().median()
        p90 = audits["audit_today_diff_pct"].abs().quantile(0.90)
        logger.info(f"Audit |diff| median={median_abs:.3f}%, p90={p90:.3f}% (computed pre-trim)")

    if output_trim:
        out = out[(out["date"] >= start.date()) & (out["date"] <= end_requested.date())]
        logger.info(f"Trimmed output to {start.date()} -> {end_requested.date()}: {len(out):,} rows")
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(out):,} rows to {out_path}")
    logger.info(f"Skip counts: {skipped}")


if __name__ == "__main__":
    main()
