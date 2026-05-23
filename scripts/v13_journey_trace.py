#!/usr/bin/env python3
"""V13 Script 2/5: Journey tracing.

Per projects/quant/v13 Section 5.2.

Per (wallet, coin), fills are walked in time order while maintaining the
running net position. Each fill is classified:

    ENTRY   net was zero, becomes non-zero
    ADDON   same-direction fill, position grows
    TRIM    opposite-direction fill, position shrinks but does not flip
    EXIT    opposite-direction fill, position returns to zero
    REVERSE opposite-direction fill large enough to flip sign of position
            (splits into close + open of opposite-side journey)

A journey is a contiguous sequence of fills bracketed by an ENTRY (or a
REVERSE_OPEN) and an EXIT (or a REVERSE_CLOSE). For each journey we record
entry/exit times, duration, max notional reached, realized PnL, fill counts
per class, journey class label (fast-flip / accumulation / scale-out /
scalp / swing / position), and the wallet's equity at the journey peak (if
the equity series is provided) for the equity-relative metrics.

Inputs:
    --start YYYY-MM-DD      Earliest fill date (default: earliest on disk)
    --end YYYY-MM-DD        Latest fill date (default: today)
    --wallets <path>        Optional newline-separated wallet filter
    --equity-series <path>  wallet_equity_series.parquet from script 1
                            (optional but needed for max_position_pct_equity)
    --output <path>         Default: app/data/v13/wallet_journeys.parquet

Outputs:
    wallet_journeys.parquet keyed by (wallet, coin, journey_id), columns:
        wallet, coin, journey_id, side,
        entry_ts, exit_ts, duration_hours,
        n_entry_fills, n_addon_fills, n_trim_fills, n_exit_fills, n_reverse_fills,
        n_fills_total,
        max_position_notional_usd, max_position_pct_equity,
        avg_seconds_between_addons, avg_seconds_between_trims,
        realized_pnl_usd, pnl_bps_of_max,
        journey_class

Usage:
    python scripts/v13_journey_trace.py --start 2026-03-01 --end 2026-05-19 \\
        --equity-series app/data/v13/wallet_equity_series.parquet
"""
from __future__ import annotations

import argparse
import logging
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_journey] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_journeys.parquet"

# Journey class thresholds (durations in seconds).
SCALP_THRESHOLD_S = 30 * 60                # < 30 min  -> scalp
SWING_THRESHOLD_S = 24 * 60 * 60           # 30 min .. 24h -> swing; > 24h -> position


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------

def load_fills(start: datetime, end: datetime, wallets: set[str] | None) -> pd.DataFrame:
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


def load_equity_series(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    eq = pd.read_parquet(path)
    eq["date"] = pd.to_datetime(eq["date"]).dt.date
    return eq[["wallet", "date", "equity_usd"]]


# ---------------------------------------------------------------------------
# Journey extraction
# ---------------------------------------------------------------------------

def _classify_duration(duration_s: float, n_addon: int, n_trim: int) -> str:
    if duration_s < SCALP_THRESHOLD_S:
        return "scalp"
    if duration_s < SWING_THRESHOLD_S:
        return "swing"
    # position-class. Subclass by shape:
    if n_trim == 0 and n_addon == 0:
        return "fast-flip"   # entry + exit, no add/trim
    if n_trim == 0:
        return "accumulation"
    if n_addon == 0:
        return "scale-out"
    return "position"


def trace_journeys_for_pair(
    wallet: str, coin: str, fills: pd.DataFrame, equity_lookup: dict
) -> list[dict]:
    """fills must be pre-sorted by time ascending, for a single (wallet,coin)."""
    if fills.empty:
        return []

    journeys: list[dict] = []
    position = 0.0
    journey_id = 0

    # Open-journey state.
    open_ts = None
    open_side = None         # +1 long, -1 short
    n_entry = 0
    n_addon = 0
    n_trim = 0
    n_exit = 0
    n_reverse = 0
    realized_pnl = 0.0
    max_notional = 0.0
    addon_times: list[int] = []
    trim_times: list[int] = []

    def _finalize_journey(close_ts: int):
        """Emit a journey record using current open-journey state + the given close_ts."""
        if open_ts is None or open_side is None:
            return None
        duration_s = max(0, (close_ts - open_ts) / 1000)
        ad_gaps = np.diff(addon_times) / 1000 if len(addon_times) >= 2 else np.array([])
        tr_gaps = np.diff(trim_times) / 1000 if len(trim_times) >= 2 else np.array([])
        max_notional_v = max(max_notional, 1e-9)
        pnl_bps = (realized_pnl / max_notional_v) * 10000

        # max_position_pct_equity uses the wallet's equity on the date of the journey peak.
        peak_date = datetime.fromtimestamp(open_ts / 1000, tz=timezone.utc).date()
        eq = equity_lookup.get((wallet, peak_date))
        max_pct = (max_notional_v / eq) if (eq and eq > 0) else None

        return {
            "wallet": wallet,
            "coin": coin,
            "journey_id": journey_id,
            "side": "long" if open_side > 0 else "short",
            "entry_ts": open_ts,
            "exit_ts": close_ts,
            "duration_hours": duration_s / 3600.0,
            "n_entry_fills": n_entry,
            "n_addon_fills": n_addon,
            "n_trim_fills": n_trim,
            "n_exit_fills": n_exit,
            "n_reverse_fills": n_reverse,
            "n_fills_total": n_entry + n_addon + n_trim + n_exit + n_reverse,
            "max_position_notional_usd": max_notional_v,
            "max_position_pct_equity": max_pct,
            "avg_seconds_between_addons": float(ad_gaps.mean()) if ad_gaps.size else None,
            "avg_seconds_between_trims": float(tr_gaps.mean()) if tr_gaps.size else None,
            "realized_pnl_usd": realized_pnl,
            "pnl_bps_of_max": pnl_bps,
            "journey_class": _classify_duration(duration_s, n_addon, n_trim),
        }

    for _, row in fills.iterrows():
        size = float(row["size"])
        if size <= 0:
            continue
        side = row.get("side", "")
        signed = size if side == "B" else (-size if side == "A" else 0.0)
        if signed == 0.0:
            continue
        price = float(row.get("price", 0)) or 0.0
        ts = int(row.get("time", 0))
        closed_pnl = float(row.get("closedPnl", 0))

        new_pos = position + signed

        if position == 0 and new_pos != 0:
            # ENTRY
            journey_id += 1
            open_ts = ts
            open_side = 1 if new_pos > 0 else -1
            n_entry = 1
            n_addon = n_trim = n_exit = n_reverse = 0
            realized_pnl = 0.0
            max_notional = abs(new_pos) * price
            addon_times = []
            trim_times = []
        elif (position > 0 and signed > 0) or (position < 0 and signed < 0):
            # ADDON (same direction, position grows)
            n_addon += 1
            max_notional = max(max_notional, abs(new_pos) * price)
            addon_times.append(ts)
        elif (position > 0 and new_pos == 0) or (position < 0 and new_pos == 0):
            # EXACT EXIT
            n_exit += 1
            realized_pnl += closed_pnl
            j = _finalize_journey(ts)
            if j is not None:
                journeys.append(j)
            # reset state
            open_ts = None
            open_side = None
        elif (position > 0 and new_pos > 0 and signed < 0) or (position < 0 and new_pos < 0 and signed > 0):
            # TRIM (partial close, same side remaining)
            n_trim += 1
            realized_pnl += closed_pnl
            trim_times.append(ts)
        elif (position > 0 and new_pos < 0) or (position < 0 and new_pos > 0):
            # REVERSE -- finalize current journey at the close portion, open new on opposite side
            # Split closed_pnl: portion attributable to closing the existing leg.
            close_size = abs(position)                       # closes the full long/short side
            open_size = abs(new_pos)                          # opens the new side
            total_size = close_size + open_size
            if total_size > 0:
                close_pnl_attrib = closed_pnl * (close_size / total_size)
            else:
                close_pnl_attrib = closed_pnl
            n_reverse += 1
            realized_pnl += close_pnl_attrib

            j = _finalize_journey(ts)
            if j is not None:
                journeys.append(j)

            # start new journey on the opposite side
            journey_id += 1
            open_ts = ts
            open_side = 1 if new_pos > 0 else -1
            n_entry = 0
            n_addon = 0
            n_trim = 0
            n_exit = 0
            n_reverse = 1   # the reverse fill counts as the opening fill of the new journey
            realized_pnl = 0.0
            max_notional = abs(new_pos) * price
            addon_times = []
            trim_times = []

        position = new_pos

    # If we end the data window holding a position, the journey is still open.
    # We do not emit it; the wallet has unrealized exposure outside the analysis window.

    return journeys


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", help="YYYY-MM-DD")
    ap.add_argument("--end", help="YYYY-MM-DD")
    ap.add_argument("--wallets", help="Optional wallet filter")
    ap.add_argument("--equity-series", help="Path to wallet_equity_series.parquet")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    files = sorted(FILLS_DIR.glob("*.parquet"))
    if not files:
        logger.error("No S3 fills found.")
        return

    if args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start = datetime.strptime(files[0].stem, "%Y%m%d").replace(tzinfo=timezone.utc)
    if args.end:
        end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end = datetime.strptime(files[-1].stem, "%Y%m%d").replace(tzinfo=timezone.utc)

    wallets = None
    if args.wallets:
        with open(args.wallets) as f:
            wallets = {w.strip().lower() for w in f if w.strip()}

    logger.info(f"Loading fills {start.date()} to {end.date()}")
    fills = load_fills(start, end, wallets)
    if fills.empty:
        logger.error("No fills loaded.")
        return
    logger.info(f"Loaded {len(fills):,} fills")

    # Equity lookup (wallet,date) -> equity_usd.
    eq_path = Path(args.equity_series) if args.equity_series else None
    eq_df = load_equity_series(eq_path)
    if not eq_df.empty:
        equity_lookup = {(r.wallet.lower(), r.date): float(r.equity_usd) for r in eq_df.itertuples(index=False)}
        logger.info(f"Loaded {len(equity_lookup):,} equity-series rows")
    else:
        equity_lookup = {}
        logger.warning("No equity series provided; max_position_pct_equity will be None.")

    # Trace journeys per (wallet, coin).
    fills = fills.sort_values(["wallet", "coin", "time"]).reset_index(drop=True)

    all_journeys: list[dict] = []
    pair_groups = fills.groupby(["wallet", "coin"], sort=False)
    n_pairs = pair_groups.ngroups
    logger.info(f"Tracing journeys across {n_pairs:,} (wallet,coin) pairs...")

    processed = 0
    for (wallet, coin), grp in pair_groups:
        try:
            js = trace_journeys_for_pair(wallet, coin, grp, equity_lookup)
            all_journeys.extend(js)
        except Exception as e:
            logger.exception(f"trace failed for {wallet[:8]} / {coin}: {e}")
        processed += 1
        if processed % 5000 == 0:
            logger.info(f"  {processed:,}/{n_pairs:,} pairs processed, {len(all_journeys):,} journeys so far")

    if not all_journeys:
        logger.error("Zero journeys extracted.")
        return

    out = pd.DataFrame(all_journeys)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(out):,} journeys to {out_path}")

    # Summary stats.
    logger.info("Journey class distribution:")
    for cls, n in out["journey_class"].value_counts().items():
        pct = 100 * n / len(out)
        logger.info(f"  {cls:>15}: {n:>8,} ({pct:5.1f}%)")
    logger.info(f"Total wallets: {out['wallet'].nunique():,}")
    logger.info(f"Median pnl_bps_of_max: {out['pnl_bps_of_max'].median():.1f} bps")
    logger.info(f"Win rate (pnl > 0): {100 * (out['realized_pnl_usd'] > 0).mean():.1f}%")


if __name__ == "__main__":
    main()
