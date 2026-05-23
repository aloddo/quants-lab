#!/usr/bin/env python3
"""V13 Script 2/5 (v2): Journey tracing with carry-in seed + REVERSE PnL fix.

Per projects/quant/v13 Section 5.2 + remediation plan v2.

Per (wallet, coin), fills are walked in time order while maintaining the
running net position. Each fill is classified ENTRY / ADDON / TRIM / EXIT /
REVERSE. A journey is a contiguous sequence of fills bracketed by ENTRY (or
REVERSE_OPEN) and EXIT (or REVERSE_CLOSE).

Fixes from v1:

1. REVERSE PnL split (codex r1 #6): the `closedPnl` on a reverse fill
   belongs ENTIRELY to the closing leg. The new opposite-side journey
   starts at zero realized PnL.

2. Carry-in seed (codex r1 #7): instead of assuming position=0 at
   window_start, we bulk-load prior fills (shared helper from
   v13_equity_reconstruct) and walk back to last-flat. (wallet, coin)
   pairs whose carry-in cannot be proven are marked PRE_WINDOW_INCOMPLETE
   and their journeys are excluded from downstream metrics.

3. Deterministic same-timestamp ordering (codex r1 #8): sort by
   (wallet, coin, time, hash, side, signed_size, price).

4. Peak-date equity (codex r1 #9 P2): max_position_pct_equity uses
   equity at the peak DATE (when max_notional was set), not at entry.

5. Schema validation (codex r3 gotcha #8): strict required-column check,
   side enum hard-fail.

Inputs:
    --start YYYY-MM-DD          Earliest fill date in window
    --end YYYY-MM-DD            Latest fill date in window
    --wallets <path>            Optional newline-separated wallet filter
    --equity-series <path>      wallet_equity_series.parquet from script 1 (REQUIRED for peak-pct)
    --walkback-days N           Carry-in walkback (default 90)
    --output <path>             app/data/v13/wallet_journeys.parquet

Outputs (parquet, one row per closed journey):
    wallet, coin, journey_id, side,
    entry_ts, exit_ts, duration_hours,
    n_entry_fills, n_addon_fills, n_trim_fills, n_exit_fills, n_reverse_fills,
    n_fills_total,
    max_position_notional_usd, max_position_pct_equity, peak_ts,
    avg_seconds_between_addons, avg_seconds_between_trims,
    realized_pnl_usd, pnl_bps_of_max,
    journey_class,
    carry_in_status                                  -- per (wallet,coin): ok / incomplete
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Reuse equity_reconstruct's shared helpers.
import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_equity_reconstruct import (    # noqa: E402
    EPS,
    REQUIRED_FILL_COLUMNS,
    validate_and_normalize_fills,
    load_fills_for_dates,
    load_prior_fills_for_wallets,
    find_carry_in_state_from_prior,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_journey] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_journeys.parquet"

SCALP_THRESHOLD_S = 30 * 60
SWING_THRESHOLD_S = 24 * 60 * 60


# ---------------------------------------------------------------------------
# Loader for equity series
# ---------------------------------------------------------------------------

def load_equity_series(path: Path | None) -> dict:
    """Returns {(wallet_lower, date): equity_usd}."""
    if path is None or not path.exists():
        return {}
    eq = pd.read_parquet(path)
    if "wallet" not in eq.columns or "date" not in eq.columns or "equity_usd" not in eq.columns:
        raise ValueError(f"equity series at {path} missing required columns")
    eq["wallet"] = eq["wallet"].str.lower()
    eq["date"] = pd.to_datetime(eq["date"]).dt.date
    return {(r.wallet, r.date): float(r.equity_usd) for r in eq.itertuples(index=False)}


# ---------------------------------------------------------------------------
# Journey classifier
# ---------------------------------------------------------------------------

def _classify_duration(duration_s: float, n_addon: int, n_trim: int) -> str:
    if duration_s < SCALP_THRESHOLD_S:
        return "scalp"
    if duration_s < SWING_THRESHOLD_S:
        return "swing"
    if n_trim == 0 and n_addon == 0:
        return "fast-flip"
    if n_trim == 0:
        return "accumulation"
    if n_addon == 0:
        return "scale-out"
    return "position"


# ---------------------------------------------------------------------------
# Per-pair journey tracer
# ---------------------------------------------------------------------------

def trace_journeys_for_pair(
    wallet: str,
    coin: str,
    fills: pd.DataFrame,                          # pre-sorted, single (wallet, coin), in-window only
    equity_lookup: dict,
    carry_in_position: float,
    carry_in_cost_basis: float,
    carry_in_status: str,
    window_start_date,
    window_end_date,
) -> list[dict]:
    """Walk fills + emit journey records.

    The carry-in seed determines the starting position. If carry_in_status is
    "incomplete", journeys touching the carry-in leg are flagged and any
    journey that closes within the window is emitted with the incomplete flag
    so downstream metrics can exclude them.
    """
    journeys: list[dict] = []
    journey_id = 0

    # Initial state seeded from carry-in.
    position = carry_in_position
    cost_basis = carry_in_cost_basis    # used for cost-basis-aware realized PnL (we still
                                        # also accept HL's closedPnl as the ground truth where present)

    # Journey-open state (may start non-empty if carry-in seed is non-zero).
    if abs(position) > EPS:
        journey_id += 1
        open_ts = int(datetime(window_start_date.year, window_start_date.month, window_start_date.day, tzinfo=timezone.utc).timestamp() * 1000)
        open_side = 1 if position > 0 else -1
        max_notional = abs(position) * cost_basis if cost_basis > 0 else 0.0
        peak_ts = open_ts
        n_entry = 0           # the carry-in opening fills happened pre-window; we don't count them
        n_carry_in = 1
        n_addon = 0
        n_trim = 0
        n_exit = 0
        n_reverse = 0
        realized_pnl = 0.0
        addon_times: list[int] = []
        trim_times: list[int] = []
        # carry-in incompleteness propagates to all carry-in-touching journeys.
        carry_taint = (carry_in_status == "incomplete")
    else:
        open_ts = None
        open_side = None
        n_entry = n_addon = n_trim = n_exit = n_reverse = n_carry_in = 0
        max_notional = 0.0
        peak_ts = None
        realized_pnl = 0.0
        addon_times = []
        trim_times = []
        carry_taint = False

    def _finalize_journey(close_ts: int) -> dict | None:
        if open_ts is None or open_side is None:
            return None
        duration_s = max(0, (close_ts - open_ts) / 1000)
        ad_gaps = np.diff(addon_times) / 1000 if len(addon_times) >= 2 else np.array([])
        tr_gaps = np.diff(trim_times) / 1000 if len(trim_times) >= 2 else np.array([])
        max_notional_v = max(max_notional, EPS)
        pnl_bps = (realized_pnl / max_notional_v) * 10000

        # max_position_pct_equity uses equity on the peak DATE (when
        # max_notional was attained), not the entry date.
        peak_date = datetime.fromtimestamp((peak_ts or open_ts) / 1000, tz=timezone.utc).date()
        eq = equity_lookup.get((wallet, peak_date))
        max_pct = (max_notional_v / eq) if (eq and eq > 0) else None

        return {
            "wallet": wallet,
            "coin": coin,
            "journey_id": journey_id,
            "side": "long" if open_side > 0 else "short",
            "entry_ts": open_ts,
            "exit_ts": close_ts,
            "peak_ts": peak_ts,
            "duration_hours": duration_s / 3600.0,
            "n_entry_fills": n_entry,
            "n_carry_in_seeds": n_carry_in,
            "n_addon_fills": n_addon,
            "n_trim_fills": n_trim,
            "n_exit_fills": n_exit,
            "n_reverse_fills": n_reverse,
            "n_fills_total": n_entry + n_carry_in + n_addon + n_trim + n_exit + n_reverse,
            "max_position_notional_usd": max_notional_v,
            "max_position_pct_equity": max_pct,
            "avg_seconds_between_addons": float(ad_gaps.mean()) if ad_gaps.size else None,
            "avg_seconds_between_trims": float(tr_gaps.mean()) if tr_gaps.size else None,
            "realized_pnl_usd": realized_pnl,
            "pnl_bps_of_max": pnl_bps,
            "journey_class": _classify_duration(duration_s, n_addon, n_trim),
            "carry_in_status": "incomplete" if carry_taint else "ok",
        }

    # Use itertuples for speed (avoid iterrows boxing).
    has_closed_pnl_col = "closedPnl" in fills.columns
    for row in fills.itertuples(index=False):
        size = float(row.size)
        if size <= EPS:
            continue
        side = row.side
        signed = size if side == "B" else (-size if side == "A" else 0.0)
        if signed == 0.0:
            continue
        price = float(row.price) or 0.0
        ts = int(row.time)
        raw_pnl = getattr(row, "closedPnl", None) if has_closed_pnl_col else None
        # closedPnl fallback: cost-basis-derived realized for trim/exit/reverse.
        # Compute fallback BEFORE we apply the fill (we still know position+cost_basis here).
        if raw_pnl is None or (isinstance(raw_pnl, float) and (raw_pnl != raw_pnl)):  # NaN check
            # signed_size is opposite to position for trim/exit; size of the closed portion
            # in coin units depends on position vs new_pos.
            new_pos_preview = position + signed
            if (position > 0 and signed < 0) or (position < 0 and signed > 0):
                if abs(new_pos_preview) < EPS:
                    closed_qty = position                       # signed; full close
                elif (position > 0 and new_pos_preview > 0) or (position < 0 and new_pos_preview < 0):
                    closed_qty = -signed                        # trim partial
                else:
                    closed_qty = position                       # reverse: closes whole old leg
                closed_pnl_fallback = (price - cost_basis) * closed_qty
                closed_pnl = float(closed_pnl_fallback)
            else:
                closed_pnl = 0.0
        else:
            closed_pnl = float(raw_pnl)

        # PRE-fill notional capture (the peak of the journey can occur at any
        # fill point, INCLUDING the moment just before a trim/exit/reverse).
        pre_notional = abs(position) * price
        if abs(position) > EPS and pre_notional > max_notional:
            max_notional = pre_notional
            peak_ts = ts

        new_pos = position + signed
        notional_after = abs(new_pos) * price

        if abs(position) < EPS and abs(new_pos) > EPS:
            # ENTRY
            journey_id += 1
            open_ts = ts
            open_side = 1 if new_pos > 0 else -1
            n_entry = 1
            n_carry_in = 0
            n_addon = n_trim = n_exit = n_reverse = 0
            realized_pnl = 0.0
            max_notional = notional_after
            peak_ts = ts
            addon_times = []
            trim_times = []
            cost_basis = price
            carry_taint = False
        elif (position > 0 and signed > 0) or (position < 0 and signed < 0):
            # ADDON: same direction, position grows.
            n_addon += 1
            cost_basis = (cost_basis * abs(position) + price * abs(signed)) / abs(new_pos)
            if notional_after > max_notional:
                max_notional = notional_after
                peak_ts = ts
            addon_times.append(ts)
        elif (position > 0 and abs(new_pos) < EPS) or (position < 0 and abs(new_pos) < EPS):
            # EXACT EXIT
            n_exit += 1
            realized_pnl += closed_pnl
            j = _finalize_journey(ts)
            if j is not None:
                journeys.append(j)
            open_ts = None
            open_side = None
            cost_basis = 0.0
        elif (position > 0 and new_pos > 0 and signed < 0) or (position < 0 and new_pos < 0 and signed > 0):
            # TRIM
            n_trim += 1
            realized_pnl += closed_pnl
            trim_times.append(ts)
            # cost basis unchanged for the remaining portion
        elif (position > 0 and new_pos < 0) or (position < 0 and new_pos > 0):
            # REVERSE
            n_reverse += 1
            realized_pnl += closed_pnl
            j = _finalize_journey(ts)
            if j is not None:
                journeys.append(j)
            journey_id += 1
            open_ts = ts
            open_side = 1 if new_pos > 0 else -1
            n_entry = 0
            n_carry_in = 0
            n_addon = 0
            n_trim = 0
            n_exit = 0
            n_reverse = 1
            realized_pnl = 0.0
            max_notional = abs(new_pos) * price
            peak_ts = ts
            cost_basis = price
            addon_times = []
            trim_times = []
            carry_taint = False

        position = new_pos

    # End of fills. If we end with an open position the journey is incomplete
    # in the future; do not emit it for v1 (matches spec: only closed-in-window
    # journeys count).

    return journeys


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--wallets", help="Optional newline wallet filter")
    ap.add_argument("--equity-series", required=True, help="wallet_equity_series.parquet")
    ap.add_argument("--walkback-days", type=int, default=90)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    wallets_filter = None
    if args.wallets:
        with open(args.wallets) as f:
            wallets_filter = {w.strip().lower() for w in f if w.strip()}

    logger.info(f"Loading fills {start.date()} to {end.date()}")
    fills = load_fills_for_dates(start, end, wallets_filter)
    if fills.empty:
        logger.error("No in-window fills loaded.")
        return
    fills = validate_and_normalize_fills(fills)
    logger.info(f"Loaded + validated {len(fills):,} in-window fills")

    # Universe of wallets to process.
    if wallets_filter is not None:
        wallets = sorted(wallets_filter)
    else:
        wallets = sorted(fills["wallet"].unique().tolist())

    logger.info(f"Bulk-loading prior fills for carry-in walkback ({args.walkback_days} days)...")
    prior_fills = load_prior_fills_for_wallets(set(wallets), FILLS_DIR, start, max_walkback_days=args.walkback_days)
    logger.info(f"Bulk-loaded {len(prior_fills):,} prior fills")

    equity_lookup = load_equity_series(Path(args.equity_series))
    logger.info(f"Loaded {len(equity_lookup):,} equity-series rows")

    # Sort once with deterministic tie-breakers.
    sort_keys = ["wallet", "coin", "time", "side", "price", "size"]
    if "hash" in fills.columns:
        sort_keys = ["wallet", "coin", "time", "hash", "side", "price", "size"]
    fills = fills.sort_values(sort_keys, kind="stable").reset_index(drop=True)

    # Pre-group prior fills by wallet for O(1) lookup per pair.
    prior_by_wallet: dict = {}
    if not prior_fills.empty:
        for w, grp in prior_fills.groupby("wallet", sort=False):
            prior_by_wallet[w] = grp

    all_journeys: list[dict] = []
    pair_groups = fills.groupby(["wallet", "coin"], sort=False)
    n_pairs = pair_groups.ngroups
    logger.info(f"Tracing journeys across {n_pairs:,} (wallet,coin) pairs...")

    processed = 0
    carry_in_incomplete_count = 0
    incomplete_journeys_excluded = 0
    for (wallet, coin), grp in pair_groups:
        prior_for_wallet = prior_by_wallet.get(wallet, pd.DataFrame())
        pos_in, cb_in, ci_status = find_carry_in_state_from_prior(wallet, coin, prior_for_wallet)
        if ci_status == "incomplete":
            carry_in_incomplete_count += 1
        try:
            js = trace_journeys_for_pair(
                wallet, coin, grp, equity_lookup,
                pos_in, cb_in, ci_status,
                start.date(), end.date(),
            )
        except Exception as e:
            logger.exception(f"trace failed for {wallet[:8]} / {coin}: {e}")
            js = []
        # Per remediation plan + R3 gotcha #3: incomplete carry-in journeys
        # are EXCLUDED from the output (not merely flagged). Downstream
        # consumers should never see them.
        if ci_status == "incomplete":
            # Drop journeys whose carry_in_status is incomplete.
            kept = [j for j in js if j.get("carry_in_status") != "incomplete"]
            incomplete_journeys_excluded += (len(js) - len(kept))
            js = kept
        all_journeys.extend(js)
        processed += 1
        if processed % 5000 == 0:
            logger.info(f"  {processed:,}/{n_pairs:,} pairs, {len(all_journeys):,} journeys, carry_incomplete_pairs={carry_in_incomplete_count}, excluded_journeys={incomplete_journeys_excluded}")

    if not all_journeys:
        logger.error("Zero journeys extracted.")
        return

    out = pd.DataFrame(all_journeys)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(out):,} journeys to {out_path}")

    # Summary.
    logger.info("Journey class distribution:")
    for cls, n in out["journey_class"].value_counts().items():
        pct = 100 * n / len(out)
        logger.info(f"  {cls:>15}: {n:>8,} ({pct:5.1f}%)")
    logger.info(f"Total wallets: {out['wallet'].nunique():,}")
    logger.info(f"Incomplete-carry-in journeys excluded from output: {incomplete_journeys_excluded:,}")
    logger.info(f"Median pnl_bps_of_max: {out['pnl_bps_of_max'].median():.1f} bps")
    logger.info(f"Win rate (pnl > 0): {100 * (out['realized_pnl_usd'] > 0).mean():.1f}%")


if __name__ == "__main__":
    main()
