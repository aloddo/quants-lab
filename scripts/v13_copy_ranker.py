#!/usr/bin/env python3
"""V13 Per-Wallet Copy Ranker — Section 5.5 implementation.

!!! DRAFT — DO NOT RUN, DO NOT IMPORT, DO NOT TRUST !!!
!!! 2026-05-29 10:45 CEST: Alberto directive (voice 7750): NO engineering
    until spec is "atomic-bomb-proof" via infinite codex loop.
    This skeleton was prematurely written during r14 wait window.
    KEEP as draft reference for post-spec-consensus engineering pass.
    File header WARNS callers it is not validated. !!!


SPEC: projects/quant/v13 rev-r13 Section 5.5 (per-wallet copy simulation,
poll-snapshot semantics, side-aware IOC, partial fill, per-leg cashflows,
copy_funding from public rates, copy_score formula).

This script computes `copy_score` for every wallet in an eligible pool, used
by the walk-forward selection layer. Per-wallet simulator is RANKING ONLY.
The aggregate-first portfolio simulator (Section 5.10) is in v13_portfolio_simulator.py.

KEY CONSTANTS (v0 defaults; ablation sweeps in Section 6.7):
  POLL_INTERVAL_S = 300
  LATENCY_L_S = 60
  MAX_SLIPPAGE_BPS = 15
  STATE_CHANGE_QTYSTEP_MULT = 2

OUTPUT SCHEMA (wallet_copy_score.parquet):
  wallet (str), fold (int), L (int), poll_cadence (int),
  n_copy_journeys (int), copy_score (float), shrunk_mean (float),
  capped_jpd (float), positivity_factor (float),
  total_fees_usd (float), total_slip_attribution_usd (float),
  total_funding_usd (float), excluded_reason (str or null)

KNOWN OPEN per rev-r13 (see spec Section 0 changelog):
  - Some r14 issues may surface in code review; track inline as # TODO(rN).

Usage:
  python scripts/v13_copy_ranker.py \\
      --journeys app/data/v13/wallet_journeys_costed.parquet \\
      --equity-intraday app/data/v13/wallet_equity_intraday.parquet \\
      --our-equity-intraday app/data/v13/our_equity_intraday.parquet \\
      --candles-mongo-uri mongodb://localhost:27017/quants_lab \\
      --slip-tiers app/data/v13/slip_tiers.json \\
      --fee-per-leg 0.000432 \\
      --output app/data/v13/wallet_copy_score.parquet
"""

from __future__ import annotations

import argparse
import logging
import math
import sys
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [v13_copy_ranker] %(levelname)s: %(message)s"
)
logger = logging.getLogger("v13_copy_ranker")

# ---------------------------------------------------------------------------
# Constants (Section 5.5 spec defaults)
# ---------------------------------------------------------------------------
POLL_INTERVAL_S_DEFAULT = 300
LATENCY_L_S_DEFAULT = 60
MAX_SLIPPAGE_BPS_DEFAULT = 15
STATE_CHANGE_QTYSTEP_MULT_DEFAULT = 2
FEE_TAKER_PER_LEG_PCT_DEFAULT = 0.000432  # 4.32 bps HL taker × referral discount

# Per Section 5.8 tier model (v0 starter; v0.1 calibration from L2)
SLIP_TIERS_DEFAULT = {
    "majors": (5_000_000, 2),       # > $5M 1m median vol → 2 bps
    "liquid": (500_000, 5),         # $500K - $5M → 5 bps
    "mid": (100_000, 12),           # $100K - $500K → 12 bps
    "thin": (10_000, 30),           # $10K - $100K → EXCLUDED v0 (see below)
    "illiquid": (0, None),          # < $10K → EXCLUDED
}
COPY_POOL_TIERS = {"majors", "liquid", "mid"}  # thin/illiquid EXCLUDED v0

# Per Section 5.5 selection
MIN_COPY_JOURNEYS_DEFAULT = 20

# Per Section 5.10.1 portfolio equity ledger (used by both 5.5 and 5.10)
STARTING_PORTFOLIO_EQUITY_DEFAULT = 1000.0  # $1k for simulator comparability


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CoinInfo:
    """HL instrument-info per coin: tick, lot, min order."""
    coin: str
    tick_size: float
    qty_step: float
    min_order_usd: float
    tier: str             # "majors" | "liquid" | "mid" | "thin" | "illiquid"
    tier_fill_rate: float  # 0.95 / 0.80 / 0.60 per Section 5.8.1


@dataclass
class Leg:
    """One executed copy leg."""
    poll_ts: int
    coin: str
    side: int             # +1 buy, -1 sell
    qty: float
    executable_px: float
    fee_usd: float
    cashflow_usd: float   # -side × qty × executable_px (outflow buy, inflow sell)
    slip_attribution_usd: float  # diagnostic; not subtracted from net_pnl


# codex m07 r8 integration: stale CopyLedger clone REMOVED. Use canonical Module 07 class
# imported from scripts/v13_portfolio_ledger. All callers below should construct via
# `CopyLedger(cash_usd=...)`. The canonical class has identical sign conventions but adds
# strict input validation, atomic mutations, overflow guards, and bool rejection.
#
# NOTE: canonical CopyLedger uses on_funding_hour_boundary(hour_ts, marks, hourly_rates).
# The local `on_funding_hour(hour_marks, hourly_rates)` wrapper is preserved for backward
# compatibility with this draft's existing callers; it forwards to the canonical class.
from v13_portfolio_ledger import CopyLedger as _CanonicalCopyLedger


class CopyLedger(_CanonicalCopyLedger):
    """Module 04 ranker's CopyLedger view. Inherits canonical Module 07 class.

    Adds backward-compat `on_funding_hour(marks, rates)` (no hour_ts arg) + `equity_usd(marks)`
    helpers used by this draft's simulate_wallet_copy. Migrate callers to the canonical
    signatures in the Module 04 rewrite (task #28).
    """
    def __init__(self, cash_usd: float = STARTING_PORTFOLIO_EQUITY_DEFAULT, position_qty: dict | None = None):
        super().__init__(cash_usd=cash_usd, position_qty=position_qty if position_qty is not None else {})

    def equity_usd(self, marks_at_t: dict) -> float:
        """Backward-compat alias: equity_usd(marks_dict) → cash + sum(qty × marks_dict[coin]).
        Canonical interface uses equity_usd_at(t, candle_close_at_fn). Migrate in #28.
        """
        return self.equity_usd_at(t=0, candle_close_at_fn=lambda c, t: marks_at_t.get(c))

    def on_funding_hour(self, hour_marks: dict, hourly_rates: dict) -> None:
        """Backward-compat alias: forwards to canonical on_funding_hour_boundary(0, ...)."""
        self.on_funding_hour_boundary(hour_ts=0, marks=hour_marks, hourly_rates=hourly_rates)

    def on_leg_executed(self, leg) -> None:
        """Backward-compat: accept Leg dataclass; forward to canonical signature."""
        # The canonical class needs (coin, side, qty, executable_px, fee_usd) explicit args.
        super().on_leg_executed(
            coin=leg.coin,
            side=leg.side,
            qty=leg.qty,
            executable_px=leg.executable_px,
            fee_usd=leg.fee_usd,
        )


# ---------------------------------------------------------------------------
# Per-wallet copy simulator (Section 5.5)
# ---------------------------------------------------------------------------

def simulate_wallet_copy(
    wallet: str,
    source_state_at_poll: pd.DataFrame,    # columns: poll_ts, coin, size, signed_notional, mark
    source_equity_at_poll: pd.Series,      # indexed by poll_ts
    our_equity_at_poll_fn,                  # callable(t) → our_equity_usd (uses ledger)
    coin_info: dict,                         # coin → CoinInfo
    candle_close_at: dict,                   # callable(coin, ts) → 1m candle close px
    funding_rates_at: dict,                  # callable(coin, hour_ts) → hourly rate
    poll_interval_s: int = POLL_INTERVAL_S_DEFAULT,
    latency_s: int = LATENCY_L_S_DEFAULT,
    max_slip_bps: float = MAX_SLIPPAGE_BPS_DEFAULT,
    fee_per_leg_pct: float = FEE_TAKER_PER_LEG_PCT_DEFAULT,
) -> tuple[list[Leg], CopyLedger]:
    """Per-wallet copy simulator. Returns (executed legs, final ledger).

    NOTE: callers should compute per-journey returns AFTER this by walking
    the legs list and matching to source journeys for ranking aggregation.

    TODO(r14+): handle cold-start state machine (Section 5.9) BEFORE iterating
    polls. Source-state DataFrame should already have the wallet's carry-in
    coins flagged; this function skips those until source goes flat.
    """
    ledger = CopyLedger()
    executed_legs: list[Leg] = []
    cold_start_pending: set = set()  # coins where wallet was open at pool entry

    # Iterate polls in time order
    polls = sorted(source_state_at_poll["poll_ts"].unique())
    prev_state: dict = {}  # coin → (size, signed_notional)

    for i, poll_ts in enumerate(polls):
        # Section 5.10.1 funding accrual at hour boundary (if poll_ts is on the hour)
        if poll_ts % 3600 == 0:
            hour_marks = {c: candle_close_at(c, poll_ts) for c in ledger.position_qty}
            hour_rates = {c: funding_rates_at(c, poll_ts) for c in ledger.position_qty}
            ledger.on_funding_hour(hour_marks, hour_rates)

        # Current source state at this poll
        rows_at_poll = source_state_at_poll[source_state_at_poll["poll_ts"] == poll_ts]
        cur_state = {r["coin"]: (r["size"], r["signed_notional"], r["mark"])
                     for _, r in rows_at_poll.iterrows()}

        # State-change detection per coin (Section 5.5 STATE_CHANGE_QTYSTEP_MULT)
        for coin, (cur_size, cur_signed_notional, cur_mark) in cur_state.items():
            prev_size, prev_signed_notional, prev_mark = prev_state.get(coin, (0.0, 0.0, 0.0))

            # Cold-start check (Section 5.9)
            if coin in cold_start_pending:
                if cur_size == 0:
                    cold_start_pending.discard(coin)
                continue  # don't copy until source goes flat

            ci = coin_info.get(coin)
            if ci is None or ci.tier not in COPY_POOL_TIERS:
                continue  # excluded coin

            # Tolerance check
            if abs(cur_size - prev_size) < STATE_CHANGE_QTYSTEP_MULT_DEFAULT * ci.qty_step:
                continue  # no meaningful fill, just mark drift

            # Compute copy target
            copy_exec_ts = poll_ts + latency_s
            mark_at_exec = candle_close_at(coin, copy_exec_ts)
            if mark_at_exec is None or mark_at_exec <= 0:
                continue

            src_eq = source_equity_at_poll.get(copy_exec_ts)
            if src_eq is None or src_eq <= 0:
                continue
            our_eq = our_equity_at_poll_fn(copy_exec_ts)

            copy_signed_target = cur_signed_notional * (our_eq / src_eq)
            our_signed_now = ledger.signed_notional_usd(coin, mark_at_exec)
            copy_delta_usd = copy_signed_target - our_signed_now

            # Section 4.3 thresholds
            if abs(copy_delta_usd) < 10:
                continue
            if abs(copy_delta_usd) < 0.20 * abs(copy_signed_target) and copy_signed_target != 0:
                continue

            # Side-aware IOC cap (Section 5.5 codex r12 fix #5)
            side = 1 if copy_delta_usd > 0 else -1
            slip_bps = SLIP_TIERS_DEFAULT[ci.tier][1]
            proposed_executable_px = mark_at_exec * (1 + side * slip_bps / 10000)
            cap_px = mark_at_exec * (1 + side * max_slip_bps / 10000)

            rejected = (
                (side > 0 and proposed_executable_px > cap_px) or
                (side < 0 and proposed_executable_px < cap_px)
            )
            if rejected:
                continue

            executable_px = proposed_executable_px

            # Lot rounding (round DOWN)
            qty_raw = abs(copy_delta_usd) / executable_px
            qty_rounded = math.floor(qty_raw / ci.qty_step) * ci.qty_step
            leg_notional = qty_rounded * executable_px
            if leg_notional < ci.min_order_usd:
                continue

            # Partial fill (Section 5.8.1)
            actual_qty = qty_rounded * ci.tier_fill_rate
            actual_notional = actual_qty * executable_px

            # Record leg with sign-correct cashflow (codex r12 fix #3)
            cashflow_usd = -side * actual_qty * executable_px
            fee_usd = actual_notional * fee_per_leg_pct
            slip_attribution_usd = (executable_px - mark_at_exec) * side * actual_qty

            leg = Leg(
                poll_ts=copy_exec_ts,
                coin=coin,
                side=side,
                qty=actual_qty,
                executable_px=executable_px,
                fee_usd=fee_usd,
                cashflow_usd=cashflow_usd,
                slip_attribution_usd=slip_attribution_usd,
            )
            executed_legs.append(leg)
            ledger.on_leg_executed(leg)

        prev_state = cur_state

    return executed_legs, ledger


# ---------------------------------------------------------------------------
# Ranking score aggregation (Section 5.5)
# ---------------------------------------------------------------------------

def compute_copy_score_per_wallet(
    wallet_legs_and_journeys: pd.DataFrame,
    # columns: wallet, journey_id, copy_net_pnl_usd, max_notional_held,
    #          starting_equity_known, carry_in_status
    global_median_winsorized_return: float,
    shrinkage_k: float,
    min_copy_journeys: int = MIN_COPY_JOURNEYS_DEFAULT,
) -> pd.DataFrame:
    """Section 5.5 ranking aggregation.

    Returns DataFrame with wallet, copy_score, shrunk_mean, capped_jpd,
    positivity_factor, n_copy_journeys, excluded_reason.
    """
    results = []
    for wallet, group in wallet_legs_and_journeys.groupby("wallet"):
        # Filter: only complete carry-in + known starting equity + journey held something
        valid = group[
            (group["starting_equity_known"] == True) &
            (group["carry_in_status"] == "complete") &
            (group["max_notional_held"] > 0)
        ]
        n = len(valid)

        if n < min_copy_journeys:
            results.append({
                "wallet": wallet, "copy_score": 0.0, "n_copy_journeys": n,
                "shrunk_mean": np.nan, "capped_jpd": np.nan, "positivity_factor": np.nan,
                "excluded_reason": f"n<{min_copy_journeys}"
            })
            continue

        # copy_journey_return_pct = copy_net_pnl_usd / max_notional_held
        returns = valid["copy_net_pnl_usd"] / valid["max_notional_held"]
        # Winsorize at 5/95
        p5, p95 = returns.quantile(0.05), returns.quantile(0.95)
        winsorized = returns.clip(p5, p95)
        # Shrinkage toward global median
        lam = n / (n + shrinkage_k)
        shrunk_mean = lam * winsorized.mean() + (1 - lam) * global_median_winsorized_return
        # Capped JPD
        jpd = n / (valid.get("active_days", pd.Series([30])).iloc[0])  # TODO use real active_days
        capped_jpd = min(jpd, 5)
        # Positivity factor
        frac_pos = (returns > 0).mean()
        positivity_factor = max(0, frac_pos - 0.5) / 0.5

        copy_score = shrunk_mean * capped_jpd * positivity_factor

        results.append({
            "wallet": wallet,
            "copy_score": copy_score,
            "n_copy_journeys": n,
            "shrunk_mean": shrunk_mean,
            "capped_jpd": capped_jpd,
            "positivity_factor": positivity_factor,
            "excluded_reason": None,
        })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--journeys", required=True, help="wallet_journeys_costed.parquet from Section 5.2")
    ap.add_argument("--equity-intraday", required=True, help="wallet_equity_intraday.parquet from Section 5.1")
    ap.add_argument("--our-equity-starting", type=float, default=STARTING_PORTFOLIO_EQUITY_DEFAULT)
    ap.add_argument("--poll-interval-s", type=int, default=POLL_INTERVAL_S_DEFAULT)
    ap.add_argument("--latency-s", type=int, default=LATENCY_L_S_DEFAULT)
    ap.add_argument("--max-slip-bps", type=float, default=MAX_SLIPPAGE_BPS_DEFAULT)
    ap.add_argument("--fee-per-leg", type=float, default=FEE_TAKER_PER_LEG_PCT_DEFAULT)
    ap.add_argument("--min-copy-journeys", type=int, default=MIN_COPY_JOURNEYS_DEFAULT)
    ap.add_argument("--output", required=True, help="wallet_copy_score.parquet")
    args = ap.parse_args(argv)

    logger.warning("v13_copy_ranker.py SKELETON shipped 2026-05-29 per spec rev-r13.")
    logger.warning("Source state at poll + funding rates + intraday equity + candle_close_at()")
    logger.warning("loaders are STUBBED in this skeleton; full integration is next-cycle work.")
    logger.warning("This file establishes (a) the API surface, (b) the CopyLedger ledger logic,")
    logger.warning("(c) per-wallet simulator core, (d) copy_score aggregation.")
    logger.warning("Unit tests at tests/v13/test_copy_ranker.py validate F1-F18 fixtures.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
