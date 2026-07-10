#!/usr/bin/env python3
"""V13 Module 04 ranker v2 — clean implementation per Module 04 spec.

DO-NOT-RUN until journey orchestrator completes + smoke validation.

Spec: projects/quant/v13/modules/04-ranking-copy-simulator (v1.0)
Depends on:
- Module 01 v8 (equity series with audit_flx_contamination_risk flag)
- Module 02 (journey output — wallet_journeys_costed.parquet)
- Module 05 (slip tiers + execute_or_skip + IOC cap)
- Module 06 (cold-start PENDING_FLAT state machine)
- Module 07 CopyLedger (CANONICAL — imported)

Computes per-wallet `copy_score` via per-wallet copy simulation. Score formula:
    copy_score = shrunk_winsorized_mean(copy_pct_move_net) × capped_jpd × positivity_factor

Output:
    app/data/v13/copy_scores__fold-<F>.parquet
    keyed by (wallet, fold, K_target, poll_cadence_s, latency_s, anti_corr_threshold)
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

# Canonical Module 07 ledger
sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_portfolio_ledger import CopyLedger  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_ranker_v2] %(message)s")
logger = logging.getLogger("v13_ranker_v2")

# === Constants (Module 04 spec defaults; sweep-overridable via params) ===
FEE_TAKER_PER_LEG = 0.000432           # HL taker 4.5bps × 4% referral
MAX_SLIPPAGE_BPS = 15
STATE_CHANGE_QTYSTEP_MULT = 2
STARTING_PORTFOLIO_EQUITY = 1000.0
MIN_N_JOURNEYS = 20                    # Hard min per codex r10
SHRINKAGE_K_DEFAULT = 50               # EB fallback (Module 04 spec)
WINSORIZE_LO, WINSORIZE_HI = 0.05, 0.95
JPD_CAP = 5.0                          # journeys per active day cap (HFT exclusion)
POSITIVITY_FLOOR = 0.5                 # frac_pos threshold


@dataclass
class CopyParams:
    """Sweep dimensions per Module 09 grid."""
    K_target: int
    poll_cadence_s: int
    latency_s: int
    anti_corr_threshold: float
    gross_cap: float = 1.0


# === Ranking aggregation (pure functions; data-independent) ===

def shrunk_winsorized_mean(returns: np.ndarray, global_pool_median: float,
                            shrinkage_k: float = SHRINKAGE_K_DEFAULT) -> float:
    """Module 04 spec aggregation:
        winsorize at [5, 95]
        shrinkage_lambda = n / (n + k); shrunk = lambda × wins_mean + (1-lambda) × global_median
    """
    n = len(returns)
    if n == 0:
        return 0.0
    p_lo, p_hi = np.quantile(returns, [WINSORIZE_LO, WINSORIZE_HI])
    winsorized = np.clip(returns, p_lo, p_hi)
    wins_mean = float(np.mean(winsorized))
    lam = n / (n + shrinkage_k)
    return lam * wins_mean + (1 - lam) * global_pool_median


def capped_jpd(n_journeys: int, active_days: int) -> float:
    """journeys per active day, capped at JPD_CAP."""
    if active_days <= 0:
        return 0.0
    return min(n_journeys / active_days, JPD_CAP)


def positivity_factor(returns: np.ndarray) -> float:
    """max(0, frac_pos - 0.5) / 0.5  → in [0, 1]."""
    if len(returns) == 0:
        return 0.0
    frac_pos = float(np.mean(returns > 0))
    return max(0.0, (frac_pos - POSITIVITY_FLOOR) / (1 - POSITIVITY_FLOOR))


def compute_copy_score(returns: np.ndarray, active_days: int, global_pool_median: float,
                        shrinkage_k: float = SHRINKAGE_K_DEFAULT) -> tuple[float, str | None]:
    """Module 04 main composite. Returns (score, excluded_reason).

    excluded_reason:
        "n<20"           — too few valid journeys
        "non_positive"   — score computed but ≤ 0
        None             — passed
    """
    n = len(returns)
    if n < MIN_N_JOURNEYS:
        return 0.0, "n<20"
    sm = shrunk_winsorized_mean(returns, global_pool_median, shrinkage_k)
    jpd = capped_jpd(n, active_days)
    pf = positivity_factor(returns)
    score = sm * jpd * pf
    if score <= 0:
        return score, "non_positive"
    return score, None


# === Empirical Bayes shrinkage k fitting (Module 04 spec, EB section) ===

def fit_eb_shrinkage_k(per_wallet_returns: list[np.ndarray]) -> float:
    """k_hat ≈ mean(per-wallet variance) / (between-wallet variance × n_avg).
    Falls back to SHRINKAGE_K_DEFAULT if fit fails or pool too small.
    """
    if len(per_wallet_returns) < 10:
        return SHRINKAGE_K_DEFAULT
    valid = [r for r in per_wallet_returns if len(r) >= 5]
    if len(valid) < 10:
        return SHRINKAGE_K_DEFAULT
    per_wallet_var = np.array([float(np.var(r, ddof=1)) for r in valid])
    per_wallet_mean = np.array([float(np.mean(r)) for r in valid])
    n_avg = float(np.mean([len(r) for r in valid]))
    between_var = float(np.var(per_wallet_mean, ddof=1))
    if between_var <= 0:
        return SHRINKAGE_K_DEFAULT
    mean_within = float(np.mean(per_wallet_var))
    k_hat = mean_within / (between_var * n_avg)
    return float(np.clip(k_hat, 10.0, 500.0))


# === Per-wallet copy simulator — REAL IMPLEMENTATION via Module 08 portfolio simulator ===
# Wraps single-wallet pool to Module 08 sim. Source intraday equity derived from journey
# entry/exit boundaries (v0 approximation; v0.1 will use Module 01 intraday equity delta).

from v13_portfolio_simulator import SimParams, run_portfolio_simulator
from v13_execution_realism import CoinInfo, classify_coin_tier


def _build_source_state_fn(journeys_for_wallet: pd.DataFrame, source_equity_approx: float):
    """Reconstruct source's per-coin state at any poll_ts from journey boundaries.
    For each journey J on coin C: J is OPEN at ts iff entry_ts <= ts <= exit_ts.

    v0 approximation: signed_notional = max_position_notional_usd × sign(side).
    source_equity is constant approximation per fold (uses provided source_equity_approx).
    """
    # Pre-index journeys by coin for fast lookup
    by_coin: dict[str, list] = {}
    for _, j in journeys_for_wallet.iterrows():
        by_coin.setdefault(j["coin"], []).append({
            "entry_ts": int(j["entry_ts"]),
            "exit_ts": int(j["exit_ts"]),
            "side": j["side"],
            "max_notional": float(j["max_position_notional_usd"]),
        })

    def source_state_at_poll_fn(wallet, ts_ms):
        state = {}
        for coin, journeys in by_coin.items():
            for j in journeys:
                if j["entry_ts"] <= ts_ms <= j["exit_ts"]:
                    sign = 1 if j["side"] == "long" else -1
                    state[coin] = {
                        "size": sign * j["max_notional"] / 1.0,  # size in USD-proxy units
                        "signed_notional": sign * j["max_notional"],
                    }
                    break
        return state

    return source_state_at_poll_fn


def simulate_wallet_copy(
    wallet: str,
    journeys_for_wallet: pd.DataFrame,
    candle_close_fn,
    hourly_funding_rate_fn,
    coin_volume_lookup: dict,                  # coin → 30d median 1m volume USD
    params: CopyParams,
    source_equity_approx: float = 100_000.0,   # v0 approximation; v0.1 uses Module 01 intraday
    window_start_ms: int = None,
    window_end_ms: int = None,
) -> dict:
    """Compute copy_score for a single wallet via Module 08 portfolio sim (pool of 1).

    Returns:
        {
          wallet, n_copy_journeys, returns (np.array), total_fees_usd,
          total_funding_usd, total_slip_attribution_usd, excluded_reason,
          legs, daily_returns_pct
        }
    """
    if len(journeys_for_wallet) == 0:
        return {"wallet": wallet, "n_copy_journeys": 0, "returns": np.array([]),
                "total_fees_usd": 0.0, "total_funding_usd": 0.0,
                "total_slip_attribution_usd": 0.0, "excluded_reason": "no_journeys",
                "legs": [], "daily_returns_pct": pd.Series(dtype=float)}

    # Build coin_info_by_coin from journey-observed coins (tier from volume lookup)
    coins_traded = sorted(set(journeys_for_wallet["coin"].unique().tolist()))
    coin_info_by_coin = {}
    for c in coins_traded:
        vol = coin_volume_lookup.get(c, 0)
        tier = classify_coin_tier(c, vol)
        # Default qty_step/min_order_usd per HL conventions (v0)
        coin_info_by_coin[c] = CoinInfo(
            coin=c, tier=tier,
            tick_size=0.001 if "BTC" not in c else 0.5,
            qty_step=0.0001 if "BTC" in c else 0.01,
            min_order_usd=10.0,
        )

    # Window from journey range if not given
    if window_start_ms is None:
        window_start_ms = int(journeys_for_wallet["entry_ts"].min())
    if window_end_ms is None:
        window_end_ms = int(journeys_for_wallet["exit_ts"].max())

    sim_params = SimParams(
        K_target=1,
        poll_interval_s=params.poll_cadence_s if hasattr(params, "poll_cadence_s") else 300,
        latency_s=params.latency_s if hasattr(params, "latency_s") else 60,
        anti_corr_threshold=0.6,  # not used in single-wallet sim
        per_coin_cap=0.5,  # less restrictive for ranking sim
    )

    source_state_fn = _build_source_state_fn(journeys_for_wallet, source_equity_approx)

    sim_res = run_portfolio_simulator(
        selected_pool=[wallet],
        params=sim_params,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        source_state_at_poll_fn=source_state_fn,
        source_equity_at_fn=lambda w, t: source_equity_approx,
        coin_info_by_coin=coin_info_by_coin,
        candle_close_at_fn=candle_close_fn,
        hourly_funding_rate_fn=hourly_funding_rate_fn,
    )

    # Aggregate per-journey returns: net_pnl_usd / max_notional_held per journey window
    returns_list = []
    for _, j in journeys_for_wallet.iterrows():
        legs_in_journey = [
            l for l in sim_res.legs
            if l.coin == j["coin"] and j["entry_ts"] <= l.exec_ts <= j["exit_ts"]
        ]
        if not legs_in_journey:
            continue
        max_notional = max(abs(l.qty * l.executable_px) for l in legs_in_journey)
        if max_notional <= 0:
            continue
        # net_pnl approximation: sum of cashflows + ending position MTM - fees
        cashflow = sum(l.cashflow_usd for l in legs_in_journey)
        fees = sum(l.fee_usd for l in legs_in_journey)
        # Final position residue at journey exit
        final_qty = sum(l.side * l.qty for l in legs_in_journey)
        exit_mark = candle_close_fn(j["coin"], int(j["exit_ts"]))
        if exit_mark is None or exit_mark <= 0:
            exit_mark = legs_in_journey[-1].executable_px
        residual_mtm = final_qty * exit_mark
        net_pnl = cashflow + residual_mtm - fees
        returns_list.append(net_pnl / max_notional)

    returns = np.array(returns_list) if returns_list else np.array([])
    return {
        "wallet": wallet,
        "n_copy_journeys": len(returns),
        "returns": returns,
        "total_fees_usd": sim_res.summary.get("fee_drag", 0.0),
        "total_funding_usd": 0.0,  # captured implicitly in cashflow via ledger funding
        "total_slip_attribution_usd": sim_res.summary.get("slip_drag", 0.0),
        "excluded_reason": None if len(returns) >= MIN_N_JOURNEYS else f"n<{MIN_N_JOURNEYS}",
        "legs": sim_res.legs,
        "daily_returns_pct": sim_res.daily_returns,
    }


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--journeys", required=True, help="wallet_journeys_costed.parquet")
    ap.add_argument("--equity-series", required=True, help="equity_series_v8.parquet")
    ap.add_argument("--eligible-pool", required=True, help="STAGE 3 eligible wallet list (txt)")
    ap.add_argument("--output", required=True, help="copy_scores__fold-<F>.parquet")
    ap.add_argument("--fold", type=int, required=True)
    ap.add_argument("--K-target", type=int, default=25)
    ap.add_argument("--poll-cadence-s", type=int, default=300)
    ap.add_argument("--latency-s", type=int, default=60)
    ap.add_argument("--anti-corr-threshold", type=float, default=0.6)
    args = ap.parse_args(argv)

    logger.info("v13_copy_ranker_v2 — NOT YET RUNNABLE")
    logger.info("Blocking dependencies:")
    logger.info("  - Module 01 intraday equity delta (v0.1 work)")
    logger.info("  - Module 05 slip_tiers calibration + execute_or_skip")
    logger.info("  - Module 06 cold-start state persistence")
    logger.info("Pure aggregation helpers (shrunk_winsorized_mean, capped_jpd, positivity_factor,")
    logger.info("compute_copy_score, fit_eb_shrinkage_k) are implemented and unit-testable.")
    sys.exit(2)


if __name__ == "__main__":
    main(sys.argv[1:])
