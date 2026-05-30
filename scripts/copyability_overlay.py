#!/usr/bin/env python3
"""
copyability_overlay.py — empirical copyability measurement on V13 journeys.

ROUND 9 CONVERGED METHODOLOGY (codex+claude, no deferrals per Alberto voice 7196):

Per CLOSED journey produced by v13_journey_trace:
  source_pct_move = side * (exit_px - entry_px) / entry_px
  copy_entry_px_L = price at minute boundary >= entry_ts + L   (side-aware: see below)
  copy_exit_px_L  = price at minute boundary >= exit_ts  + L
  copy_pct_move_L = side * (copy_exit_px_L - copy_entry_px_L) / copy_entry_px_L
  funding_cost_bps = sum of funding payments over (entry_ts+L, exit_ts+L) for side
  slippage_bps = modeled slippage by coin liquidity + vol
  copy_net_bps_L = copy_pct_move_L*10000 - 8.64 - funding_cost_bps - slippage_bps

Per wallet, with EXECUTION REALISM AUDIT (codex r9 #7):
  Two execution proxies:
    A. NEUTRAL: 1m candle close at the target minute (optimistic baseline)
    B. CONSERVATIVE: side-aware adverse price (longs pay high, shorts pay low)
                     within the next 60s post-latency, using 1m candle high/low.
  Rank wallets independently under A and B. Only A-B-consistent wallets are
  reliable copyability candidates.

Per wallet ranking (codex r9 #2):
  copy_score = shrunk_winsorized_mean(copy_net_bps_L)
             * min(journeys_per_active_day, JPD_CAP)
             * max(0, fraction_positive - 0.5) / 0.5

Where:
  shrunk_winsorized_mean = winsor at p5/p95, mean, shrunk toward 0 by sqrt(n_journeys/(n_journeys+n_prior))
  JPD_CAP = 20  (prevents 1000-trade/day churners from dominating)
  reliability_penalty = 0 if fraction_positive < 0.5, scales to 1 at 1.0

Window splits (codex r9 #3, three required):
  S1: Train Feb-Mar -> Test Apr
  S2: Train Mar-Apr -> Test May
  S3: Train Feb-Apr -> Test May
  Wallet credible only if persists across >=2 splits OR has explicit regime label.

Carry-in handling (codex r9 #6):
  Report carry_in / carry_out counts per wallet. Wallets with >20% of position
  count or notional in carry-in are labeled INCOMPLETE_WINDOW (excluded from
  comparison vs fast wallets).

Inputs:
  --journeys      wallet_journeys.parquet  (from v13_journey_trace)
  --funding-mongo (uses hyperliquid_funding_rates collection)
  --candles-mongo (uses hyperliquid_candles_1m collection)
  --train-start, --train-end, --test-start, --test-end
  --latencies "60,300,900"  (default seconds)

Outputs:
  per_wallet_copyability_{split}.parquet
"""
from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [copyability] %(levelname)s: %(message)s")
log = logging.getLogger("copyability")

FEE_RT_BPS_DEFAULT = 8.64                # Alberto-locked 2026-05-26: keep 8.64 RT; graduate VIP later
JPD_CAP = 20.0
SHRINK_PRIOR_N = 30                      # journeys "prior" pulling toward 0 estimate
WINSOR_LO = 0.05
WINSOR_HI = 0.95
MIN_ORDER_NOTIONAL_DEFAULT = 10.0        # HL floor
MAX_COPY_LEVERAGE_DEFAULT = 10.0         # Alberto-locked 2026-05-26: HL perp cap, NOT 1.0x preconception
FALLBACK_COPY_FRACTION_DEFAULT = 0.10    # 10% of capital_scale when max_position_pct_equity is missing
DEFAULT_LATENCIES = "120,300"            # Alberto-locked 2026-05-26: 120s primary, 300s stress
DEFAULT_CAPITAL_SCALES = "500,1000,5000,10000,50000"  # Alberto-locked scaling roadmap
PRIMARY_LATENCY_DEFAULT = 120
PRIMARY_CAPITAL_SCALE_DEFAULT = 1000


# ----------------------------------------------------------------------------
# Reference price loaders (with side-aware conservative execution audit)
# ----------------------------------------------------------------------------

def load_candles_1m(coins: List[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Load 1m candles for selected coins. Returns df indexed (coin, ts_minute).
    Columns: open, high, low, close.
    """
    log.info("loading 1m candles for %d coins...", len(coins))
    client = MongoClient("mongodb://localhost:27017")
    # Correct collection name: hyperliquid_candles (47.9M docs, interval='1m').
    # hyperliquid_candles_1m exists but is empty.
    coll = client["quants_lab"]["hyperliquid_candles"]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    cur = coll.find(
        {
            "coin": {"$in": coins},
            "interval": "1m",
            "timestamp_utc": {"$gte": start_ms, "$lt": end_ms},
        },
        {"_id": 0, "coin": 1, "timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1},
    )
    rows = list(cur)
    client.close()
    if not rows:
        return pd.DataFrame(columns=["coin", "ts_minute", "open", "high", "low", "close"])
    df = pd.DataFrame(rows)
    df["ts_minute"] = (df["timestamp_utc"] // 60_000) * 60_000
    df = df.drop_duplicates(["coin", "ts_minute"]).sort_values(["coin", "ts_minute"])
    log.info("candles loaded: %d rows", len(df))
    return df


def build_coin_index(candles: pd.DataFrame) -> Dict[str, dict]:
    """Build per-coin numpy arrays for fast lookup at minute granularity."""
    out: Dict[str, dict] = {}
    for coin, sub in candles.groupby("coin", sort=False):
        sub = sub.sort_values("ts_minute").reset_index(drop=True)
        out[coin] = {
            "ts": sub["ts_minute"].to_numpy(dtype=np.int64),
            "open": sub["open"].to_numpy(dtype=np.float64),
            "high": sub["high"].to_numpy(dtype=np.float64),
            "low": sub["low"].to_numpy(dtype=np.float64),
            "close": sub["close"].to_numpy(dtype=np.float64),
        }
    return out


def load_funding(coins: List[str], start: datetime, end: datetime) -> pd.DataFrame:
    """Load funding rates. Schema {coin, timestamp_utc, rate_bps_per_8h or similar}.
    Returns df indexed (coin, ts_funding) with rate_8h column.
    """
    log.info("loading funding rates for %d coins...", len(coins))
    client = MongoClient("mongodb://localhost:27017")
    coll = client["quants_lab"]["hyperliquid_funding_rates"]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    cur = coll.find(
        {
            "coin": {"$in": coins},
            "timestamp_utc": {"$gte": start_ms, "$lt": end_ms},
        },
        {"_id": 0, "coin": 1, "timestamp_utc": 1, "funding_rate": 1, "premium": 1},
    )
    rows = list(cur)
    client.close()
    if not rows:
        log.warning("no funding rows loaded")
        return pd.DataFrame(columns=["coin", "timestamp_utc", "funding_rate"])
    df = pd.DataFrame(rows)
    df = df.sort_values(["coin", "timestamp_utc"])
    log.info("funding loaded: %d rows", len(df))
    return df


def build_funding_index(funding: pd.DataFrame) -> Dict[str, dict]:
    """Per-coin numpy arrays for funding lookup."""
    out: Dict[str, dict] = {}
    if funding.empty:
        return out
    for coin, sub in funding.groupby("coin", sort=False):
        sub = sub.sort_values("timestamp_utc").reset_index(drop=True)
        out[coin] = {
            "ts": sub["timestamp_utc"].to_numpy(dtype=np.int64),
            "rate": sub["funding_rate"].to_numpy(dtype=np.float64),
        }
    return out


# ----------------------------------------------------------------------------
# Copy execution price at latency L (neutral + conservative)
# ----------------------------------------------------------------------------

# Maximum forward-gap allowed when looking up a candle for a target minute.
# 60_000 ms means the picked candle must start within the same minute as the
# target, NOT some arbitrary minute later (codex r10 #2).
MAX_LOOKUP_GAP_MS = 120_000  # accept up to 2-minute forward gap (allows occasional missing candle)


def copy_price_at(
    coin_arr: dict | None,
    target_ms: int,
    side: int,
    proxy: str,
) -> float:
    """Return copy execution price at minute >= target_ms, within MAX_LOOKUP_GAP_MS.

    proxy == "neutral": 1m candle close at target minute.
    proxy == "conservative": side-aware adverse price (buy fills near minute HIGH,
            sell fills near minute LOW), using high/low of the minute containing
            target_ms (assumes our market order can be filled anywhere in that minute).

    Returns NaN if no candle exists within MAX_LOOKUP_GAP_MS of target_ms (codex r10 #2).
    """
    if coin_arr is None:
        return float("nan")
    ts_arr = coin_arr["ts"]
    idx = int(np.searchsorted(ts_arr, target_ms))
    if idx >= len(ts_arr):
        return float("nan")
    # Coverage check: don't fabricate a price from a future minute.
    gap_ms = int(ts_arr[idx]) - target_ms
    if gap_ms > MAX_LOOKUP_GAP_MS:
        return float("nan")
    if proxy == "neutral":
        return float(coin_arr["close"][idx])
    elif proxy == "conservative":
        # Buying (side=+1 for entry long, side=-1 for exit short) pays high.
        # Selling (side=-1 for entry short, side=+1 for exit long) gets low.
        # Caller passes +side for entry leg, -side for exit leg.
        return float(coin_arr["high"][idx] if side > 0 else coin_arr["low"][idx])
    else:
        raise ValueError(f"unknown proxy {proxy}")


def funding_cost_bps_over(
    coin_arr: dict | None,
    entry_ms: int,
    exit_ms: int,
    side: int,
) -> Tuple[float, float]:
    """Approximate funding cost (in bps of notional, signed for our side) over
    the holding period.

    HL funding is computed hourly. Long pays positive rate; short receives it
    (and vice versa). The series stores per-hour funding_rate.

    Returns (funding_bps, coverage_frac) where coverage_frac in [0,1] is the
    fraction of expected hourly funding rows that were present. Coverage < 0.9
    indicates suspicious gaps; caller should consider invalidating (codex r10 #7).

    A trade lasting < 1 hour incurs no funding (HL funding charged at hourly
    boundaries only).
    """
    if coin_arr is None:
        return (float("nan"), 0.0)
    ts_arr = coin_arr["ts"]
    rate_arr = coin_arr["rate"]
    lo = int(np.searchsorted(ts_arr, entry_ms))
    hi = int(np.searchsorted(ts_arr, exit_ms))
    expected_hours = max(0, (exit_ms - entry_ms) // 3_600_000)
    if expected_hours == 0:
        return (0.0, 1.0)  # short hold, no funding period crossed, full coverage
    if lo >= len(ts_arr) or hi <= lo:
        return (float("nan"), 0.0)
    actual_rows = hi - lo
    coverage = min(1.0, actual_rows / max(1, expected_hours))
    paid = rate_arr[lo:hi].sum()
    # Long pays POSITIVE rate (cost); short pays NEGATIVE rate (income).
    return (float(paid * side * 10000.0), float(coverage))


def parse_side(raw) -> int:
    """Strict side parser. Returns +1, -1, or 0 (invalid). (codex r10 #6)"""
    if raw is None:
        return 0
    s = str(raw).strip().lower()
    if s == "long":
        return 1
    if s == "short":
        return -1
    return 0


# ----------------------------------------------------------------------------
# Per-journey copy evaluation
# ----------------------------------------------------------------------------

# Simple slippage model (codex r10 #3, hedge-fund-grade v0):
# slippage_bps = base + k_vol * recent_minute_range_bps
# Where range = (high - low) / close at the entry minute.
SLIPPAGE_BASE_BPS = 1.0  # baseline microstructure impact
SLIPPAGE_K_VOL = 0.10    # 10% of the minute's range as impact
SLIPPAGE_CAP_BPS = 25.0  # cap to prevent extreme outliers dominating


def slippage_bps_estimate(coin_arr: dict | None, target_ms: int) -> float:
    """Estimate slippage in bps at execution time using the 1m candle's range.

    Range proxy: (high - low) / close at the minute the order lands.
    Slippage ≈ BASE + K * range_bps, capped at CAP. (codex r10 #3)
    """
    if coin_arr is None:
        return float("nan")
    ts_arr = coin_arr["ts"]
    idx = int(np.searchsorted(ts_arr, target_ms))
    if idx >= len(ts_arr) or int(ts_arr[idx]) - target_ms > MAX_LOOKUP_GAP_MS:
        return float("nan")
    o, h, l, c = (coin_arr[k][idx] for k in ("open", "high", "low", "close"))
    if not (c > 0):
        return float("nan")
    range_bps = (h - l) / c * 10000.0
    slip = SLIPPAGE_BASE_BPS + SLIPPAGE_K_VOL * range_bps
    return float(min(slip, SLIPPAGE_CAP_BPS))


def evaluate_journey(
    journey: pd.Series,
    candles_idx: Dict[str, dict],
    funding_idx: Dict[str, dict],
    latency_seconds: List[int],
    fee_rt_bps: float = FEE_RT_BPS_DEFAULT,
) -> dict:
    """Compute copy_net_bps_L for each L and proxy. Returns dict to merge into journey row."""
    coin = journey["coin"]
    side = parse_side(journey.get("side"))
    out = {"side_valid": side != 0}
    if side == 0:
        for L in latency_seconds:
            for proxy in ("neutral", "conservative"):
                out[f"copy_gross_bps_{L}s_{proxy}"] = float("nan")
                out[f"copy_net_bps_{L}s_{proxy}"] = float("nan")
            out[f"funding_bps_{L}s"] = float("nan")
            out[f"funding_coverage_{L}s"] = 0.0
            out[f"slippage_bps_{L}s"] = float("nan")
            out[f"coverage_ok_{L}s"] = False
        out["source_pnl_bps"] = float("nan")
        return out

    entry_ms = int(journey["entry_ts"])
    exit_ms = int(journey["exit_ts"])
    coin_arr = candles_idx.get(coin)
    fund_arr = funding_idx.get(coin)

    # Source % move expressed in bps of max_position_notional (V13 schema).
    pnl_bps = journey.get("pnl_bps_of_max")
    out["source_pnl_bps"] = float(pnl_bps) if pnl_bps is not None and not (isinstance(pnl_bps, float) and np.isnan(pnl_bps)) else float("nan")

    for L in latency_seconds:
        target_entry = entry_ms + L * 1000
        target_exit = exit_ms + L * 1000

        # Neutral proxy (close at minute boundary)
        cep_n = copy_price_at(coin_arr, target_entry, side, "neutral")
        cxp_n = copy_price_at(coin_arr, target_exit, side, "neutral")
        if cep_n > 0 and cxp_n > 0:
            gross_n_bps = side * (cxp_n - cep_n) / cep_n * 10000.0
        else:
            gross_n_bps = float("nan")

        # Conservative: entry leg adverse (+side), exit leg adverse (-side)
        cep_c = copy_price_at(coin_arr, target_entry, side, "conservative")
        cxp_c = copy_price_at(coin_arr, target_exit, -side, "conservative")
        if cep_c > 0 and cxp_c > 0:
            gross_c_bps = side * (cxp_c - cep_c) / cep_c * 10000.0
        else:
            gross_c_bps = float("nan")

        fund_bps, fund_cov = funding_cost_bps_over(fund_arr, target_entry, target_exit, side)
        slip_entry = slippage_bps_estimate(coin_arr, target_entry)
        slip_exit = slippage_bps_estimate(coin_arr, target_exit)
        slip_bps = (slip_entry + slip_exit) if (np.isfinite(slip_entry) and np.isfinite(slip_exit)) else float("nan")

        # Coverage check (codex r10 #5+#7): require candles AND funding coverage
        coverage_ok = (
            np.isfinite(cep_n) and np.isfinite(cxp_n) and
            np.isfinite(cep_c) and np.isfinite(cxp_c) and
            np.isfinite(fund_bps) and fund_cov >= 0.9 and
            np.isfinite(slip_bps)
        )

        out[f"copy_gross_bps_{L}s_neutral"] = gross_n_bps
        out[f"copy_gross_bps_{L}s_conservative"] = gross_c_bps
        out[f"funding_bps_{L}s"] = fund_bps
        out[f"funding_coverage_{L}s"] = fund_cov
        out[f"slippage_bps_{L}s"] = slip_bps
        out[f"coverage_ok_{L}s"] = bool(coverage_ok)
        if coverage_ok:
            out[f"copy_net_bps_{L}s_neutral"] = gross_n_bps - fee_rt_bps - fund_bps - slip_bps
            out[f"copy_net_bps_{L}s_conservative"] = gross_c_bps - fee_rt_bps - fund_bps - slip_bps
        else:
            out[f"copy_net_bps_{L}s_neutral"] = float("nan")
            out[f"copy_net_bps_{L}s_conservative"] = float("nan")

    return out


# ----------------------------------------------------------------------------
# Per-wallet aggregation + ranking
# ----------------------------------------------------------------------------

def compute_copy_notional(
    journey: pd.Series | dict,
    capital_scale: float,
    max_copy_leverage: float = MAX_COPY_LEVERAGE_DEFAULT,
    fallback_copy_fraction: float = FALLBACK_COPY_FRACTION_DEFAULT,
) -> Tuple[float, str]:
    """Compute OUR copy notional for a given source journey at capital_scale.

    Per codex r17 Q1 + Alberto-locked 2026-05-26 max_copy_leverage=10 (NOT 1.0):

    Primary rule: mirror source's percent-of-equity sizing.
        our_copy_notional = capital_scale * max_position_pct_equity
        clamp at capital_scale * max_copy_leverage (HL perp 10x cap default)

    Fallback (when max_position_pct_equity is missing or zero):
        our_copy_notional = min(capital_scale * fallback_copy_fraction,
                                source_max_notional,
                                capital_scale * max_copy_leverage)
        sizing_scope = "fallback_fixed_fraction_clamped_by_source_notional"

    Returns (our_copy_notional_usd, sizing_scope_label).
    """
    source_pct = journey.get("max_position_pct_equity") if hasattr(journey, "get") else None
    source_notional = journey.get("max_position_notional_usd") if hasattr(journey, "get") else None

    cap_notional = capital_scale * max_copy_leverage

    if source_pct is not None and np.isfinite(source_pct) and source_pct > 0:
        return min(capital_scale * source_pct, cap_notional), "source_pct_equity"

    if source_notional is not None and np.isfinite(source_notional) and source_notional > 0:
        notional = min(
            capital_scale * fallback_copy_fraction,
            source_notional,
            cap_notional,
        )
        return notional, "fallback_fixed_fraction_clamped_by_source_notional"

    return float("nan"), "missing_sizing_inputs"


def winsorized_mean(arr: np.ndarray, lo: float = WINSOR_LO, hi: float = WINSOR_HI) -> float:
    arr = arr[~np.isnan(arr)]
    if len(arr) < 2:
        return float("nan")
    p_lo, p_hi = np.percentile(arr, [lo * 100, hi * 100])
    clipped = np.clip(arr, p_lo, p_hi)
    return float(clipped.mean())


def shrunk_mean(arr: np.ndarray) -> float:
    """Winsorized mean shrunk toward 0 by empirical-Bayes weight n/(n+prior).

    Codex r10 #4: linear shrinkage (not sqrt) is the standard EB posterior
    weight assuming equal-variance prior centered at 0. With 30 prior journeys,
    a wallet with n=30 gets 50% weight, n=120 gets 80% weight.
    """
    w = winsorized_mean(arr)
    if np.isnan(w):
        return float("nan")
    n = int(np.sum(~np.isnan(arr)))
    shrink = n / (n + SHRINK_PRIOR_N)
    return w * shrink


def aggregate_wallet(
    journeys: pd.DataFrame,
    latency_seconds: List[int],
    capital_scales: List[float],
) -> pd.DataFrame:
    """Aggregate per-wallet stats per (capital_scale, latency, proxy). LONG FORMAT.

    Stage 1 step C (codex r17 + Alberto lock 2026-05-26):
    - Group by (wallet, capital_scale). One row per (wallet, capital_scale, latency, proxy).
    - Primary metric: copy_net_return_pct_equity (= copy_net_pnl_usd / capital_scale).
    - Score: shrunk_winsorized_mean * capped_jpd * max(0, frac_pos - 0.5) / 0.5
    - Unexecutable journeys (our_copy_notional < min_order) EXCLUDED from return metrics,
      counted in executable_n / unexecutable_n / executable_frac.
    - Audit columns: sizing_scope_dominant, complex_journey_frac, position_replay_mode.

    Required columns in `journeys`:
      - capital_scale (added in main expansion)
      - our_copy_notional_usd
      - executable (bool)
      - sizing_scope
      - copy_net_pnl_usd_{L}s_{proxy}, copy_net_return_pct_equity_{L}s_{proxy} for each L/proxy
      - coverage_ok_{L}s
      - source_pnl_bps
      - carry_in_status, max_position_pct_equity, n_addon_fills, n_trim_fills
    """
    output_rows = []
    if journeys.empty:
        return pd.DataFrame()

    for (wallet, capital_scale), sub in journeys.groupby(["wallet", "capital_scale"], sort=False):
        n_raw = len(sub)
        if n_raw < 5:
            continue

        n_carry_incomplete = int((sub.get("carry_in_status", "ok") != "ok").sum()) if "carry_in_status" in sub.columns else 0
        n_complete = sub[sub.get("carry_in_status", "ok") == "ok"] if "carry_in_status" in sub.columns else sub

        # Source stats (over complete journeys)
        source_arr = n_complete["source_pnl_bps"].to_numpy(dtype=np.float64) if "source_pnl_bps" in n_complete.columns else np.array([])
        win_rate_source = float((source_arr > 0).mean()) if len(source_arr) else float("nan")

        # Executable counts
        if "executable" in sub.columns:
            executable_journeys = sub[sub["executable"] == True]
            executable_n = len(executable_journeys)
            unexecutable_n = n_raw - executable_n
            executable_frac = executable_n / n_raw if n_raw > 0 else 0.0
        else:
            executable_journeys = sub
            executable_n = n_raw
            unexecutable_n = 0
            executable_frac = 1.0

        # Sizing scope dominant (mode)
        sizing_scope_dominant = (
            sub["sizing_scope"].mode().iloc[0]
            if "sizing_scope" in sub.columns and not sub["sizing_scope"].mode().empty
            else "unknown"
        )

        # Complex-journey fraction: journeys with addons or trims (codex r17 Q7 audit)
        if "n_addon_fills" in sub.columns and "n_trim_fills" in sub.columns:
            complex = (sub["n_addon_fills"].fillna(0) > 0) | (sub["n_trim_fills"].fillna(0) > 0)
            complex_journey_frac = float(complex.sum()) / n_raw if n_raw > 0 else 0.0
        else:
            complex_journey_frac = float("nan")

        # max_position_pct_equity null-rate (codex r17 missing items audit)
        if "max_position_pct_equity" in sub.columns:
            pct_eq_null_frac = float(sub["max_position_pct_equity"].isna().sum()) / n_raw if n_raw > 0 else 0.0
        else:
            pct_eq_null_frac = 1.0

        for L in latency_seconds:
            cov_col = f"coverage_ok_{L}s"
            cov_and_exec = sub.copy()
            if cov_col in cov_and_exec.columns:
                cov_and_exec = cov_and_exec[cov_and_exec[cov_col] == True]
            if "executable" in cov_and_exec.columns:
                cov_and_exec = cov_and_exec[cov_and_exec["executable"] == True]
            valid_n = len(cov_and_exec)
            coverage_frac = valid_n / n_raw if n_raw > 0 else 0.0

            if valid_n >= 2:
                ts_min = cov_and_exec["entry_ts"].min()
                ts_max = cov_and_exec["exit_ts"].max()
                span_days = max(1.0, (ts_max - ts_min) / 1000.0 / 86400.0)
                jpd_valid = valid_n / span_days
            else:
                jpd_valid = 0.0

            for proxy in ("neutral", "conservative"):
                pct_eq_col = f"copy_net_return_pct_equity_{L}s_{proxy}"
                pnl_usd_col = f"copy_net_pnl_usd_{L}s_{proxy}"
                bps_col = f"copy_net_bps_{L}s_{proxy}"

                row = {
                    "wallet": wallet,
                    "capital_scale": capital_scale,
                    "latency_seconds": L,
                    "proxy": proxy,
                    "n_journeys_raw": n_raw,
                    "n_coins": int(sub["coin"].nunique()) if "coin" in sub.columns else 0,
                    "n_carry_in_incomplete": n_carry_incomplete,
                    "executable_n": executable_n,
                    "unexecutable_n": unexecutable_n,
                    "executable_frac": executable_frac,
                    "valid_n": valid_n,
                    "coverage_frac": coverage_frac,
                    "jpd_valid": jpd_valid,
                    "win_rate_source": win_rate_source,
                    "source_pnl_bps_mean": float(np.nanmean(source_arr)) if len(source_arr) else float("nan"),
                    "source_pnl_bps_median": float(np.nanmedian(source_arr)) if len(source_arr) else float("nan"),
                    "sizing_scope_dominant": sizing_scope_dominant,
                    "complex_journey_frac": complex_journey_frac,
                    "pct_equity_null_frac": pct_eq_null_frac,
                    "position_replay_mode": "journey_level_v0",  # Stage 1; Stage 2 will swap to fill_level
                    # Metrics filled below
                    "copy_net_return_pct_equity_shrunk_mean": float("nan"),
                    "copy_net_return_pct_equity_median": float("nan"),
                    "copy_net_return_pct_equity_frac_pos": float("nan"),
                    "copy_net_pnl_usd_sum": float("nan"),
                    "copy_net_pnl_usd_mean": float("nan"),
                    "copy_net_bps_shrunk_mean": float("nan"),
                    "copy_score": float("nan"),
                }

                if valid_n >= 5 and pct_eq_col in cov_and_exec.columns:
                    arr_pct = cov_and_exec[pct_eq_col].to_numpy(dtype=np.float64)
                    arr_pct_clean = arr_pct[~np.isnan(arr_pct)]
                    if len(arr_pct_clean) >= 5:
                        mean_w_pct = shrunk_mean(arr_pct)
                        frac_pos = float((arr_pct_clean > 0).mean())
                        jpd_cap = min(jpd_valid, JPD_CAP)
                        reliability = max(0.0, frac_pos - 0.5) / 0.5
                        score = mean_w_pct * jpd_cap * reliability

                        row["copy_net_return_pct_equity_shrunk_mean"] = mean_w_pct
                        row["copy_net_return_pct_equity_median"] = float(np.nanmedian(arr_pct))
                        row["copy_net_return_pct_equity_frac_pos"] = frac_pos
                        row["copy_score"] = score

                        if pnl_usd_col in cov_and_exec.columns:
                            pnl_arr = cov_and_exec[pnl_usd_col].to_numpy(dtype=np.float64)
                            row["copy_net_pnl_usd_sum"] = float(np.nansum(pnl_arr))
                            row["copy_net_pnl_usd_mean"] = float(np.nanmean(pnl_arr))

                        if bps_col in cov_and_exec.columns:
                            bps_arr = cov_and_exec[bps_col].to_numpy(dtype=np.float64)
                            row["copy_net_bps_shrunk_mean"] = shrunk_mean(bps_arr)

                output_rows.append(row)

    if not output_rows:
        return pd.DataFrame()

    out = pd.DataFrame(output_rows)
    return out


# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--journeys", required=True, help="wallet_journeys.parquet from v13_journey_trace")
    p.add_argument("--train-start", required=True, help="YYYY-MM-DD")
    p.add_argument("--train-end", required=True, help="YYYY-MM-DD")
    p.add_argument("--test-start", required=True, help="YYYY-MM-DD")
    p.add_argument("--test-end", required=True, help="YYYY-MM-DD")
    p.add_argument("--latencies", default=DEFAULT_LATENCIES, help="comma-sep seconds")
    p.add_argument("--capital-scales", default=DEFAULT_CAPITAL_SCALES,
                   help="comma-sep USD capital scales for the sweep")
    p.add_argument("--out-prefix", required=True,
                   help="output prefix e.g. app/data/v13/copyability/cop_apr_train "
                        "(NOT /tmp/ — lost on reboot per 2026-05-29 OOM-2 lesson)")
    p.add_argument("--min-journeys", type=int, default=10)
    p.add_argument("--fee-rt-bps", type=float, default=FEE_RT_BPS_DEFAULT,
                   help="OUR copy fee per RT in bps (Alberto-locked 8.64; graduate later)")
    p.add_argument("--max-copy-leverage", type=float, default=MAX_COPY_LEVERAGE_DEFAULT,
                   help="HL perp leverage cap (default 10.0; Alberto-locked NOT 1.0x)")
    p.add_argument("--fallback-copy-fraction", type=float, default=FALLBACK_COPY_FRACTION_DEFAULT,
                   help="When max_position_pct_equity missing, deploy this fraction of capital")
    p.add_argument("--min-order-notional", type=float, default=MIN_ORDER_NOTIONAL_DEFAULT,
                   help="HL min order USD; unexecutable journeys excluded from return metrics")
    p.add_argument("--primary-latency", type=int, default=PRIMARY_LATENCY_DEFAULT,
                   help="Primary latency for ranking display (default 120s)")
    p.add_argument("--primary-capital-scale", type=float, default=PRIMARY_CAPITAL_SCALE_DEFAULT,
                   help="Primary capital scale for ranking display (default $1000)")
    p.add_argument("--only-test", action="store_true", default=True,
                   help="Write only test-window output (default True per codex r17)")
    p.add_argument("--write-train", action="store_true", default=False,
                   help="Also write train-window output (diagnostic)")
    return p.parse_args()


def main():
    args = parse_args()
    latencies = [int(x) for x in args.latencies.split(",")]
    capital_scales = [float(x) for x in args.capital_scales.split(",")]

    log.info(
        "config: latencies=%s capital_scales=%s fee_rt_bps=%.2f max_copy_lev=%.1f fallback_frac=%.2f min_order=$%.0f primary_lat=%ds primary_cap=$%.0f",
        latencies, capital_scales, args.fee_rt_bps, args.max_copy_leverage,
        args.fallback_copy_fraction, args.min_order_notional,
        args.primary_latency, args.primary_capital_scale,
    )

    log.info("loading journeys: %s", args.journeys)
    journeys = pd.read_parquet(args.journeys)
    log.info("journeys total: %d", len(journeys))

    # Filter to closed (exit_ts not null and finite)
    journeys = journeys[journeys["exit_ts"].notna()].copy()

    train_start = datetime.fromisoformat(args.train_start).replace(tzinfo=timezone.utc)
    train_end = datetime.fromisoformat(args.train_end).replace(tzinfo=timezone.utc)
    test_start = datetime.fromisoformat(args.test_start).replace(tzinfo=timezone.utc)
    test_end = datetime.fromisoformat(args.test_end).replace(tzinfo=timezone.utc)

    train_ms_lo, train_ms_hi = int(train_start.timestamp() * 1000), int(train_end.timestamp() * 1000)
    test_ms_lo, test_ms_hi = int(test_start.timestamp() * 1000), int(test_end.timestamp() * 1000)

    journeys_train = journeys[
        (journeys["entry_ts"] >= train_ms_lo) &
        (journeys["exit_ts"] <= train_ms_hi)
    ].copy()
    journeys_test = journeys[
        (journeys["entry_ts"] >= test_ms_lo) &
        (journeys["exit_ts"] <= test_ms_hi)
    ].copy()
    log.info("train journeys: %d, test journeys: %d", len(journeys_train), len(journeys_test))

    # Coins universe + reference data window
    coins = sorted(set(journeys_train["coin"]).union(journeys_test["coin"]))
    data_start = min(train_start, test_start)
    data_end = max(train_end, test_end) + timedelta(days=1)
    candles = load_candles_1m(coins, data_start, data_end)
    candles_idx = build_coin_index(candles)
    funding = load_funding(coins, data_start, data_end)
    funding_idx = build_funding_index(funding)

    # Splits to write: test always; train only if --write-train (codex r17 Q8 + Alberto)
    splits = [("test", journeys_test)]
    if args.write_train:
        splits.insert(0, ("train", journeys_train))

    for tag, jdf in splits:
        log.info("evaluating %s journeys: %d", tag, len(jdf))
        if jdf.empty:
            log.warning("%s window has zero journeys, writing empty output", tag)
            empty = aggregate_wallet(jdf, latencies, capital_scales)
            empty.to_parquet(f"{args.out_prefix}_{tag}.parquet", index=False)
            continue

        # Per-journey copy evaluation (latency-only; capital sweep expanded after)
        outs = []
        for j in jdf.itertuples(index=False):
            outs.append(evaluate_journey(
                pd.Series(j._asdict()),
                candles_idx,
                funding_idx,
                latencies,
                fee_rt_bps=args.fee_rt_bps,
            ))
        jdf_eval = pd.concat([jdf.reset_index(drop=True), pd.DataFrame(outs)], axis=1)

        # CAPITAL SWEEP: expand jdf_eval across all capital scales (long format)
        # Per codex r17: tmp = jdf_eval.copy() per scale; assign capital_scale +
        # our_copy_notional_usd + sizing_scope + executable +
        # copy_net_pnl_usd_{L}s_{proxy} + copy_net_return_pct_equity_{L}s_{proxy}
        cap_frames = []
        for cap in capital_scales:
            tmp = jdf_eval.copy()
            tmp["capital_scale"] = cap

            # Sizing per row
            sizing_out = tmp.apply(
                lambda r: compute_copy_notional(
                    r, cap,
                    max_copy_leverage=args.max_copy_leverage,
                    fallback_copy_fraction=args.fallback_copy_fraction,
                ),
                axis=1,
            )
            tmp["our_copy_notional_usd"] = sizing_out.apply(lambda x: x[0])
            tmp["sizing_scope"] = sizing_out.apply(lambda x: x[1])
            tmp["executable"] = tmp["our_copy_notional_usd"].fillna(0) >= args.min_order_notional

            # Compute copy_net_pnl_usd and copy_net_return_pct_equity per (L, proxy)
            for L in latencies:
                for proxy in ("neutral", "conservative"):
                    bps_col = f"copy_net_bps_{L}s_{proxy}"
                    pnl_col = f"copy_net_pnl_usd_{L}s_{proxy}"
                    pct_eq_col = f"copy_net_return_pct_equity_{L}s_{proxy}"
                    if bps_col in tmp.columns:
                        tmp[pnl_col] = tmp["our_copy_notional_usd"] * tmp[bps_col] / 10000.0
                        tmp[pct_eq_col] = tmp[pnl_col] / cap
                    else:
                        tmp[pnl_col] = float("nan")
                        tmp[pct_eq_col] = float("nan")

            cap_frames.append(tmp)
        jdf_long = pd.concat(cap_frames, ignore_index=True)
        log.info("expanded to long format: %d rows (= %d journeys * %d capital scales)",
                 len(jdf_long), len(jdf_eval), len(capital_scales))

        # Per-wallet aggregation (long format: per (wallet, capital_scale, latency, proxy))
        wallets_agg = aggregate_wallet(jdf_long, latencies, capital_scales)

        # Min-journeys gate on PRIMARY (latency, capital_scale, proxy=conservative)
        primary_lat = args.primary_latency
        primary_cap = args.primary_capital_scale
        primary_mask = (
            (wallets_agg["latency_seconds"] == primary_lat)
            & (wallets_agg["capital_scale"] == primary_cap)
            & (wallets_agg["proxy"] == "conservative")
        )
        primary_pass_wallets = set(
            wallets_agg.loc[
                primary_mask & (wallets_agg["valid_n"] >= args.min_journeys),
                "wallet",
            ]
        )
        wallets_agg = wallets_agg[wallets_agg["wallet"].isin(primary_pass_wallets)]

        out_path = f"{args.out_prefix}_{tag}.parquet"
        wallets_agg.to_parquet(out_path, index=False)
        log.info("wrote %s: %d (wallet, capital_scale, latency, proxy) rows from %d wallets",
                 out_path, len(wallets_agg), len(primary_pass_wallets))

        # Top 10 by PRIMARY copy_score
        if not wallets_agg.empty:
            primary_view = wallets_agg[
                (wallets_agg["latency_seconds"] == primary_lat)
                & (wallets_agg["capital_scale"] == primary_cap)
                & (wallets_agg["proxy"] == "conservative")
            ]
            if not primary_view.empty and primary_view["copy_score"].notna().any():
                log.info(
                    "top 10 by copy_score (%s, lat=%ds, cap=$%.0f, conservative):",
                    tag, primary_lat, primary_cap,
                )
                top10 = primary_view.nlargest(10, "copy_score")
                for _, r in top10.iterrows():
                    log.info(
                        "  %s n=%d jpd=%.1f pct_eq_shrunk=%.4f%% pct_eq_frac_pos=%.2f score=%.4f exec_frac=%.2f",
                        str(r["wallet"])[:16],
                        int(r["valid_n"]),
                        float(r["jpd_valid"]),
                        float(r["copy_net_return_pct_equity_shrunk_mean"]) * 100,
                        float(r["copy_net_return_pct_equity_frac_pos"]),
                        float(r["copy_score"]),
                        float(r["executable_frac"]),
                    )


if __name__ == "__main__":
    main()
