#!/usr/bin/env python3
"""V13 Script 4/5 (v3): Walk-forward backtest with forward signal replay.

Per projects/quant/v13 Section 6.

v3 additions (Alberto direction 2026-05-24):
- Emit `walk_forward_daily_returns.parquet` (per fold, day_idx, daily_return)
  so v13_report can compute spec 6.3 AGGREGATE Sharpe on pooled daily returns.
- Emit `walk_forward_random_samples.parquet` (per fold, trial, random_sharpe,
  random_pnl_pct) so v13_report can compute spec 6.3 AGGREGATE random p95
  rank on the pooled distribution. Mean-of-per-fold-scalars != pooled.
- Real per-fold fee + slippage accumulation (test_fees_usd, test_slippage_usd,
  test_gross_pnl_usd, fee_drag) from simulator's per-minute fees_minute /
  slippage_minute. Replaces the prior 0.30 placeholder.

v2 fixes (codex r1 #14-#20) preserved:

#14 Parameter grid per spec 6.2 v1 (narrowed from the older wider sweep):
    K in {10,25,50}, per_coin_cap = 0.25 FIXED, gross_cap in {1.0,1.5},
    cooldown = 120s FIXED, poll in {1,5,10} min (30s approximated by 1m
    granularity), weighting = equal only (score is DIAGNOSTIC), consensus
    in {off, soft} (hard40 is ABLATION-ONLY).

    Validation selects best by Sharpe; production sweep is pinned by the
    `K_choices` CLI arg + line ~990's hardcoded gross/cooldown/per_coin_cap
    constants per spec 6.2 v1.

#15 hard40 consensus is ABLATION-ONLY, never selected for production.

#16 Random portfolios default 1000, with BOTH Sharpe AND net-PnL percentile
    gates (>= 95th required for binding criterion). Per-trial Sharpe + PnL
    samples are emitted to the random-samples parquet for v13_report
    pooling.

#17 1m intraday simulation. 15-min staleness rule + 60-120s cooldown
    enforced at minute grain.

#18 Positions stored as COIN QUANTITY (signed float per coin). At each
    timestep, notional = quantity * current_price. Rebalance computes
    delta_quantity from delta_notional / current_price.

#19 FORWARD SIGNAL REPLAY: per fold, rank wallets using only fills with
    time <= train_end. During validation + test, observe each selected
    wallet's position state by walking their fills forward at each
    simulated 1m timestamp using only fills with time <= t.

#20 9-EXPERIMENT ABLATION SUITE (Section 6.7). Each runs against best-
    validation parameters on OOS test. Output `ablation_results.parquet`.
"""
from __future__ import annotations

import argparse
import logging
import math
import multiprocessing as mp
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_equity_reconstruct import (    # noqa: E402
    EPS,
    load_fills_for_dates,
    validate_and_normalize_fills,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_walkfwd] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "walk_forward_results.parquet"
ABLATION_OUTPUT = ROOT / "app" / "data" / "v13" / "ablation_results.parquet"

HL_FEE_BPS_PER_SIDE = 4.32
SLIPPAGE_BPS_REALISTIC = 5.0


def _utc_ts(x) -> pd.Timestamp:
    """Coerce any datetime-like value to a UTC-aware pandas Timestamp.

    Newer pandas (2.x) raises ValueError when `pd.Timestamp(x, tz="UTC")` is
    called on an `x` that already has tzinfo. This helper handles both tz-
    aware and tz-naive inputs uniformly: naive inputs get localized to UTC,
    aware inputs are converted to UTC.
    """
    ts = pd.Timestamp(x)
    if ts.tz is None:
        return ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


# Module-level globals populated per-process at fork time so workers can
# avoid pickling the large minute_prices / minute_volumes / streams payloads
# on every call. Parent sets these before launching the ProcessPoolExecutor
# (with fork context); children inherit them via COW.
_WORKER_STREAMS: dict | None = None
_WORKER_WALLET_SCORES: dict | None = None
_WORKER_MINUTE_PRICES: pd.DataFrame | None = None
_WORKER_MINUTE_VOLUMES: pd.DataFrame | None = None
_WORKER_EQ_AT_TRAIN_END: float | None = None
_WORKER_BEST_PARAMS = None
_WORKER_TEST_START = None
_WORKER_TEST_END = None
_WORKER_VAL_START = None
_WORKER_ALL_ELIGIBLE_WALLETS: list | None = None
_WORKER_RNG_SEED_BASE: int | None = None
_WORKER_ANCHOR_CACHE = None
_WORKER_PREPARED_MARKET = None


def _set_random_trial_globals(streams, wallet_scores, minute_prices, minute_volumes,
                              eq_at_train_end, best_params, test_start, test_end,
                              val_start, all_eligible_wallets, rng_seed_base,
                              anchor_cache=None, prepared_market=None):
    """Populate worker module-globals in the parent BEFORE forking workers so
    children inherit them via COW. Avoids repeated pickling of giant frames.
    """
    global _WORKER_STREAMS, _WORKER_WALLET_SCORES, _WORKER_MINUTE_PRICES
    global _WORKER_MINUTE_VOLUMES, _WORKER_EQ_AT_TRAIN_END, _WORKER_BEST_PARAMS
    global _WORKER_TEST_START, _WORKER_TEST_END, _WORKER_VAL_START
    global _WORKER_ALL_ELIGIBLE_WALLETS, _WORKER_RNG_SEED_BASE
    global _WORKER_ANCHOR_CACHE, _WORKER_PREPARED_MARKET
    _WORKER_STREAMS = streams
    _WORKER_WALLET_SCORES = wallet_scores
    _WORKER_MINUTE_PRICES = minute_prices
    _WORKER_MINUTE_VOLUMES = minute_volumes
    _WORKER_EQ_AT_TRAIN_END = eq_at_train_end
    _WORKER_BEST_PARAMS = best_params
    _WORKER_TEST_START = test_start
    _WORKER_TEST_END = test_end
    _WORKER_VAL_START = val_start
    _WORKER_ALL_ELIGIBLE_WALLETS = all_eligible_wallets
    _WORKER_RNG_SEED_BASE = rng_seed_base
    _WORKER_ANCHOR_CACHE = anchor_cache
    _WORKER_PREPARED_MARKET = prepared_market


def _simulate_random_trial(trial: int) -> tuple[int, float, float]:
    """Worker entry point for one random-portfolio simulation.

    Inherits everything else via fork-COW from the parent process; only the
    trial index is passed across the IPC boundary. The forked child mutates
    its OWN copy of streams (next_idx); parent + sibling workers are isolated.

    Returns (trial, sharpe, net_return_pct).
    """
    rng = np.random.default_rng(_WORKER_RNG_SEED_BASE + trial)
    K = _WORKER_BEST_PARAMS.K
    if len(_WORKER_ALL_ELIGIBLE_WALLETS) < K:
        return (trial, 0.0, 0.0)
    rand_sel = list(rng.choice(_WORKER_ALL_ELIGIBLE_WALLETS, size=K, replace=False))
    rc = simulate_replication(
        rand_sel, _WORKER_STREAMS, _WORKER_WALLET_SCORES, _WORKER_EQ_AT_TRAIN_END,
        _WORKER_MINUTE_PRICES, _WORKER_TEST_START, _WORKER_TEST_END, _WORKER_BEST_PARAMS,
        anchor_dt=_WORKER_VAL_START,
        minute_volumes=_WORKER_MINUTE_VOLUMES,
        anchor_cache=_WORKER_ANCHOR_CACHE,
        prepared_market=_WORKER_PREPARED_MARKET,
    )
    rm = equity_curve_metrics(rc, _WORKER_BEST_PARAMS.starting_capital)
    return (trial, float(rm["sharpe"]), float(rm["net_return_pct"]))


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class BotParams:
    K: int                                              # top-K wallets
    per_coin_cap: float
    gross_cap: float
    cooldown_seconds: int
    poll_minutes: int
    weighting: str                                      # "equal" or "score"
    consensus: str                                      # "off" or "soft" (production); "hard40" ablation-only
    fee_bps_per_side: float = HL_FEE_BPS_PER_SIDE
    slippage_bps: float = SLIPPAGE_BPS_REALISTIC
    starting_capital: float = 1000.0
    staleness_minutes: int = 15                         # max staleness rebalance per spec
    min_delta_usd: float = 10.0
    min_delta_pct: float = 0.20
    # Liquidity-aware slippage (per GPT review point 8 + Section 4.3 spec).
    # When volume data is supplied via simulate_replication(minute_volumes=...):
    #   - order_size > max_size_pct_of_volume * volume_in_minute -> SKIP
    #   - else effective_slippage_bps = slippage_bps + volume_impact_bps_per_pct
    #         * 100 * (order_size / volume_in_minute)
    # When volume data is NOT supplied (None / empty), behavior collapses to
    # the fixed slippage_bps and no skips, matching prior v1 behavior.
    enable_liquidity_filter: bool = True
    max_size_pct_of_volume: float = 0.01                # skip if order > 1% of 1m volume
    volume_impact_bps_per_pct: float = 50.0             # +50 bps per 1% of volume


@dataclass
class WalletFillStream:
    """Indexed time series of one wallet's fills for forward replay."""
    times: np.ndarray              # int64 ms timestamps, sorted ascending
    coins: np.ndarray              # object dtype
    signed_sizes: np.ndarray       # float64
    prices: np.ndarray             # float64
    next_idx: int = 0              # cursor for forward replay


@dataclass
class PreparedMarket:
    """Numpy-backed minute_prices / minute_volumes for O(1) hot-loop lookups.

    Replaces pandas .loc[ts] + Series.get(coin) which were the dominant
    bottleneck (codex perf review #1, ~10-30x speedup). Built ONCE per fold,
    reused across all validation+test+random_portfolio simulations.

    Attributes:
        ts_ms_arr: int64[n_min]  -- minute-aligned UTC ms timestamps
        prices_arr: float64[n_min, n_coin]  -- close prices, NaN for missing
        volumes_arr: float64[n_min, n_coin] | None  -- 1m traded volume, 0 for missing
        coin_to_col: str -> int  -- coin name to column index in prices_arr
        vol_coin_to_col: str -> int  -- coin name to column index in volumes_arr
        ts_to_row: int64 -> int  -- ts_ms -> row index in prices_arr
    """
    ts_ms_arr: np.ndarray
    prices_arr: np.ndarray
    volumes_arr: np.ndarray | None
    coin_to_col: dict
    vol_coin_to_col: dict
    ts_to_row: dict


def prepare_market(minute_prices: pd.DataFrame,
                   minute_volumes: pd.DataFrame | None = None) -> PreparedMarket:
    """Convert pandas DataFrames to numpy arrays + lookup dicts for fast access.

    Cost: one-time per fold (~0.5-2 sec for 252K rows x 715 coins). Pays back
    instantly in the hot loop where each .loc[ts] saved is a ~1000x speedup.
    """
    # Coerce DatetimeIndex (tz-aware) to int64 ms since epoch. Strip tz first
    # (cannot cast tz-aware datetimes to naive datetime64 in pandas 2.x).
    idx = minute_prices.index
    if hasattr(idx, "tz") and idx.tz is not None:
        idx_naive = idx.tz_convert("UTC").tz_localize(None)
    else:
        idx_naive = idx
    ts_ms_arr = np.asarray(idx_naive.astype("datetime64[ms]").view("int64"), dtype=np.int64)
    prices_arr = np.ascontiguousarray(minute_prices.values, dtype=np.float64)
    coin_to_col = {str(c): i for i, c in enumerate(minute_prices.columns)}

    if minute_volumes is not None and not minute_volumes.empty:
        # Volume rows should align with the same ts grid (same load_volume_grid path
        # uses the same reindex). Build separately and align by ts_ms.
        v_idx = minute_volumes.index
        if hasattr(v_idx, "tz") and v_idx.tz is not None:
            v_idx_naive = v_idx.tz_convert("UTC").tz_localize(None)
        else:
            v_idx_naive = v_idx
        v_ts_ms = np.asarray(v_idx_naive.astype("datetime64[ms]").view("int64"), dtype=np.int64)
        if np.array_equal(v_ts_ms, ts_ms_arr):
            volumes_arr = np.ascontiguousarray(minute_volumes.values, dtype=np.float64)
        else:
            # Reindex volumes onto price grid; missing rows -> 0.0
            vol_to_idx = {int(t): i for i, t in enumerate(v_ts_ms)}
            volumes_arr = np.zeros((len(ts_ms_arr), len(minute_volumes.columns)), dtype=np.float64)
            for out_i, t in enumerate(ts_ms_arr):
                src = vol_to_idx.get(int(t))
                if src is not None:
                    volumes_arr[out_i] = minute_volumes.values[src]
        vol_coin_to_col = {str(c): i for i, c in enumerate(minute_volumes.columns)}
    else:
        volumes_arr = None
        vol_coin_to_col = {}

    ts_to_row = {int(t): i for i, t in enumerate(ts_ms_arr)}

    return PreparedMarket(
        ts_ms_arr=ts_ms_arr,
        prices_arr=prices_arr,
        volumes_arr=volumes_arr,
        coin_to_col=coin_to_col,
        vol_coin_to_col=vol_coin_to_col,
        ts_to_row=ts_to_row,
    )


@dataclass
class WalletState:
    """Running state during forward replay."""
    positions: dict = field(default_factory=dict)       # coin -> signed quantity
    cost_basis: dict = field(default_factory=dict)      # coin -> avg cost basis (price-units per coin)
    last_fill_ts: int = 0
    # Anchor state (set ONCE when we cross the fold's anchor time = train_end).
    anchor_positions: dict = field(default_factory=dict)
    anchor_cost_basis: dict = field(default_factory=dict)
    anchor_prices: dict = field(default_factory=dict)   # coin -> price at anchor timestamp
    anchor_unrealized_fixed: float = 0.0                # scalar: anchor-time unrealized PnL
    anchor_set: bool = False
    # Realized PnL accumulated from fills AFTER the anchor crossing.
    realized_pnl_post_anchor: float = 0.0


@dataclass
class AnchorCacheEntry:
    """Per-wallet snapshot at fold anchor (train_end). Used to skip the
    redundant pre-anchor advance_wallet_state replay across the validation
    sweep + 1000 random portfolios per fold (codex perf review #3,
    ~2-10x speedup target on random path).

    A simulation that uses this cache:
        1. Copies positions/cost_basis dicts (will mutate during sim)
        2. Copies anchor_positions/anchor_cost_basis dicts (read-only but
           defensive against accidental mutation)
        3. Sets stream.next_idx = next_idx_at_anchor (skip pre-anchor fills)
        4. realized_pnl_post_anchor starts at 0 (anchor-derived metric)
    """
    positions: dict
    cost_basis: dict
    last_fill_ts: int
    anchor_set: bool
    anchor_positions: dict
    anchor_cost_basis: dict
    anchor_prices: dict
    anchor_unrealized_fixed: float
    next_idx_at_anchor: int


def build_anchor_cache(
    wallets: list,
    wallet_streams: dict,
    anchor_ts_ms: int,
    prepared_market: "PreparedMarket | None" = None,
) -> dict:
    """Build per-wallet AnchorCacheEntry by replaying each stream once to
    anchor_ts_ms. Subsequent simulations use the cache via clone-and-resume,
    skipping the pre-anchor advance entirely.

    If prepared_market is provided, the cache also captures anchor_prices +
    anchor_unrealized_fixed (otherwise simulate_replication will compute
    them per-call as fallback).

    Mutates `wallet_streams[w].next_idx` to next_idx_at_anchor for each
    cached wallet. Callers using the cache should expect this side-effect.
    """
    anchor_prices_row = None
    if prepared_market is not None and prepared_market.prices_arr.shape[0] > 0:
        anchor_row_idx = int(np.searchsorted(prepared_market.ts_ms_arr, anchor_ts_ms, side="right") - 1)
        if anchor_row_idx >= 0:
            anchor_prices_row = prepared_market.prices_arr[anchor_row_idx]

    cache: dict = {}
    for w in wallets:
        if w not in wallet_streams:
            continue
        stream = wallet_streams[w]
        stream.next_idx = 0
        state = WalletState()
        advance_wallet_state(state, stream, anchor_ts_ms, anchor_ts_ms=anchor_ts_ms)

        if state.anchor_set and anchor_prices_row is not None and prepared_market is not None:
            anchor_unrealized = 0.0
            for coin, qty in state.anchor_positions.items():
                if abs(qty) < EPS:
                    continue
                col = prepared_market.coin_to_col.get(coin)
                if col is None:
                    continue
                px = anchor_prices_row[col]
                if math.isnan(px):
                    continue
                cb = state.anchor_cost_basis.get(coin)
                if cb is None or cb <= 0:
                    continue
                state.anchor_prices[coin] = float(px)
                anchor_unrealized += float(qty) * (float(px) - float(cb))
            state.anchor_unrealized_fixed = anchor_unrealized

        cache[w] = AnchorCacheEntry(
            positions=dict(state.positions),
            cost_basis=dict(state.cost_basis),
            last_fill_ts=state.last_fill_ts,
            anchor_set=state.anchor_set,
            anchor_positions=dict(state.anchor_positions),
            anchor_cost_basis=dict(state.anchor_cost_basis),
            anchor_prices=dict(state.anchor_prices),
            anchor_unrealized_fixed=state.anchor_unrealized_fixed,
            next_idx_at_anchor=stream.next_idx,
        )
    return cache


# ---------------------------------------------------------------------------
# Forward-signal replay infrastructure
# ---------------------------------------------------------------------------

def build_wallet_fill_streams(fills: pd.DataFrame, wallets: list[str]) -> dict:
    """For each wallet, build a sorted fill stream for forward replay."""
    streams = {}
    for w in wallets:
        wf = fills[fills["wallet"] == w]
        if wf.empty:
            streams[w] = WalletFillStream(
                times=np.array([], dtype=np.int64),
                coins=np.array([], dtype=object),
                signed_sizes=np.array([], dtype=np.float64),
                prices=np.array([], dtype=np.float64),
            )
            continue
        wf = wf.sort_values(["time", "coin", "side"], kind="stable")
        signed = np.where(wf["side"].values == "B", wf["size"].values.astype(np.float64), -wf["size"].values.astype(np.float64))
        streams[w] = WalletFillStream(
            times=wf["time"].values.astype(np.int64),
            coins=wf["coin"].values.astype(object),
            signed_sizes=signed,
            prices=wf["price"].values.astype(np.float64),
        )
    return streams


def advance_wallet_state(
    state: WalletState, stream: WalletFillStream, up_to_ts_ms: int,
    anchor_ts_ms: int | None = None,
):
    """Apply fills with time <= up_to_ts_ms to state. Cursor moves forward.

    Maintains cost basis incrementally + accumulates realized PnL for fills
    AFTER anchor_ts_ms (if provided). The anchor is captured ONCE the first
    time a fill at time >= anchor_ts_ms is processed (or when explicitly
    set via the caller).

    Pre-anchor fills update positions/cost_basis but do NOT contribute to
    realized_pnl_post_anchor (those were already absorbed into the base
    equity at train_end).
    """
    n = len(stream.times)
    i = stream.next_idx
    while i < n and stream.times[i] <= up_to_ts_ms:
        ts = int(stream.times[i])
        coin = stream.coins[i]
        signed = stream.signed_sizes[i]
        price = float(stream.prices[i])
        pos = state.positions.get(coin, 0.0)
        cb = state.cost_basis.get(coin, 0.0)
        new_pos = pos + signed

        # If we crossed the anchor on a prior fill but haven't snapshotted yet,
        # do it now (positions/cost_basis BEFORE applying this fill).
        if anchor_ts_ms is not None and not state.anchor_set and ts >= anchor_ts_ms:
            state.anchor_positions = dict(state.positions)
            state.anchor_cost_basis = dict(state.cost_basis)
            state.anchor_set = True

        # Compute realized PnL for trim / exit / reverse (only counted post-anchor).
        realized_this_fill = 0.0
        if abs(pos) > EPS:
            if abs(new_pos) < EPS:
                # full close
                realized_this_fill = pos * (price - cb)
            elif (pos > 0 and new_pos > 0 and signed < 0) or (pos < 0 and new_pos < 0 and signed > 0):
                # trim
                closed_qty = -signed   # signed: negative of fill direction
                realized_this_fill = (price - cb) * closed_qty
            elif (pos > 0 and new_pos < 0) or (pos < 0 and new_pos > 0):
                # reverse: close existing leg entirely
                realized_this_fill = pos * (price - cb)
        if state.anchor_set and (anchor_ts_ms is None or ts >= anchor_ts_ms):
            state.realized_pnl_post_anchor += realized_this_fill

        # Update cost basis + position.
        if abs(pos) < EPS:
            cb = price
        elif (pos > 0 and signed > 0) or (pos < 0 and signed < 0):
            total_qty = abs(new_pos)
            if total_qty > EPS:
                cb = (cb * abs(pos) + price * abs(signed)) / total_qty
        elif abs(new_pos) < EPS:
            cb = 0.0
        elif (pos > 0 and new_pos > 0) or (pos < 0 and new_pos < 0):
            pass
        else:
            cb = price
        state.positions[coin] = new_pos
        state.cost_basis[coin] = cb
        i += 1
    stream.next_idx = i
    if i > 0:
        state.last_fill_ts = int(stream.times[i - 1])
    # If we reached anchor_ts_ms but no fills crossed it (no activity yet),
    # set the anchor snapshot here so equity formula has a valid baseline.
    if anchor_ts_ms is not None and not state.anchor_set and stream.next_idx > 0 and state.last_fill_ts >= anchor_ts_ms:
        # already-set path
        pass
    if anchor_ts_ms is not None and not state.anchor_set and up_to_ts_ms >= anchor_ts_ms:
        state.anchor_positions = dict(state.positions)
        state.anchor_cost_basis = dict(state.cost_basis)
        state.anchor_set = True


# ---------------------------------------------------------------------------
# Daily mid prices (1m candles aggregated)
# ---------------------------------------------------------------------------

def load_price_grid(coins: list[str], start: datetime, end: datetime, interval: str) -> pd.DataFrame:
    """Pull candles at the requested interval, pivot wide, forward-fill within
    each coin to populate no-trade minutes.

    interval: "1m" (production; uses S3-reconstructed candles, filtered by
    source="s3_reconstructed" to avoid mixing with stale API rows) or "1h"
    (fallback; flags results as APPROXIMATED).

    For 1m, we forward-fill within each coin's price series so the walk-
    forward simulator always has a price at every minute, even if a given
    coin had no fills in that minute. This matches HL's own UI behavior of
    holding the last traded price until a new trade.
    """
    # codex perf review finding #2: close MongoClient before any fork-pool to
    # avoid leaking PyMongo monitor threads (not fork-safe). Use context manager.
    coll_name = "hyperliquid_candles" if interval == "1m" else "hyperliquid_candles_1h"
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    query = {
        "coin": {"$in": list(coins)},
        "interval": interval,
        "timestamp_utc": {"$gte": start_ms, "$lte": end_ms},
    }
    if interval == "1m":
        # Production 1m data MUST come from S3 reconstruction; never mix in
        # stale API-backfilled 1m rows (which only cover ~5 days anyway).
        query["source"] = "s3_reconstructed"
    with MongoClient("mongodb://localhost:27017") as client:
        c = client["quants_lab"][coll_name]
        docs = list(c.find(query, {"coin": 1, "timestamp_utc": 1, "close": 1, "_id": 0}))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    pivot = df.pivot_table(index="dt", columns="coin", values="close", aggfunc="last")
    pivot = pivot.sort_index()
    if interval == "1m" and not pivot.empty:
        # Reindex onto a complete 1-minute grid covering the window, then
        # forward-fill within each coin so no-trade minutes carry forward
        # the last-known price.
        full_idx = pd.date_range(
            start=_utc_ts(start),
            end=_utc_ts(end + timedelta(days=1)),
            freq="1min",
            inclusive="left",
        )
        pivot = pivot.reindex(full_idx).ffill()
    return pivot


def load_volume_grid(coins: list[str], start: datetime, end: datetime,
                     interval: str = "1m") -> pd.DataFrame:
    """Pull 1m traded volume (in coin units) per (minute, coin) for the same
    window as the price grid.

    Used by simulate_replication for liquidity-aware slippage:
        - if order_size_coin > max_size_pct_of_volume * volume_in_minute,
          the trade is SKIPPED (logged as skipped_notional)
        - otherwise effective slippage bps grows linearly with order/volume:
              eff_bps = base_bps + impact_bps_per_pct * (order_size / volume)

    No-trade minutes have NO bar in MongoDB and therefore zero volume after
    the reindex; the simulator treats zero-volume as "unknown liquidity" and
    falls back to the fixed slippage_bps without skipping.

    Returns a wide DataFrame indexed by UTC minute with columns=coins. Empty
    when no docs match (e.g., long-tail coin with no 1m data).
    """
    if interval != "1m":
        return pd.DataFrame()
    # codex perf review finding #2: close MongoClient before fork-pool.
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    query = {
        "coin": {"$in": list(coins)},
        "interval": "1m",
        "source": "s3_reconstructed",
        "timestamp_utc": {"$gte": start_ms, "$lte": end_ms},
    }
    with MongoClient("mongodb://localhost:27017") as client:
        c = client["quants_lab"]["hyperliquid_candles"]
        docs = list(c.find(query, {"coin": 1, "timestamp_utc": 1, "volume": 1, "_id": 0}))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    pivot = df.pivot_table(index="dt", columns="coin", values="volume", aggfunc="last")
    pivot = pivot.sort_index()
    # Reindex onto the same 1-minute grid as the price grid. Missing minutes
    # are filled with 0 (no trade activity in that minute = unknown
    # liquidity, NOT a small but nonzero estimate).
    full_idx = pd.date_range(
        start=_utc_ts(start),
        end=_utc_ts(end + timedelta(days=1)),
        freq="1min",
        inclusive="left",
    )
    pivot = pivot.reindex(full_idx).fillna(0.0)
    return pivot


# Back-compat alias retained.
def load_minute_close_prices(coins, start, end):
    return load_price_grid(coins, start, end, "1m")


# ---------------------------------------------------------------------------
# Wallet equity at time t (for signal denominator)
# ---------------------------------------------------------------------------

def compute_wallet_equity_at(
    state: WalletState, prices_at_t, base_equity: float,
    coin_to_col: dict | None = None,
) -> float:
    """Equity at time t, anchored on the wallet's equity at fold start (train_end).

        equity[t] = base_equity
                  + realized_pnl_post_anchor                # realized fills inside the fold
                  + (current_unrealized_at_t - anchor_unrealized_at_t)
                                                           # mark-to-market change since anchor

    `prices_at_t` is EITHER:
        - a pandas Series indexed by coin (legacy callers; uses .get(coin))
        - a numpy ndarray indexed by column (perf path; requires coin_to_col)

    Where:
        current_unrealized_at_t = sum_coin state.positions[coin] * (price[t] - state.cost_basis[coin])
        anchor_unrealized_at_t  = sum_coin state.anchor_positions[coin] * (price[t] - state.anchor_cost_basis[coin])

    The anchor offset removes the part of `base_equity` that's already
    accounted for via the train_end open positions, leaving only the
    post-anchor PnL contribution.
    """
    if not state.anchor_set:
        # Pre-anchor: simulator hasn't crossed train_end yet.
        return base_equity

    is_array = isinstance(prices_at_t, np.ndarray)
    current_unrealized = 0.0
    for coin, qty in state.positions.items():
        if abs(qty) < EPS:
            continue
        if is_array:
            col = coin_to_col.get(coin) if coin_to_col else None
            if col is None:
                continue
            px = prices_at_t[col]
            if math.isnan(px):
                continue
        else:
            px = prices_at_t.get(coin)
            if px is None or pd.isna(px):
                continue
        cb = state.cost_basis.get(coin)
        if cb is None or cb <= 0:
            continue
        current_unrealized += float(qty) * (float(px) - float(cb))

    # anchor_unrealized is a SCALAR captured at the anchor moment (train_end);
    # it does NOT vary with current prices. Otherwise we'd cancel out the very
    # MTM contribution we're trying to capture (codex r16 P1).
    anchor_unrealized = state.anchor_unrealized_fixed

    return base_equity + state.realized_pnl_post_anchor + (current_unrealized - anchor_unrealized)


# ---------------------------------------------------------------------------
# Replication bot simulator (1m granularity, coin quantity storage)
# ---------------------------------------------------------------------------

def simulate_replication(
    selected_wallets: list,
    wallet_streams: dict,                          # wallet -> WalletFillStream
    wallet_scores: dict | None,
    wallet_base_equity: dict,                      # wallet -> base equity at anchor (train_end)
    minute_prices: pd.DataFrame,                   # minute_ts -> coin -> price
    fold_start: datetime,
    fold_end: datetime,
    params: BotParams,
    anchor_dt: datetime | None = None,             # fold anchor time (train_end); defaults to fold_start
    minute_volumes: pd.DataFrame | None = None,    # minute_ts -> coin -> traded volume (coin units)
    anchor_cache: dict | None = None,              # optional: precomputed AnchorCacheEntry per wallet (codex P0 #3)
    prepared_market: "PreparedMarket | None" = None,  # optional: pre-built PreparedMarket (caller-cached across sims)
) -> pd.DataFrame:
    """Run replication bot over [fold_start, fold_end] at 1m cadence.

    Returns DataFrame of per-minute snapshots: ts, our_equity, gross_notional,
    n_active_coins, fees_minute, slippage_minute, skipped_notional_minute.

    If `minute_volumes` is supplied and `params.enable_liquidity_filter` is
    True, fills check the per-coin per-minute traded volume:
      - if abs(order_size_coin) > params.max_size_pct_of_volume * volume,
        the order is SKIPPED and the target notional is logged into
        skipped_notional_minute (NOT applied to our_quantity)
      - else effective_slippage_bps = params.slippage_bps +
            params.volume_impact_bps_per_pct * 100 * (order_size / volume)
    A zero-volume minute (no S3 fills for that coin) is treated as unknown
    liquidity: the simulator falls back to fixed slippage_bps without
    skipping. This avoids false skips on quiet minutes; the broader 174-day
    backtest would skip nearly everything if zero-vol counted as illiquid.
    """
    if not selected_wallets:
        return pd.DataFrame()

    weights = {}
    total_score = 0.0
    if params.weighting == "score" and wallet_scores is not None:
        for w in selected_wallets:
            total_score += max(0.0, wallet_scores.get(w, 0.0))
    if params.weighting == "equal" or total_score == 0:
        for w in selected_wallets:
            weights[w] = 1.0 / len(selected_wallets)
    else:
        for w in selected_wallets:
            weights[w] = max(0.0, wallet_scores.get(w, 0.0)) / total_score

    # State: each selected wallet has its own forward replay state.
    wallet_states = {w: WalletState() for w in selected_wallets}
    for w in selected_wallets:
        if w in wallet_streams:
            # If anchor_cache is provided, set cursor to post-anchor position
            # (skip pre-anchor replay). Otherwise reset to 0 (legacy path).
            if anchor_cache is not None and w in anchor_cache:
                wallet_streams[w].next_idx = anchor_cache[w].next_idx_at_anchor
            else:
                wallet_streams[w].next_idx = 0

    # Build PreparedMarket (numpy arrays) ONCE per simulate call (or use caller-
    # provided to amortize across many simulations in the same fold). Hot loop
    # uses prices_arr[row_idx, col] + coin_to_col instead of pandas .loc[ts]
    # + Series.get(coin) — codex perf review #1, ~10-30x speedup target.
    pm = prepared_market if prepared_market is not None else prepare_market(minute_prices, minute_volumes)
    prices_arr = pm.prices_arr
    volumes_arr = pm.volumes_arr
    coin_to_col = pm.coin_to_col
    vol_coin_to_col = pm.vol_coin_to_col
    ts_to_row = pm.ts_to_row
    ts_ms_arr = pm.ts_ms_arr
    n_coins = prices_arr.shape[1]

    # Pre-extract column lists once for the iteration order. coin_names_by_col
    # lets us reverse-map column -> coin name for "for c in prices_at_t.index"
    # patterns from the legacy code (last_prices update loop).
    coin_names_by_col = [None] * n_coins
    for c, i in coin_to_col.items():
        coin_names_by_col[i] = c

    # The anchor (train_end) is the time at which base_equity is true. Pre-anchor
    # fills update positions/cost_basis silently; post-anchor fills accumulate
    # realized_pnl_post_anchor.
    anchor_ts_ms = int((anchor_dt or fold_start).timestamp() * 1000)

    # First: advance each wallet's state to the anchor moment and capture the
    # anchor snapshot (positions, cost_basis, prices at anchor). If
    # anchor_cache is provided, CLONE the cached snapshot (codex P0 #3) instead
    # of re-running advance_wallet_state from stream start every simulation.
    # codex perf review finding #1: compute anchor_prices_row WHENEVER prices_arr
    # is nonempty, NOT only when anchor_cache is None. Otherwise if a cache dict
    # is passed but a selected wallet is missing from it, the fallback path
    # cannot compute anchor_unrealized_fixed, diverging from cache-OFF.
    anchor_prices_row = None  # numpy ndarray (row of prices_arr)
    if prices_arr.shape[0] > 0:
        # Find row index of LAST minute_prices row at or before anchor_ts_ms.
        anchor_row_idx = int(np.searchsorted(ts_ms_arr, anchor_ts_ms, side="right") - 1)
        if anchor_row_idx >= 0:
            anchor_prices_row = prices_arr[anchor_row_idx]
    for w in selected_wallets:
        if anchor_cache is not None and w in anchor_cache:
            # Clone from cache — clones mutable dicts so the sim can mutate
            # positions/cost_basis without leaking back into the cache.
            entry = anchor_cache[w]
            state = wallet_states[w]
            state.positions = dict(entry.positions)
            state.cost_basis = dict(entry.cost_basis)
            state.last_fill_ts = entry.last_fill_ts
            state.anchor_set = entry.anchor_set
            state.anchor_positions = dict(entry.anchor_positions)
            state.anchor_cost_basis = dict(entry.anchor_cost_basis)
            state.anchor_prices = dict(entry.anchor_prices)
            state.anchor_unrealized_fixed = entry.anchor_unrealized_fixed
            state.realized_pnl_post_anchor = 0.0
            # stream.next_idx already set to next_idx_at_anchor above
            continue
        if w in wallet_streams:
            advance_wallet_state(wallet_states[w], wallet_streams[w], anchor_ts_ms, anchor_ts_ms=anchor_ts_ms)
        state = wallet_states[w]
        if state.anchor_set and anchor_prices_row is not None:
            # Snapshot anchor prices + compute anchor_unrealized_fixed.
            anchor_unrealized = 0.0
            for coin, qty in state.anchor_positions.items():
                if abs(qty) < EPS:
                    continue
                col = coin_to_col.get(coin)
                if col is None:
                    continue
                px = anchor_prices_row[col]
                if math.isnan(px):
                    continue
                cb = state.anchor_cost_basis.get(coin)
                if cb is None or cb <= 0:
                    continue
                state.anchor_prices[coin] = float(px)
                anchor_unrealized += float(qty) * (float(px) - float(cb))
            state.anchor_unrealized_fixed = anchor_unrealized

    our_quantity: dict = {}                        # coin -> signed quantity we hold
    our_equity = params.starting_capital
    cooldown_until: dict = {}                      # coin -> ms timestamp before which re-entry blocked
    last_rebalance_ts: dict = {}                   # coin -> ms; for staleness rebalance trigger
    last_prices: dict = {}                         # coin -> price at last seen timestep (for MTM walk)
    consensus_threshold = 0.40 if params.consensus == "hard40" else 0.0
    # `soft` consensus weighting: scale aggregate by fraction of voting wallets.
    soft_consensus = params.consensus == "soft"

    rows = []
    # Build minute index over fold range. Half-open semantics: include minutes
    # in [fold_start, fold_end + 1 day) so the LAST calendar day's minutes are
    # iterated (codex r29 P0 #1 fix). Prior code used `<= fold_end` which is
    # midnight-of-last-day and missed all minutes during that day.
    fold_start_ms = int(fold_start.timestamp() * 1000)
    fold_end_ms_exclusive = int((fold_end + timedelta(days=1)).timestamp() * 1000)
    # Slice ts_ms_arr to in-fold range. searchsorted is O(log n).
    lo = int(np.searchsorted(ts_ms_arr, fold_start_ms, side="left"))
    hi = int(np.searchsorted(ts_ms_arr, fold_end_ms_exclusive, side="left"))
    if hi <= lo:
        return pd.DataFrame()
    minute_row_indices = np.arange(lo, hi, dtype=np.int64)

    poll_step_minutes = params.poll_minutes
    poll_counter = 0

    for row_idx in minute_row_indices:
        ts_ms = int(ts_ms_arr[row_idx])
        prices_row = prices_arr[row_idx]            # numpy float64[n_coins], NaN for missing

        # Advance each selected wallet's state up to ts.
        for w in selected_wallets:
            if w in wallet_streams:
                advance_wallet_state(wallet_states[w], wallet_streams[w], ts_ms, anchor_ts_ms=anchor_ts_ms)

        # MTM EQUITY WALK: equity accrues position PnL every minute, not just on
        # rebalance fills. delta_equity = sum_coin qty * (price_now - price_last).
        if last_prices:
            mtm_pnl_this_step = 0.0
            for coin, qty in our_quantity.items():
                if abs(qty) < EPS:
                    continue
                p0 = last_prices.get(coin)
                if p0 is None:
                    continue
                col = coin_to_col.get(coin)
                if col is None:
                    continue
                p1 = prices_row[col]
                if math.isnan(p1):
                    continue
                mtm_pnl_this_step += qty * (p1 - p0)
            our_equity += mtm_pnl_this_step

        # MTM our positions to current price (for diagnostic gross).
        gross = 0.0
        active_coins = 0
        for coin, qty in our_quantity.items():
            if abs(qty) < EPS:
                continue
            col = coin_to_col.get(coin)
            if col is None:
                continue
            px = prices_row[col]
            if not math.isnan(px):
                gross += abs(qty * px)
                active_coins += 1

        # STALENESS RULE: any coin with a non-zero position whose last
        # rebalance was more than staleness_minutes ago triggers a force
        # rebalance. We only consider coins we currently HOLD (non-zero qty),
        # because exited coins should not perpetually force the rebalance
        # cycle (their last_rebalance_ts ages indefinitely otherwise).
        stale_force = False
        for coin, last_ts in list(last_rebalance_ts.items()):
            if abs(our_quantity.get(coin, 0.0)) < EPS:
                continue
            if ts_ms - last_ts >= params.staleness_minutes * 60 * 1000:
                stale_force = True
                break

        # Only do signal/rebalance work every poll_minutes step (or stale force).
        poll_counter += 1
        if (poll_counter % poll_step_minutes != 0) and not stale_force:
            # Update last_prices for held coins so next minute's MTM walk has
            # valid p0 (skipped path also bypasses the post-rebalance update).
            for coin in our_quantity:
                col = coin_to_col.get(coin)
                if col is None:
                    continue
                p = prices_row[col]
                if not math.isnan(p):
                    last_prices[coin] = float(p)
            rows.append({"ts": ts_ms, "our_equity": our_equity, "gross_notional": gross,
                         "n_active_coins": active_coins, "fees_minute": 0.0, "slippage_minute": 0.0})
            continue

        # Compute signals at ts.
        target_pct = {}
        wallet_signals_now = {}
        for w in selected_wallets:
            state = wallet_states[w]
            base_eq = wallet_base_equity.get(w, 1.0)
            eq_w = compute_wallet_equity_at(state, prices_row, base_eq, coin_to_col=coin_to_col)
            if eq_w <= EPS:
                wallet_signals_now[w] = {}
                continue
            sig = {}
            for coin, qty in state.positions.items():
                if abs(qty) < EPS:
                    continue
                col = coin_to_col.get(coin)
                if col is None:
                    continue
                px = prices_row[col]
                if math.isnan(px):
                    continue
                signed_notional = qty * px
                sig[coin] = signed_notional / eq_w
            wallet_signals_now[w] = sig

        all_coins = set()
        for sig in wallet_signals_now.values():
            all_coins.update(sig.keys())

        for coin in all_coins:
            agg = 0.0
            n_voting = 0
            for w in selected_wallets:
                s = wallet_signals_now[w].get(coin, 0.0)
                if abs(s) > EPS:
                    n_voting += 1
                agg += weights[w] * s
            voting_frac = n_voting / max(1, len(selected_wallets))
            if params.consensus == "hard40":
                if voting_frac < consensus_threshold:
                    agg = 0.0
            elif soft_consensus:
                # Soft consensus: scale aggregate by fraction of wallets voting.
                # This differentiates soft from off: an isolated single-wallet
                # signal is downweighted by 1/N.
                agg *= voting_frac
            target_pct[coin] = agg

        # Apply per-coin cap.
        for coin in target_pct:
            target_pct[coin] = max(-params.per_coin_cap, min(params.per_coin_cap, target_pct[coin]))

        # Apply gross cap.
        total_gross = sum(abs(v) for v in target_pct.values())
        if total_gross > params.gross_cap and total_gross > 0:
            scale = params.gross_cap / total_gross
            target_pct = {c: v * scale for c, v in target_pct.items()}

        # Compute target notional + rebalance.
        # Liquidity-aware slippage (per Section 4.3 + GPT review point 8):
        # When minute_volumes is supplied we look up the coin's traded volume
        # in the SAME minute and either SKIP (order too big) or apply a
        # volume-impact slippage adder.
        liquidity_aware = (
            params.enable_liquidity_filter
            and volumes_arr is not None
        )
        vols_row = volumes_arr[row_idx] if liquidity_aware else None

        fees_minute = 0.0
        slippage_minute = 0.0
        skipped_notional_minute = 0.0
        for coin in target_pct:
            target_notional = target_pct[coin] * our_equity
            col = coin_to_col.get(coin)
            if col is None:
                continue
            px = prices_row[col]
            if math.isnan(px) or px <= EPS:
                continue
            current_qty = our_quantity.get(coin, 0.0)
            target_qty = target_notional / px
            delta_qty = target_qty - current_qty
            delta_notional = delta_qty * px
            if abs(delta_notional) <= params.min_delta_usd:
                continue
            if abs(delta_notional) <= params.min_delta_pct * abs(target_notional + EPS):
                continue
            # Cooldown check on re-entries.
            if abs(current_qty) < EPS and abs(target_qty) > EPS:
                if cooldown_until.get(coin, 0) > ts_ms:
                    continue
            # Liquidity check (only when minute-volume data is available AND
            # the volume in that minute is meaningfully > 0; zero-volume
            # minutes are treated as "unknown liquidity, fall back to fixed
            # slippage" rather than "illiquid, skip").
            eff_slip_bps = params.slippage_bps
            if liquidity_aware and vols_row is not None:
                vcol = vol_coin_to_col.get(coin)
                if vcol is not None:
                    vol_coin = vols_row[vcol]
                    if not math.isnan(vol_coin) and vol_coin > EPS:
                        size_frac = abs(delta_qty) / vol_coin
                        if size_frac > params.max_size_pct_of_volume:
                            skipped_notional_minute += abs(delta_notional)
                            continue
                        # 100*size_frac is "size as % of volume"; bps adder is
                        # impact_bps_per_pct * pct.
                        eff_slip_bps = params.slippage_bps + params.volume_impact_bps_per_pct * (100.0 * size_frac)
            # Execute fill.
            fee = abs(delta_notional) * params.fee_bps_per_side / 10000.0
            slip = abs(delta_notional) * eff_slip_bps / 10000.0
            fees_minute += fee
            slippage_minute += slip
            our_quantity[coin] = target_qty
            last_rebalance_ts[coin] = ts_ms
            # On full exit, set cooldown.
            if abs(target_qty) < EPS:
                cooldown_until[coin] = ts_ms + params.cooldown_seconds * 1000

        # Close positions for coins no longer in target. Liquidity-aware
        # PARTIAL exits (codex r29 P1 #5 fix): if the full position exceeds
        # the per-minute liquidity cap, exit ONLY up to the cap this minute
        # and drain the remainder over subsequent minutes. Position is NOT
        # left stuck; partial fills accumulate to a full exit as liquidity
        # allows.
        for coin in list(our_quantity.keys()):
            if coin not in target_pct and abs(our_quantity[coin]) > EPS:
                col = coin_to_col.get(coin)
                if col is None:
                    continue
                px = prices_row[col]
                if math.isnan(px) or px <= EPS:
                    continue
                # Default: full exit at fixed slippage.
                exit_qty = our_quantity[coin]
                eff_slip_bps_exit = params.slippage_bps
                if liquidity_aware and vols_row is not None:
                    vcol = vol_coin_to_col.get(coin)
                    if vcol is not None:
                        vol_coin = vols_row[vcol]
                        if not math.isnan(vol_coin) and vol_coin > EPS:
                            cap_qty = params.max_size_pct_of_volume * vol_coin
                            if abs(exit_qty) > cap_qty:
                                # Partial exit: trim by the cap (signed).
                                signed_cap = cap_qty if exit_qty > 0 else -cap_qty
                                unfilled = exit_qty - signed_cap
                                exit_qty = signed_cap
                                # Log the deferred notional (qty * px) as skipped
                                # for THIS minute; the remainder stays open and
                                # gets drained next minute.
                                skipped_notional_minute += abs(unfilled * px)
                                size_frac = params.max_size_pct_of_volume
                            else:
                                size_frac = abs(exit_qty) / vol_coin
                            eff_slip_bps_exit = params.slippage_bps + params.volume_impact_bps_per_pct * (100.0 * size_frac)
                exit_notional = exit_qty * px
                fee = abs(exit_notional) * params.fee_bps_per_side / 10000.0
                slip = abs(exit_notional) * eff_slip_bps_exit / 10000.0
                fees_minute += fee
                slippage_minute += slip
                # Reduce by the exit_qty actually transacted.
                our_quantity[coin] = our_quantity[coin] - exit_qty
                if abs(our_quantity[coin]) < EPS:
                    our_quantity[coin] = 0.0
                    cooldown_until[coin] = ts_ms + params.cooldown_seconds * 1000

        # Max-staleness rebalance trigger (every staleness_minutes).
        # (We approximate by always running the rebalance on poll cycles; staleness
        # rule fires implicitly when no fill has happened in N minutes.)

        # Apply MTM PnL to equity: for next-minute, recompute gross from updated qty.
        # Equity evolves as: prev_equity + position_pnl - fees - slippage.
        # We compute position_pnl from the next-minute step.
        our_equity -= (fees_minute + slippage_minute)

        # Update last_prices for next step's MTM walk. AFTER rebalance so that
        # newly-opened positions in our_quantity also get their price seeded
        # for next minute's MTM (codex P1 #9 — only HELD coins, not all 715
        # columns). Matches the OLD code's pre-rebalance "all-columns" update
        # for coins that end up held.
        for coin in our_quantity:
            col = coin_to_col.get(coin)
            if col is None:
                continue
            p = prices_row[col]
            if not math.isnan(p):
                last_prices[coin] = float(p)

        # Net signed notional for stress-regime accounting. Positive = net
        # long, negative = net short. A uniform adverse move on all coins
        # hurts net_long by adverse_pct and helps net_short by adverse_pct,
        # so the symmetric stress impact is -adverse_pct * net_long_notional.
        net_long_notional = 0.0
        gross_notional = 0.0
        n_active = 0
        for c, q in our_quantity.items():
            if abs(q) < EPS:
                continue
            col = coin_to_col.get(c)
            if col is None:
                continue
            px = prices_row[col]
            if math.isnan(px):
                continue
            net_long_notional += q * px
            gross_notional += abs(q * px)
            n_active += 1
        rows.append({
            "ts": ts_ms,
            "our_equity": our_equity,
            "gross_notional": gross_notional,
            "net_long_notional": net_long_notional,
            "n_active_coins": n_active,
            "fees_minute": fees_minute,
            "slippage_minute": slippage_minute,
            "skipped_notional_minute": skipped_notional_minute,
        })

    if not rows:
        return pd.DataFrame()
    out = pd.DataFrame(rows)
    # Compute MTM equity walk (positions held between rebalances accrue PnL).
    # Walk through minutes: for each minute, equity[t+1] = equity[t] + sum_coin qty[coin] * (price[t+1] - price[t])
    # Approximate by computing PnL per minute on holding period.
    # For v1, simpler: equity already reflects fee deductions. MTM growth is captured
    # by gross_notional changes implicitly. Sharpe is computed on equity diffs which
    # WILL incorporate fee drag + position market moves at end of each rebalance.
    return out


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def equity_curve_metrics(curve: pd.DataFrame, starting_capital: float,
                         stress_adverse_pct: float = 0.10) -> dict:
    """Compute Sharpe / max-DD / net-return / worst-day on the daily curve,
    plus a synthetic stress-regime overlay (Section 10 spec item):

      - net_long_notional per minute is tracked in `curve` (filled by the
        simulator). For each day, take the LARGEST abs net_long across all
        minutes in that day; this is the "worst exposure peak" the day
        carried.
      - Stress: apply a uniform `stress_adverse_pct` move on ALL coins on
        the day with the LARGEST `peak_net_long` exposure. The portfolio
        takes a hypothetical loss = adverse_pct * peak_net_long on that day.
      - Output `stress_max_single_day_loss_pct` = the synthetic loss as
        a percentage of starting capital. Layer it onto the actual curve to
        compute `stress_max_dd_pct`: max DD if the stress event hit the day
        with the worst already-actualized return.

    This is a post-hoc, no-resimulation stress test. It captures the spirit
    of "a uniform 10% adverse move across all coins on a single day" without
    re-running the bot under a perturbed price grid.
    """
    if curve.empty or "our_equity" not in curve.columns:
        return {"sharpe": 0.0, "max_dd_pct": 0.0, "net_return_pct": 0.0, "worst_day_pct": 0.0,
                "n_minutes": 0,
                "stress_max_single_day_loss_pct": 0.0,
                "stress_max_dd_pct": 0.0,
                "stress_peak_net_long_usd": 0.0,
                "stress_peak_net_long_day": None}
    eq = curve["our_equity"]
    # Resample to daily for Sharpe (matches spec).
    curve = curve.copy()
    curve["dt"] = pd.to_datetime(curve["ts"], unit="ms", utc=True)
    daily_eq = curve.set_index("dt")["our_equity"].resample("1D").last().dropna()
    daily_returns = daily_eq.pct_change().dropna()
    sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(365)) if not daily_returns.empty and daily_returns.std() > 0 else 0.0
    peak = daily_eq.cummax()
    dd = ((daily_eq - peak) / peak.replace(0, np.nan)).fillna(0)
    max_dd = float(-dd.min() * 100)
    net = float((daily_eq.iloc[-1] / starting_capital - 1.0) * 100)
    worst = float(daily_returns.min() * 100) if not daily_returns.empty else 0.0

    # Stress regime overlay.
    if "net_long_notional" in curve.columns and not curve.empty and starting_capital > EPS:
        curve["abs_net_long"] = curve["net_long_notional"].abs()
        daily_peak_net_long = curve.set_index("dt")["abs_net_long"].resample("1D").max().dropna()
        if not daily_peak_net_long.empty:
            worst_exposure_day = daily_peak_net_long.idxmax()
            peak_net_long = float(daily_peak_net_long.loc[worst_exposure_day])
            stress_loss_usd = stress_adverse_pct * peak_net_long
            stress_single_day_loss_pct = -100.0 * stress_loss_usd / starting_capital
            # Layer the synthetic loss onto the WORST already-bad day in the
            # actual daily returns, then re-walk equity day-by-day anchored on
            # the actual day-0 equity. We DO NOT anchor on starting_capital
            # because the day-0 equity may already differ from starting_capital
            # due to day-1 fees/slippage; anchoring on starting_capital would
            # create a discontinuity that distorts the drawdown calc.
            daily_returns_stressed = daily_returns.copy()
            if not daily_returns_stressed.empty:
                worst_day_idx = daily_returns_stressed.idxmin()
                daily_returns_stressed.loc[worst_day_idx] = daily_returns_stressed.loc[worst_day_idx] + stress_single_day_loss_pct / 100.0
                # Walk equity with stressed returns, day-0 anchored.
                stressed_factors = (1.0 + daily_returns_stressed).cumprod()
                eq_stressed_tail = daily_eq.iloc[0] * stressed_factors
                eq_stressed = pd.concat([daily_eq.iloc[:1], eq_stressed_tail])
                # Drop any duplicate index entries (in case day-0 leaks into tail).
                eq_stressed = eq_stressed[~eq_stressed.index.duplicated(keep="first")]
                peak_s = eq_stressed.cummax()
                dd_s = ((eq_stressed - peak_s) / peak_s.replace(0, np.nan)).fillna(0)
                stress_max_dd = float(-dd_s.min() * 100)
            else:
                stress_max_dd = max_dd
        else:
            peak_net_long = 0.0
            stress_single_day_loss_pct = 0.0
            stress_max_dd = max_dd
            worst_exposure_day = None
    else:
        peak_net_long = 0.0
        stress_single_day_loss_pct = 0.0
        stress_max_dd = max_dd
        worst_exposure_day = None

    return {
        "sharpe": sharpe, "max_dd_pct": max_dd, "net_return_pct": net,
        "worst_day_pct": worst, "n_minutes": len(curve),
        "stress_max_single_day_loss_pct": stress_single_day_loss_pct,
        "stress_max_dd_pct": stress_max_dd,
        "stress_peak_net_long_usd": peak_net_long,
        "stress_peak_net_long_day": str(worst_exposure_day) if worst_exposure_day is not None else None,
    }


# ---------------------------------------------------------------------------
# Fold pipeline
# ---------------------------------------------------------------------------

def run_fold(
    fold_idx: int,
    train_start: datetime, train_end: datetime,
    val_start: datetime, val_end: datetime,
    test_start: datetime, test_end: datetime,
    fills: pd.DataFrame,
    journeys: pd.DataFrame,
    equity: pd.DataFrame,
    minute_prices: pd.DataFrame,
    n_random: int,
    K_choices: list,
    minute_volumes: pd.DataFrame | None = None,
) -> dict:
    """One fold of the walk-forward.

    Train: rank wallets via composite score using ONLY fills before val_start
    (i.e., fills strictly within the train window's calendar days).
    Validation: parameter sweep on [val_start, test_start).
    Test: best-validation params evaluated ONCE on [test_start, test_end + 1d).

    Window semantics (codex r29 P0 #1 fix). Fold dates are MIDNIGHT UTC
    timestamps. Without explicit half-open semantics the original code had
    an off-by-one: train_fills filtered with `time <= train_end_ms` (=
    midnight of train_end_day) EXCLUDED fills DURING train_end_day, but
    eq_at_train_end took the END-OF-day equity of train_end_day. That
    mismatch let the post-anchor forward simulator double-count fills on
    train_end_day. The fix: use HALF-OPEN windows everywhere:
      train: [train_start, val_start)  -- fills strictly before val_start
      val:   [val_start, test_start)
      test:  [test_start, test_end + 1d)
    Anchor is val_start (= midnight of train_end_day + 1). Anchor equity is
    end-of-day equity of train_end_day, which corresponds exactly to the
    val_start anchor moment.

    Backtest invariants enforced by assert at the boundaries:
      I1) train_start < val_start = train_end + 1d
      I2) train_fills.time < val_start_ms (no test/val data in train)
      I3) wallet_scores recomputed per fold from train-only data
      I4) eq_at_anchor uses equity[date == train_end.date()] (end-of-train-window day)
    """
    # I1: window ordering invariant (half-open).
    assert train_start < val_start, f"train invariant: {train_start} !< {val_start}"
    assert val_start < val_end + timedelta(days=1), f"val invariant: {val_start} not before {val_end + timedelta(days=1)}"
    assert val_end < test_start, f"val/test invariant: val_end {val_end} >= test_start {test_start}"
    assert test_start <= test_end, f"test invariant: {test_start} > {test_end}"

    # Half-open boundary timestamps.
    train_start_ms = int(train_start.timestamp() * 1000)
    val_start_ms = int(val_start.timestamp() * 1000)

    # 1) Compute wallet metrics on train window only.
    from importlib import util
    spec = util.spec_from_file_location("v13_metrics_mod", ROOT / "scripts" / "v13_wallet_metrics.py")
    mod = util.module_from_spec(spec); spec.loader.exec_module(mod)
    market = mod.load_market_daily_returns(train_start, train_end)
    train_fills = fills[
        (fills["time"] >= train_start_ms)
        & (fills["time"] < val_start_ms)
    ]
    # I2: assert no future leak in train_fills (strict half-open top-bound).
    if not train_fills.empty:
        assert train_fills["time"].max() < val_start_ms, (
            f"fold {fold_idx} train_fills leak: max_time {train_fills['time'].max()} >= val_start_ms {val_start_ms}"
        )
    train_rows = []
    for w in sorted(set(equity["wallet"].unique())):
        eq_w = equity[equity["wallet"] == w]
        jr_w = journeys[journeys["wallet"] == w]
        fl_w = train_fills[train_fills["wallet"] == w]
        if eq_w.empty:
            continue
        try:
            train_rows.append(mod.compute_metrics_for_wallet(w, eq_w, jr_w, fl_w, market, train_start, train_end))
        except Exception as e:
            logger.exception(f"train metrics failed for {w[:10]}: {e}")
    train_metrics = pd.DataFrame(train_rows)
    if train_metrics.empty:
        return {"fold": fold_idx, "status": "no_train_metrics"}

    # Compute the additive z-score ranking per Section 5.5 (pool-level z-scores).
    if hasattr(mod, "compute_additive_zscore_ranking"):
        train_metrics = mod.compute_additive_zscore_ranking(train_metrics)

    eligible = train_metrics[train_metrics["eligible"]]
    if eligible.empty:
        return {"fold": fold_idx, "status": "no_eligible"}

    # I3: scores must be NaN for non-eligible (pool-level z-score logic invariant).
    non_elig = train_metrics[~train_metrics["eligible"]]
    if not non_elig.empty:
        assert non_elig["wallet_score"].isna().all(), (
            f"fold {fold_idx}: non-eligible wallets must have NaN wallet_score"
        )

    eligible_sorted = eligible.sort_values("wallet_score", ascending=False).reset_index(drop=True)
    wallet_scores = dict(zip(eligible_sorted["wallet"], eligible_sorted["wallet_score"]))
    all_eligible_wallets = eligible_sorted["wallet"].tolist()

    # Wallet base equities at the ANCHOR moment = val_start.
    #
    # Per Alberto rule 16 + decision A (2026-05-26 17:09 CEST): the SIZING
    # anchor is spot_usdc_today (a wallet-level scalar), NOT the daily
    # reconstructed perp series. Capital references = spot per rule 16.
    # spot_usdc_today is constant per wallet (attached by reconstruction at
    # bucket-loop time), so picking the value for any single date in the
    # equity df yields the same scalar; we keep the date-filter to be
    # explicit about WHEN we anchor (the value happens to be invariant in
    # the date dim for v3-A, but that may change if a future v4 reconstructs
    # historical spot too).
    eq_at_train_end = (
        equity[equity["date"] == train_end.date()]
        .dropna(subset=["spot_usdc_today"])
        .set_index("wallet")["spot_usdc_today"]
        .astype(float)
        .to_dict()
    )

    # Build fill streams for forward replay (all eligible wallets).
    streams = build_wallet_fill_streams(fills, all_eligible_wallets)

    # Perf v3 (codex P0 #1 + #3): build PreparedMarket + anchor_cache ONCE per
    # fold, then pass to every simulate_replication call below. This avoids
    # rebuilding numpy arrays for each of the 36 validation combos + 1 test +
    # 1000 random portfolios + 9 ablations (~1047 sims per fold).
    perf_pm = prepare_market(minute_prices, minute_volumes)
    perf_anchor_ts_ms = int(val_start.timestamp() * 1000)
    perf_anchor_cache = build_anchor_cache(
        all_eligible_wallets, streams, perf_anchor_ts_ms, perf_pm
    )
    logger.info(f"Fold {fold_idx}: prepared market + anchor cache for {len(perf_anchor_cache)} wallets")

    # 2) Parameter sweep on VALIDATION. v1 sweep per spec Section 6.2:
    #     K in {10, 25, 50}      -- 5 / 100 dropped (too concentrated / too dilute)
    #     per_coin_cap = 0.25    -- FIXED (no sweep)
    #     gross_cap in {1.0, 1.5} -- 2.0 dropped (too aggressive for v1)
    #     cooldown = 120s        -- FIXED (no sweep)
    #     poll in {1, 5, 10} minutes
    #     weighting = "equal"    -- FIXED (score-weighted is ABLATION DIAGNOSTIC only)
    #     consensus in {off, soft} -- hard40 ABLATION DIAGNOSTIC only
    # The wider sweep (K=5/100, score-weighted, 2.0x gross, 60s cooldown,
    # 15%/35% per-coin cap) is exercised by the 9 ablations as diagnostics
    # but never selected for production parameters.
    best_params, best_val_sharpe = None, -1e9
    K_grid = [k for k in K_choices if k <= len(eligible_sorted)]
    for K in K_grid:
        selected = eligible_sorted.head(K)["wallet"].tolist()
        for gross in [1.0, 1.5]:
            for poll in [1, 5, 10]:
                for consensus in ["off", "soft"]:    # hard40 ABLATION-ONLY
                    p = BotParams(
                        K=K, per_coin_cap=0.25, gross_cap=gross,
                        cooldown_seconds=120, poll_minutes=poll,
                        weighting="equal", consensus=consensus,
                    )
                    # simulate_replication() internally resets stream cursors per call.
                    curve = simulate_replication(
                        selected, streams, wallet_scores, eq_at_train_end,
                        minute_prices, val_start, val_end, p,
                        anchor_dt=val_start,    # half-open anchor: val_start = end-of-train moment
                        minute_volumes=minute_volumes,
                        anchor_cache=perf_anchor_cache,
                        prepared_market=perf_pm,
                    )
                    m = equity_curve_metrics(curve, p.starting_capital)
                    if m["sharpe"] > best_val_sharpe:
                        best_val_sharpe = m["sharpe"]
                        best_params = p

    if best_params is None:
        return {"fold": fold_idx, "status": "no_valid_params"}

    # 3) OOS TEST with best validation params.
    selected = eligible_sorted.head(best_params.K)["wallet"].tolist()
    # Reset streams.
    for w in selected:
        if w in streams:
            streams[w].next_idx = 0
    test_curve = simulate_replication(
        selected, streams, wallet_scores, eq_at_train_end,
        minute_prices, test_start, test_end, best_params,
        anchor_dt=val_start,    # half-open anchor: val_start = end-of-train moment
        minute_volumes=minute_volumes,
        anchor_cache=perf_anchor_cache,
        prepared_market=perf_pm,
    )
    test_metrics = equity_curve_metrics(test_curve, best_params.starting_capital)

    # Real per-fold fee + slippage accumulation for spec 6.3 row 4 (aggregate
    # fee drag). The simulator already emits per-minute fees_minute +
    # slippage_minute in test_curve; we just sum them and derive gross PnL.
    # gross_pnl = net_pnl + fees + slippage  (since simulator subtracts both
    # from our_equity at each minute, the equity curve's net_return_pct is
    # net-of-cost). fee_drag = (fees + slippage) / gross_pnl_usd. When gross
    # PnL is non-positive the gate is undefined; report as 1.0 (= 100%) so it
    # fails the < 30% test conservatively.
    if not test_curve.empty:
        test_fees_usd = float(test_curve["fees_minute"].sum()) if "fees_minute" in test_curve.columns else 0.0
        test_slippage_usd = float(test_curve["slippage_minute"].sum()) if "slippage_minute" in test_curve.columns else 0.0
    else:
        test_fees_usd = 0.0
        test_slippage_usd = 0.0
    test_net_pnl_usd = float(test_metrics["net_return_pct"]) / 100.0 * best_params.starting_capital
    test_gross_pnl_usd = test_net_pnl_usd + test_fees_usd + test_slippage_usd
    if test_gross_pnl_usd > EPS:
        test_fee_drag = (test_fees_usd + test_slippage_usd) / test_gross_pnl_usd
    else:
        test_fee_drag = 1.0  # gross non-positive -> conservative FAIL

    # 4) Random portfolio percentile on OOS test (BOTH Sharpe + net-PnL gates).
    rng = np.random.default_rng(42 + fold_idx)
    # Parallelize 1000 random portfolios across all available cores.
    # Each random trial is independent (different seed -> different wallet
    # selection -> independent simulation). Pure-Python single-thread was the
    # bottleneck per HB29 (Fold 0 ran 2h+ with no result; observed ~430
    # iter/sec on one core). Use fork-based pool so workers inherit
    # streams/prices/volumes via COW (avoiding ~3GB pickle per IPC call).
    n_workers = max(1, min(os.cpu_count() or 1, 12))
    rng_seed_base = 42 + fold_idx
    logger.info(f"Fold {fold_idx}: launching {n_random} random portfolios across {n_workers} workers...")
    _set_random_trial_globals(
        streams=streams,
        wallet_scores=wallet_scores,
        minute_prices=minute_prices,
        minute_volumes=minute_volumes,
        eq_at_train_end=eq_at_train_end,
        best_params=best_params,
        test_start=test_start,
        test_end=test_end,
        val_start=val_start,
        all_eligible_wallets=all_eligible_wallets,
        rng_seed_base=rng_seed_base,
        anchor_cache=perf_anchor_cache,
        prepared_market=perf_pm,
    )
    random_sharpes = [0.0] * n_random
    random_pnls = [0.0] * n_random
    fork_ctx = mp.get_context("fork")
    progress_step = max(50, n_random // 20)  # log every ~5%
    with ProcessPoolExecutor(max_workers=n_workers, mp_context=fork_ctx) as pool:
        futures = [pool.submit(_simulate_random_trial, t) for t in range(n_random)]
        completed = 0
        for future in as_completed(futures):
            trial, sh, pn = future.result()
            random_sharpes[trial] = sh
            random_pnls[trial] = pn
            completed += 1
            if completed % progress_step == 0 or completed == n_random:
                logger.info(f"Fold {fold_idx}: {completed}/{n_random} random portfolios done")
    random_sharpes_sorted = sorted(random_sharpes)
    random_pnls_sorted = sorted(random_pnls)
    sharpe_pct_rank = (np.searchsorted(random_sharpes_sorted, test_metrics["sharpe"]) / max(1, len(random_sharpes_sorted))) * 100
    pnl_pct_rank = (np.searchsorted(random_pnls_sorted, test_metrics["net_return_pct"]) / max(1, len(random_pnls_sorted))) * 100

    # 5) Robustness: K-aware top-N removal.
    # K-aware ablation set (GPT review point 7):
    #   K >= 25: remove_top_1, remove_top_5, remove_top_10
    #   K >= 10: remove_top_1, remove_top_3            (5 and 10 would gut the portfolio)
    #   K <  10: leave-one-out only (top 1 removal)
    # Skipped removal levels are emitted as None to keep the schema stable.
    if best_params.K >= 25:
        k_removes = [1, 5, 10]
    elif best_params.K >= 10:
        k_removes = [1, 3]
    else:
        k_removes = [1]
    robust = {f"remove_top{r}_sharpe": None for r in (1, 3, 5, 10)}
    for k_remove in k_removes:
        if best_params.K - k_remove <= 0:
            continue
        remaining = eligible_sorted.iloc[k_remove:k_remove + best_params.K]["wallet"].tolist()
        if len(remaining) < best_params.K:
            remaining = eligible_sorted.iloc[k_remove:]["wallet"].tolist()
        for w in remaining[:best_params.K]:
            if w in streams:
                streams[w].next_idx = 0
        rc = simulate_replication(remaining[:best_params.K], streams, wallet_scores, eq_at_train_end,
                                  minute_prices, test_start, test_end, best_params,
                                  anchor_dt=val_start,    # half-open anchor: val_start = end-of-train moment
                                  minute_volumes=minute_volumes,
                                  anchor_cache=perf_anchor_cache,
                                  prepared_market=perf_pm)
        rm = equity_curve_metrics(rc, best_params.starting_capital)
        robust[f"remove_top{k_remove}_sharpe"] = rm["sharpe"]

    # Latest-fold profitability check (criterion 8) -- this fold's test.
    latest_fold_profitable = test_metrics["sharpe"] > 0

    # Propagate the deployment_blocked_by_near_liq flag (from train_metrics).
    deployment_blocked_by_near_liq = bool(train_metrics.get("deployment_blocked_by_near_liq", pd.Series([True])).any())

    # Capture per-fold pooled-aggregate inputs for v13_report Section 6.3
    # AGGREGATE evaluation (spec 6.3 criteria 1-6 across all 8 windows
    # POOLED). The report needs raw daily returns and the full random-
    # portfolio sample distribution per fold; per-fold scalar summaries do
    # not aggregate correctly (mean-of-Sharpes != pooled Sharpe).
    test_daily_returns: list[float] = []
    if not test_curve.empty and "our_equity" in test_curve.columns:
        _tcc = test_curve.copy()
        _tcc["dt"] = pd.to_datetime(_tcc["ts"], unit="ms", utc=True)
        _td_eq = _tcc.set_index("dt")["our_equity"].resample("1D").last().dropna()
        test_daily_returns = _td_eq.pct_change().dropna().astype(float).tolist()

    # Build the ablation context so caller can run the 9 ablations later.
    # CRITICAL: minute_prices slice must include >= train_end so the simulator
    # can capture anchor_prices at the anchor moment (compute_wallet_equity_at
    # depends on this). Slicing to test_start..test_end would lose the anchor
    # bar and zero out anchor_unrealized_fixed.
    ctx = AblationContext(
        fold_idx=fold_idx,
        eligible_sorted=eligible_sorted,
        all_eligible_wallets=all_eligible_wallets,
        wallet_scores=wallet_scores,
        wallet_streams=streams,
        eq_at_train_end=eq_at_train_end,
        minute_prices=daily_close_or_prices(train_end, test_end, minute_prices),
        train_end=train_end,
        val_start=val_start,
        test_start=test_start,
        test_end=test_end,
        best_params=best_params,
        random_p95_sharpe=float(np.percentile(random_sharpes, 95)) if random_sharpes else 0.0,
        random_p95_pnl=float(np.percentile(random_pnls, 95)) if random_pnls else 0.0,
        robust=robust,
        fills=fills,
        minute_volumes=minute_volumes,
    )

    return {
        "fold": fold_idx,
        "status": "ok",
        "train_start": train_start.date(), "train_end": train_end.date(),
        "val_start": val_start.date(), "val_end": val_end.date(),
        "test_start": test_start.date(), "test_end": test_end.date(),
        "n_eligible": len(eligible),
        "best_K": best_params.K,
        "best_per_coin_cap": best_params.per_coin_cap,
        "best_gross": best_params.gross_cap,
        "best_cooldown_seconds": best_params.cooldown_seconds,
        "best_poll_minutes": best_params.poll_minutes,
        "best_weighting": best_params.weighting,
        "best_consensus": best_params.consensus,
        "val_sharpe": best_val_sharpe,
        "test_sharpe": test_metrics["sharpe"],
        "test_max_dd_pct": test_metrics["max_dd_pct"],
        "test_net_return_pct": test_metrics["net_return_pct"],
        "test_worst_day_pct": test_metrics["worst_day_pct"],
        "random_sharpe_pct_rank": float(sharpe_pct_rank),
        "random_pnl_pct_rank": float(pnl_pct_rank),
        "random_p50_sharpe": float(np.percentile(random_sharpes, 50)),
        "random_p95_sharpe": float(np.percentile(random_sharpes, 95)),
        "random_p50_pnl": float(np.percentile(random_pnls, 50)),
        "random_p95_pnl": float(np.percentile(random_pnls, 95)),
        "latest_fold_profitable": latest_fold_profitable,
        "deployment_blocked_by_near_liq": deployment_blocked_by_near_liq,
        # Real per-fold fee_drag = (fees + slippage) / gross_pnl, derived from
        # simulator's per-minute fees_minute + slippage_minute. Falls back to
        # 1.0 when gross PnL is non-positive (gate undefined; conservative FAIL).
        "fee_drag": test_fee_drag,
        "test_fees_usd": test_fees_usd,
        "test_slippage_usd": test_slippage_usd,
        "test_gross_pnl_usd": test_gross_pnl_usd,
        "test_net_pnl_usd": test_net_pnl_usd,
        "_ablation_context": ctx,
        "_test_metrics": test_metrics,
        "_test_daily_returns": test_daily_returns,
        "_random_sharpes_sample": list(map(float, random_sharpes)),
        "_random_pnls_sample": list(map(float, random_pnls)),
        **robust,
    }


def daily_close_or_prices(start, end, prices):
    """Return prices subset to [start, end + 1d) for ablation use. Half-open
    end so the LAST calendar day's minutes are included (matches the
    simulate_replication change for codex r29 P0 #1)."""
    if prices.empty:
        return prices
    idx = prices.index
    end_exclusive = _utc_ts(end) + pd.Timedelta(days=1)
    mask = (idx >= _utc_ts(start)) & (idx < end_exclusive)
    return prices.loc[mask]


# ---------------------------------------------------------------------------
# 9-experiment ablation suite (Section 6.7)
# ---------------------------------------------------------------------------

ABLATIONS = [
    "top_vs_random",
    "top_vs_beta",
    "K_sensitivity",
    "remove_top_1_5_10",
    "latency_cadence",
    "fees_multiplier",
    "slippage_multiplier",
    "consensus_off_soft_hard40",
    "weighting_equal_vs_score",
]

# Pass criteria per ablation:
# - top_vs_random       : strategy Sharpe > random P95 Sharpe
# - top_vs_beta         : strategy beats EVERY benchmark on net PnL %
# - K_sensitivity       : worst K's Sharpe >= 0.7 * best K's Sharpe (robustness band)
# - remove_top_1_5_10   : all 3 remove-top variants stay profitable (Sharpe > 0)
# - latency_cadence     : worst poll's Sharpe >= 0.7 * best poll's Sharpe
# - fees_multiplier     : 2x-fee Sharpe > 0 (survives doubled fees)
# - slippage_multiplier : punitive-slippage Sharpe > 0
# - consensus_off_soft  : best mode matches validation choice; off vs soft delta < 30%
# - weighting           : equal vs score delta < 30%


@dataclass
class AblationContext:
    fold_idx: int
    eligible_sorted: pd.DataFrame
    all_eligible_wallets: list
    wallet_scores: dict
    wallet_streams: dict
    eq_at_train_end: dict
    minute_prices: pd.DataFrame
    train_end: datetime
    val_start: datetime              # = train_end + 1d (anchor moment)
    test_start: datetime
    test_end: datetime
    best_params: BotParams
    random_p95_sharpe: float
    random_p95_pnl: float
    robust: dict
    fills: pd.DataFrame
    minute_volumes: pd.DataFrame | None = None


def _ablation_row(fold_idx, exp, variant, sharpe, pnl, status, note=""):
    return {
        "fold": fold_idx, "experiment": exp, "variant": variant,
        "test_sharpe": float(sharpe), "test_net_pnl_pct": float(pnl),
        "status": status, "note": note,
    }


def _simulate(ctx: AblationContext, selected: list, params: BotParams) -> dict:
    for w in selected:
        if w in ctx.wallet_streams:
            ctx.wallet_streams[w].next_idx = 0
    curve = simulate_replication(
        selected, ctx.wallet_streams, ctx.wallet_scores, ctx.eq_at_train_end,
        ctx.minute_prices, ctx.test_start, ctx.test_end, params,
        anchor_dt=ctx.val_start,
        minute_volumes=getattr(ctx, "minute_volumes", None),
    )
    return equity_curve_metrics(curve, params.starting_capital)


def _benchmark_buy_hold_curve(coin: str, minute_prices: pd.DataFrame,
                              test_start: datetime, test_end: datetime,
                              starting_capital: float) -> dict:
    """Compute buy-and-hold metrics for one coin over the test window.

    Timestamps derived from prices.index AFTER dropna, ensuring equity values
    line up with their actual bar timestamps (codex r23 #2 fix).
    """
    # Half-open end (codex r30 P1 #2): include last test-day minutes.
    test_end_exclusive = _utc_ts(test_end) + pd.Timedelta(days=1)
    idx_full = minute_prices.index
    idx_in = idx_full[(idx_full >= _utc_ts(test_start)) & (idx_full < test_end_exclusive)]
    if len(idx_in) < 2 or coin not in minute_prices.columns:
        return {"sharpe": 0.0, "net_return_pct": 0.0}
    prices = minute_prices.loc[idx_in, coin].dropna()
    if len(prices) < 2:
        return {"sharpe": 0.0, "net_return_pct": 0.0}
    equity = starting_capital * (prices / prices.iloc[0])
    ts_ms = (prices.index.astype("int64") // 10**6).values
    df = pd.DataFrame({"ts": ts_ms, "our_equity": equity.values})
    return equity_curve_metrics(df, starting_capital)


def _equal_weight_basket_curve(coins: list, minute_prices: pd.DataFrame,
                               test_start: datetime, test_end: datetime,
                               starting_capital: float) -> dict:
    # Half-open end (codex r30 P1 #2): include last test-day minutes.
    test_end_exclusive = _utc_ts(test_end) + pd.Timedelta(days=1)
    idx_full = minute_prices.index
    idx_in = idx_full[(idx_full >= _utc_ts(test_start)) & (idx_full < test_end_exclusive)]
    if len(idx_in) < 2:
        return {"sharpe": 0.0, "net_return_pct": 0.0}
    valid_coins = [c for c in coins if c in minute_prices.columns]
    if not valid_coins:
        return {"sharpe": 0.0, "net_return_pct": 0.0}
    sub = minute_prices.loc[idx_in, valid_coins].dropna(axis=1, how="any")
    if sub.empty:
        return {"sharpe": 0.0, "net_return_pct": 0.0}
    rel = sub / sub.iloc[0]
    basket = rel.mean(axis=1)
    equity = starting_capital * basket
    ts_ms = (basket.index.astype("int64") // 10**6).values
    df = pd.DataFrame({"ts": ts_ms, "our_equity": equity.values})
    return equity_curve_metrics(df, starting_capital)


def run_ablations_for_fold(ctx: AblationContext, test_metrics: dict) -> list[dict]:
    """Execute all 9 ablations for one fold. Returns list of dict rows."""
    rows = []
    K = ctx.best_params.K

    # ---- 1) top_vs_random ----
    # Strategy must beat P95 of random portfolios on BOTH Sharpe AND net PnL.
    strat_sh = float(test_metrics["sharpe"])
    strat_pnl_local = float(test_metrics["net_return_pct"])
    p95_sh = float(ctx.random_p95_sharpe)
    p95_pnl = float(ctx.random_p95_pnl)
    sharpe_pass = strat_sh > p95_sh
    pnl_pass = strat_pnl_local > p95_pnl
    rows.append(_ablation_row(ctx.fold_idx, "top_vs_random", "sharpe_vs_p95",
                              strat_sh, strat_pnl_local,
                              "pass" if sharpe_pass else "fail",
                              f"strat_sh={strat_sh:.2f} vs p95_sh={p95_sh:.2f}"))
    rows.append(_ablation_row(ctx.fold_idx, "top_vs_random", "pnl_vs_p95",
                              strat_sh, strat_pnl_local,
                              "pass" if pnl_pass else "fail",
                              f"strat_pnl={strat_pnl_local:.2f}% vs p95_pnl={p95_pnl:.2f}%"))
    rows.append(_ablation_row(ctx.fold_idx, "top_vs_random", "both_gates",
                              strat_sh, strat_pnl_local,
                              "pass" if (sharpe_pass and pnl_pass) else "fail",
                              "both Sharpe + PnL must beat P95"))

    # ---- 2) top_vs_beta (vs 4 benchmarks present in 1h cache) ----
    benchmarks = {
        "BTC": _benchmark_buy_hold_curve("BTC", ctx.minute_prices, ctx.test_start, ctx.test_end, ctx.best_params.starting_capital),
        "ETH": _benchmark_buy_hold_curve("ETH", ctx.minute_prices, ctx.test_start, ctx.test_end, ctx.best_params.starting_capital),
        "HYPE": _benchmark_buy_hold_curve("HYPE", ctx.minute_prices, ctx.test_start, ctx.test_end, ctx.best_params.starting_capital),
    }
    perp_index = _equal_weight_basket_curve(list(ctx.minute_prices.columns),
                                            ctx.minute_prices, ctx.test_start, ctx.test_end,
                                            ctx.best_params.starting_capital)
    benchmarks["HL_INDEX"] = perp_index
    alt_basket = _equal_weight_basket_curve(
        [c for c in ctx.minute_prices.columns if c not in ("BTC", "ETH")],
        ctx.minute_prices, ctx.test_start, ctx.test_end, ctx.best_params.starting_capital,
    )
    benchmarks["ALT_BASKET"] = alt_basket

    strat_pnl = float(test_metrics["net_return_pct"])
    beats_all = True
    for name, m in benchmarks.items():
        beat = strat_pnl > float(m["net_return_pct"])
        rows.append(_ablation_row(ctx.fold_idx, "top_vs_beta", f"vs_{name}",
                                  strat_sh, strat_pnl,
                                  "pass" if beat else "fail",
                                  f"strat_pnl={strat_pnl:.2f}% vs bench_pnl={m['net_return_pct']:.2f}%"))
        if not beat:
            beats_all = False
    rows.append(_ablation_row(ctx.fold_idx, "top_vs_beta", "all_benchmarks",
                              strat_sh, strat_pnl,
                              "pass" if beats_all else "fail",
                              "strategy beat all benchmarks" if beats_all else "strategy did NOT beat all"))

    # ---- 3) K_sensitivity ----
    sharpes_by_K = {}
    for K_var in [5, 10, 25, 50, 100]:
        if K_var > len(ctx.all_eligible_wallets):
            continue
        sel = ctx.eligible_sorted.head(K_var)["wallet"].tolist()
        p = BotParams(
            K=K_var, per_coin_cap=ctx.best_params.per_coin_cap, gross_cap=ctx.best_params.gross_cap,
            cooldown_seconds=ctx.best_params.cooldown_seconds, poll_minutes=ctx.best_params.poll_minutes,
            weighting=ctx.best_params.weighting, consensus=ctx.best_params.consensus,
        )
        m = _simulate(ctx, sel, p)
        sharpes_by_K[K_var] = m["sharpe"]
        rows.append(_ablation_row(ctx.fold_idx, "K_sensitivity", f"K={K_var}",
                                  m["sharpe"], m["net_return_pct"], "pass",
                                  ""))
    if sharpes_by_K:
        worst = min(sharpes_by_K.values())
        best = max(sharpes_by_K.values())
        # Robustness criterion (spec): worst >= 0.7 * best. No additional "best > 0" gate.
        # Handle zero/negative best safely: if best <= 0, treat as not-robust FAIL.
        if best <= 0:
            criterion_met = False
        else:
            criterion_met = worst >= 0.7 * best
        rows.append(_ablation_row(ctx.fold_idx, "K_sensitivity", "best_vs_worst",
                                  best, worst,
                                  "pass" if criterion_met else "fail",
                                  f"best_K_sh={best:.2f}, worst_K_sh={worst:.2f}"))

    # ---- 4) remove_top_1_5_10 (already computed in run_fold) ----
    all_pos = True
    for k in [1, 5, 10]:
        col = f"remove_top{k}_sharpe"
        val = ctx.robust.get(col)
        if val is None:
            rows.append(_ablation_row(ctx.fold_idx, "remove_top_1_5_10", f"k={k}",
                                      0.0, 0.0, "fail", "missing data"))
            all_pos = False
            continue
        rows.append(_ablation_row(ctx.fold_idx, "remove_top_1_5_10", f"k={k}",
                                  float(val), 0.0,
                                  "pass" if float(val) > 0 else "fail",
                                  f"sharpe_after_remove={float(val):.2f}"))
        if float(val) <= 0:
            all_pos = False
    rows.append(_ablation_row(ctx.fold_idx, "remove_top_1_5_10", "all_3",
                              0.0, 0.0,
                              "pass" if all_pos else "fail",
                              "all 3 remove variants stay profitable" if all_pos else "concentration risk"))

    # ---- 5) latency_cadence ----
    sharpes_by_poll = {}
    selected = ctx.eligible_sorted.head(K)["wallet"].tolist()
    for poll in [1, 5, 10]:
        p = BotParams(
            K=K, per_coin_cap=ctx.best_params.per_coin_cap, gross_cap=ctx.best_params.gross_cap,
            cooldown_seconds=ctx.best_params.cooldown_seconds, poll_minutes=poll,
            weighting=ctx.best_params.weighting, consensus=ctx.best_params.consensus,
        )
        m = _simulate(ctx, selected, p)
        sharpes_by_poll[poll] = m["sharpe"]
        rows.append(_ablation_row(ctx.fold_idx, "latency_cadence", f"poll={poll}m",
                                  m["sharpe"], m["net_return_pct"], "pass", ""))
    if sharpes_by_poll:
        best_poll_sh = max(sharpes_by_poll.values())
        worst_poll_sh = min(sharpes_by_poll.values())
        if best_poll_sh <= 0:
            criterion_met = False
        else:
            criterion_met = worst_poll_sh >= 0.7 * best_poll_sh
        rows.append(_ablation_row(ctx.fold_idx, "latency_cadence", "best_vs_worst",
                                  best_poll_sh, worst_poll_sh,
                                  "pass" if criterion_met else "fail",
                                  f"best={best_poll_sh:.2f} worst={worst_poll_sh:.2f}"))

    # ---- 6) fees_multiplier ----
    survives_2x = False
    sh_2x = 0.0
    for mult in [1.0, 1.5, 2.0]:
        p = BotParams(
            K=K, per_coin_cap=ctx.best_params.per_coin_cap, gross_cap=ctx.best_params.gross_cap,
            cooldown_seconds=ctx.best_params.cooldown_seconds, poll_minutes=ctx.best_params.poll_minutes,
            weighting=ctx.best_params.weighting, consensus=ctx.best_params.consensus,
            fee_bps_per_side=HL_FEE_BPS_PER_SIDE * mult,
        )
        m = _simulate(ctx, selected, p)
        if mult == 2.0:
            survives_2x = m["sharpe"] > 0
            sh_2x = m["sharpe"]
        rows.append(_ablation_row(ctx.fold_idx, "fees_multiplier", f"x{mult}",
                                  m["sharpe"], m["net_return_pct"],
                                  "pass" if (mult < 2.0 or m["sharpe"] > 0) else "fail",
                                  ""))
    rows.append(_ablation_row(ctx.fold_idx, "fees_multiplier", "x2_summary",
                              sh_2x, 0.0,
                              "pass" if survives_2x else "fail",
                              f"sh_at_2x_fees={sh_2x:.2f}"))

    # ---- 7) slippage_multiplier ----
    survives_punitive = False
    sh_punitive = 0.0
    for slip in [0.0, 5.0, 15.0]:
        p = BotParams(
            K=K, per_coin_cap=ctx.best_params.per_coin_cap, gross_cap=ctx.best_params.gross_cap,
            cooldown_seconds=ctx.best_params.cooldown_seconds, poll_minutes=ctx.best_params.poll_minutes,
            weighting=ctx.best_params.weighting, consensus=ctx.best_params.consensus,
            slippage_bps=slip,
        )
        m = _simulate(ctx, selected, p)
        if slip == 15.0:
            survives_punitive = m["sharpe"] > 0
            sh_punitive = m["sharpe"]
        rows.append(_ablation_row(ctx.fold_idx, "slippage_multiplier", f"slip={slip}bps",
                                  m["sharpe"], m["net_return_pct"],
                                  "pass" if (slip < 15.0 or m["sharpe"] > 0) else "fail",
                                  ""))
    rows.append(_ablation_row(ctx.fold_idx, "slippage_multiplier", "punitive_summary",
                              sh_punitive, 0.0,
                              "pass" if survives_punitive else "fail",
                              f"sh_at_15bps_slippage={sh_punitive:.2f}"))

    # ---- 8) consensus_off_soft_hard40 ----
    sh_by_cons = {}
    for cons in ["off", "soft", "hard40"]:
        p = BotParams(
            K=K, per_coin_cap=ctx.best_params.per_coin_cap, gross_cap=ctx.best_params.gross_cap,
            cooldown_seconds=ctx.best_params.cooldown_seconds, poll_minutes=ctx.best_params.poll_minutes,
            weighting=ctx.best_params.weighting, consensus=cons,
        )
        m = _simulate(ctx, selected, p)
        sh_by_cons[cons] = m["sharpe"]
        rows.append(_ablation_row(ctx.fold_idx, "consensus_off_soft_hard40", cons,
                                  m["sharpe"], m["net_return_pct"], "pass", ""))
    if sh_by_cons:
        best_cons = max(sh_by_cons, key=sh_by_cons.get)
        # Two-part criterion: best mode matches val choice AND modes are
        # within 30% of each other (i.e., robust to consensus choice).
        best_sh = sh_by_cons[best_cons]
        worst_sh = min(sh_by_cons.values())
        within_30pct = (best_sh > 0 and worst_sh >= 0.7 * best_sh)
        rows.append(_ablation_row(ctx.fold_idx, "consensus_off_soft_hard40", "best_matches_val",
                                  best_sh, 0.0,
                                  "pass" if best_cons == ctx.best_params.consensus else "fail",
                                  f"best_mode={best_cons}, val_chose={ctx.best_params.consensus}"))
        rows.append(_ablation_row(ctx.fold_idx, "consensus_off_soft_hard40", "delta_under_30pct",
                                  best_sh, worst_sh,
                                  "pass" if within_30pct else "fail",
                                  f"best={best_sh:.2f}, worst={worst_sh:.2f}"))

    # ---- 9) weighting_equal_vs_score ----
    sh_by_w = {}
    for w in ["equal", "score"]:
        p = BotParams(
            K=K, per_coin_cap=ctx.best_params.per_coin_cap, gross_cap=ctx.best_params.gross_cap,
            cooldown_seconds=ctx.best_params.cooldown_seconds, poll_minutes=ctx.best_params.poll_minutes,
            weighting=w, consensus=ctx.best_params.consensus,
        )
        m = _simulate(ctx, selected, p)
        sh_by_w[w] = m["sharpe"]
        rows.append(_ablation_row(ctx.fold_idx, "weighting_equal_vs_score", w,
                                  m["sharpe"], m["net_return_pct"], "pass", ""))
    if sh_by_w:
        best_w = max(sh_by_w, key=sh_by_w.get)
        best_sh = sh_by_w[best_w]
        worst_sh = min(sh_by_w.values())
        within_30pct = (best_sh > 0 and worst_sh >= 0.7 * best_sh)
        rows.append(_ablation_row(ctx.fold_idx, "weighting_equal_vs_score", "best_matches_val",
                                  best_sh, 0.0,
                                  "pass" if best_w == ctx.best_params.weighting else "fail",
                                  f"best={best_w}, val={ctx.best_params.weighting}"))
        rows.append(_ablation_row(ctx.fold_idx, "weighting_equal_vs_score", "delta_under_30pct",
                                  best_sh, worst_sh,
                                  "pass" if within_30pct else "fail",
                                  f"best={best_sh:.2f}, worst={worst_sh:.2f}"))

    return rows


_ABLATION_SCHEMA = ["fold", "experiment", "variant", "test_sharpe",
                    "test_net_pnl_pct", "status", "note"]


def _empty_ablation_df() -> pd.DataFrame:
    """Return an empty DataFrame with the canonical ablation schema."""
    return pd.DataFrame({col: pd.Series(dtype=object) for col in _ABLATION_SCHEMA})


def run_ablations(
    fold_results: pd.DataFrame,
    fills: pd.DataFrame, equity: pd.DataFrame, minute_prices: pd.DataFrame,
    fold_contexts: dict | None = None,
    fold_test_metrics: dict | None = None,
) -> pd.DataFrame:
    """Run 9 ablations per successful fold. Emits "not_implemented" rows for
    failed folds so script 5 can detect missing data.

    Always returns a DataFrame with the canonical _ABLATION_SCHEMA, even when
    there are zero rows -- so downstream consumers can dedupe / iterate
    without column-presence checks.
    """
    fold_contexts = fold_contexts or {}
    fold_test_metrics = fold_test_metrics or {}
    out_rows: list[dict] = []
    if fold_results.empty:
        return _empty_ablation_df()
    for _, fold_row in fold_results.iterrows():
        fi = int(fold_row["fold"])
        if fold_row.get("status") != "ok":
            for exp in ABLATIONS:
                out_rows.append({
                    "fold": fi, "experiment": exp, "variant": "n/a",
                    "test_sharpe": 0.0, "test_net_pnl_pct": 0.0,
                    "status": "not_implemented",
                    "note": f"fold_status={fold_row.get('status')}",
                })
            continue
        if fi not in fold_contexts:
            for exp in ABLATIONS:
                out_rows.append({
                    "fold": fi, "experiment": exp, "variant": "n/a",
                    "test_sharpe": 0.0, "test_net_pnl_pct": 0.0,
                    "status": "not_implemented",
                    "note": "fold context not captured",
                })
            continue
        ctx = fold_contexts[fi]
        tm = fold_test_metrics.get(fi, {"sharpe": 0.0, "net_return_pct": 0.0})
        rows = run_ablations_for_fold(ctx, tm)
        out_rows.extend(rows)
    if not out_rows:
        return _empty_ablation_df()
    df = pd.DataFrame(out_rows)
    # Ensure all canonical columns present.
    for col in _ABLATION_SCHEMA:
        if col not in df.columns:
            df[col] = None
    return df[_ABLATION_SCHEMA]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--equity-series", required=True)
    ap.add_argument("--journeys", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--train-days", type=int, default=30)
    ap.add_argument("--val-days", type=int, default=15)
    ap.add_argument("--test-days", type=int, default=15)
    ap.add_argument("--step-days", type=int, default=15)
    ap.add_argument("--K-choices", default="10,25,50",
                    help="Production sweep values per spec Section 6.2 v1. Pass '5,10,25,50,100' explicitly to widen for ablation.")
    ap.add_argument("--random-portfolios", type=int, default=1000)
    ap.add_argument("--fills-dir", default=None)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--ablation-output", default=str(ABLATION_OUTPUT))
    ap.add_argument("--daily-returns-output", default=None,
                    help="Path to write per-(fold, day) daily returns. Required "
                         "for spec 6.3 aggregate Sharpe (pooled across folds). "
                         "Defaults to <output dir>/walk_forward_daily_returns.parquet.")
    ap.add_argument("--random-samples-output", default=None,
                    help="Path to write per-(fold, trial) random portfolio "
                         "samples. Required for spec 6.3 aggregate random "
                         "p95 (pooled across folds). Defaults to <output dir>/"
                         "walk_forward_random_samples.parquet.")
    ap.add_argument("--price-interval", choices=["1m", "1h"], default="1m",
                    help="Price granularity. 1m is the production default "
                         "(S3-reconstructed; spec Section 5.7). 1h is a "
                         "fallback that flags results as APPROXIMATED.")
    ap.add_argument("--checkpoint-dir", default=None,
                    help="Directory to write per-fold checkpoint parquets. "
                         "Each fold's result + daily returns + random samples "
                         "are persisted immediately after completion. "
                         "Defaults to <output dir>/walk_forward_checkpoints/.")
    ap.add_argument("--resume", action="store_true",
                    help="Skip folds whose checkpoint files already exist. "
                         "Lets you resume a crashed multi-hour run without "
                         "losing completed fold work.")
    args = ap.parse_args()
    import sys

    if args.fills_dir:
        import v13_equity_reconstruct as _veq
        _veq.FILLS_DIR = Path(args.fills_dir)

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    K_choices = [int(x) for x in args.K_choices.split(",")]

    logger.info(f"Backtest window: {start.date()} to {end.date()}")
    logger.info(f"Folds: train={args.train_days}d val={args.val_days}d test={args.test_days}d step={args.step_days}d")

    equity = pd.read_parquet(args.equity_series)
    equity["wallet"] = equity["wallet"].str.lower()
    equity["date"] = pd.to_datetime(equity["date"]).dt.date

    journeys = pd.read_parquet(args.journeys)
    journeys["wallet"] = journeys["wallet"].str.lower()

    logger.info("Loading full-window fills...")
    fills = load_fills_for_dates(start, end, set(equity["wallet"].unique()))
    if not fills.empty:
        fills = validate_and_normalize_fills(fills)
    logger.info(f"Loaded {len(fills):,} fills")

    coins = sorted(fills["coin"].dropna().unique().tolist()) if not fills.empty else []
    logger.info(f"Loading {args.price_interval} candles for {len(coins)} coins...")
    minute_prices = load_price_grid(coins, start, end, args.price_interval)
    if minute_prices.empty:
        logger.error(f"No {args.price_interval} candle data for window. ABORTING (cannot simulate without prices).")
        sys.exit(1)
    logger.info(f"Loaded {len(minute_prices):,} price bars at {args.price_interval} granularity")
    if args.price_interval == "1h":
        logger.warning("Using 1h candles -- cadence/cooldown/staleness results are APPROXIMATED, not validated.")

    # Volume grid for liquidity-aware slippage (only meaningful at 1m).
    minute_volumes = None
    if args.price_interval == "1m":
        logger.info(f"Loading 1m volume grid for {len(coins)} coins (liquidity-aware slippage)...")
        minute_volumes = load_volume_grid(coins, start, end, "1m")
        if minute_volumes is None or minute_volumes.empty:
            logger.warning("Volume grid empty; liquidity-aware slippage will collapse to fixed bps.")
        else:
            logger.info(f"Loaded volume grid: {minute_volumes.shape[0]:,} minutes x {minute_volumes.shape[1]} coins")

    # Enumerate folds.
    folds = []
    t = start
    fold_idx = 0
    while True:
        train_start = t
        train_end = t + timedelta(days=args.train_days - 1)
        val_start = train_end + timedelta(days=1)
        val_end = val_start + timedelta(days=args.val_days - 1)
        test_start = val_end + timedelta(days=1)
        test_end = test_start + timedelta(days=args.test_days - 1)
        if test_end > end:
            break
        folds.append((fold_idx, train_start, train_end, val_start, val_end, test_start, test_end))
        fold_idx += 1
        t += timedelta(days=args.step_days)

    logger.info(f"Folds enumerated: {len(folds)}")

    # Per-fold checkpointing: each fold writes its result + daily returns +
    # random samples to disk IMMEDIATELY after completion, so a crash mid-run
    # only loses the in-flight fold, not the completed work. --resume skips
    # folds whose checkpoint files already exist (idempotent re-launches).
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ckpt_dir = Path(args.checkpoint_dir) if args.checkpoint_dir else (out_path.parent / "walk_forward_checkpoints")
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    def _fold_ckpt_paths(fi: int) -> tuple[Path, Path, Path]:
        return (
            ckpt_dir / f"fold_{fi:02d}_result.parquet",
            ckpt_dir / f"fold_{fi:02d}_daily_returns.parquet",
            ckpt_dir / f"fold_{fi:02d}_random_samples.parquet",
        )

    results = []
    fold_contexts: dict = {}
    fold_test_metrics: dict = {}
    daily_returns_by_fold: dict = {}
    random_samples_by_fold: dict = {}
    for (fi, ts, te, vs, ve, tts, tte) in folds:
        res_p, dr_p, rs_p = _fold_ckpt_paths(fi)
        if args.resume and res_p.exists() and dr_p.exists() and rs_p.exists():
            logger.info(f"Fold {fi}: --resume; checkpoint exists at {res_p.name}, skipping compute")
            # Reload from checkpoint into in-memory collectors so end-of-run
            # concat picks them up; ablation context is NOT recoverable so
            # ablations will skip this fold (will surface in report).
            r_df = pd.read_parquet(res_p)
            if not r_df.empty:
                results.append(r_df.iloc[0].to_dict())
            dr_df_load = pd.read_parquet(dr_p)
            daily_returns_by_fold[fi] = dr_df_load["daily_return"].astype(float).tolist() if not dr_df_load.empty else []
            rs_df_load = pd.read_parquet(rs_p)
            if not rs_df_load.empty:
                random_samples_by_fold[fi] = (
                    rs_df_load["random_sharpe"].astype(float).tolist(),
                    rs_df_load["random_pnl_pct"].astype(float).tolist(),
                )
            else:
                random_samples_by_fold[fi] = ([], [])
            continue

        logger.info(f"Fold {fi}: train {ts.date()}..{te.date()} val {vs.date()}..{ve.date()} test {tts.date()}..{tte.date()}")
        r = run_fold(fi, ts, te, vs, ve, tts, tte,
                     fills, journeys, equity, minute_prices,
                     n_random=args.random_portfolios, K_choices=K_choices,
                     minute_volumes=minute_volumes)
        # Capture context + test_metrics for ablations BEFORE stripping from
        # the dict that goes to parquet. Also strip list-valued aggregate
        # payloads so they do not poison the DataFrame schema.
        daily_returns_by_fold[fi] = r.pop("_test_daily_returns", [])
        random_samples_by_fold[fi] = (
            r.pop("_random_sharpes_sample", []),
            r.pop("_random_pnls_sample", []),
        )
        if r.get("status") == "ok":
            fold_contexts[fi] = r.pop("_ablation_context")
            fold_test_metrics[fi] = r.pop("_test_metrics")
        else:
            r.pop("_ablation_context", None)
            r.pop("_test_metrics", None)
        results.append(r)
        logger.info(f"  -> status={r.get('status')} test_sharpe={r.get('test_sharpe', 'NA')} rank_sharpe={r.get('random_sharpe_pct_rank', 'NA')} rank_pnl={r.get('random_pnl_pct_rank', 'NA')}")

        # Immediately persist this fold's checkpoint trio so a crash doesn't
        # lose this fold's work. Use .tmp + os.replace for atomic write.
        try:
            r_df_one = pd.DataFrame([r])
            for col in r_df_one.columns:
                # Drop any non-scalar columns (e.g. nested objects) — checkpoint
                # is for scalar fold-level fields only.
                pass
            dr_rows_one = [{"fold": fi, "day_idx": i, "daily_return": v}
                           for i, v in enumerate(daily_returns_by_fold[fi])]
            shs, pls = random_samples_by_fold[fi]
            rs_rows_one = [{"fold": fi, "trial": i, "random_sharpe": s, "random_pnl_pct": p}
                           for i, (s, p) in enumerate(zip(shs, pls))]
            r_df_one.to_parquet(str(res_p) + ".tmp", index=False, compression="snappy")
            pd.DataFrame(dr_rows_one, columns=["fold", "day_idx", "daily_return"]).to_parquet(str(dr_p) + ".tmp", index=False, compression="snappy")
            pd.DataFrame(rs_rows_one, columns=["fold", "trial", "random_sharpe", "random_pnl_pct"]).to_parquet(str(rs_p) + ".tmp", index=False, compression="snappy")
            os.replace(str(res_p) + ".tmp", res_p)
            os.replace(str(dr_p) + ".tmp", dr_p)
            os.replace(str(rs_p) + ".tmp", rs_p)
            logger.info(f"Fold {fi} checkpoint written: {res_p.name}")
        except Exception as e:
            logger.error(f"Fold {fi} checkpoint write FAILED: {e}; in-memory state retained, continuing")

    df = pd.DataFrame(results)
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(df)} fold results to {out_path}")

    # Spec 6.3 aggregate-evaluation supporting outputs: per-fold daily returns
    # and per-fold random-portfolio samples. Report pools these across all
    # folds to compute aggregate Sharpe + aggregate p95 (not approximable
    # from per-fold scalar summaries; mean-of-Sharpes != pooled Sharpe).
    dr_default = out_path.parent / "walk_forward_daily_returns.parquet"
    rs_default = out_path.parent / "walk_forward_random_samples.parquet"
    dr_path = Path(args.daily_returns_output) if args.daily_returns_output else dr_default
    rs_path = Path(args.random_samples_output) if args.random_samples_output else rs_default

    dr_rows = [
        {"fold": fi, "day_idx": i, "daily_return": v}
        for fi, drs in daily_returns_by_fold.items()
        for i, v in enumerate(drs)
    ]
    rs_rows = [
        {"fold": fi, "trial": i, "random_sharpe": s, "random_pnl_pct": p}
        for fi, (shs, pls) in random_samples_by_fold.items()
        for i, (s, p) in enumerate(zip(shs, pls))
    ]
    dr_path.parent.mkdir(parents=True, exist_ok=True)
    rs_path.parent.mkdir(parents=True, exist_ok=True)
    # Build with explicit schema so empty dataframes still have typed columns
    # (otherwise downstream `.astype` blows up on a 0-column frame).
    dr_df = pd.DataFrame(dr_rows, columns=["fold", "day_idx", "daily_return"])
    rs_df = pd.DataFrame(rs_rows, columns=["fold", "trial", "random_sharpe", "random_pnl_pct"])
    if not dr_df.empty:
        dr_df = dr_df.astype({"fold": "int64", "day_idx": "int64", "daily_return": "float64"})
    if not rs_df.empty:
        rs_df = rs_df.astype({"fold": "int64", "trial": "int64", "random_sharpe": "float64", "random_pnl_pct": "float64"})
    dr_df.to_parquet(dr_path, index=False, compression="snappy")
    rs_df.to_parquet(rs_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(dr_rows)} daily-return rows to {dr_path}")
    logger.info(f"Wrote {len(rs_rows)} random-sample rows to {rs_path}")

    # 6.7 ablation suite (9 experiments, real implementations).
    logger.info(f"Running 9 ablations across {len(fold_contexts)} successful folds...")
    abl = run_ablations(df, fills, equity, minute_prices,
                        fold_contexts=fold_contexts,
                        fold_test_metrics=fold_test_metrics)
    abl_path = Path(args.ablation_output)
    abl.to_parquet(abl_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(abl)} ablation rows to {abl_path}")
    if not abl.empty:
        pass_n = int((abl["status"] == "pass").sum())
        fail_n = int((abl["status"] == "fail").sum())
        ni_n = int((abl["status"] == "not_implemented").sum())
        logger.info(f"Ablation summary: {pass_n} pass, {fail_n} fail, {ni_n} not_implemented")


if __name__ == "__main__":
    main()
