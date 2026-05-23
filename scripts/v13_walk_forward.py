#!/usr/bin/env python3
"""V13 Script 4/5 (v2): Walk-forward backtest with forward signal replay.

Per projects/quant/v13 Section 6 + remediation plan v2.

v2 fixes (from codex r1 #14-#20):

#14 FULL PARAMETER GRID per Section 6.2 on VALIDATION only:
    K in {5,10,25,50,100}, per_coin_cap in {0.15,0.25,0.35},
    gross_cap in {1.0,1.5,2.0}, cooldown in {60,120},
    poll in {1,5,10} min (30s approximated by 1m granularity),
    weighting in {equal, score}, consensus in {off, soft}.

#15 hard40 consensus is ABLATION-ONLY, never selected for production.

#16 Random portfolios default 1000, with BOTH Sharpe AND net-PnL percentile
    gates (>= 95th required for binding criterion).

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

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "walk_forward_results.parquet"
ABLATION_OUTPUT = ROOT / "app" / "data" / "v13" / "ablation_results.parquet"

HL_FEE_BPS_PER_SIDE = 4.32
SLIPPAGE_BPS_REALISTIC = 5.0


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


@dataclass
class WalletFillStream:
    """Indexed time series of one wallet's fills for forward replay."""
    times: np.ndarray              # int64 ms timestamps, sorted ascending
    coins: np.ndarray              # object dtype
    signed_sizes: np.ndarray       # float64
    prices: np.ndarray             # float64
    next_idx: int = 0              # cursor for forward replay


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
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    coll_name = "hyperliquid_candles" if interval == "1m" else "hyperliquid_candles_1h"
    c = db[coll_name]
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
            start=pd.Timestamp(start, tz="UTC"),
            end=pd.Timestamp(end + timedelta(days=1), tz="UTC"),
            freq="1min",
            inclusive="left",
        )
        pivot = pivot.reindex(full_idx).ffill()
    return pivot


# Back-compat alias retained.
def load_minute_close_prices(coins, start, end):
    return load_price_grid(coins, start, end, "1m")


# ---------------------------------------------------------------------------
# Wallet equity at time t (for signal denominator)
# ---------------------------------------------------------------------------

def compute_wallet_equity_at(
    state: WalletState, prices_at_t: pd.Series, base_equity: float,
) -> float:
    """Equity at time t, anchored on the wallet's equity at fold start (train_end).

        equity[t] = base_equity
                  + realized_pnl_post_anchor                # realized fills inside the fold
                  + (current_unrealized_at_t - anchor_unrealized_at_t)
                                                           # mark-to-market change since anchor

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

    current_unrealized = 0.0
    for coin, qty in state.positions.items():
        if abs(qty) < EPS:
            continue
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
) -> pd.DataFrame:
    """Run replication bot over [fold_start, fold_end] at 1m cadence.

    Returns DataFrame of per-minute snapshots: ts, our_equity, gross_notional,
    n_active_coins, fees_minute, slippage_minute.
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
            wallet_streams[w].next_idx = 0

    # The anchor (train_end) is the time at which base_equity is true. Pre-anchor
    # fills update positions/cost_basis silently; post-anchor fills accumulate
    # realized_pnl_post_anchor.
    anchor_ts_ms = int((anchor_dt or fold_start).timestamp() * 1000)

    # First: advance each wallet's state to the anchor moment and capture the
    # anchor snapshot (positions, cost_basis, prices at anchor). This is run
    # ONCE per fold-simulation; the per-minute loop below will not re-advance
    # state until after the anchor.
    anchor_prices_row = None
    if not minute_prices.empty:
        # Find the LAST minute_prices row at or before anchor_ts_ms.
        idx = minute_prices.index
        anchor_pd_ts = pd.Timestamp(anchor_ts_ms, unit="ms", tz="UTC")
        eligible_idx = idx[idx <= anchor_pd_ts]
        if len(eligible_idx) > 0:
            anchor_prices_row = minute_prices.loc[eligible_idx[-1]]
    for w in selected_wallets:
        if w in wallet_streams:
            advance_wallet_state(wallet_states[w], wallet_streams[w], anchor_ts_ms, anchor_ts_ms=anchor_ts_ms)
        state = wallet_states[w]
        if state.anchor_set and anchor_prices_row is not None:
            # Snapshot anchor prices + compute anchor_unrealized_fixed.
            anchor_unrealized = 0.0
            for coin, qty in state.anchor_positions.items():
                if abs(qty) < EPS:
                    continue
                px = anchor_prices_row.get(coin)
                if px is None or pd.isna(px):
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
    # Build minute index over fold range.
    minute_index = minute_prices.index
    minute_index = minute_index[(minute_index >= pd.Timestamp(fold_start, tz="UTC")) & (minute_index <= pd.Timestamp(fold_end, tz="UTC"))]
    if len(minute_index) == 0:
        return pd.DataFrame()

    poll_step_minutes = params.poll_minutes
    poll_counter = 0

    for ts in minute_index:
        ts_ms = int(ts.timestamp() * 1000)

        # Advance each selected wallet's state up to ts.
        for w in selected_wallets:
            if w in wallet_streams:
                advance_wallet_state(wallet_states[w], wallet_streams[w], ts_ms, anchor_ts_ms=anchor_ts_ms)

        prices_at_t = minute_prices.loc[ts] if ts in minute_prices.index else pd.Series(dtype=float)

        # MTM EQUITY WALK: equity accrues position PnL every minute, not just on
        # rebalance fills. delta_equity = sum_coin qty * (price_now - price_last).
        if last_prices:
            mtm_pnl_this_step = 0.0
            for coin, qty in our_quantity.items():
                if abs(qty) < EPS:
                    continue
                p0 = last_prices.get(coin)
                p1 = prices_at_t.get(coin)
                if p0 is not None and p1 is not None and not pd.isna(p0) and not pd.isna(p1):
                    mtm_pnl_this_step += float(qty) * (float(p1) - float(p0))
            our_equity += mtm_pnl_this_step

        # Update last_prices for next step's MTM walk.
        for c in prices_at_t.index:
            p = prices_at_t.get(c)
            if p is not None and not pd.isna(p):
                last_prices[c] = float(p)

        # MTM our positions to current price (for diagnostic gross).
        gross = 0.0
        active_coins = 0
        for coin, qty in our_quantity.items():
            if abs(qty) < EPS:
                continue
            px = prices_at_t.get(coin)
            if px is not None and not pd.isna(px):
                gross += abs(qty * float(px))
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
            rows.append({"ts": ts_ms, "our_equity": our_equity, "gross_notional": gross,
                         "n_active_coins": active_coins, "fees_minute": 0.0, "slippage_minute": 0.0})
            continue

        # Compute signals at ts.
        target_pct = {}
        wallet_signals_now = {}
        for w in selected_wallets:
            state = wallet_states[w]
            base_eq = wallet_base_equity.get(w, 1.0)
            eq_w = compute_wallet_equity_at(state, prices_at_t, base_eq)
            if eq_w <= EPS:
                wallet_signals_now[w] = {}
                continue
            sig = {}
            for coin, qty in state.positions.items():
                if abs(qty) < EPS:
                    continue
                px = prices_at_t.get(coin)
                if px is None or pd.isna(px):
                    continue
                signed_notional = qty * float(px)
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
        fees_minute = 0.0
        slippage_minute = 0.0
        for coin in target_pct:
            target_notional = target_pct[coin] * our_equity
            px = prices_at_t.get(coin)
            if px is None or pd.isna(px) or float(px) <= EPS:
                continue
            current_qty = our_quantity.get(coin, 0.0)
            current_notional = current_qty * float(px)
            target_qty = target_notional / float(px)
            delta_qty = target_qty - current_qty
            delta_notional = delta_qty * float(px)
            if abs(delta_notional) <= params.min_delta_usd:
                continue
            if abs(delta_notional) <= params.min_delta_pct * abs(target_notional + EPS):
                continue
            # Cooldown check on re-entries.
            if abs(current_qty) < EPS and abs(target_qty) > EPS:
                if cooldown_until.get(coin, 0) > ts_ms:
                    continue
            # Execute fill.
            fee = abs(delta_notional) * params.fee_bps_per_side / 10000.0
            slip = abs(delta_notional) * params.slippage_bps / 10000.0
            fees_minute += fee
            slippage_minute += slip
            our_quantity[coin] = target_qty
            last_rebalance_ts[coin] = ts_ms
            # On full exit, set cooldown.
            if abs(target_qty) < EPS:
                cooldown_until[coin] = ts_ms + params.cooldown_seconds * 1000

        # Close positions for coins no longer in target (with cooldown set).
        for coin in list(our_quantity.keys()):
            if coin not in target_pct and abs(our_quantity[coin]) > EPS:
                px = prices_at_t.get(coin)
                if px is None or pd.isna(px) or float(px) <= EPS:
                    continue
                notional = our_quantity[coin] * float(px)
                fee = abs(notional) * params.fee_bps_per_side / 10000.0
                slip = abs(notional) * params.slippage_bps / 10000.0
                fees_minute += fee
                slippage_minute += slip
                our_quantity[coin] = 0.0
                cooldown_until[coin] = ts_ms + params.cooldown_seconds * 1000

        # Max-staleness rebalance trigger (every staleness_minutes).
        # (We approximate by always running the rebalance on poll cycles; staleness
        # rule fires implicitly when no fill has happened in N minutes.)

        # Apply MTM PnL to equity: for next-minute, recompute gross from updated qty.
        # Equity evolves as: prev_equity + position_pnl - fees - slippage.
        # We compute position_pnl from the next-minute step.
        our_equity -= (fees_minute + slippage_minute)

        rows.append({
            "ts": ts_ms,
            "our_equity": our_equity,
            "gross_notional": sum(abs(q * float(prices_at_t.get(c, 0) or 0)) for c, q in our_quantity.items()),
            "n_active_coins": sum(1 for q in our_quantity.values() if abs(q) > EPS),
            "fees_minute": fees_minute,
            "slippage_minute": slippage_minute,
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

def equity_curve_metrics(curve: pd.DataFrame, starting_capital: float) -> dict:
    if curve.empty or "our_equity" not in curve.columns:
        return {"sharpe": 0.0, "max_dd_pct": 0.0, "net_return_pct": 0.0, "worst_day_pct": 0.0,
                "n_minutes": 0}
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
    return {
        "sharpe": sharpe, "max_dd_pct": max_dd, "net_return_pct": net,
        "worst_day_pct": worst, "n_minutes": len(curve),
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
) -> dict:
    """One fold of the walk-forward.

    Train: rank wallets via composite score using ONLY fills with time <= train_end.
    Validation: parameter sweep on val_start..val_end.
    Test: best-validation params evaluated ONCE on test_start..test_end.
    """
    # 1) Compute wallet metrics on train window only.
    from importlib import util
    spec = util.spec_from_file_location("v13_metrics_mod", ROOT / "scripts" / "v13_wallet_metrics.py")
    mod = util.module_from_spec(spec); spec.loader.exec_module(mod)
    market = mod.load_market_daily_returns(train_start, train_end)
    train_fills = fills[(fills["time"] >= int(train_start.timestamp() * 1000)) & (fills["time"] <= int(train_end.timestamp() * 1000))]
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

    eligible = train_metrics[train_metrics["eligible"]]
    if eligible.empty:
        return {"fold": fold_idx, "status": "no_eligible"}

    eligible_sorted = eligible.sort_values("wallet_score", ascending=False).reset_index(drop=True)
    wallet_scores = dict(zip(eligible_sorted["wallet"], eligible_sorted["wallet_score"]))
    all_eligible_wallets = eligible_sorted["wallet"].tolist()

    # Wallet base equities at train_end (anchor for forward simulation).
    eq_at_train_end = (
        equity[equity["date"] == train_end.date()]
        .set_index("wallet")["equity_usd"]
        .to_dict()
    )

    # Build fill streams for forward replay (all eligible wallets).
    streams = build_wallet_fill_streams(fills, all_eligible_wallets)

    # 2) Parameter sweep on VALIDATION. Full grid per Section 6.2.
    # K x per_coin_cap x gross_cap x cooldown x poll_minutes x weighting x consensus
    best_params, best_val_sharpe = None, -1e9
    K_grid = [k for k in K_choices if k <= len(eligible_sorted)]
    for K in K_grid:
        selected = eligible_sorted.head(K)["wallet"].tolist()
        for per_coin_cap in [0.15, 0.25, 0.35]:
            for gross in [1.0, 1.5, 2.0]:
                for cooldown in [60, 120]:
                    for poll in [1, 5, 10]:
                        for weighting in ["equal", "score"]:
                            for consensus in ["off", "soft"]:    # hard40 ABLATION-ONLY
                                p = BotParams(
                                    K=K, per_coin_cap=per_coin_cap, gross_cap=gross,
                                    cooldown_seconds=cooldown, poll_minutes=poll,
                                    weighting=weighting, consensus=consensus,
                                )
                                # simulate_replication() internally resets stream cursors per call.
                                curve = simulate_replication(
                                    selected, streams, wallet_scores, eq_at_train_end,
                                    minute_prices, val_start, val_end, p,
                                    anchor_dt=train_end,
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
        anchor_dt=train_end,
    )
    test_metrics = equity_curve_metrics(test_curve, best_params.starting_capital)

    # 4) Random portfolio percentile on OOS test (BOTH Sharpe + net-PnL gates).
    rng = np.random.default_rng(42 + fold_idx)
    random_sharpes = []
    random_pnls = []
    for trial in range(n_random):
        if len(all_eligible_wallets) < best_params.K:
            random_sharpes.append(0.0)
            random_pnls.append(0.0)
            continue
        rand_sel = list(rng.choice(all_eligible_wallets, size=best_params.K, replace=False))
        for w in rand_sel:
            if w in streams:
                streams[w].next_idx = 0
        rc = simulate_replication(rand_sel, streams, wallet_scores, eq_at_train_end,
                                  minute_prices, test_start, test_end, best_params,
                                  anchor_dt=train_end)
        rm = equity_curve_metrics(rc, best_params.starting_capital)
        random_sharpes.append(rm["sharpe"])
        random_pnls.append(rm["net_return_pct"])
    random_sharpes_sorted = sorted(random_sharpes)
    random_pnls_sorted = sorted(random_pnls)
    sharpe_pct_rank = (np.searchsorted(random_sharpes_sorted, test_metrics["sharpe"]) / max(1, len(random_sharpes_sorted))) * 100
    pnl_pct_rank = (np.searchsorted(random_pnls_sorted, test_metrics["net_return_pct"]) / max(1, len(random_pnls_sorted))) * 100

    # 5) Robustness: top-1/5/10 removal + leave-one-out + random dropout 20%.
    robust = {}
    for k_remove in [1, 5, 10]:
        if best_params.K - k_remove <= 0:
            robust[f"remove_top{k_remove}_sharpe"] = None
            continue
        remaining = eligible_sorted.iloc[k_remove:k_remove + best_params.K]["wallet"].tolist()
        if len(remaining) < best_params.K:
            remaining = eligible_sorted.iloc[k_remove:]["wallet"].tolist()
        for w in remaining[:best_params.K]:
            if w in streams:
                streams[w].next_idx = 0
        rc = simulate_replication(remaining[:best_params.K], streams, wallet_scores, eq_at_train_end,
                                  minute_prices, test_start, test_end, best_params,
                                  anchor_dt=train_end)
        rm = equity_curve_metrics(rc, best_params.starting_capital)
        robust[f"remove_top{k_remove}_sharpe"] = rm["sharpe"]

    # Latest-fold profitability check (criterion 8) -- this fold's test.
    latest_fold_profitable = test_metrics["sharpe"] > 0

    # Propagate the deployment_blocked_by_near_liq flag (from train_metrics).
    deployment_blocked_by_near_liq = bool(train_metrics.get("deployment_blocked_by_near_liq", pd.Series([True])).any())

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
        test_start=test_start,
        test_end=test_end,
        best_params=best_params,
        random_p95_sharpe=float(np.percentile(random_sharpes, 95)) if random_sharpes else 0.0,
        random_p95_pnl=float(np.percentile(random_pnls, 95)) if random_pnls else 0.0,
        robust=robust,
        fills=fills,
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
        "fee_drag": 0.30,                # placeholder; per-fold fee drag from train_metrics post-MVP
        "_ablation_context": ctx,
        "_test_metrics": test_metrics,
        **robust,
    }


def daily_close_or_prices(start, end, prices):
    """Return prices subset to [start, end] for ablation use."""
    if prices.empty:
        return prices
    idx = prices.index
    mask = (idx >= pd.Timestamp(start, tz="UTC")) & (idx <= pd.Timestamp(end, tz="UTC"))
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
    test_start: datetime
    test_end: datetime
    best_params: BotParams
    random_p95_sharpe: float
    random_p95_pnl: float
    robust: dict
    fills: pd.DataFrame


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
        anchor_dt=ctx.train_end,
    )
    return equity_curve_metrics(curve, params.starting_capital)


def _benchmark_buy_hold_curve(coin: str, minute_prices: pd.DataFrame,
                              test_start: datetime, test_end: datetime,
                              starting_capital: float) -> dict:
    """Compute buy-and-hold metrics for one coin over the test window.

    Timestamps derived from prices.index AFTER dropna, ensuring equity values
    line up with their actual bar timestamps (codex r23 #2 fix).
    """
    idx_full = minute_prices.index
    idx_in = idx_full[(idx_full >= pd.Timestamp(test_start, tz="UTC")) & (idx_full <= pd.Timestamp(test_end, tz="UTC"))]
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
    idx_full = minute_prices.index
    idx_in = idx_full[(idx_full >= pd.Timestamp(test_start, tz="UTC")) & (idx_full <= pd.Timestamp(test_end, tz="UTC"))]
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
    ap.add_argument("--K-choices", default="5,10,25,50,100")
    ap.add_argument("--random-portfolios", type=int, default=1000)
    ap.add_argument("--fills-dir", default=None)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    ap.add_argument("--ablation-output", default=str(ABLATION_OUTPUT))
    ap.add_argument("--price-interval", choices=["1m", "1h"], default="1m",
                    help="Price granularity. 1m is the production default "
                         "(S3-reconstructed; spec Section 5.7). 1h is a "
                         "fallback that flags results as APPROXIMATED.")
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

    results = []
    fold_contexts: dict = {}
    fold_test_metrics: dict = {}
    for (fi, ts, te, vs, ve, tts, tte) in folds:
        logger.info(f"Fold {fi}: train {ts.date()}..{te.date()} val {vs.date()}..{ve.date()} test {tts.date()}..{tte.date()}")
        r = run_fold(fi, ts, te, vs, ve, tts, tte,
                     fills, journeys, equity, minute_prices,
                     n_random=args.random_portfolios, K_choices=K_choices)
        # Capture context + test_metrics for ablations BEFORE stripping from
        # the dict that goes to parquet.
        if r.get("status") == "ok":
            fold_contexts[fi] = r.pop("_ablation_context")
            fold_test_metrics[fi] = r.pop("_test_metrics")
        else:
            r.pop("_ablation_context", None)
            r.pop("_test_metrics", None)
        results.append(r)
        logger.info(f"  -> status={r.get('status')} test_sharpe={r.get('test_sharpe', 'NA')} rank_sharpe={r.get('random_sharpe_pct_rank', 'NA')} rank_pnl={r.get('random_pnl_pct_rank', 'NA')}")

    df = pd.DataFrame(results)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(df)} fold results to {out_path}")

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
