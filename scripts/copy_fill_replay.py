#!/usr/bin/env python3
"""
copy_fill_replay.py -- Stage 2 of Step C, fill-level copy replay.

Per Alberto-locked spec 2026-05-26 (no deferrals) + codex r18 architecture review.

Replaces journey-level v0 in copyability_overlay.py with FILL-LEVEL replay.
For each (wallet, coin) per (capital_scale, latency, proxy), walks source fills
in time order and maintains OUR position state via TARGET-POSITION REPLAY:

    source_qty_after = source_qty_before + source_signed_delta
    source_pct_after = source_qty_after * source_px / source_equity_at_fill_day
    target_our_notional_after = clamp(source_pct_after * capital_scale,
                                      -capital_scale * max_leverage,
                                      capital_scale * max_leverage)
    target_our_qty_after = target_our_notional_after / copy_px_at_our_latency
    our_delta_qty = target_our_qty_after - our_qty_before
    apply our_delta_qty at copy_px (taker)

This is the codex r9 #5 fix: addons, trims, reverses are all captured as
sequential target-position updates instead of a single entry/exit pair.

Inputs:
    --fills-dir         app/data/hl_s3_fills/ (S3 fills daily parquets)
    --journeys          wallet_journeys_costed.parquet (Step B output, for fallback pct)
    --equity-series     wallet_equity_series.parquet (optional; if missing -> journey fallback mode)
    --train-start, --train-end, --test-start, --test-end YYYY-MM-DD
    --latencies         "120,300"
    --capital-scales    "500,1000,5000,10000,50000"
    --fee-rt-bps        OUR fee per round trip (default 8.64 = 4.32/side)
    --max-copy-leverage default 10.0
    --min-order-notional default 10.0
    --primary-latency, --primary-capital-scale (for ranking display)
    --out-prefix        e.g. /tmp/cop_fill_replay
    --wallets           optional newline-separated wallet filter

Outputs:
    {out-prefix}_journeys_{train|test}.parquet   -- per OUR-journey rows
    {out-prefix}_wallets_{train|test}.parquet    -- aggregate per (wallet, capital_scale, latency, proxy)

Notes:
    - Stage 1 (journey-level v0) overlay stays AS-IS for diagnostic comparison
    - Stage 2 is the production ranking
    - position_replay_mode column = "fill_level_v1"
"""
from __future__ import annotations

import argparse
import logging
import os
import resource
import signal
import sys
import threading
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import psutil


# v3 OOM PREVENTION (Alberto-locked 2026-05-26): code-enforced memory guards.
def install_memory_guards(rlimit_data_gb: float = 6.0, rss_abort_gb: float = 4.0) -> None:
    """Install OOM prevention guards. Call at start of main()."""
    try:
        cap_bytes = int(rlimit_data_gb * 1024 ** 3)
        resource.setrlimit(resource.RLIMIT_DATA, (cap_bytes, cap_bytes))
    except (ValueError, OSError):
        pass
    pid = os.getpid()
    abort_bytes = int(rss_abort_gb * 1024 ** 3)
    def monitor():
        proc = psutil.Process(pid)
        while True:
            try:
                if proc.memory_info().rss > abort_bytes:
                    os.kill(pid, signal.SIGTERM)
                    return
                _time.sleep(10)
            except psutil.NoSuchProcess:
                return
            except Exception:
                pass
    threading.Thread(target=monitor, daemon=True, name="rss_monitor").start()

# Reuse copyability_overlay helpers + constants
sys.path.insert(0, str(Path(__file__).resolve().parent))
from copyability_overlay import (    # noqa: E402
    FEE_RT_BPS_DEFAULT,
    MIN_ORDER_NOTIONAL_DEFAULT,
    MAX_COPY_LEVERAGE_DEFAULT,
    FALLBACK_COPY_FRACTION_DEFAULT,
    DEFAULT_LATENCIES,
    DEFAULT_CAPITAL_SCALES,
    PRIMARY_LATENCY_DEFAULT,
    PRIMARY_CAPITAL_SCALE_DEFAULT,
    JPD_CAP,
    load_candles_1m,
    build_coin_index,
    load_funding,
    build_funding_index,
    copy_price_at,
    funding_cost_bps_over,
    shrunk_mean,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [copy_replay] %(levelname)s: %(message)s",
)
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR_DEFAULT = ROOT / "app" / "data" / "hl_s3_fills"

EPS = 1e-12


@dataclass
class PositionState:
    """Mutable per (wallet, coin, capital_scale, latency, proxy) replay state."""
    qty: float = 0.0
    cost_basis: float = 0.0
    open_ts: int | None = None
    side: int = 0
    realized_pnl: float = 0.0
    fees_paid: float = 0.0
    slippage_paid: float = 0.0    # reserved for future bps->USD slippage
    funding_paid: float = 0.0     # reserved for OUR funding integration
    max_notional: float = 0.0
    abs_notional_time_usd_s: float = 0.0
    last_ts: int | None = None
    n_fills: int = 0
    n_skipped_min_order: int = 0
    skipped_min_order_notional: float = 0.0
    close_below_min_order: bool = False


def signed_fill_qty(row) -> float:
    size = float(row.size)
    if row.side == "B":
        return size
    if row.side == "A":
        return -size
    return 0.0


def equity_at_day(equity_lookup: dict, wallet: str, ts_ms: int) -> float | None:
    """Lookup source equity for the wallet.

    Per Alberto rule 16 + decision A on 2026-05-26 17:09 CEST: equity = spot
    USDC TODAY (wallet-level scalar, NOT per-date). The ts_ms argument is
    accepted for API stability but no longer used — we don't have historical
    spot USDC reconstruction (would require a separate spot-ledger walk).
    Returns None if equity is missing or <= 0.
    """
    if not equity_lookup:
        return None
    eq = equity_lookup.get(wallet)
    return float(eq) if eq and eq > 0 else None


def update_time_integral(state: PositionState, ts: int, mark_px: float) -> None:
    """Accumulate abs(qty) * mark_px * dt for utilization metric."""
    if state.last_ts is None:
        state.last_ts = ts
        return
    dt_s = max(0.0, (ts - state.last_ts) / 1000.0)
    state.abs_notional_time_usd_s += abs(state.qty) * mark_px * dt_s
    state.last_ts = ts


def accrue_funding(
    state: PositionState,
    ts_from: int,
    ts_to: int,
    funding_arr_for_coin: dict | None,
) -> None:
    """Codex r19 #4: accrue OUR funding over [ts_from, ts_to] for our current position.

    Uses the funding_idx for this coin: array of (timestamp_ms, funding_rate).
    Each funding event applies: position_usd * funding_rate / 10000 cashflow.
    Sign: source/our perps pay funding to longs when rate <0, to shorts when rate >0.
    Per HL convention: usdc cashflow positive = received, negative = paid.
    Approximation: rate × notional held at funding tick (snapshot position at tick).
    """
    if funding_arr_for_coin is None or abs(state.qty) < EPS:
        return
    # build_funding_index in copyability_overlay uses keys "ts" + "rate"
    times = funding_arr_for_coin.get("ts")
    rates = funding_arr_for_coin.get("rate")
    if times is None or rates is None:
        return
    # Find funding ticks in (ts_from, ts_to]
    if hasattr(times, "size"):
        n = times.size
    else:
        n = len(times)
    if n == 0:
        return
    # Linear scan (funding ticks are hourly; small array)
    for i in range(n):
        t = int(times[i])
        if t <= ts_from:
            continue
        if t > ts_to:
            break
        rate = float(rates[i])  # per-period funding rate (NOT bps; HL convention as fraction)
        # OUR cashflow: long pays positive-rate funding, short pays negative-rate (gets neg).
        # HL convention: usdc = -position_notional × rate
        position_notional_at_tick = state.qty * (state.cost_basis if abs(state.cost_basis) > EPS else 0)
        # Cashflow: -position_notional × rate, but more accurate to use mark at the tick.
        # We don't have mark at the tick; approximate with cost_basis (close enough hourly).
        cashflow = -position_notional_at_tick * rate
        state.funding_paid += cashflow  # positive cashflow = received


def finalize_journey(state: PositionState, close_ts: int) -> dict:
    gross = state.realized_pnl
    # funding_paid is a SIGNED cashflow (positive = received, negative = paid)
    # following the B step convention (net = realized - fees + funding_net).
    net = gross - state.fees_paid - state.slippage_paid + state.funding_paid
    max_notional = max(state.max_notional, EPS)
    return {
        "entry_ts": state.open_ts,
        "exit_ts": close_ts,
        "side": "long" if state.side > 0 else "short",
        "realized_pnl_usd": gross,
        "fees_paid_usd": state.fees_paid,
        "slippage_paid_usd": state.slippage_paid,
        "funding_paid_usd": state.funding_paid,
        "copy_net_pnl_usd": net,
        "copy_net_bps_of_max": (net / max_notional) * 10000.0,
        "max_position_notional_usd": max_notional,
        "abs_notional_time_usd_s": state.abs_notional_time_usd_s,
        "n_our_fills": state.n_fills,
        "n_skipped_min_order": state.n_skipped_min_order,
        "skipped_min_order_notional": state.skipped_min_order_notional,
        "close_below_min_order": state.close_below_min_order,
        "position_replay_mode": "fill_level_v1",
    }


def reset_state(state: PositionState) -> None:
    state.__dict__.update(PositionState().__dict__)


def apply_our_delta(
    state: PositionState,
    ts: int,
    px: float,
    delta_qty: float,
    min_order_notional: float,
    fee_rate_one_way: float,
    force_close: bool = False,
) -> list[dict]:
    """Apply OUR delta to position. Returns list of finalized journeys."""
    out: list[dict] = []
    notional = abs(delta_qty) * px
    if abs(delta_qty) < EPS:
        return out

    if not force_close and notional < min_order_notional:
        state.n_skipped_min_order += 1
        state.skipped_min_order_notional += notional
        return out

    if force_close and notional < min_order_notional:
        state.close_below_min_order = True

    old_qty = state.qty
    new_qty = old_qty + delta_qty

    # Fees on this fill (one-way taker)
    fee = notional * fee_rate_one_way
    state.fees_paid += fee
    state.n_fills += 1

    # Opening from flat
    if abs(old_qty) < EPS and abs(new_qty) > EPS:
        state.open_ts = ts
        state.side = 1 if new_qty > 0 else -1
        state.cost_basis = px
        state.qty = new_qty
        state.max_notional = max(state.max_notional, abs(new_qty) * px)
        state.last_ts = ts
        return out

    # Same-side addon
    if old_qty * delta_qty > 0:
        state.cost_basis = (
            state.cost_basis * abs(old_qty) + px * abs(delta_qty)
        ) / max(EPS, abs(new_qty))
        state.qty = new_qty
        state.max_notional = max(state.max_notional, abs(new_qty) * px)
        return out

    # Trim / close (delta opposite to position)
    closed_qty = min(abs(old_qty), abs(delta_qty))
    pnl = (px - state.cost_basis) * closed_qty * (1 if old_qty > 0 else -1)
    state.realized_pnl += pnl
    state.qty = new_qty

    if abs(new_qty) < EPS:
        out.append(finalize_journey(state, ts))
        reset_state(state)
        return out

    # Should not cross through zero here (caller should split reverse beforehand).
    return out


def replay_wallet_coin(
    fills: pd.DataFrame,
    wallet: str,
    coin: str,
    equity_lookup: dict,
    journey_pct_fallback: dict,  # (wallet,coin)->float median max_position_pct_equity
    candles_idx: dict,
    funding_idx: dict,
    capital_scale: float,
    latency_s: int,
    proxy: str,
    min_order_notional: float,
    max_copy_leverage: float,
    fallback_copy_fraction: float,
    fee_rate_one_way: float,
    window_end_ms: int,
) -> list[dict]:
    """Replay one (wallet, coin) sorted source fills for one (cap, lat, proxy)."""
    source_qty = 0.0
    our = PositionState()
    journeys: list[dict] = []
    cap_notional = capital_scale * max_copy_leverage
    coin_funding = funding_idx.get(coin) if funding_idx else None

    sizing_scope_count = {
        "fill_source_pct_equity": 0,
        "journey_pct_fallback": 0,
        "missing_no_sizing": 0,
    }
    dominant_sizing_scope = "unknown"

    prev_ts_copy = None

    for row in fills.itertuples(index=False):
        src_delta = signed_fill_qty(row)
        if abs(src_delta) < EPS:
            continue

        ts_source = int(row.time)
        ts_copy = ts_source + latency_s * 1000
        src_px = float(row.price)

        trade_side = 1 if src_delta > 0 else -1
        # Per codex r18 Q9: per-fill exec side. buy=conservative high, sell=conservative low.
        copy_px = copy_price_at(candles_idx.get(coin), ts_copy, trade_side, proxy)
        if not np.isfinite(copy_px) or copy_px <= 0:
            continue

        # Codex r19 #4: accrue funding on OUR position over (prev_ts_copy, ts_copy]
        # using the coin's funding rate timeline. funding_paid is signed (negative=paid).
        if prev_ts_copy is not None:
            accrue_funding(our, prev_ts_copy, ts_copy, coin_funding)
        update_time_integral(our, ts_copy, copy_px)
        prev_ts_copy = ts_copy

        source_qty_after = source_qty + src_delta

        # Source pct-equity at fill time
        eq = equity_at_day(equity_lookup, wallet, ts_source)
        if eq is not None and eq > 0:
            source_pct_after = source_qty_after * src_px / eq
            sizing_scope = "fill_source_pct_equity"
        else:
            # Fallback to journey-level constant pct (Stage 1 path)
            j_pct = journey_pct_fallback.get((wallet, coin))
            if j_pct is not None and np.isfinite(j_pct) and j_pct > 0:
                # We need direction: use sign of source_qty_after
                source_pct_after = j_pct * np.sign(source_qty_after) if abs(source_qty_after) > EPS else 0.0
                sizing_scope = "journey_pct_fallback"
            else:
                # No sizing reference at all -> use FALLBACK fraction of capital scaled by sign
                source_pct_after = fallback_copy_fraction * np.sign(source_qty_after) if abs(source_qty_after) > EPS else 0.0
                sizing_scope = "missing_no_sizing"

        sizing_scope_count[sizing_scope] = sizing_scope_count.get(sizing_scope, 0) + 1

        # Target our notional clamped by max leverage
        target_notional_after = float(np.clip(
            source_pct_after * capital_scale,
            -cap_notional,
            cap_notional,
        ))
        target_qty_after = target_notional_after / copy_px if copy_px > 0 else 0.0
        delta_our = target_qty_after - our.qty

        # Reverse: split close + open at same ts
        if our.qty * target_qty_after < 0:
            close_delta = -our.qty
            journeys.extend(apply_our_delta(
                our, ts_copy, copy_px, close_delta,
                min_order_notional, fee_rate_one_way, force_close=True,
            ))
            open_delta = target_qty_after
            journeys.extend(apply_our_delta(
                our, ts_copy, copy_px, open_delta,
                min_order_notional, fee_rate_one_way, force_close=False,
            ))
        else:
            force_close = abs(target_qty_after) < EPS and abs(our.qty) > EPS
            journeys.extend(apply_our_delta(
                our, ts_copy, copy_px, delta_our,
                min_order_notional, fee_rate_one_way, force_close=force_close,
            ))

        source_qty = source_qty_after

    # Dominant sizing scope across all fills processed
    if sizing_scope_count:
        dominant_sizing_scope = max(sizing_scope_count.items(), key=lambda kv: kv[1])[0]

    # Carry-out: if OUR position is still open at window end, emit incomplete journey.
    # Codex r19 #2+#3 fix: use apply_our_delta with -our.qty + force_close=True so:
    #   - execution side is -our.side (selling a long uses conservative low; buying a short uses conservative high)
    #   - closing fee is charged via apply_our_delta wrapper
    #   - missing price emits an explicit audit flag carry_out_price_missing=True instead of silent skip
    if abs(our.qty) > EPS:
        coin_arr = candles_idx.get(coin)
        close_side = -1 if our.qty > 0 else 1  # closing a long needs to SELL (side=-1); closing a short needs to BUY (side=+1)
        end_px = copy_price_at(coin_arr, window_end_ms, close_side, proxy)
        if np.isfinite(end_px) and end_px > 0:
            close_journeys = apply_our_delta(
                our, window_end_ms, float(end_px),
                delta_qty=-our.qty,
                min_order_notional=min_order_notional,
                fee_rate_one_way=fee_rate_one_way,
                force_close=True,
            )
            for j in close_journeys:
                j["carry_out_at_window_end"] = True
                journeys.append(j)
        else:
            # Codex r19 #3: emit explicit audit row so this wallet/fold can be excluded
            # from ranking instead of silently dropping the position.
            journeys.append({
                "entry_ts": our.open_ts,
                "exit_ts": window_end_ms,
                "side": "long" if our.side > 0 else "short",
                "realized_pnl_usd": our.realized_pnl,
                "fees_paid_usd": our.fees_paid,
                "slippage_paid_usd": our.slippage_paid,
                "funding_paid_usd": our.funding_paid,
                "copy_net_pnl_usd": float("nan"),
                "copy_net_bps_of_max": float("nan"),
                "max_position_notional_usd": our.max_notional,
                "abs_notional_time_usd_s": our.abs_notional_time_usd_s,
                "n_our_fills": our.n_fills,
                "n_skipped_min_order": our.n_skipped_min_order,
                "skipped_min_order_notional": our.skipped_min_order_notional,
                "close_below_min_order": our.close_below_min_order,
                "position_replay_mode": "fill_level_v1",
                "carry_out_at_window_end": True,
                "carry_out_price_missing": True,
            })

    # Tag every journey row with shared metadata
    return [
        {
            **j,
            "wallet": wallet,
            "coin": coin,
            "capital_scale": capital_scale,
            "latency_seconds": latency_s,
            "proxy": proxy,
            "sizing_scope_dominant": dominant_sizing_scope,
            "carry_out_price_missing": j.get("carry_out_price_missing", False),
        }
        for j in journeys
    ]


def build_journey_pct_fallback(journeys_df: pd.DataFrame) -> dict:
    """Build (wallet, coin) -> median max_position_pct_equity fallback table."""
    if "max_position_pct_equity" not in journeys_df.columns:
        return {}
    f = journeys_df[journeys_df["max_position_pct_equity"].notna() &
                    (journeys_df["max_position_pct_equity"] > 0)]
    if f.empty:
        return {}
    grouped = f.groupby(["wallet", "coin"])["max_position_pct_equity"].median()
    return {tuple(k): float(v) for k, v in grouped.items()}


def load_equity_series(path: Path | None) -> dict:
    """Returns {wallet_lower: spot_usdc_today}. Empty dict if path missing.

    Per Alberto rule 16 + decision A (2026-05-26 17:09 CEST). See
    v13_journey_trace.load_equity_series for the canonical contract.
    """
    if path is None or not path.exists():
        return {}
    eq = pd.read_parquet(path)
    if "wallet" not in eq.columns:
        return {}
    if "spot_usdc_today" not in eq.columns:
        raise ValueError(
            f"equity series at {path} missing 'spot_usdc_today' column. "
            f"Re-run scripts/v13_equity_reconstruct.py with v3-A code first."
        )
    eq["wallet"] = eq["wallet"].str.lower()
    return (
        eq.dropna(subset=["spot_usdc_today"])
          .groupby("wallet")["spot_usdc_today"]
          .first()
          .astype(float)
          .to_dict()
    )


def load_fills_for_window(
    fills_dir: Path,
    start: datetime,
    end: datetime,
    wallets: set[str] | None,
) -> pd.DataFrame:
    """Memory-safe per-day pyarrow filter pushdown.

    Window is half-open: [start 00:00 UTC, (end + 1d) 00:00 UTC) per codex r19 #1.
    Reads daily parquets covering the range AND filters by time AFTER read to
    strictly enforce the half-open boundary (eliminates fills late on end date
    from being replayed without a matching carry-out close).
    """
    cols = ["wallet", "coin", "side", "size", "price", "time", "dir", "closedPnl"]
    paths = []
    cur = start.date()
    end_date = end.date()
    while cur <= end_date:
        p = fills_dir / f"{cur.strftime('%Y%m%d')}.parquet"
        if p.exists():
            paths.append(p)
        cur += timedelta(days=1)
    if not paths:
        return pd.DataFrame(columns=cols)

    wallets_lc = [w.lower() for w in wallets] if wallets else None
    filt = [("wallet", "in", wallets_lc)] if wallets_lc else None

    # Half-open boundary: include [start, end+1day) — codex r19 #1
    start_ms = int(start.timestamp() * 1000)
    end_excl_ms = int((end + timedelta(days=1)).timestamp() * 1000)

    frames = []
    import gc
    for p in paths:
        df = pd.read_parquet(p, columns=cols, filters=filt)
        if not df.empty:
            # Enforce half-open boundary explicitly (don't trust daily parquet boundary)
            df = df[(df["time"] >= start_ms) & (df["time"] < end_excl_ms)]
            if not df.empty:
                frames.append(df)
        gc.collect()
    if not frames:
        return pd.DataFrame(columns=cols)
    out = pd.concat(frames, ignore_index=True)
    out["wallet"] = out["wallet"].str.lower()
    # Deterministic ordering (codex r18 Q9)
    sort_keys = ["wallet", "coin", "time", "side", "price", "size"]
    out = out.sort_values(sort_keys, kind="stable").reset_index(drop=True)
    return out


def aggregate_wallets(journeys_df: pd.DataFrame) -> pd.DataFrame:
    """Group OUR journeys by (wallet, capital_scale, latency, proxy) -> aggregate row."""
    if journeys_df.empty:
        return pd.DataFrame()

    rows = []
    grouped = journeys_df.groupby(
        ["wallet", "capital_scale", "latency_seconds", "proxy"], sort=False
    )
    for (wallet, cap, lat, proxy), sub in grouped:
        # Filter to non-carry-out by default for ranking (proper boolean mask)
        if "carry_out_at_window_end" in sub.columns:
            carry_mask = sub["carry_out_at_window_end"].fillna(False).astype(bool)
            ranking_sub = sub[~carry_mask]
        else:
            ranking_sub = sub
        n_our_journeys = len(ranking_sub)
        if n_our_journeys < 1:
            continue

        cap_f = float(cap)
        copy_net_pnl_usd_arr = ranking_sub["copy_net_pnl_usd"].to_numpy(dtype=np.float64)
        copy_net_pct_equity_arr = copy_net_pnl_usd_arr / cap_f

        win_rate_copy = float((copy_net_pnl_usd_arr > 0).mean()) if len(copy_net_pnl_usd_arr) else float("nan")
        copy_net_pnl_usd_sum = float(np.nansum(copy_net_pnl_usd_arr))
        copy_net_pnl_usd_mean = float(np.nanmean(copy_net_pnl_usd_arr)) if len(copy_net_pnl_usd_arr) else float("nan")
        pct_eq_shrunk = shrunk_mean(copy_net_pct_equity_arr) if len(copy_net_pct_equity_arr) else float("nan")
        pct_eq_median = float(np.nanmedian(copy_net_pct_equity_arr)) if len(copy_net_pct_equity_arr) else float("nan")
        frac_pos = float((copy_net_pct_equity_arr > 0).mean()) if len(copy_net_pct_equity_arr) else 0.0

        # JPD over OUR journey spans
        if n_our_journeys >= 2:
            ts_min = ranking_sub["entry_ts"].min()
            ts_max = ranking_sub["exit_ts"].max()
            span_days = max(1.0, (ts_max - ts_min) / 1000.0 / 86400.0)
            jpd = n_our_journeys / span_days
        else:
            jpd = 0.0

        jpd_cap = min(jpd, JPD_CAP)
        reliability = max(0.0, frac_pos - 0.5) / 0.5
        score = (pct_eq_shrunk if np.isfinite(pct_eq_shrunk) else 0.0) * jpd_cap * reliability

        # Audit metrics
        n_skipped_total = int(ranking_sub["n_skipped_min_order"].sum()) if "n_skipped_min_order" in ranking_sub.columns else 0
        n_our_fills_total = int(ranking_sub["n_our_fills"].sum()) if "n_our_fills" in ranking_sub.columns else 0
        skip_frac = n_skipped_total / max(n_our_fills_total + n_skipped_total, 1)
        if "carry_out_at_window_end" in sub.columns:
            carry_out_count = int(sub["carry_out_at_window_end"].fillna(False).astype(bool).sum())
        else:
            carry_out_count = 0
        utilization_time_usd_s = float(ranking_sub["abs_notional_time_usd_s"].sum()) if "abs_notional_time_usd_s" in ranking_sub.columns else 0.0

        rows.append({
            "wallet": wallet,
            "capital_scale": cap_f,
            "latency_seconds": int(lat),
            "proxy": proxy,
            "n_our_journeys": n_our_journeys,
            "n_carry_out_excluded": carry_out_count,
            "n_our_fills_total": n_our_fills_total,
            "n_skipped_min_order_total": n_skipped_total,
            "min_order_skip_frac": skip_frac,
            "n_coins": int(ranking_sub["coin"].nunique()) if "coin" in ranking_sub.columns else 0,
            "jpd_valid": jpd,
            "win_rate_copy": win_rate_copy,
            "copy_net_pnl_usd_sum": copy_net_pnl_usd_sum,
            "copy_net_pnl_usd_mean": copy_net_pnl_usd_mean,
            "copy_net_return_pct_equity_shrunk_mean": pct_eq_shrunk,
            "copy_net_return_pct_equity_median": pct_eq_median,
            "copy_net_return_pct_equity_frac_pos": frac_pos,
            "copy_score": score,
            "abs_notional_time_usd_s": utilization_time_usd_s,
            "sizing_scope_dominant": (
                ranking_sub["sizing_scope_dominant"].mode().iloc[0]
                if "sizing_scope_dominant" in ranking_sub.columns and not ranking_sub["sizing_scope_dominant"].mode().empty
                else "unknown"
            ),
            "position_replay_mode": "fill_level_v1",
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--fills-dir", default=str(FILLS_DIR_DEFAULT))
    p.add_argument("--journeys", required=True, help="wallet_journeys_costed.parquet for fallback pct + filter")
    p.add_argument("--equity-series", default=None, help="wallet_equity_series.parquet (optional)")
    p.add_argument("--train-start", required=True)
    p.add_argument("--train-end", required=True)
    p.add_argument("--test-start", required=True)
    p.add_argument("--test-end", required=True)
    p.add_argument("--latencies", default=DEFAULT_LATENCIES)
    p.add_argument("--capital-scales", default=DEFAULT_CAPITAL_SCALES)
    p.add_argument("--fee-rt-bps", type=float, default=FEE_RT_BPS_DEFAULT)
    p.add_argument("--max-copy-leverage", type=float, default=MAX_COPY_LEVERAGE_DEFAULT)
    p.add_argument("--fallback-copy-fraction", type=float, default=FALLBACK_COPY_FRACTION_DEFAULT)
    p.add_argument("--min-order-notional", type=float, default=MIN_ORDER_NOTIONAL_DEFAULT)
    p.add_argument("--primary-latency", type=int, default=PRIMARY_LATENCY_DEFAULT)
    p.add_argument("--primary-capital-scale", type=float, default=PRIMARY_CAPITAL_SCALE_DEFAULT)
    p.add_argument("--out-prefix", required=True)
    p.add_argument("--wallets", default=None, help="optional newline wallet filter")
    p.add_argument("--write-train", action="store_true", default=False)
    p.add_argument("--rlimit-data-gb", type=float, default=6.0,
                   help="Hard memory cap via RLIMIT_DATA (kernel-enforced). Default 6.0GB.")
    p.add_argument("--rss-abort-gb", type=float, default=4.0,
                   help="Soft RSS threshold (psutil monitor). Process self-SIGTERMs above. Default 4.0GB.")
    return p.parse_args()


def main():
    args = parse_args()
    install_memory_guards(args.rlimit_data_gb, args.rss_abort_gb)
    latencies = [int(x) for x in args.latencies.split(",")]
    capital_scales = [float(x) for x in args.capital_scales.split(",")]
    fee_rate_one_way = (args.fee_rt_bps / 2.0) / 10000.0

    log.info(
        "config: latencies=%s caps=%s fee_rt=%.2f max_lev=%.1f fallback=%.2f min_order=$%.0f",
        latencies, capital_scales, args.fee_rt_bps, args.max_copy_leverage,
        args.fallback_copy_fraction, args.min_order_notional,
    )

    train_start = datetime.fromisoformat(args.train_start).replace(tzinfo=timezone.utc)
    train_end = datetime.fromisoformat(args.train_end).replace(tzinfo=timezone.utc)
    test_start = datetime.fromisoformat(args.test_start).replace(tzinfo=timezone.utc)
    test_end = datetime.fromisoformat(args.test_end).replace(tzinfo=timezone.utc)

    # Load journeys for fallback pct + wallet filter
    log.info("loading journeys: %s", args.journeys)
    journeys_df = pd.read_parquet(args.journeys)
    journey_pct_fallback = build_journey_pct_fallback(journeys_df)
    log.info("journey_pct_fallback entries: %d (wallet,coin) keys", len(journey_pct_fallback))

    # Wallet filter
    wallets_filter = None
    if args.wallets:
        with open(args.wallets) as f:
            wallets_filter = {w.strip().lower() for w in f if w.strip()}
    else:
        # Default: wallets present in the journeys file
        wallets_filter = set(journeys_df["wallet"].str.lower().unique())
    log.info("wallets to replay: %d", len(wallets_filter))

    # Equity series (optional)
    equity_lookup = load_equity_series(Path(args.equity_series) if args.equity_series else None)
    log.info("equity_lookup entries: %d (wallet,date) keys", len(equity_lookup))

    # Splits
    splits = [("test", test_start, test_end)]
    if args.write_train:
        splits.insert(0, ("train", train_start, train_end))

    # Reference data window covers train+test+latency tail
    data_start = min(train_start, test_start) - timedelta(days=1)
    data_end = max(train_end, test_end) + timedelta(days=1)

    for tag, win_start, win_end in splits:
        log.info("=== split %s window %s -> %s ===", tag, win_start.date(), win_end.date())
        # Load source fills (memory-safe per-day pyarrow)
        fills = load_fills_for_window(Path(args.fills_dir), win_start, win_end, wallets_filter)
        log.info("loaded %d fills in %s window for %d wallets",
                 len(fills), tag, len(wallets_filter))
        if fills.empty:
            log.warning("no fills in %s window; writing empty", tag)
            pd.DataFrame().to_parquet(f"{args.out_prefix}_journeys_{tag}.parquet")
            pd.DataFrame().to_parquet(f"{args.out_prefix}_wallets_{tag}.parquet")
            continue

        # Load reference price + funding indexes
        coins = sorted(fills["coin"].unique())
        log.info("loading 1m candles for %d coins...", len(coins))
        candles = load_candles_1m(coins, data_start, data_end)
        candles_idx = build_coin_index(candles)
        log.info("candles loaded: %d rows -> %d coin indexes", len(candles), len(candles_idx))
        funding = load_funding(coins, data_start, data_end)
        funding_idx = build_funding_index(funding)
        log.info("funding loaded: %d rows", len(funding))

        # Per-wallet, per-coin replay across all (cap, lat, proxy)
        win_end_ms = int(win_end.timestamp() * 1000)
        all_our_journeys = []
        pair_groups = fills.groupby(["wallet", "coin"], sort=False)
        n_pairs = pair_groups.ngroups
        log.info("replaying %d (wallet,coin) pairs across %d caps x %d lats x 2 proxies = %d total replays",
                 n_pairs, len(capital_scales), len(latencies), n_pairs * len(capital_scales) * len(latencies) * 2)

        processed = 0
        for (wallet, coin), grp in pair_groups:
            for cap in capital_scales:
                for lat in latencies:
                    for proxy in ("neutral", "conservative"):
                        try:
                            js = replay_wallet_coin(
                                grp, wallet, coin,
                                equity_lookup, journey_pct_fallback,
                                candles_idx, funding_idx,
                                cap, lat, proxy,
                                args.min_order_notional, args.max_copy_leverage,
                                args.fallback_copy_fraction, fee_rate_one_way,
                                win_end_ms,
                            )
                            all_our_journeys.extend(js)
                        except Exception as e:
                            log.warning(f"replay failed {wallet[:10]}/{coin} cap={cap} lat={lat} proxy={proxy}: {e}")
            processed += 1
            if processed % 100 == 0:
                log.info("  processed %d/%d pairs, %d OUR journeys so far",
                         processed, n_pairs, len(all_our_journeys))

        # Output journey-level
        our_journeys_df = pd.DataFrame(all_our_journeys)
        out_j_path = f"{args.out_prefix}_journeys_{tag}.parquet"
        our_journeys_df.to_parquet(out_j_path, index=False)
        log.info("wrote %s: %d OUR journey rows", out_j_path, len(our_journeys_df))

        # Aggregate
        agg_df = aggregate_wallets(our_journeys_df)
        out_w_path = f"{args.out_prefix}_wallets_{tag}.parquet"
        agg_df.to_parquet(out_w_path, index=False)
        log.info("wrote %s: %d (wallet, cap, lat, proxy) rows", out_w_path, len(agg_df))

        # Top 10 by primary score
        if not agg_df.empty:
            primary = agg_df[
                (agg_df["latency_seconds"] == args.primary_latency)
                & (agg_df["capital_scale"] == args.primary_capital_scale)
                & (agg_df["proxy"] == "conservative")
            ]
            if not primary.empty and primary["copy_score"].notna().any():
                log.info(
                    "top 10 by copy_score (%s, lat=%ds, cap=$%.0f, conservative):",
                    tag, args.primary_latency, args.primary_capital_scale,
                )
                for _, r in primary.nlargest(10, "copy_score").iterrows():
                    log.info(
                        "  %s n=%d jpd=%.2f pct_eq_shrunk=%.4f%% frac_pos=%.2f score=%.4f sizing=%s",
                        str(r["wallet"])[:16], int(r["n_our_journeys"]),
                        float(r["jpd_valid"]),
                        float(r["copy_net_return_pct_equity_shrunk_mean"]) * 100,
                        float(r["copy_net_return_pct_equity_frac_pos"]),
                        float(r["copy_score"]),
                        r["sizing_scope_dominant"],
                    )


if __name__ == "__main__":
    main()
