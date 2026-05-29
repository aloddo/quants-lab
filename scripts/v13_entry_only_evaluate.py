#!/usr/bin/env python3
"""V13 ENTRY-ONLY pool evaluator (4+ Sharpe hunt, 2026-05-29 night).

Codex consult #1+#2 consensus (handoffs/quant-engineer/2026-05-29-night-4sharpe):

The V13 portfolio_simulator continuous-mirror rebalances every poll based on follower
equity drift — even when source positions are stable. That causes 75 legs/day on the
top-30 pool smoke test and drives net PnL negative.

This evaluator bypasses portfolio_simulator's poll-rebalance and instead drives an
EVENT loop directly from journey entry_ts / exit_ts. Each wallet has a per-wallet
position book; when a source wallet opens a journey we open a follower leg sized at
1/K_target of follower equity AT ENTRY TIME, frozen for the life of the journey.
When the source closes, the follower closes that exact qty.

This is consistent with codex consensus #2:
- Entry = first nonzero exposure for a new source journey_id
- Exit = first ts after entry where journey closes (use journey.exit_ts)
- No mid-journey resize
- Equal-weight per wallet, K_target = pool_size
- Cooldown 1800s default
- Emit per-entry net PnL instrumentation

Reuses V13 infra:
- v13_execution_realism.execute_or_skip (slippage, fees, tier eligibility, IOC cap)
- v13_portfolio_ledger.CopyLedger (cash + positions + funding ledger)
- v13_walk_forward_folds.build_folds (8 folds 30/15/15)
- v13_pass_fail_gates.evaluate_gates (G1-G6)
- v13_pool_sweep_shard.load_marks_from_npz + make_candle_close_fn_npz
- v13_strict_random_null (G6 input)

Pool variants:
  - V-C: top-1 by copy_score (single wallet: 0x824)
  - V-B: top-3 by copy_score
  - V-A: copy_score > 0.02 AND n_copy_j >= 30 (relax n_src per codex if pool < 3)

Usage:
    python scripts/v13_entry_only_evaluate.py \
        --sweep-results /tmp/v13_sharded_v3_1591.parquet \
        --journeys-glob 'app/data/v13/journey_chunks/chunk_*.parquet' \
        --marks-npz-dir /tmp/v13_marks_npz/ \
        --variant V-C \
        --output /tmp/v13_4sharpe_hunt/V-C.json
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_portfolio_ledger import CopyLedger
from v13_execution_realism import (
    CoinInfo, classify_coin_tier, execute_or_skip, SLIP_TIERS, FEE_TAKER_PER_LEG_PCT,
)
from v13_walk_forward_folds import build_folds, tag_fold_regime
from v13_pass_fail_gates import FoldResult, evaluate_gates
from v13_pool_sweep_shard import load_marks_from_npz, make_candle_close_fn_npz

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_entry] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("v13_entry")


# ============================================================
# Pool selection (codex consult #2)
# ============================================================

def select_pool(sweep_df: pd.DataFrame, variant: str) -> list[str]:
    """Pool selection per codex consensus.

    V-C: top-1 by copy_score
    V-B: top-3 by copy_score
    V-A: copy_score > 0.02 AND n_copy_j >= 30 AND n_src >= 50 (relax n_src if < 3)
    V-A2: copy_score > 0.01 AND n_copy_j >= 30 (looser variant for follow-up)
    """
    df = sweep_df[sweep_df["copy_score"] > 0].sort_values("copy_score", ascending=False).reset_index(drop=True)
    if variant == "V-C":
        return df.head(1)["wallet"].tolist()
    if variant == "V-B":
        return df.head(3)["wallet"].tolist()
    if variant == "V-A":
        strict = df[(df["copy_score"] > 0.02) & (df["n_copy_j"] >= 30) & (df["n_src"] >= 50)]
        if len(strict) >= 3:
            return strict["wallet"].tolist()
        relax = df[(df["copy_score"] > 0.02) & (df["n_copy_j"] >= 30)]
        return relax["wallet"].tolist()
    if variant == "V-A2":
        return df[(df["copy_score"] > 0.01) & (df["n_copy_j"] >= 30)]["wallet"].tolist()
    # Variants picked from single-wallet evaluation (handoff 2026-05-29 night)
    if variant == "V-G":
        # Top-1 + winner-of-folds-3-to-8 (post-hoc selection — flag as overfit risk)
        return [
            "0x8242a6c699d898f8f7260848a0b38497dfaceaaa",  # 2.35 sharpe, folds 1,2,3,8
            "0x7bbfea8bcb34a30ead8a77bb207fa593e279a8d0",  # 4.85 sharpe, folds 3-8
        ]
    if variant == "V-H":
        # Top-5 by single-wallet Sharpe (DD-controlled, positive PnL only)
        return [
            "0x7bbfea8bcb34a30ead8a77bb207fa593e279a8d0",  # 4.85 sharpe
            "0xb947763b6e64b4441b02562a63fda94f6b805162",  # 3.07 sharpe (3/8 active)
            "0x8242a6c699d898f8f7260848a0b38497dfaceaaa",  # 2.35 sharpe
            "0x1404109f8cd4a79a0447365edbb7a13acd0b2f27",  # 1.41 sharpe (8/8 active)
            "0xfc8b2ac3c0cb313ad1a4bd984cb57c5b3de822b3",  # 1.73 sharpe
        ]
    if variant == "V-I":
        # Just the goldmine: 0x7bb single
        return ["0x7bbfea8bcb34a30ead8a77bb207fa593e279a8d0"]
    raise ValueError(f"Unknown variant {variant}")


# ============================================================
# Entry/exit event extraction
# ============================================================

@dataclass
class CopyEvent:
    """Either ENTRY or EXIT event for a (wallet, coin, journey_id)."""
    ts_ms: int                  # source event timestamp (entry_ts or exit_ts)
    event: str                  # "ENTRY" or "EXIT"
    wallet: str
    coin: str
    journey_id: int
    side: str                   # "long" or "short" (the SOURCE side)
    source_pct_eq: float        # max_position_pct_equity from journey (informational)
    source_max_notional: float  # max_position_notional_usd


def build_events(journeys: pd.DataFrame, fold_start_ms: int, fold_end_ms: int,
                  selected_wallets: set[str]) -> list[CopyEvent]:
    """Extract entry+exit events for journeys that OPEN within the fold window.

    Excludes:
    - Journeys whose entry_ts < fold_start_ms (would be stale/already-open at start)
    - Journeys whose entry_ts > fold_end_ms (out of window)
    Includes journeys whose exit_ts > fold_end_ms (we force-close at fold_end_ms,
    captured later in event processing).
    """
    df = journeys[journeys["wallet"].isin(selected_wallets)].copy()
    df = df[(df["entry_ts"] >= fold_start_ms) & (df["entry_ts"] < fold_end_ms)]
    events: list[CopyEvent] = []
    for _, j in df.iterrows():
        entry_ts = int(j["entry_ts"])
        exit_ts = int(j["exit_ts"])
        # Cap exit_ts at fold_end_ms — force-close
        capped_exit = min(exit_ts, fold_end_ms)
        events.append(CopyEvent(
            ts_ms=entry_ts, event="ENTRY",
            wallet=str(j["wallet"]), coin=str(j["coin"]),
            journey_id=int(j["journey_id"]), side=str(j["side"]),
            source_pct_eq=float(j.get("max_position_pct_equity", 0.0)),
            source_max_notional=float(j["max_position_notional_usd"]),
        ))
        events.append(CopyEvent(
            ts_ms=capped_exit, event="EXIT",
            wallet=str(j["wallet"]), coin=str(j["coin"]),
            journey_id=int(j["journey_id"]), side=str(j["side"]),
            source_pct_eq=float(j.get("max_position_pct_equity", 0.0)),
            source_max_notional=float(j["max_position_notional_usd"]),
        ))
    # Sort by ts then EXIT-before-ENTRY at same ts (close before opens)
    events.sort(key=lambda e: (e.ts_ms, 0 if e.event == "EXIT" else 1))
    return events


# ============================================================
# Coin info (defaults from execution_realism.classify_coin_tier)
# ============================================================

def build_coin_info(coins: list[str], default_vol_usd: float = 200_000.0) -> dict[str, CoinInfo]:
    """Build CoinInfo for each coin. Default volume = 200K → 'mid' tier (12 bps slip,
    60% fill rate). Codex m08 r5 already bumped default from 50K to 200K to keep
    most alts in the simulation universe pending real volume lookups.
    """
    out = {}
    for c in coins:
        tier = classify_coin_tier(c, volume_30d_median_1m_usd=default_vol_usd)
        out[c] = CoinInfo(
            coin=c, tier=tier,
            tick_size=0.01,
            qty_step=0.0001,
            min_order_usd=10.0,
        )
    return out


# ============================================================
# Per-entry instrumentation
# ============================================================

@dataclass
class EntryRecord:
    wallet: str
    coin: str
    journey_id: int
    side: str
    entry_ts: int
    exit_ts: int
    entry_executed: bool
    exit_executed: bool
    entry_notional: float = 0.0
    exit_notional: float = 0.0
    entry_qty: float = 0.0
    exit_qty: float = 0.0
    gross_pnl: float = 0.0
    fees: float = 0.0
    slippage_attribution: float = 0.0
    net_pnl: float = 0.0
    hold_minutes: float = 0.0
    reject_reason_entry: Optional[str] = None
    reject_reason_exit: Optional[str] = None


# ============================================================
# Entry-only fold sim
# ============================================================

@dataclass
class FoldSimResult:
    fold_n: int
    daily_returns: pd.Series
    summary: dict
    entry_records: list[EntryRecord]
    selected_wallets: list[str]


def _funding_hour_boundaries_in(start_ms: int, end_ms: int) -> list[int]:
    """Hour boundaries in (start_ms, end_ms] for funding accrual."""
    hour_ms = 3600 * 1000
    first = ((start_ms // hour_ms) + 1) * hour_ms
    return list(range(first, end_ms + 1, hour_ms))


def run_entry_only_fold(
    fold_n: int,
    fold_start_ms: int,
    fold_end_ms: int,
    selected_wallets: list[str],
    journeys: pd.DataFrame,
    candle_close_at_fn,
    K_target: int,
    latency_s: int,
    cooldown_s: int,
    starting_cash_usd: float,
    coin_info_by_coin: dict[str, CoinInfo],
    regime_tags: dict,
) -> FoldSimResult:
    """Event-driven entry-only fold simulator.

    Per-wallet target_notional at entry = follower_equity_at_entry / K_target × sign(side).
    Held constant for the journey. Exit liquidates exactly that qty.

    Cooldown: per (wallet, coin) — if last close was within cooldown_s, skip the new entry.
    """
    ledger = CopyLedger(cash_usd=starting_cash_usd, position_qty={})

    events = build_events(journeys, fold_start_ms, fold_end_ms, set(selected_wallets))

    # Per-wallet position book: (wallet, coin, journey_id) -> open qty (signed)
    # We need to track per JOURNEY because same wallet can open multiple journeys on same coin
    open_legs: dict[tuple[str, str, int], dict] = {}
    # Cooldown tracker: (wallet, coin) -> earliest_next_entry_ts_ms
    cooldown_until: dict[tuple[str, str], int] = {}

    # Funding boundary timeline (apply at each hour)
    funding_boundaries = _funding_hour_boundaries_in(fold_start_ms, fold_end_ms)
    next_funding_idx = 0

    # Daily returns tracking: snapshot equity each day at 00:00 UTC
    daily_equity: dict[date, float] = {}

    def _apply_funding_until(ts_ms: int):
        nonlocal next_funding_idx
        while next_funding_idx < len(funding_boundaries) and funding_boundaries[next_funding_idx] <= ts_ms:
            hour_ts = funding_boundaries[next_funding_idx]
            # Funding rate = 0 default (no funding source plumbed in for v0; matches
            # current pool_evaluate.py hourly_funding_rate_fn = lambda c, t: 0.0).
            # If we want funding wired in later, hourly_rate_fn = lambda c, t: lookup(c, t)
            marks = {coin: candle_close_at_fn(coin, hour_ts) for coin in list(ledger.position_qty.keys())}
            marks = {c: m for c, m in marks.items() if m is not None and m > 0}
            rates = {c: 0.0 for c in marks}
            try:
                ledger.on_funding_hour_boundary(hour_ts, marks, rates)
            except Exception as e:
                logger.warning(f"  funding boundary {hour_ts} failed: {e}")
            # Snapshot daily equity at midnight if this hour is a midnight
            day = datetime.fromtimestamp(hour_ts / 1000, tz=timezone.utc).date()
            if datetime.fromtimestamp(hour_ts / 1000, tz=timezone.utc).hour == 0:
                try:
                    eq = ledger.equity_usd_at(hour_ts, candle_close_at_fn)
                    daily_equity[day] = eq
                except Exception:
                    pass
            next_funding_idx += 1

    entry_records: list[EntryRecord] = []

    # State for matching entries to exits: (wallet, coin, journey_id) -> EntryRecord index
    record_idx_by_key: dict[tuple[str, str, int], int] = {}

    for ev in events:
        # Sanity bound
        if ev.ts_ms < fold_start_ms or ev.ts_ms > fold_end_ms:
            continue

        # Apply funding up to this ts (before any cash change)
        _apply_funding_until(ev.ts_ms)

        # Execution time = source event ts + latency
        exec_ts = ev.ts_ms + latency_s * 1000
        if exec_ts > fold_end_ms:
            continue
        mark = candle_close_at_fn(ev.coin, exec_ts)
        if mark is None or mark <= 0:
            continue
        ci = coin_info_by_coin.get(ev.coin)
        if ci is None:
            continue

        key = (ev.wallet, ev.coin, ev.journey_id)
        cd_key = (ev.wallet, ev.coin)

        if ev.event == "ENTRY":
            # Cooldown gate
            if cooldown_until.get(cd_key, 0) > exec_ts:
                continue
            # Sizing: equal weight per wallet, K_target slot
            try:
                eq_now = ledger.equity_usd_at(exec_ts, candle_close_at_fn)
            except ValueError:
                continue
            slot_notional = eq_now / K_target
            side_sign = 1 if ev.side == "long" else -1
            delta_usd = side_sign * slot_notional
            qty_before = ledger.position_qty.get(ev.coin, 0.0)
            res = execute_or_skip(ev.coin, exec_ts, delta_usd, mark, ledger, ci)
            rec = EntryRecord(
                wallet=ev.wallet, coin=ev.coin, journey_id=ev.journey_id, side=ev.side,
                entry_ts=ev.ts_ms, exit_ts=0,
                entry_executed=res.executed, exit_executed=False,
                reject_reason_entry=res.reject_reason,
            )
            if res.executed:
                leg = res.leg
                rec.entry_qty = side_sign * leg.qty
                rec.entry_notional = leg.qty * leg.executable_px * side_sign
                rec.fees += leg.fee_usd
                rec.slippage_attribution += leg.slip_attribution_usd
                open_legs[key] = {
                    "qty_signed": side_sign * leg.qty,
                    "entry_px": leg.executable_px,
                    "entry_ts_exec": exec_ts,
                }
            entry_records.append(rec)
            record_idx_by_key[key] = len(entry_records) - 1

        elif ev.event == "EXIT":
            leg_state = open_legs.pop(key, None)
            idx = record_idx_by_key.pop(key, None)
            if leg_state is None or idx is None:
                # Entry was not executed (rejected). Nothing to close.
                continue
            qty_open = leg_state["qty_signed"]
            if qty_open == 0:
                continue
            # Closing trade: opposite sign
            close_side_sign = -np.sign(qty_open)
            close_delta_usd = close_side_sign * abs(qty_open) * mark
            res = execute_or_skip(ev.coin, exec_ts, close_delta_usd, mark, ledger, ci)
            rec = entry_records[idx]
            rec.exit_ts = ev.ts_ms
            rec.exit_executed = res.executed
            if res.executed:
                leg = res.leg
                rec.exit_qty = close_side_sign * leg.qty
                rec.exit_notional = leg.qty * leg.executable_px * close_side_sign
                rec.fees += leg.fee_usd
                rec.slippage_attribution += leg.slip_attribution_usd
                # gross pnl = qty_open × (exit_px - entry_px)
                #          = qty_signed × (exit - entry)  ✓ same as PnL convention
                rec.gross_pnl = qty_open * (leg.executable_px - leg_state["entry_px"])
                rec.net_pnl = rec.gross_pnl - rec.fees
                rec.hold_minutes = (ev.ts_ms - leg_state["entry_ts_exec"]) / 60_000.0
            else:
                rec.reject_reason_exit = res.reject_reason
                # If exit rejected, we still need to flatten; force a flat trade at fold end
                # later or accept that the position carries to fold end and is MTM'd.
            # Set cooldown
            cooldown_until[cd_key] = exec_ts + cooldown_s * 1000

    # End-of-fold MTM: any remaining open positions priced at fold_end_ms
    end_eq = None
    try:
        # Apply final funding
        _apply_funding_until(fold_end_ms)
        # Force-close any still-open legs at fold_end_ms mark
        for key, leg_state in list(open_legs.items()):
            wallet, coin, jid = key
            mark = candle_close_at_fn(coin, fold_end_ms)
            if mark is None or mark <= 0:
                continue
            ci = coin_info_by_coin.get(coin)
            if ci is None:
                continue
            qty_open = leg_state["qty_signed"]
            if qty_open == 0:
                continue
            close_side_sign = -np.sign(qty_open)
            close_delta_usd = close_side_sign * abs(qty_open) * mark
            res = execute_or_skip(coin, fold_end_ms, close_delta_usd, mark, ledger, ci)
            idx = record_idx_by_key.get(key)
            if idx is not None and res.executed:
                leg = res.leg
                rec = entry_records[idx]
                rec.exit_ts = fold_end_ms
                rec.exit_executed = True
                rec.exit_qty = close_side_sign * leg.qty
                rec.exit_notional = leg.qty * leg.executable_px * close_side_sign
                rec.fees += leg.fee_usd
                rec.slippage_attribution += leg.slip_attribution_usd
                rec.gross_pnl = qty_open * (leg.executable_px - leg_state["entry_px"])
                rec.net_pnl = rec.gross_pnl - rec.fees
                rec.hold_minutes = (fold_end_ms - leg_state["entry_ts_exec"]) / 60_000.0
            open_legs.pop(key, None)
        end_eq = ledger.equity_usd_at(fold_end_ms, candle_close_at_fn)
    except ValueError as e:
        logger.warning(f"  fold end equity overflow: {e}")
        end_eq = starting_cash_usd  # graceful

    # Build daily_returns from daily_equity snapshots
    sorted_days = sorted(daily_equity.keys())
    if len(sorted_days) >= 2:
        # daily return = eq[d] / eq[d-1] - 1 ... but cleaner: convert eq series to daily pct change
        eq_series = pd.Series([daily_equity[d] for d in sorted_days], index=pd.to_datetime(sorted_days))
        daily_returns = eq_series.pct_change().dropna()
    else:
        daily_returns = pd.Series(dtype=float)

    # Summary metrics
    net_pnl = (end_eq - starting_cash_usd) if end_eq is not None else 0.0
    if len(daily_returns) > 0 and daily_returns.std() > 0:
        sharpe = float(daily_returns.mean() / daily_returns.std() * np.sqrt(365))
    else:
        sharpe = 0.0
    if len(daily_returns) > 0:
        eq_curve = (1 + daily_returns).cumprod()
        peak = eq_curve.cummax()
        dd = (eq_curve - peak) / peak
        max_dd = float(abs(dd.min())) if len(dd) > 0 else 0.0
        worst_day = float(daily_returns.min())
    else:
        max_dd = 0.0
        worst_day = 0.0

    n_legs = sum(1 for r in entry_records if r.entry_executed) + \
             sum(1 for r in entry_records if r.exit_executed)
    total_fees = sum(r.fees for r in entry_records)
    total_slip = sum(r.slippage_attribution for r in entry_records)

    summary = {
        "net_pnl": net_pnl,
        "sharpe": sharpe,
        "max_dd_pct": max_dd,
        "worst_day_pct": worst_day,
        "n_legs": n_legs,
        "n_entries_attempted": sum(1 for r in entry_records),
        "n_entries_executed": sum(1 for r in entry_records if r.entry_executed),
        "n_exits_executed": sum(1 for r in entry_records if r.exit_executed),
        "fee_drag": total_fees,
        "slip_drag": total_slip,
        "starting_cash": starting_cash_usd,
        "end_equity": end_eq,
    }

    return FoldSimResult(
        fold_n=fold_n,
        daily_returns=daily_returns,
        summary=summary,
        entry_records=entry_records,
        selected_wallets=selected_wallets,
    )


# ============================================================
# Main
# ============================================================

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sweep-results", required=True)
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--marks-npz-dir", required=True)
    ap.add_argument("--variant", required=True,
                    choices=["V-A", "V-A2", "V-B", "V-C", "V-G", "V-H", "V-I"])
    ap.add_argument("--latency-s", type=int, default=60)
    ap.add_argument("--cooldown-s", type=int, default=1800)
    ap.add_argument("--starting-cash", type=float, default=10_000.0)
    ap.add_argument("--n-folds", type=int, default=8)
    ap.add_argument("--window-start", default="2025-12-01")
    ap.add_argument("--output", required=True)
    ap.add_argument("--per-entry-out", default=None,
                    help="If set, write per-entry records to this CSV")
    ap.add_argument("--exclude-coins-prefix", default="",
                    help="Comma-separated coin name PREFIXES to exclude (e.g. 'xyz:,flx:' to drop HL xyz/flx dexes)")
    ap.add_argument("--min-hold-minutes", type=float, default=0.0,
                    help="Filter out source journeys with duration_hours below this threshold")
    args = ap.parse_args()

    sweep = pd.read_parquet(args.sweep_results)
    selected = select_pool(sweep, args.variant)
    K_target = max(1, len(selected))
    logger.info(f"Variant {args.variant}: pool size {len(selected)}, K_target={K_target}")
    for w in selected:
        sc = float(sweep[sweep["wallet"] == w]["copy_score"].iloc[0])
        logger.info(f"  {w[:18]}...  copy_score={sc:.5f}")

    # Load journeys for selected wallets only
    chunks = sorted(glob.glob(args.journeys_glob))
    if not chunks:
        logger.error(f"No journey chunks at {args.journeys_glob}")
        sys.exit(1)
    sel_set = set(selected)
    logger.info(f"Loading {len(chunks)} chunks (filter to {len(sel_set)} wallets)...")
    dfs = []
    for c in chunks:
        d = pd.read_parquet(c, filters=[("wallet", "in", list(sel_set))])
        if len(d) > 0:
            dfs.append(d)
    journeys = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    logger.info(f"  {len(journeys):,} journeys for selected pool")

    # Coin prefix exclusion (e.g. xyz:, flx:)
    if args.exclude_coins_prefix:
        prefixes = [p.strip() for p in args.exclude_coins_prefix.split(",") if p.strip()]
        before = len(journeys)
        mask = pd.Series(False, index=journeys.index)
        for p in prefixes:
            mask = mask | journeys["coin"].str.startswith(p)
        journeys = journeys[~mask]
        logger.info(f"  excluded {before - len(journeys):,} journeys with coin prefix in {prefixes}; "
                    f"{len(journeys):,} remain")
    # Min hold filter
    if args.min_hold_minutes > 0 and "duration_hours" in journeys.columns:
        before = len(journeys)
        journeys = journeys[journeys["duration_hours"] * 60.0 >= args.min_hold_minutes]
        logger.info(f"  excluded {before - len(journeys):,} journeys with hold < {args.min_hold_minutes}min; "
                    f"{len(journeys):,} remain")

    # Coin info
    coins_needed = set(journeys["coin"].unique().tolist())
    logger.info(f"  {len(coins_needed)} distinct coins in pool journeys")
    coin_info = build_coin_info(list(coins_needed))

    # Marks
    mark_arrays = load_marks_from_npz(args.marks_npz_dir, coins_needed)
    candle_fn = make_candle_close_fn_npz(mark_arrays)

    # Folds
    window_start = date.fromisoformat(args.window_start)
    folds = build_folds(window_start, n_folds=args.n_folds)
    logger.info(f"Built {len(folds)} folds")

    fold_results: list[FoldResult] = []
    all_entry_records: list[dict] = []
    fold_summaries_full = []

    for fold in folds:
        logger.info(f"=== Fold {fold.n}: test {fold.test_start} → {fold.test_end} ===")
        test_start_ms = int(datetime.combine(fold.test_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
        test_end_ms = int(datetime.combine(fold.test_end + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)

        # Regime tag
        def _market_data_at(d):
            ts_ms = int(datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
            btc_close = candle_fn("BTC", ts_ms)
            return {"btc_price": btc_close, "hl_perp_price": btc_close, "btc_dvol": None}
        try:
            regime_tags = tag_fold_regime(fold, _market_data_at)
        except Exception as e:
            logger.warning(f"  regime tag fail: {e}")
            regime_tags = {"trend": "UNKNOWN", "vol": "UNKNOWN"}

        t0 = time.time()
        try:
            fold_sim = run_entry_only_fold(
                fold_n=fold.n,
                fold_start_ms=test_start_ms,
                fold_end_ms=test_end_ms,
                selected_wallets=selected,
                journeys=journeys,
                candle_close_at_fn=candle_fn,
                K_target=K_target,
                latency_s=args.latency_s,
                cooldown_s=args.cooldown_s,
                starting_cash_usd=args.starting_cash,
                coin_info_by_coin=coin_info,
                regime_tags=regime_tags,
            )
        except Exception as e:
            logger.error(f"  fold {fold.n} sim failed: {e}", exc_info=True)
            fold_results.append(FoldResult(
                fold_n=fold.n, daily_returns=pd.Series(dtype=float),
                summary={"net_pnl": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0,
                         "worst_day_pct": 0.0, "n_legs": 0, "fee_drag": 0.0, "slip_drag": 0.0},
                regime_tags=regime_tags, anti_corr_pruned=False,
            ))
            continue
        dt = time.time() - t0
        logger.info(f"  fold {fold.n}: sharpe={fold_sim.summary['sharpe']:.3f}, "
                    f"net_pnl=${fold_sim.summary['net_pnl']:.0f}, "
                    f"max_dd={fold_sim.summary['max_dd_pct']:.1%}, "
                    f"n_legs={fold_sim.summary['n_legs']}, "
                    f"entries={fold_sim.summary['n_entries_executed']}/{fold_sim.summary['n_entries_attempted']}, "
                    f"wall={dt:.1f}s")

        fold_results.append(FoldResult(
            fold_n=fold.n,
            daily_returns=fold_sim.daily_returns,
            summary=fold_sim.summary,
            regime_tags=regime_tags,
            anti_corr_pruned=False,
        ))
        fold_summaries_full.append({
            "fold_n": fold.n,
            "test_start": str(fold.test_start),
            "test_end": str(fold.test_end),
            "summary": fold_sim.summary,
            "regime_tags": regime_tags,
        })
        for r in fold_sim.entry_records:
            all_entry_records.append({
                "fold_n": fold.n,
                **{k: v for k, v in r.__dict__.items()},
            })

    # Module 10 gates (no random_null this pass — we'll add G6 in follow-up if any
    # variant clears G1-G5)
    decision = evaluate_gates(fold_results=fold_results, random_null=None)
    logger.info(f"")
    logger.info(f"=== {args.variant} Decision ===")
    logger.info(f"  go: {decision.go}")
    logger.info(f"  failures: {decision.failures}")
    logger.info(f"  summary: {decision.summary}")

    out = {
        "variant": args.variant,
        "pool_size": len(selected),
        "pool_wallets": selected,
        "K_target": K_target,
        "latency_s": args.latency_s,
        "cooldown_s": args.cooldown_s,
        "n_folds": args.n_folds,
        "fold_summaries": fold_summaries_full,
        "decision": {
            "go": decision.go,
            "failures": list(decision.failures),
            "summary": decision.summary,
        },
        "total_entry_records": len(all_entry_records),
    }
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    Path(args.output).write_text(json.dumps(out, indent=2, default=str))
    logger.info(f"Wrote {args.output}")

    if args.per_entry_out and all_entry_records:
        Path(args.per_entry_out).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(all_entry_records).to_csv(args.per_entry_out, index=False)
        logger.info(f"Wrote per-entry CSV: {args.per_entry_out}")


if __name__ == "__main__":
    main()
