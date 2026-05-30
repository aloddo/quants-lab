#!/usr/bin/env python3
"""V13 Module 08 — Portfolio Simulator (aggregate-first).

Per spec: projects/quant/v13/modules/08-portfolio-simulator

Aggregate-first simulator: at each poll, gather ALL selected wallet states, AGGREGATE
into target_pct per coin (1/N weighted), diff vs OUR actual position, generate ONE
rebalance per coin per poll.

This is the V13 LIVE engine architecture. Backtest MUST match for valid validation.

Differs from Module 04 (per-wallet ranking sim) by:
- Iterates selected POOL, not per-wallet
- Aggregates signals BEFORE generating fills (no double-counting offsetting positions)
- Output: daily returns + leg ledger + summary metrics (for Module 10 gates)

Dependencies (all canonical/tested):
- Module 05 v13_execution_realism.execute_or_skip + CoinInfo + SLIP_TIERS
- Module 06 v13_cold_start.ColdStartState
- Module 07 v13_portfolio_ledger.CopyLedger

THIS REPLACES the prior DRAFT/DO-NOT-RUN file (2026-05-29 10:45 stub).
"""
from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_cold_start import ColdStartState  # noqa: E402
from v13_execution_realism import (  # noqa: E402
    CoinInfo,
    SLIP_TIERS,
    execute_or_skip,
)
from v13_portfolio_ledger import CopyLedger  # noqa: E402

logger = logging.getLogger("v13_portfolio_simulator")

# === Constants (Module 08 spec) ===
PER_COIN_CAP_DEFAULT = 0.25      # 25% per coin max
GROSS_CAP_DEFAULT = 1.0          # 1.0x gross during live-shadow
COOLDOWN_S_DEFAULT = 120         # post-flat cooldown
STARTING_PORTFOLIO_EQUITY = 1000.0


@dataclass
class SimParams:
    K_target: int
    poll_interval_s: int
    latency_s: int
    # anti_corr_threshold: used by Module 09 for pool SELECTION, not by this simulator.
    # Carried in SimParams for parameter-identity (param-hash key consistency); has NO effect here.
    anti_corr_threshold: float
    gross_cap: float = GROSS_CAP_DEFAULT
    per_coin_cap: float = PER_COIN_CAP_DEFAULT
    cooldown_s: int = COOLDOWN_S_DEFAULT


@dataclass
class SimResult:
    daily_returns: pd.Series           # index date → daily return pct
    legs: list                         # list of Leg objects
    summary: dict                      # net_pnl, sharpe, max_dd_pct, worst_day_pct, n_legs, fee_drag, slip_drag


def _funding_hour_boundaries_in(window_start_ms: int, window_end_ms: int) -> list[int]:
    """Generate UTC hour boundaries (ms epochs) in [window_start_ms, window_end_ms]."""
    HOUR_MS = 3600_000
    first = ((window_start_ms + HOUR_MS - 1) // HOUR_MS) * HOUR_MS
    last = (window_end_ms // HOUR_MS) * HOUR_MS
    return list(range(first, last + 1, HOUR_MS))


def run_portfolio_simulator(
    selected_pool: list[str],
    params: SimParams,
    window_start_ms: int,
    window_end_ms: int,
    source_state_at_poll_fn: Callable[[str, int], dict[str, dict]],
    source_equity_at_fn: Callable[[str, int], Optional[float]],
    coin_info_by_coin: dict[str, CoinInfo],
    candle_close_at_fn: Callable[[str, int], Optional[float]],
    hourly_funding_rate_fn: Callable[[str, int], Optional[float]],
    cold_start_seed: Optional[dict[str, dict[str, float]]] = None,
    starting_cash_usd: float = STARTING_PORTFOLIO_EQUITY,
) -> SimResult:
    """Run a single portfolio simulator pass.

    Args:
        selected_pool: wallet addresses to copy
        params: SimParams
        window_start_ms, window_end_ms: simulation window (ms epoch)
        source_state_at_poll_fn(wallet, ts_ms) → {coin: {"size": float, "signed_notional": float}}
        source_equity_at_fn(wallet, ts_ms) → float | None
        coin_info_by_coin: tier/qty_step/min_order_usd per coin
        candle_close_at_fn(coin, ts_ms) → float | None
        hourly_funding_rate_fn(coin, hour_ms) → float | None
        cold_start_seed: optional {wallet: {coin: size}} for pool entry pre-existing positions
        starting_cash_usd: initial equity

    Returns SimResult.
    """
    ledger = CopyLedger(cash_usd=starting_cash_usd)
    cold_starts: dict[str, ColdStartState] = {}
    if cold_start_seed:
        for w, positions in cold_start_seed.items():
            cs = ColdStartState()
            cs.initialize_pool_entry(positions)
            cold_starts[w] = cs

    poll_ts_list = list(range(window_start_ms, window_end_ms, params.poll_interval_s * 1000))
    # codex m08 r14 fix: funding boundaries must span through the LATEST possible effective
    # poll execution time, not just window_end_ms. A poll at window_end_ms - 30s with
    # latency_s=60 executes at window_end_ms + 30s; if a funding boundary at HH:00 falls
    # between, it must be in the event list so the position held at HH:00 is funded.
    latency_ms_for_funding = params.latency_s * 1000
    max_effective_ts = (max(poll_ts_list) + latency_ms_for_funding) if poll_ts_list else window_end_ms
    funding_end_ms = max(window_end_ms, max_effective_ts)
    funding_hours = _funding_hour_boundaries_in(window_start_ms, funding_end_ms)
    # codex m08 r13 fix: events must be ordered by EFFECTIVE-time, not poll_ts. A poll's
    # ledger mutation happens at copy_exec_ts = poll_ts + latency_s. If a funding boundary
    # falls between poll_ts and copy_exec_ts, the prior implementation sorted by poll_ts so
    # the fill landed BEFORE the funding boundary in iteration order, causing funding to be
    # charged on a position that didn't exist yet. By keying on effective-time, the funding
    # event interleaves correctly.
    latency_ms = params.latency_s * 1000
    # Build (effective_ts, type, original_ts) tuples then sort. type: "funding" or "poll".
    typed_events: list[tuple[int, str, int]] = []
    poll_ts_set = set(poll_ts_list)
    funding_set = set(funding_hours)
    for t in funding_set:
        typed_events.append((t, "funding", t))
    for t in poll_ts_set:
        typed_events.append((t + latency_ms, "poll", t))
    # Sort by effective_ts; if a funding and poll-exec collide on the same ms, funding wins
    # ("funding" < "poll" lexically as desired — funding applies first under HL semantics).
    typed_events.sort(key=lambda e: (e[0], e[1]))

    cooldown_until: dict[str, int] = {}
    # codex m08 r3 fix: track which coins are in "sign-flip close-only mode" since residue
    # from partial fills would otherwise trigger sign-flip detection every subsequent poll
    # and prevent ever opening the deferred opposite-side position.
    sign_flip_close_pending: dict[str, int] = {}   # coin → ms ts when close-only was first set
    executed_legs: list = []
    # codex m08 r1 CRITICAL fix: track per-day EQUITY (cash + MTM), not cash deltas.
    # Cash deltas treat opening a long as a "loss" — fatal for Sharpe/DD/worst-day metrics.
    daily_equity_close: dict = {}   # date → end-of-day equity

    for effective_ts, ev_type, original_ts in typed_events:
        # 1) Apply funding boundary at its TRUE time (effective_ts == funding_ts since
        #    funding has no latency).
        if ev_type == "funding":
            if ledger.position_qty:
                marks = {c: candle_close_at_fn(c, effective_ts) for c in ledger.position_qty}
                rates = {c: hourly_funding_rate_fn(c, effective_ts) for c in ledger.position_qty}
                marks = {c: m for c, m in marks.items() if m is not None and m > 0}
                rates = {c: r for c, r in rates.items() if r is not None}
                try:
                    ledger.on_funding_hour_boundary(effective_ts, marks, rates)
                except ValueError as e:
                    logger.warning(f"funding boundary {effective_ts} skipped: {e}")
                # codex m08 r12 fix: update daily_equity_close after funding so funding-only
                # events (no later poll on the same day, or funding after the final poll
                # before window_end) are reflected in summary equity / daily_returns / net_pnl.
                # codex m08 r13 fix: bucket by EFFECTIVE-time day (funding_ts).
                try:
                    eq_after_funding = ledger.equity_usd_at(effective_ts, candle_close_at_fn)
                    # codex m08 perf: int arithmetic for day-bucket (pd.Timestamp.floor was hot)
                    funding_day = (effective_ts // 86_400_000) * 86_400_000
                    daily_equity_close[funding_day] = eq_after_funding
                except ValueError:
                    pass  # mark missing → leave prior EoD intact
            continue

        # 2) Poll logic — only reached for ev_type == "poll"
        poll_ts = original_ts

        # codex m08 r1 DESIGN-NIT fix: cache poll state per wallet (was called 2x — cold-start + signals).
        poll_state_cache: dict[str, dict] = {w: source_state_at_poll_fn(w, poll_ts) for w in selected_pool}

        # Update cold-start state per wallet
        for w in selected_pool:
            if w not in cold_starts:
                continue
            src_state = poll_state_cache[w]
            poll_sizes = {coin: pos.get("size", 0.0) for coin, pos in src_state.items()}
            cold_starts[w].update_from_poll(poll_sizes)

        # Gather wallet signals; track wallets with valid equity for 1/N denominator.
        # codex m08 r1 fix #2: 1/N denominator must include ALL pool-eligible wallets (with valid
        # equity), not just those emitting signals. Otherwise flat wallets get excluded → over-weight.
        wallet_signals: dict[str, dict[str, float]] = {}
        n_pool_eligible = 0
        for w in selected_pool:
            src_state = poll_state_cache[w]
            src_eq = source_equity_at_fn(w, poll_ts)
            if src_eq is None or src_eq <= 0:
                continue
            n_pool_eligible += 1
            cs = cold_starts.get(w)
            for coin, pos in src_state.items():
                if cs and not cs.is_allowed(coin):
                    continue
                ci = coin_info_by_coin.get(coin)
                if ci is None or ci.tier not in SLIP_TIERS["pool_eligible_tiers"]:
                    continue
                signed_notional = pos.get("signed_notional", 0.0)
                wallet_signals.setdefault(w, {})[coin] = signed_notional / src_eq

        # codex m08 r1: target_pct may be empty if all wallets flat → we still need to close
        # existing positions via coins_to_check ∪ current_positions below.
        target_pct: dict[str, float] = {}
        if wallet_signals and n_pool_eligible > 0:
            all_coins = set()
            for sigs in wallet_signals.values():
                all_coins.update(sigs.keys())
            for coin in all_coins:
                raw = sum(wallet_signals.get(w, {}).get(coin, 0.0) for w in wallet_signals) / n_pool_eligible
                target_pct[coin] = max(-params.per_coin_cap, min(params.per_coin_cap, raw))

        # Gross cap rescale
        gross = sum(abs(v) for v in target_pct.values())
        if gross > params.gross_cap:
            target_pct = {c: v * params.gross_cap / gross for c, v in target_pct.items()}

        # Generate rebalances
        copy_exec_ts = poll_ts + params.latency_s * 1000
        try:
            our_eq_now = ledger.equity_usd_at(copy_exec_ts, candle_close_at_fn)
        except ValueError:
            logger.warning(f"equity overflow at poll {poll_ts}; skipping")
            continue

        coins_to_check = set(target_pct.keys()) | {c for c, q in ledger.position_qty.items() if q != 0}

        for coin in sorted(coins_to_check):
            target_notional = target_pct.get(coin, 0.0) * our_eq_now
            current_qty = ledger.position_qty.get(coin, 0.0)

            # codex m08 r10 fix: stale-pending lifecycle clearing MUST happen BEFORE any
            # `continue` gate (missing mark, missing coin_info, early delta skip), because
            # those gates would otherwise let a stale pending marker survive into a later
            # poll where the source flips again. Sign-of-flip needs only the signs of
            # `target_notional` and `current_qty` (no mark needed for sign).
            # codex m08 r11 fix: also clear the cooldown that was paired with pending when
            # State A set it. Without this, a same-side rebuild after the flip is cancelled
            # remains blocked by stale cooldown until expiry → underexposure window.
            # Safe because pending uniquely marks State A's lifecycle; the post-flat re-entry
            # cooldown is set on a separate path (without pending). When pending is popped,
            # any cooldown set by State A is dead too.
            tgt_sign = np.sign(target_notional)
            cur_sign = np.sign(current_qty)
            sign_flip_active = (cur_sign != 0 and tgt_sign != 0 and cur_sign != tgt_sign)
            if coin in sign_flip_close_pending and not sign_flip_active:
                sign_flip_close_pending.pop(coin, None)
                cooldown_until.pop(coin, None)

            mark = candle_close_at_fn(coin, copy_exec_ts)
            if mark is None or mark <= 0:
                continue
            current_notional = current_qty * mark
            delta_usd = target_notional - current_notional

            ci = coin_info_by_coin.get(coin)
            if ci is None:
                continue
            # codex m08 r9 fix: compute is_sign_flip BEFORE early-delta-skip so cooldown
            # branch reads consistent state. (Stale clear already done above; the local
            # `is_sign_flip` here is recomputed with mark for downstream State A/B/C logic.)
            target_qty_signed = target_notional / mark if mark > 0 else 0.0
            is_open_or_reentry = (
                (current_qty == 0 and target_qty_signed != 0) or
                (current_qty != 0 and np.sign(target_qty_signed) == np.sign(current_qty)
                 and abs(target_qty_signed) > abs(current_qty))
            )
            is_sign_flip = (
                current_qty != 0 and target_qty_signed != 0
                and np.sign(target_qty_signed) != np.sign(current_qty)
            )
            if abs(delta_usd) < 10 or (target_notional != 0 and abs(delta_usd) < 0.20 * abs(target_notional)):
                continue
            # Cooldown-active block (existing).
            if is_open_or_reentry and cooldown_until.get(coin, 0) > copy_exec_ts:
                continue
            # codex m08 r2+r3 fix: sign-flip state machine
            #   State A: sign-flip detected, no close-only pending → CLOSE-ONLY, set pending+cooldown
            #   State B: sign-flip with close-only pending + cooldown ACTIVE → skip (waiting)
            #   State C: sign-flip with close-only pending + cooldown EXPIRED → clear pending,
            #            allow NORMAL execution (will execute full delta = close residue + open opposite)
            if is_sign_flip:
                pending = sign_flip_close_pending.get(coin)
                # codex m08 r5+r6 fix: dust shortcut is PRE-State-A only — must NOT bypass
                # State B cooldown. If pending exists AND cooldown active, skip (State B).
                if pending is not None and cooldown_until.get(coin, 0) > copy_exec_ts:
                    continue
                # codex m08 r5 fix: if no pending AND dust residue, treat as flat
                # (execute_or_skip would reject min_order; just open opposite directly).
                if pending is None and abs(current_notional) < ci.min_order_usd:
                    # Fall through to normal execute_or_skip
                    pass
                elif pending is None:
                    # State A: first sign-flip detection
                    close_delta_usd = -current_notional
                    res_close = execute_or_skip(coin, copy_exec_ts, close_delta_usd, mark, ledger, ci)
                    if res_close.executed:
                        executed_legs.append(res_close.leg)
                    # codex m08 r5 fix: ALWAYS set pending + cooldown after attempting State A,
                    # regardless of execution result. Prevents infinite retry on rejection.
                    cooldown_until[coin] = copy_exec_ts + params.cooldown_s * 1000
                    sign_flip_close_pending[coin] = copy_exec_ts
                    continue
                else:
                    # State C: pending exists, cooldown expired → attempt normal exec.
                    res = execute_or_skip(coin, copy_exec_ts, delta_usd, mark, ledger, ci)
                    if res.executed:
                        executed_legs.append(res.leg)
                        post_qty = ledger.position_qty.get(coin, 0.0)
                        if abs(post_qty) < 1e-9 or (post_qty != 0 and np.sign(post_qty) != np.sign(current_qty)):
                            sign_flip_close_pending.pop(coin, None)
                        else:
                            cooldown_until[coin] = copy_exec_ts + params.cooldown_s * 1000
                    else:
                        # codex m08 r5 fix: State C rejection → reset cooldown to backoff
                        # (prevents retry every poll when execution can't make progress).
                        cooldown_until[coin] = copy_exec_ts + params.cooldown_s * 1000
                    continue  # State C handled — don't fall through to normal path

            res = execute_or_skip(coin, copy_exec_ts, delta_usd, mark, ledger, ci)
            if res.executed:
                executed_legs.append(res.leg)
                if abs(res.qty_before) > 0 and abs(res.qty_after) < 1e-9:
                    cooldown_until[coin] = copy_exec_ts + params.cooldown_s * 1000
                # codex m08 r7 fix: clear stale sign_flip_close_pending when normal exec
                # rebuilds a same-side position (no longer in close-only residue lifecycle).
                # Otherwise next flip would skip State A and execute full delta in one poll.
                if coin in sign_flip_close_pending:
                    post_qty = ledger.position_qty.get(coin, 0.0)
                    # If position is meaningfully non-residue, clear pending
                    if abs(post_qty * mark) >= ci.min_order_usd:
                        sign_flip_close_pending.pop(coin, None)

        # codex m08 r1 CRITICAL fix #1: track end-of-poll EQUITY (cash + MTM), not cash delta.
        # Cash deltas treat opening a position as loss → Sharpe/DD/worst-day all wrong.
        # codex m08 r13 fix: bucket by EFFECTIVE-time day (copy_exec_ts) not poll_ts. A
        # late-night poll at 23:59:30 with 60s latency executes at 00:00:30 of the next
        # UTC day; its equity belongs to the next day's bucket (was distorting Sharpe / DD
        # / worst_day around midnight).
        try:
            eq_after_poll = ledger.equity_usd_at(copy_exec_ts, candle_close_at_fn)
        except ValueError:
            eq_after_poll = our_eq_now  # fallback
        # codex m08 perf: int arithmetic for day-bucket (pd.Timestamp.floor was hot in profiler)
        day = (copy_exec_ts // 86_400_000) * 86_400_000
        # End-of-day equity = LATEST equity within that day (last poll's eq_after)
        daily_equity_close[day] = eq_after_poll

    # Build daily returns from EQUITY (not cash deltas — codex m08 r1 CRITICAL fix)
    eq_series = pd.Series(daily_equity_close).sort_index()
    if len(eq_series) == 0:
        return SimResult(
            daily_returns=pd.Series(dtype=float),
            legs=executed_legs,
            summary={
                "net_pnl": 0.0, "sharpe": 0.0, "max_dd_pct": 0.0, "worst_day_pct": 0.0,
                "n_legs": len(executed_legs), "fee_drag": 0.0, "slip_drag": 0.0,
            },
        )
    # Prepend starting equity at "day before first poll" so first daily return is well-defined
    # codex m08 perf: int day-bucket means index is int ms; subtract 1 day in ms
    first_day = int(eq_series.index[0]) - 86_400_000
    eq_with_start = pd.concat([pd.Series([starting_cash_usd], index=[first_day]), eq_series])
    eq_with_start = eq_with_start.sort_index()
    # Daily return = pct change in equity
    daily_returns_pct = eq_with_start.pct_change().dropna()
    daily_pnl_usd = eq_with_start.diff().dropna()

    sharpe = 0.0
    if len(daily_returns_pct) > 1 and daily_returns_pct.std() > 0:
        sharpe = float(daily_returns_pct.mean() / daily_returns_pct.std() * np.sqrt(365))

    # Max DD = peak-to-trough drawdown in equity
    running_max = eq_with_start.cummax()
    dd = (eq_with_start - running_max) / running_max.replace(0, np.nan)
    max_dd = float(abs(dd.min())) if not dd.empty and not dd.isna().all() else 0.0
    worst_day = float(daily_returns_pct.min()) if len(daily_returns_pct) > 0 else 0.0
    net_pnl = float(eq_series.iloc[-1] - starting_cash_usd)

    total_fee = sum(l.fee_usd for l in executed_legs)
    total_slip = sum(l.slip_attribution_usd for l in executed_legs)

    return SimResult(
        daily_returns=daily_returns_pct,
        legs=executed_legs,
        summary={
            "net_pnl": net_pnl,
            "sharpe": sharpe,
            "max_dd_pct": max_dd,
            "worst_day_pct": worst_day,
            "n_legs": len(executed_legs),
            "fee_drag": total_fee,
            "slip_drag": total_slip,
        },
    )
