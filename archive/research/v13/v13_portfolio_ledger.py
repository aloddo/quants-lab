#!/usr/bin/env python3
"""V13 Portfolio Equity Ledger (Module 07).

Single source of truth for our recursive equity state — cash + signed positions per coin +
funding accrual. Used by:
  - Module 04 (ranking-copy-simulator) — per-wallet copy sim
  - Module 08 (portfolio-simulator) — aggregate portfolio sim
  - Live engine — persistent state via reconcile against clearinghouseState

Per Module 07 spec at projects/quant/v13/modules/07-portfolio-equity-ledger.

CORE INVARIANTS:
- position_qty: signed contract qty per coin (e.g., 0.1 BTC; -2.0 ETH for short)
- cash_usd: USD balance (deposits, withdrawals, leg cashflows, fees, funding accruals)
- equity_usd_at(t, candle_close_at_fn) = cash + sum(qty × mark_at_t)

SIGN CONVENTIONS:
- BUY (side=+1): cash OUTFLOW (-), position qty INCREASE (+qty)
- SELL (side=-1): cash INFLOW (+), position qty DECREASE (-qty)
- Funding hourly rate POSITIVE means longs pay shorts:
    funding_cashflow = -signed_notional × hourly_rate
    long + positive rate → negative cash (we pay)
    short + positive rate → positive cash (we receive)

NO PARTIAL-HOUR PRORATION. HL settles funding at top of each hour. Position held 90 sec into
hour pays nothing for that hour; position held past next boundary pays full hour.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable

EPS = 1e-9


def _is_valid_finite(x: float) -> bool:
    """True iff x is finite (not NaN, not Inf).

    codex m07 r3 fix: explicitly REJECT bool. In Python `bool` subclasses `int`, so
    `isinstance(True, (int, float))` is True. Without this filter, side=True/qty=False
    would pass numeric validators and corrupt the ledger.

    codex m07 r6 fix: huge Python int → float(x) raises OverflowError; catch and return False
    so caller's ValueError contract is preserved (no leaked OverflowError).
    """
    if isinstance(x, bool):
        return False
    if not isinstance(x, (int, float)):
        return False
    try:
        return math.isfinite(float(x))
    except (OverflowError, ValueError):
        return False


def _is_valid_positive_mark(x: float | None) -> bool:
    """True iff x is a non-None, non-bool, finite, positive mark."""
    if x is None:
        return False
    if not _is_valid_finite(x):
        return False
    return float(x) > 0


@dataclass
class CopyLedger:
    """Per-wallet (Module 04) or per-pool (Module 08) ledger.

    Same class for backtest and live. Backtest initializes with cash_usd=1000.0 and empty
    positions; live engine loads from persisted state file and reconciles against HL
    clearinghouseState periodically.

    codex m07 r2 fix: __post_init__ rejects NaN/Inf on cash_usd and any position qty.
    Persisted/restored state with bad values would otherwise poison every subsequent
    equity/funding computation.
    """
    cash_usd: float = 1000.0
    position_qty: dict[str, float] = field(default_factory=dict)
    # For Module 04+08 staleness override on near-zero deltas:
    target_first_changed_ts: dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        # codex m07 r3 fix: _is_valid_finite now rejects bool implicitly.
        if not _is_valid_finite(self.cash_usd):
            raise ValueError(f"CopyLedger init: cash_usd must be finite number (not bool), got {self.cash_usd!r}")
        for coin, qty in self.position_qty.items():
            if not _is_valid_finite(qty):
                raise ValueError(f"CopyLedger init: position_qty[{coin!r}] must be finite number (not bool), got {qty!r}")

    def signed_notional_usd(self, coin: str, mark_usd: float) -> float:
        """codex m07 r4 fix: validate mark.
        codex m07 r5 fix: raise on arithmetic overflow (was silently returning 0 → masked exposure).
        Invalid mark returns 0 (semantics: caller doesn't have valid market data).
        """
        if not _is_valid_positive_mark(mark_usd):
            return 0.0
        result = self.position_qty.get(coin, 0.0) * mark_usd
        if not _is_valid_finite(result):
            raise ValueError(
                f"signed_notional_usd: qty × mark overflow for {coin!r} "
                f"(qty={self.position_qty.get(coin, 0.0)}, mark={mark_usd})"
            )
        return result

    def equity_usd_at(self, t: int, candle_close_at_fn: Callable[[str, int], float | None]) -> float:
        """Returns cash + sum(qty × mark) at time t.

        codex m07 r1 fix: marks invalid → coin contributes 0.
        codex m07 r4 fix: per-coin qty × mark overflow → coin contributes 0; final result
        overflow → raises ValueError (callers must handle; ranking/sizing/risk cannot consume Inf).
        """
        # codex m07 r5 fix: RAISE on per-coin contribution overflow (was silently skipping →
        # falsely-finite equity that hides real exposure). Same for accumulator + final result.
        mtm = 0.0
        for coin, qty in self.position_qty.items():
            mark = candle_close_at_fn(coin, t)
            if not _is_valid_positive_mark(mark):
                continue
            contribution = qty * mark
            if not _is_valid_finite(contribution):
                raise ValueError(
                    f"equity_usd_at: qty × mark overflow for {coin!r} "
                    f"(qty={qty}, mark={mark}) — equity cannot be computed honestly"
                )
            mtm += contribution
            if not _is_valid_finite(mtm):
                raise ValueError(f"equity_usd_at: MTM accumulator overflowed at coin {coin!r} qty={qty} mark={mark}")
        result = self.cash_usd + mtm
        if not _is_valid_finite(result):
            raise ValueError(f"equity_usd_at: result not finite (cash={self.cash_usd}, mtm={mtm})")
        return result

    def on_leg_executed(self, coin: str, side: int, qty: float, executable_px: float, fee_usd: float) -> None:
        """Sign-correct cashflow: side=+1 buy, side=-1 sell. qty is unsigned magnitude.

        cashflow_usd = -side × qty × executable_px (buy outflow, sell inflow)
        cash -= fee_usd always
        position_qty[coin] += side × qty

        codex m07 r1 fix: STRICT input validation.
        codex m07 r3 fix: bool rejected (was passing as int); ATOMIC commit — compute next
        cash + pos locally, validate finite, then commit. Overflow that would produce Inf
        raises ValueError (caller's units are wrong).
        """
        # Explicit bool reject — bool is subclass of int.
        if isinstance(side, bool) or side not in (1, -1):
            raise ValueError(f"on_leg_executed: side must be int +1 or -1, got {side!r}")
        if not _is_valid_finite(qty) or qty < 0:
            raise ValueError(f"on_leg_executed: qty must be finite non-negative number, got {qty!r}")
        if not _is_valid_positive_mark(executable_px):
            raise ValueError(f"on_leg_executed: executable_px must be finite positive, got {executable_px!r}")
        if not _is_valid_finite(fee_usd) or fee_usd < 0:
            raise ValueError(f"on_leg_executed: fee_usd must be finite non-negative number, got {fee_usd!r}")
        if abs(qty) < EPS:
            # codex m07 r4 fix: dust qty cannot carry a real fee.
            # Either reject (caller bug) or silently no-op if fee also 0.
            if fee_usd > 0:
                raise ValueError(f"on_leg_executed: dust qty {qty!r} with nonzero fee {fee_usd!r} is ambiguous")
            return
        cashflow_usd = -side * qty * executable_px
        next_cash = self.cash_usd + cashflow_usd - fee_usd
        next_pos = self.position_qty.get(coin, 0.0) + side * qty
        # codex m07 r3 fix: ATOMIC validation — reject overflow.
        if not _is_valid_finite(next_cash):
            raise ValueError(f"on_leg_executed: result cash_usd not finite ({next_cash!r}) — input overflow")
        if not _is_valid_finite(next_pos):
            raise ValueError(f"on_leg_executed: result position_qty[{coin!r}] not finite ({next_pos!r}) — input overflow")
        # Commit
        self.cash_usd = next_cash
        if abs(next_pos) < EPS:
            self.position_qty.pop(coin, None)
        else:
            self.position_qty[coin] = next_pos

    def on_funding_hour_boundary(self, hour_ts: int, marks: dict[str, float], hourly_rates: dict[str, float]) -> None:
        """HL settles funding at the top of each hour. Applied to position held AT THE INSTANT
        BEFORE the boundary. Caller MUST invoke BEFORE any same-ts execution at hour_ts.

        Sign per HL convention:
            funding_cashflow = -signed_notional_usd × hourly_rate

        codex m07 r1 fix: STRICT mark + rate validation. NaN/Inf/non-positive mark or NaN/Inf
        rate → coin skipped (logged via warning if logger configured; caller responsible to
        track skipped boundaries). Prevents cash poisoning by bad market data.

        Idempotency: caller owns dedup. Module 04+08 outer loops never replay the same hour_ts.
        Live engine MUST maintain a "last_funding_hour_applied" record to avoid double-apply
        on reconnect/replay.
        """
        # codex m07 r7 fix: ATOMIC commit. Compute all per-coin funding into a local accumulator
        # FIRST, validate all and the final cash, then assign cash_usd once. Avoids partial
        # mutation on overflow that would leave ledger inconsistent and retry-unsafe.
        total_funding_cashflow = 0.0
        for coin, qty in list(self.position_qty.items()):
            if qty == 0 or coin not in hourly_rates:
                continue
            mark = marks.get(coin)
            if not _is_valid_positive_mark(mark):
                continue
            rate = hourly_rates[coin]
            if not _is_valid_finite(rate):
                continue
            signed_notional_usd = qty * mark
            funding_cashflow = -signed_notional_usd * rate
            if not _is_valid_finite(signed_notional_usd) or not _is_valid_finite(funding_cashflow):
                raise ValueError(
                    f"on_funding_hour_boundary: arithmetic overflow for {coin!r} "
                    f"(qty={qty}, mark={mark}, rate={rate}) — no cash mutated"
                )
            total_funding_cashflow += funding_cashflow
            if not _is_valid_finite(total_funding_cashflow):
                raise ValueError(
                    f"on_funding_hour_boundary: accumulator overflow at {coin!r} — no cash mutated"
                )
        next_cash = self.cash_usd + total_funding_cashflow
        if not _is_valid_finite(next_cash):
            raise ValueError(
                f"on_funding_hour_boundary: result cash not finite "
                f"(cash={self.cash_usd}, total_funding={total_funding_cashflow}) — no cash mutated"
            )
        self.cash_usd = next_cash

    def on_cash_event(self, amount_usd: float, event_type: str = "deposit") -> None:
        """Live engine only: deposits/withdrawals/internal transfers.
        Backtest never calls this (cash_usd is fixed at init; only mutates via leg/funding).

        codex m07 r2 fix: validate amount_usd is finite.
        codex m07 r3 fix: ATOMIC overflow check on result.
        """
        if not _is_valid_finite(amount_usd):
            raise ValueError(f"on_cash_event: amount_usd must be finite, got {amount_usd!r}")
        next_cash = self.cash_usd + amount_usd
        if not _is_valid_finite(next_cash):
            raise ValueError(f"on_cash_event: result cash_usd not finite ({next_cash!r}) — overflow")
        self.cash_usd = next_cash
