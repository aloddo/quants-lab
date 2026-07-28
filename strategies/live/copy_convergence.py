"""Faithful proportional copy — the ONE implementation of target sizing + convergence.

WHY THIS MODULE EXISTS
Codex's original gate on this system was "runtime == replay": the code that decides a live order must be
the SAME code the backtest calls. Every mismatch we have paid for this week came from violating that
(selecting on the LEADER's journey PnL, executing something else). So the target/convergence math lives
here, is PURE (no I/O, no clock, no engine state), and is imported by BOTH:
  - strategies/live/hl_copy_trader_v17.py   (live decisions)
  - the replay harness                       (validation on historical fills)

DESIGN (Fable review 2026-07-26, after it found the addon multiplier was a placebo):

1. TWO ANCHORS. `first_entry_target` is the PRIMARY one (Alberto 2026-07-26):
     target = our_first_entry_usd * (leader_position_now / leader_first_entry_size)
   "If I put in $100 and they do a 20% addon, I add $20." Only the RATIO to the size they opened that
   position with matters, so it needs NO leader-equity feed -- which is decisive, because
   `target_exposure_pct` is 100% NULL in our actions store while `position_after` is fully populated.
   It is also the intuitive definition of copying, and it is simulatable TODAY.

   `proportional_target` (leader exposure / leader equity * our slice) is the alternative Fable argued
   for. It is anchor-free and self-normalising, but it requires the equity column we do not have. Kept
   because M7's `sizing_mode="leader_equity"` uses exactly this shape, so the two can be compared once
   M01 equity is wired in.

   Known weakness of the first-entry anchor, stated rather than hidden: the anchor is a single fill, so a
   leader who opens with a probe and scales hard yields a large multiple (measured peak/first p90 runs
   3.8x to 859x across our candidates). The leverage clamp and gross caps bound it; under this anchor
   they are NOT optional.

2. EVERYTHING IS DELTA CONVERGENCE. Opens, adds, trims, exits, missed fills, restarts and backfill all
   reduce to one operation: trade (target - current) when it clears a threshold. This is why the
   minimum-order problem dissolves: a sub-minimum add is simply a delta that stays on the books until
   the next event pushes it over the line. Tracking error is bounded by the threshold in BOTH directions
   and self-liquidates. Never round up (that would hold us systematically beyond the leader, in their
   direction, compounding into a levered tilt nobody selected for).

3. LEVERAGE CLAMP IS MANDATORY. In proportional mode the engine deliberately skips the per-coin notional
   cap, so this clamp is its replacement. It is what stops us following a martingaling leader into their
   own liquidation: as they add, their equity FALLS, so exposure_pct rises on both numerator and
   denominator.

Everything here is pure and unit-testable. Run `python strategies/live/copy_convergence.py` for the
self-test.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

# Hyperliquid minimum order is ~$10 notional. Buffer above it so a rounding wobble cannot
# produce a rejected order.
DEFAULT_MIN_ORDER_USD = 11.0
# Residual below this is unmanageable dust -> close fully instead of stranding it.
DEFAULT_DUST_USD = 12.0
# Relative dead-band: do not chase deltas smaller than this fraction of current exposure.
# Calibrate in replay against fee drag (Fable: target < ~20% of gross per-wallet alpha).
DEFAULT_DEADBAND_FRAC = 0.10
# Hard ceiling on mirrored leader leverage (exposure_pct). Replaces the per-coin notional cap.
DEFAULT_LEVERAGE_CLAMP = 4.0


@dataclass(frozen=True)
class ConvergenceDecision:
    """What to do about one (wallet, coin). `delta_usd` is SIGNED in OUR position's terms."""
    delta_usd: float          # >0 = increase long / reduce short; 0 = do nothing
    reason: str               # machine-readable, for logs + replay assertions
    is_full_close: bool = False
    clamped: bool = False     # True when the leverage clamp bound the target (track as tracking error)

    @property
    def should_trade(self) -> bool:
        return self.delta_usd != 0.0 or self.is_full_close


def proportional_target(
    leader_szi: float,
    leader_equity: Optional[float],
    mark: Optional[float],
    our_slice_usd: Optional[float],
    leverage_clamp: float = DEFAULT_LEVERAGE_CLAMP,
) -> tuple[Optional[float], bool]:
    """OUR target SIGNED notional for one (wallet, coin).

    Returns (target_usd, clamped). target_usd is None when it CANNOT be sized safely — the caller must
    then SKIP, never guess. Sizing off stale or missing leader equity is exactly how you end up mirroring
    a number you did not measure.

    Sign convention: positive = long, negative = short, matching leader_szi's sign.
    """
    if mark is None or mark <= 0:
        return None, False
    if leader_equity is None or leader_equity <= 0:
        return None, False
    if our_slice_usd is None or our_slice_usd <= 0:
        return None, False

    exposure_pct = (leader_szi * mark) / leader_equity      # signed; their per-coin leverage
    clamped = False
    if leverage_clamp is not None and abs(exposure_pct) > leverage_clamp:
        exposure_pct = leverage_clamp if exposure_pct > 0 else -leverage_clamp
        clamped = True
    return exposure_pct * our_slice_usd, clamped


def first_entry_target(
    leader_pos_now: float,
    leader_first_entry_sz: Optional[float],
    our_first_entry_usd: float,
) -> Optional[float]:
    """FIRST-ENTRY ANCHOR (Alberto 2026-07-26). The simple, correct definition of copying:

        our_target = our_first_entry_usd * (leader_position_now / leader_first_entry_size)

    "If I put in $100 and they do a 20% addon, I add $20." The leader opening with $1M or $100 is
    irrelevant — only the RATIO to the size they opened that position with matters. No leader-equity feed
    required, which matters because `target_exposure_pct` is 100% null in our actions store while
    `position_after` is fully populated.

    Sign follows leader_pos_now. Returns None when there is no usable anchor (position started before we
    were watching), in which case the caller must skip rather than guess.

    NOTE the known weakness, worth logging rather than pretending away: the anchor is a single fill, so a
    leader who opens with a small probe and then scales hard produces a large multiple (measured p90 of
    peak/first runs 3.8x to 859x across our candidates). The leverage clamp and the gross caps are what
    bound that; they are not optional under this anchor.
    """
    if not leader_first_entry_sz or leader_first_entry_sz == 0:
        return None
    ratio = leader_pos_now / abs(leader_first_entry_sz)
    return our_first_entry_usd * ratio


def convergence_delta(
    target_usd: Optional[float],
    current_usd: float,
    leader_is_flat: bool = False,
    min_order_usd: float = DEFAULT_MIN_ORDER_USD,
    deadband_frac: float = DEFAULT_DEADBAND_FRAC,
    dust_usd: float = DEFAULT_DUST_USD,
    clamped: bool = False,
) -> ConvergenceDecision:
    """The single decision function. Signed USD delta to trade, or a full-close instruction.

    Ordering of the rules matters and is deliberate:
      1. Leader flat  -> ALWAYS fully close us. Overrides every dead-band. A leader exiting is the one
         signal we must never damp.
      2. Target unavailable -> do nothing (caller falls back to the validated conservative exit path).
      3. Sign flip -> close fully; the caller re-opens the new side through the normal entry gates.
         Never a single through-zero order (v1 rule).
      4. Would-be residual is dust -> close fully rather than strand something unmanageable.
      5. Otherwise trade the delta only if it clears BOTH the exchange minimum and the relative dead-band.
    """
    if leader_is_flat:
        if abs(current_usd) > 0:
            return ConvergenceDecision(-current_usd, "leader_flat_full_close", is_full_close=True,
                                       clamped=clamped)
        return ConvergenceDecision(0.0, "already_flat")

    if target_usd is None:
        return ConvergenceDecision(0.0, "target_unavailable")

    # Sign flip: leader crossed through zero to the other side.
    if current_usd != 0.0 and target_usd != 0.0 and (current_usd > 0) != (target_usd > 0):
        return ConvergenceDecision(-current_usd, "sign_flip_close_first", is_full_close=True,
                                   clamped=clamped)

    delta = target_usd - current_usd
    if delta == 0.0:
        return ConvergenceDecision(0.0, "at_target", clamped=clamped)

    # Reducing toward a residual that would be dust -> just close.
    residual = abs(target_usd)
    reducing = abs(target_usd) < abs(current_usd)
    if reducing and residual < dust_usd:
        return ConvergenceDecision(-current_usd, "residual_would_be_dust_full_close",
                                   is_full_close=True, clamped=clamped)

    threshold = max(min_order_usd, deadband_frac * abs(current_usd))
    if abs(delta) < threshold:
        # Deliberately do nothing. The shortfall PERSISTS as a delta and is picked up whole by the next
        # event -- this is the accumulate-for-free property that makes a separate accumulator unnecessary.
        return ConvergenceDecision(0.0, f"within_threshold({threshold:.2f})", clamped=clamped)

    return ConvergenceDecision(delta, "converge", clamped=clamped)


def our_slice_usd(our_equity: float, n_copy_wallets: int) -> Optional[float]:
    """Equal-split allocation (Alberto 2026-06-01): each leader mirrors its OWN leverage inside its own
    equal slice, so a 10x leader cannot eat the book and crowd the others out."""
    if not our_equity or our_equity <= 0 or not n_copy_wallets or n_copy_wallets <= 0:
        return None
    return our_equity / n_copy_wallets


def granularity_rungs(our_slice: float, typical_exposure_pct: float,
                      min_order_usd: float = DEFAULT_MIN_ORDER_USD) -> float:
    """SELECTION-TIME gate (Fable section 3): how many minimum-orders fit in a leader's TYPICAL position
    at our slice. Below ~3-5 rungs we copy a step function of their strategy rather than the strategy,
    and their selection statistics do not transfer. Used to refuse wallets we cannot represent."""
    typical_notional = abs(typical_exposure_pct) * our_slice
    if min_order_usd <= 0:
        return float("inf")
    return typical_notional / min_order_usd


# ----------------------------------------------------------------------------- self-test
def _selftest() -> None:
    ok = 0

    def check(cond, label):
        nonlocal ok
        assert cond, f"FAIL: {label}"
        ok += 1

    # target sizing
    t, c = proportional_target(leader_szi=10.0, leader_equity=10_000.0, mark=100.0, our_slice_usd=94.0)
    check(abs(t - 9.4) < 1e-9, "1000/10000 = 0.1x leverage -> 0.1 * 94 = 9.4")
    check(not c, "not clamped at 0.1x")

    t, c = proportional_target(leader_szi=-10.0, leader_equity=10_000.0, mark=100.0, our_slice_usd=94.0)
    check(abs(t + 9.4) < 1e-9, "short mirrors negative")

    t, c = proportional_target(leader_szi=1000.0, leader_equity=10_000.0, mark=100.0, our_slice_usd=94.0)
    check(c and abs(t - 4.0 * 94.0) < 1e-9, "10x leverage clamps to 4x")

    for bad in [dict(leader_equity=None), dict(leader_equity=0.0), dict(mark=None), dict(our_slice_usd=None)]:
        kw = dict(leader_szi=10.0, leader_equity=10_000.0, mark=100.0, our_slice_usd=94.0)
        kw.update(bad)
        t, _ = proportional_target(**kw)
        check(t is None, f"unsizeable -> None ({list(bad)[0]})")

    # convergence
    # first-entry anchor (Alberto 2026-07-26): "$100 in, they do a 20% addon, I add $20"
    t = first_entry_target(leader_pos_now=1.2, leader_first_entry_sz=1.0, our_first_entry_usd=100.0)
    check(abs(t - 120.0) < 1e-9, "leader +20% of their opening -> our target 120")
    t = first_entry_target(leader_pos_now=1_200_000.0, leader_first_entry_sz=1_000_000.0,
                           our_first_entry_usd=100.0)
    check(abs(t - 120.0) < 1e-9, "SAME answer at $1M scale -- only the ratio matters")
    t = first_entry_target(leader_pos_now=0.5, leader_first_entry_sz=1.0, our_first_entry_usd=100.0)
    check(abs(t - 50.0) < 1e-9, "leader trimmed half -> our target 50")
    t = first_entry_target(leader_pos_now=-1.0, leader_first_entry_sz=1.0, our_first_entry_usd=100.0)
    check(abs(t + 100.0) < 1e-9, "leader flipped short -> target sign flips")
    check(first_entry_target(1.0, None, 100.0) is None, "no anchor -> None (skip, never guess)")
    check(first_entry_target(1.0, 0.0, 100.0) is None, "zero anchor -> None")

    # the two anchors agree when the leader opens at 1x their own typical size
    fe = first_entry_target(leader_pos_now=2.0, leader_first_entry_sz=1.0, our_first_entry_usd=94.4)
    check(abs(fe - 188.8) < 1e-9, "2x their opening -> 2x our opening")

    d = convergence_delta(target_usd=0.0, current_usd=50.0, leader_is_flat=True)
    check(d.is_full_close and abs(d.delta_usd + 50.0) < 1e-9, "leader flat -> full close regardless of band")

    d = convergence_delta(target_usd=None, current_usd=50.0)
    check(not d.should_trade and d.reason == "target_unavailable", "no target -> no trade")

    d = convergence_delta(target_usd=-40.0, current_usd=50.0)
    check(d.is_full_close and d.reason == "sign_flip_close_first", "sign flip closes first")

    d = convergence_delta(target_usd=100.0, current_usd=94.0)
    check(not d.should_trade, "6 dollar add below min order -> wait (accumulates)")

    d = convergence_delta(target_usd=120.0, current_usd=94.0)
    check(d.should_trade and abs(d.delta_usd - 26.0) < 1e-9, "26 dollar add clears threshold")

    # the accumulate-for-free property: two sub-threshold adds become one order
    d1 = convergence_delta(target_usd=100.0, current_usd=94.0)   # +6, below the 11 threshold
    d2 = convergence_delta(target_usd=104.0, current_usd=94.0)   # +10, still below
    check(not d1.should_trade and not d2.should_trade, "each small add alone stays below")
    d3 = convergence_delta(target_usd=112.0, current_usd=94.0)
    check(d3.should_trade and abs(d3.delta_usd - 18.0) < 1e-9,
          "accumulated shortfall captured WHOLE by one later order")

    d = convergence_delta(target_usd=5.0, current_usd=94.0)
    check(d.is_full_close and d.reason == "residual_would_be_dust_full_close", "dust residual -> full close")

    d = convergence_delta(target_usd=94.0, current_usd=94.0)
    check(not d.should_trade and d.reason == "at_target", "at target -> nothing")

    # dead-band scales with position size
    d = convergence_delta(target_usd=1050.0, current_usd=1000.0)
    check(not d.should_trade, "5% move on a 1000 dollar position is inside the 10% band")
    d = convergence_delta(target_usd=1200.0, current_usd=1000.0)
    check(d.should_trade, "20% move clears the band")

    # granularity gate
    check(granularity_rungs(94.0, 1.0) > 8, "1x leader at 94 dollar slice = ~8.5 rungs")
    check(granularity_rungs(94.0, 0.1) < 1, "0.1x leader is unrepresentable at this slice")

    print(f"copy_convergence self-test PASSED ({ok} assertions)")


if __name__ == "__main__":
    _selftest()
