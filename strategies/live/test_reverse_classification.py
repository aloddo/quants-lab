"""Isolated logic test for the _on_hl_trade CLASSIFICATION chain (V16CopyTrader).

Same pattern as test_sweep_logic.py: does NOT import the engine (needs mongo/env). Re-implements the
exact predicate chain from hl_copy_trader_v17.py::V16CopyTrader._on_hl_trade and pins the behaviour of
all four Alberto verbs -- OPEN / ADD / TRIM / REVERSE -- plus the two paths item 6 of
card/quant-engineer/faithful-copy-engine asks to verify.

WHY THIS EXISTS: the engine classifies every leader fill with three booleans (is_open, is_add,
we_hold) and routes on them. Four of the eight combinations do materially different things, one of
them writes to the DB only for forensics, and one of them (TRUE REVERSE while we hold) is routed to a
base handler that was written for opens and exits, not for flips. That last case is a real gap and is
routed to _reverse_once as of 2026-07-28 (Alberto TG 11978); the boundaries are pinned here so the
verb cannot regress silently.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from copy_convergence import classify_leader_fill, DEFAULT_REVERSE_MIN_USD  # noqa: E402

REVERSE_MIN_NOTIONAL = DEFAULT_REVERSE_MIN_USD


def classify(prev, sz, is_buy, px, we_hold):
    """Thin adapter over the PRODUCTION classifier, mapping verb -> (verb, after, route).

    The route is the routing CONTRACT each verb carries in V16CopyTrader._on_hl_trade. It is asserted
    here because the routing is what the P1s were actually about: a flip reaching the base handler
    gets re-entered on the leader's own exit fill.
    """
    verb, after = classify_leader_fill(prev, sz, is_buy, px, we_hold,
                                       reverse_min_usd=REVERSE_MIN_NOTIONAL)
    route = {
        "ADD": "converge_add_once" if we_hold else "tracked_only",
        "REVERSE": "reverse_once",
        "REDUCE_NOT_HELD": "tracked_only+forensic_row",
        "OPEN": "base_handler",
        "REDUCE_WE_HOLD": "base_handler",
    }[verb]
    return verb, after, route


PX = 100.0

# ── 1. OPEN: leader flat -> nonzero. The only entry V16 acts on (entry purity). ──────────────────
v, after, route = classify(prev=0.0, sz=10, is_buy=True, px=PX, we_hold=False)
assert (v, after, route) == ("OPEN", 10.0, "base_handler"), (v, after, route)

# 1b. A dust residual under $1 still reads as flat -> OPEN, not ADD. This is what makes the
#     first-entry anchor (_v16_leg_first) reset correctly on a genuinely new leg.
v, after, _ = classify(prev=0.005, sz=10, is_buy=True, px=PX, we_hold=False)   # 0.005*100 = $0.50
assert v == "OPEN", v

# ── 2. ADD: same direction as the tracked position. ──────────────────────────────────────────────
v, after, route = classify(prev=10.0, sz=5, is_buy=True, px=PX, we_hold=True)
assert (v, after, route) == ("ADD", 15.0, "converge_add_once"), (v, after, route)

# 2b. ADD on a leg we never opened: tracked so the denominator stays honest, but NO order. The entry
#     path owns that decision -- there is nothing to add to.
v, after, route = classify(prev=10.0, sz=5, is_buy=True, px=PX, we_hold=False)
assert route == "tracked_only", route

# 2c. SHORT-side add (prev<0, sell) is an ADD too -- (prev>0)==is_buy is False==False.
v, after, _ = classify(prev=-10.0, sz=5, is_buy=False, px=PX, we_hold=True)
assert (v, after) == ("ADD", -15.0), (v, after)

# ── 3. TRIM / CLOSE on a coin we do NOT hold. ────────────────────────────────────────────────────
#     This is the branch that used to write NOTHING at all -- no DB row, no log line -- so
#     v17_target_fills systematically lost every leader CLOSE on an uncopied coin. That hole made 3
#     of 6 orphans look "never seen by the engine" when the engine had in fact seen them here.
#     findings/quant/2026-07-27-target-fills-missing-closes. Now it writes v16_class=
#     "reverse_suppressed_not_copied". It must STILL never reach the base handler: the base handler's
#     own (stale) view would read a leader CLOSE as an opening trade and copy it as our OPEN.
v, after, route = classify(prev=10.0, sz=10, is_buy=False, px=PX, we_hold=False)
assert v == "REDUCE_NOT_HELD" and after == 0.0, (v, after)
assert route == "tracked_only+forensic_row", route
assert route != "base_handler", "REGRESSION: leader close on an unheld coin must never reach the base handler"

# 3b. Partial trim, we do not hold -> same branch, tracker follows the leader down.
v, after, route = classify(prev=10.0, sz=4, is_buy=False, px=PX, we_hold=False)
assert (v, after, route) == ("REDUCE_NOT_HELD", 6.0, "tracked_only+forensic_row"), (v, after, route)

# 3c. A TRUE REVERSE through zero on a coin we do not hold lands here too, and the tracker must end
#     up on the correct SIGN -- not clamped at zero. If this ever clamps, every later fill on the new
#     short leg misclassifies as an ADD and the engine silently mirrors the wrong direction.
v, after, route = classify(prev=10.0, sz=15, is_buy=False, px=PX, we_hold=False)
assert (v, after) == ("REDUCE_NOT_HELD", -5.0), (v, after)
assert route != "base_handler", route

# ── 4. TRIM / CLOSE while we DO hold -> base handler owns the exit machinery. ────────────────────
v, after, route = classify(prev=10.0, sz=4, is_buy=False, px=PX, we_hold=True)
assert (v, after, route) == ("REDUCE_WE_HOLD", 6.0, "base_handler"), (v, after, route)

# ── 5. TRUE REVERSE while we hold -- IMPLEMENTED 2026-07-28 (Alberto TG 11978). ─────────────────
#     Leader is long 10, sells 15: they are now SHORT 5. We hold a long.
#     Previously classified identically to a plain trim and handed to super()._on_hl_trade, which
#     (a) ran the leader's EXIT fill through the ENTRY path and (b) fed
#     _exit_twap_buffer['reverse_notional'], whose 0.85 trigger divides by the inflated
#     _position_accumulated (findings/quant/2026-07-27-mirror-exit-denominator-orphans).
#     Now: intercepted before the base handler and routed to _reverse_once -> flatten, confirm flat,
#     then open the opposite leg. Fail-closed: no flatten confirmation, no opposite leg.
v, after, route = classify(prev=10.0, sz=15, is_buy=False, px=PX, we_hold=True)
assert (v, after, route) == ("REVERSE", -5.0, "reverse_once"), (v, after, route)
assert route != "base_handler", "a flip must NEVER reach the base handler -- it would re-enter on it"

# 5b. Symmetric: leader short 10, buys 15 -> now LONG 5, we hold a short.
v, after, route = classify(prev=-10.0, sz=15, is_buy=True, px=PX, we_hold=True)
assert (v, after, route) == ("REVERSE", 5.0, "reverse_once"), (v, after, route)

# 5c. A flip we are NOT copying stays in the suppressed branch -- there is nothing to flatten, and
#     routing it to _reverse_once would open a naked leg on a leg we never held.
v, after, route = classify(prev=10.0, sz=15, is_buy=False, px=PX, we_hold=False)
assert (v, route) == ("REDUCE_NOT_HELD", "tracked_only+forensic_row"), (v, route)

# 5d. THRESHOLD: a flip that lands on DUST is a CLOSE, not a reverse. 10 -> -0.05 at $100 = $5 of
#     new leg, under the $10 floor. It must fall through to the exit machinery, NOT open a $5 leg
#     that cannot even clear min_entry_notional.
v, after, route = classify(prev=10.0, sz=10.05, is_buy=False, px=PX, we_hold=True)
assert abs(after - (-0.05)) < 1e-9, after
assert v == "REDUCE_WE_HOLD" and route == "base_handler", (v, route)

# 5e. Exactly AT the floor ($10) reverses -- the comparison is >=, so the boundary is inclusive and
#     does not silently fall into the trim path. Sized in whole units at px=$1 so the boundary is
#     EXACT: at px=100, `10.0 - 10.10` is -0.09999999999999964, i.e. $9.999999999999964, which is
#     genuinely BELOW the floor. That float dust is not a bug here (a leg within a rounding error of
#     the floor is dust either way) but it does mean the floor is not bit-exact at arbitrary prices,
#     which is why this assertion is written at a price where it is.
v, after, route = classify(prev=10.0, sz=20.0, is_buy=False, px=1.0, we_hold=True)
assert abs(after - (-10.0)) < 1e-12 and abs(after) * 1.0 == REVERSE_MIN_NOTIONAL, after
assert v == "REVERSE", (v, after)

# 5f-pre. One tick BELOW the floor at the same price is a trim, not a reverse.
v, after, route = classify(prev=10.0, sz=19.9, is_buy=False, px=1.0, we_hold=True)
assert v == "REDUCE_WE_HOLD", (v, after)

# 5f. A full CLOSE to exactly flat is not a reverse (abs(after) > 1e-12 fails) -- it is an exit.
v, after, route = classify(prev=10.0, sz=10.0, is_buy=False, px=PX, we_hold=True)
assert (v, after, route) == ("REDUCE_WE_HOLD", 0.0, "base_handler"), (v, after, route)

# 5g. A partial trim that does NOT cross zero is never a reverse, however large.
v, after, route = classify(prev=10.0, sz=9.99, is_buy=False, px=PX, we_hold=True)
assert v == "REDUCE_WE_HOLD", (v, after)

# ── 5h. REGRESSION, codex 2026-07-28 P1 #4 -- DUST-ORIGIN FLIP. ─────────────────────────────────
#     The leader sits on $0.50 of residual long (below the $1 leg-dust floor, so `is_open` is TRUE)
#     and sells into a material short while WE still hold the copied long. The first implementation
#     gated reverse detection on `not is_open`, so this routed to the base handler as an OPEN and the
#     reverse was silently missed -- leaving us long into a short leader, the exact orphan shape the
#     verb exists to prevent. The old test had this SHAPE (case 1b) but only with we_hold=False, so
#     it stayed green over the hole. That is why the classifier is now imported, not re-implemented.
v, after, route = classify(prev=0.005, sz=10.0, is_buy=False, px=PX, we_hold=True)
assert abs(after - (-9.995)) < 1e-9, after
assert v == "REVERSE", f"dust-origin flip must reverse, got {v}"
assert route == "reverse_once", route

# 5i. Same dust origin, but we do NOT hold -> nothing to flatten, so it is an OPEN for the base
#     handler, exactly as before. The fix must not widen the reverse path to legs we never copied.
v, after, route = classify(prev=0.005, sz=10.0, is_buy=False, px=PX, we_hold=False)
assert (v, route) == ("OPEN", "base_handler"), (v, route)

# 5j. prev == 0 exactly, we hold: a fresh leader OPEN must NEVER read as a reverse. The naive
#     `(prev > 0) != (after > 0)` sign test treats 0 as negative and would have called this a
#     REVERSE, flattening a leg on a leader who simply opened.
v, after, route = classify(prev=0.0, sz=10.0, is_buy=True, px=PX, we_hold=True)
assert v == "OPEN", f"prev==0 must never classify as REVERSE, got {v}"

# ── 6. Exhaustive sign-consistency sweep: the tracker must ALWAYS equal prev+signed, in every verb.
#     A tracker that drifts from the leader's true position is the root cause of the orphan class.
for prev in (-10.0, -0.005, 0.0, 0.005, 10.0):
    for sz in (1.0, 4.0, 10.0, 15.0):
        for is_buy in (True, False):
            for we_hold in (True, False):
                _, after, _ = classify(prev, sz, is_buy, PX, we_hold)
                expect = prev + (sz if is_buy else -sz)
                assert abs(after - expect) < 1e-12, (prev, sz, is_buy, after, expect)

print("reverse-classification self-test PASSED (8 groups, 39 assertions + 80-case sign sweep)")
print("  REVERSE verb IMPLEMENTED: flip -> flatten, confirm flat, open opposite (fail-closed)")
