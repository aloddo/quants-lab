"""Isolated logic test for the _on_hl_trade CLASSIFICATION chain (V16CopyTrader).

Same pattern as test_sweep_logic.py: does NOT import the engine (needs mongo/env). Re-implements the
exact predicate chain from hl_copy_trader_v17.py::V16CopyTrader._on_hl_trade and pins the behaviour of
all four Alberto verbs -- OPEN / ADD / TRIM / REVERSE -- plus the two paths item 6 of
card/quant-engineer/faithful-copy-engine asks to verify.

WHY THIS EXISTS: the engine classifies every leader fill with three booleans (is_open, is_add,
we_hold) and routes on them. Four of the eight combinations do materially different things, one of
them writes to the DB only for forensics, and one of them (TRUE REVERSE while we hold) is routed to a
base handler that was written for opens and exits, not for flips. That last case is a real gap and is
pinned here as a KNOWN GAP so it cannot regress silently or be quietly "fixed" without a decision.
"""

MIN_NOTIONAL = 1.0


def classify(prev, sz, is_buy, px, we_hold):
    """Returns (verb, leader_pos_after, routed_to).

    Mirrors the live chain exactly:
        prev_notional = abs(prev) * px
        is_open = prev_notional < 1.0
        is_add  = (not is_open) and ((prev > 0) == is_buy)
        reverse-not-held  -> tracked, forensic DB row, NEVER routed to base   (return)
        add               -> tracked, converge task, forensic DB row          (return)
        everything else   -> tracked, routed to super()._on_hl_trade
    """
    signed = sz if is_buy else -sz
    prev_notional = abs(prev) * px
    is_open = prev_notional < MIN_NOTIONAL
    is_add = (not is_open) and ((prev > 0) == is_buy)
    after = prev + signed

    if is_add:
        return "ADD", after, "converge_add_once" if we_hold else "tracked_only"
    if (not is_open) and (not we_hold):
        # covers leader TRIM and leader CLOSE on a coin we do not hold
        return "REDUCE_NOT_HELD", after, "tracked_only+forensic_row"
    if is_open:
        return "OPEN", after, "base_handler"
    return "REDUCE_WE_HOLD", after, "base_handler"


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

# ── 5. KNOWN GAP -- TRUE REVERSE while we hold. ──────────────────────────────────────────────────
#     Leader is long 10, sells 15: they are now SHORT 5. We hold a long.
#     The chain classifies this identically to a plain trim (REDUCE_WE_HOLD) and hands it to
#     super()._on_hl_trade, which does two things with it:
#       (a) runs it through _handle_instant_entry/_handle_twap_entry -- an ENTRY path, and
#       (b) accumulates it into _exit_twap_buffer['reverse_notional'] for the exit.
#     The exit half then fires on reverse_notional / _position_accumulated >= 0.85, whose denominator
#     is inflated by every leader ADD while our leg stays flat at $100 --
#     findings/quant/2026-07-27-mirror-exit-denominator-orphans.
#     So the REVERSE verb is NOT faithfully implemented: nothing in the chain says "flatten, then open
#     the opposite leg". It is pinned here so the gap is explicit rather than assumed-working.
v, after, route = classify(prev=10.0, sz=15, is_buy=False, px=PX, we_hold=True)
assert v == "REDUCE_WE_HOLD", v
assert after == -5.0, f"tracker must carry the flipped sign, got {after}"
assert route == "base_handler", route
# The gap, stated as an assertion so a future faithful implementation BREAKS this test and forces the
# author to come back and delete it deliberately:
assert v != "REVERSE", (
    "KNOWN GAP (item 6, card/quant-engineer/faithful-copy-engine): a leader flip through zero is "
    "still classified as a trim. When the reverse verb is implemented this assert MUST be removed "
    "and replaced with the faithful expectation."
)

# 5b. Symmetric case: leader short 10, buys 15 -> now LONG 5, we hold a short. Same gap.
v, after, _ = classify(prev=-10.0, sz=15, is_buy=True, px=PX, we_hold=True)
assert (v, after) == ("REDUCE_WE_HOLD", 5.0), (v, after)

# ── 6. Exhaustive sign-consistency sweep: the tracker must ALWAYS equal prev+signed, in every verb.
#     A tracker that drifts from the leader's true position is the root cause of the orphan class.
for prev in (-10.0, -0.005, 0.0, 0.005, 10.0):
    for sz in (1.0, 4.0, 10.0, 15.0):
        for is_buy in (True, False):
            for we_hold in (True, False):
                _, after, _ = classify(prev, sz, is_buy, PX, we_hold)
                expect = prev + (sz if is_buy else -sz)
                assert abs(after - expect) < 1e-12, (prev, sz, is_buy, after, expect)

print("reverse-classification self-test PASSED (6 groups, 20 assertions + 80-case sign sweep)")
print("  KNOWN GAP pinned: true reverse while we hold is classified as a trim (item 6, not yet built)")
