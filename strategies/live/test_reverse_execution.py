"""Execution-path test for the REVERSE verb. Exercises the REAL V16CopyTrader method.

WHY THIS EXISTS: codex raised the same P2 in BOTH review rounds -- the only reverse test covered
CLASSIFICATION, so it stayed green while the execution path was broken (twelve P1s across two
rounds: unreachable far-side entry, stale-row collisions, persistence key deletion, non-durable
latch). Classification tests cannot catch any of those. This binds the actual
`_execute_pending_reverse` to a stub carrying only the state it touches, so the assertions are about
production code rather than a re-implementation.

Run: python strategies/live/test_reverse_execution.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from hl_copy_trader_v17 import V16CopyTrader  # noqa: E402

ok = 0


class StubEngine:
    """Minimal surface `_execute_pending_reverse` actually touches. Every exchange-facing call is
    scripted so each failure mode is reachable deterministically."""

    def __init__(self, exit_ok=True, exch_after_exit=0.0, leader_pos=-5.0, mid=100.0,
                 reverse_enabled=True, exch_raises=False):
        self._exit_ok = exit_ok
        self._exch_after_exit = exch_after_exit
        self._exch_raises = exch_raises
        self.copy_reverse_enabled = reverse_enabled
        self.reverse_min_notional = 10.0
        self.mid_prices = {"CHIP": mid}
        self._v16_leader_pos = {("0xA", "CHIP"): leader_pos}
        self._reverse_opens = []
        # call log, so ORDERING is assertable -- ordering is what every prior version got wrong
        self.calls = []
        self.persisted = {}
        self.removed = []

    async def _exit_position(self, pos, trim_size=None):
        self.calls.append("exit")
        return self._exit_ok

    def _exchange_position_size(self, coin):
        self.calls.append("exch_check")
        if self._exch_raises:
            raise RuntimeError("REST down")
        return self._exch_after_exit

    def _persist_position(self, pos):
        self.calls.append("persist")
        self.persisted[(pos.get("wallet"), pos.get("coin"))] = dict(pos)

    def _remove_persisted_position(self, wallet, coin):
        self.calls.append("remove_persisted")
        self.removed.append((wallet, coin))

    # bind the REAL method under test
    _execute_pending_reverse = V16CopyTrader._execute_pending_reverse


def mkpos(**kw):
    p = {"coin": "CHIP", "wallet": "0xA", "side": "BUY", "size": 10.0, "filled": True,
         "_pending_reverse": {"target_long": False, "gen": 1, "leader_pos": -5.0,
                              "requested_ts": 0.0}}
    p.update(kw)
    return p


def run(eng, pos):
    return asyncio.run(eng._execute_pending_reverse(pos))


# ── 1. HAPPY PATH: exit fills, exchange confirms flat, far side queued. ──────────────────────────
eng = StubEngine(exit_ok=True, exch_after_exit=0.0, leader_pos=-5.0)
pos = mkpos()
reaped = run(eng, pos)
assert reaped is True, "a confirmed flatten must be reapable"
assert eng._reverse_opens == [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1}], eng._reverse_opens
assert "_pending_reverse" not in pos, "intent must be cleared once executed"
# ORDERING: the persisted row must be dropped BEFORE any far-side entry is queued, or the old row's
# (wallet, coin)-keyed delete would later wipe the new leg's row (codex r2).
assert eng.calls == ["exit", "exch_check", "remove_persisted"], eng.calls
assert eng.removed == [("0xA", "CHIP")], eng.removed
ok += 5

# ── 2. FAILED FLATTEN: not reapable, intent RETAINED and re-persisted, nothing queued. ──────────
eng = StubEngine(exit_ok=False)
pos = mkpos()
reaped = run(eng, pos)
assert reaped is False, "a failed flatten must NOT be reaped -- the leg is still live"
assert pos.get("_pending_reverse"), "intent must survive a failed flatten so the next cycle retries"
assert pos["_reverse_attempts"] == 1, pos.get("_reverse_attempts")
assert eng._reverse_opens == [], "never open the far side without a confirmed flatten"
assert "persist" in eng.calls, "the retained intent must be RE-PERSISTED (restart durability)"
assert "remove_persisted" not in eng.calls, "must not drop the DB row for a leg that is still open"
ok += 6

# 2b. Bounded retries: on the 5th consecutive failure it escalates to the existing force-exit
#     machinery and abandons the far side, so it can never spin forever.
eng = StubEngine(exit_ok=False)
pos = mkpos(_reverse_attempts=4)
reaped = run(eng, pos)
assert reaped is False, reaped
assert pos.get("_force_exit") is True, "must escalate to _force_exit after bounded retries"
assert "_pending_reverse" not in pos, "intent cleared on escalation -- force-exit owns the leg now"
ok += 3

# ── 3. EXCHANGE NOT FLAT after a 'successful' exit. ─────────────────────────────────────────────
#     `_exit_position` returns True in cases that are NOT flat (codex r2: the tracked side merely
#     disagreeing with an already-opposite net position). Adding opposite risk on top would be the
#     worst outcome, so the far side is skipped even though the leg is reaped.
eng = StubEngine(exit_ok=True, exch_after_exit=3.5)
pos = mkpos()
reaped = run(eng, pos)
assert reaped is True, "the leg is done with as far as this engine is concerned"
assert eng._reverse_opens == [], "NEVER open the far side while the exchange shows size on the coin"
assert eng.removed == [("0xA", "CHIP")], eng.removed
ok += 3

# 3b. Exchange check RAISES -> fail closed: not reaped, retried next cycle, nothing opened.
eng = StubEngine(exit_ok=True, exch_raises=True)
pos = mkpos()
reaped = run(eng, pos)
assert reaped is False, "an unverifiable exchange state must not be treated as flat"
assert eng._reverse_opens == [], eng._reverse_opens
ok += 2

# ── 4. FLAG OFF: still flattens and still reaps, but does NOT open the far side. ────────────────
#     The first implementation returned WITHOUT flattening when the flag was off, leaving us on the
#     wrong side of a leader who had gone the other way -- documented as fail-closed, actually
#     fail-open.
eng = StubEngine(reverse_enabled=False)
pos = mkpos()
reaped = run(eng, pos)
assert reaped is True, reaped
assert "exit" in eng.calls, "flattening is NOT optional when the leader has left our side"
assert eng._reverse_opens == [], "flag off means no far-side entry"
ok += 3

# ── 5. DOUBLE FLIP: the leader is re-read at execution time, so a stale intent cannot win. ──────
#     The intent says target_long=False (leader went short) but by the time the flatten completes
#     the leader is LONG again. Last-writer-wins on the tracker is the correct semantics.
eng = StubEngine(leader_pos=+7.0)
pos = mkpos()
run(eng, pos)
assert eng._reverse_opens[0]["is_buy"] is True, (
    "must open the side the leader is on NOW, not the one captured when the intent was written")
ok += 1

# ── 6. LEADER BACK UNDER THE FLOOR after the flatten -> stay flat, do not open a sub-floor leg. ──
eng = StubEngine(leader_pos=-0.05)          # 0.05 * $100 = $5, under the $10 floor
pos = mkpos()
reaped = run(eng, pos)
assert reaped is True and eng._reverse_opens == [], eng._reverse_opens
ok += 2

# 6b. No mid price -> cannot size or judge the floor -> stay flat.
eng = StubEngine(mid=0.0)
pos = mkpos()
reaped = run(eng, pos)
assert reaped is True and eng._reverse_opens == [], eng._reverse_opens
ok += 2

print(f"reverse-EXECUTION self-test PASSED ({ok} assertions, real V16CopyTrader method)")
print("  covers: happy path + ordering, failed flatten, bounded escalation, exchange-not-flat,")
print("          REST failure, flag-off, double flip, sub-floor leader, missing mid")
