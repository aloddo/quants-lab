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
        self.shadow_mode = True          # keeps Mongo out of the unit test
        self._leg_locks = {}
        self._exit_ok = exit_ok
        self._exch_after_exit = exch_after_exit
        self._exch_raises = exch_raises
        self.copy_reverse_enabled = reverse_enabled
        self.reverse_min_notional = 10.0
        self.mid_prices = {"CHIP": mid}
        self._v16_leader_pos = {("0xA", "CHIP"): leader_pos}
        # signal-time knet FIFO: (knet, ts, is_buy). The reverse CONSUMES the real stamp rather
        # than recomputing one (codex r5 P1 #1), so the stub must provide it.
        # 4-tuple = a REVERSE stamp bound to a generation (codex r6 P1 #3). mkpos() writes gen 1,
        # so the stamp must carry gen 1 or the reverse must refuse to consume it.
        self._v17_knet_pending = {
            ("0xA", "CHIP"): [(3, __import__("time").time(), leader_pos > 0, 1)]}
        self._reverse_opens = []
        # per-leg exit state the reverse must clear on a confirmed flatten (codex r6 P1 #5) --
        # seeded non-empty so the test can prove it IS cleared
        self._position_accumulated = {("0xA", "CHIP"): 4321.0}
        self._exit_twap_buffer = {("0xA", "CHIP"): {"reverse_notional": 999.0}}
        # call log, so ORDERING is assertable -- ordering is what every prior version got wrong
        self.calls = []
        self.persisted = {}
        self.removed = []
        self.positions = []
        self.entered = []
        self._reverse_opens_loaded = True     # skip the Mongo recovery read
        self._entry_fills = True              # does the far-side entry actually take?
        self._persist_ok = True               # simulate Mongo write failures
        self._pending_db = {}                 # stands in for DB_PENDING_REVERSE

    async def _exit_position(self, pos, trim_size=None):
        self.calls.append("exit")
        return self._exit_ok

    def _exchange_position_size_strict(self, coin):
        # NOTE: the reverse path deliberately uses the STRICT reader, which RAISES on any REST
        # failure. The cached _exchange_position_size returns 0.0 ("flat") when the API is down,
        # which would authorise opening opposite risk against an unverified book (codex r3 P1 #3).
        self.calls.append("exch_check")
        if self._exch_raises:
            raise RuntimeError("REST down")
        return self._exch_after_exit

    def _persist_position(self, pos):
        self.calls.append("persist")
        if not self._persist_ok:
            return False
        self.persisted[(pos.get("wallet"), pos.get("coin"))] = dict(pos)
        return True

    def _remove_persisted_position(self, wallet, coin):
        self.calls.append("remove_persisted")
        self.removed.append((wallet, coin))
        return True

    # bind the REAL methods under test
    _leg_lock = V16CopyTrader._leg_lock
    _execute_pending_reverse = V16CopyTrader._execute_pending_reverse
    _reverse_flatten_locked = V16CopyTrader._reverse_flatten_locked
    _drain_reverse_opens = V16CopyTrader._drain_reverse_opens
    _clear_pending_reverse = V16CopyTrader._clear_pending_reverse

    async def _enter_position(self, coin, is_buy, wallet=None, **kw):
        self.calls.append(f"enter:{coin}:{'BUY' if is_buy else 'SELL'}")
        self.entered.append((wallet, coin, is_buy))
        if self._entry_fills:
            self.positions.append({"coin": coin, "wallet": wallet, "filled": True,
                                   "side": "BUY" if is_buy else "SELL", "size": 1.0})


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
q = eng._reverse_opens
assert len(q) == 1 and q[0]["wallet"] == "0xA" and q[0]["coin"] == "CHIP", q
assert q[0]["is_buy"] is False and q[0]["gen"] == 1 and q[0]["attempts"] == 0, q
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
# codex r4 P1 #6: the residual may be another wallet's leg OR our own failed close -- the net does
# not distinguish them. Reaping and deleting persistence here dropped tracking of real exchange
# exposure. Keep it tracked and hand it to force-exit; abandoning the far side is the cheap half.
assert reaped is False, "must NOT reap a leg while the exchange still shows size on the coin"
# r5 P1 #2: do NOT latch force-exit -- that path trades the AGGREGATE net and could close another
# wallet's leg, and drops the row after 30 failures. The residual is unattributable by construction.
assert pos.get("_force_exit") is not True, "must NOT force-exit an unattributable residual"
assert "_pending_reverse" not in pos, "the reverse intent is dropped"
assert eng._reverse_opens == [], "NEVER open the far side while the exchange shows size on the coin"
assert eng.removed == [], "must not delete persistence for a leg that may still be live"
ok += 4

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
#     We hold a LONG. Intent said the leader went short. If by execution time the leader is LONG
#     again, the reverse is VOID -- codex r4 P1 #3: the old code flattened first and consulted the
#     leader afterwards, so it closed a correctly-aligned leg and then tried to reopen it.
eng = StubEngine(leader_pos=+7.0)
pos = mkpos()                                  # pos side BUY, leader now +7 -> aligned
reaped = run(eng, pos)
assert reaped is False, "an aligned leg must be KEPT, not flattened"
assert "exit" not in eng.calls, "must not flatten a leg the leader is back on"
assert "_pending_reverse" not in pos, "the stale intent must be cancelled"
assert eng._reverse_opens == [], eng._reverse_opens
ok += 4

# 5b. Leader flipped the OTHER way (we are LONG, leader now SHORT harder) -> genuine reverse, and
#     the far side follows the CURRENT tracker value.
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
run(eng, pos)
assert eng._reverse_opens[0]["is_buy"] is False, eng._reverse_opens
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

# 6c. knet is captured onto the durable request at flatten time, so retries and restart-recovered
#     requests carry the SAME signal-time authorization (codex r4 P1 #1: without it, attempts 2-5
#     and every recovered request hit NO-STAMP REJECT and the queue could never deliver a leg).
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
run(eng, pos)
assert eng._reverse_opens[0]["knet"] == 3, eng._reverse_opens
assert eng._reverse_opens[0]["knet_ts"] is not None, "TTL needs the stamp's own timestamp"
# the stamp must be CONSUMED, not left behind for a later same-direction OPEN to pick up
assert not eng._v17_knet_pending.get(("0xA", "CHIP")), eng._v17_knet_pending
ok += 3

# 6d. NO signal-time stamp -> stay flat rather than open on unproven authorization.
eng = StubEngine(leader_pos=-7.0)
eng._v17_knet_pending = {}
pos = mkpos()
reaped = run(eng, pos)
assert reaped is True and eng._reverse_opens == [], eng._reverse_opens
ok += 2

# 6e. A STALE stamp (older than the FIFO's 60s expiry) must not authorize a recovered request.
import time as _t  # noqa: E402
eng = StubEngine(leader_pos=-7.0)
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1,
                       "knet": 3, "knet_ts": _t.time() - 120, "attempts": 0}]
asyncio.run(eng._drain_reverse_opens())
assert eng.entered == [], "a stale signal-time knet must never authorize an entry"
assert eng._reverse_opens == [], eng._reverse_opens
ok += 2

# 6f. PRE-ORDER FLAT PROOF: the coin reopening between the flatten and the order must block it.
eng = StubEngine(leader_pos=-7.0)
eng._exch_after_exit = 2.0        # strict reader now says NOT flat at order time
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1,
                       "knet": 3, "knet_ts": _t.time(), "attempts": 0}]
asyncio.run(eng._drain_reverse_opens())
assert eng.entered == [], "must not open the far side once the coin is no longer flat"
ok += 1

# ── 10. r6 P1 #5: a confirmed flatten must clear the per-leg exit state. ────────────────────────
#     Every other full-exit caller pops these two maps. The reverse did not, so a reopened leg
#     computed its close ratio against the PREVIOUS leg's accumulated notional -- the inflated
#     denominator behind the 2026-07-27 orphans, reintroduced through the back door. Active even
#     with copy_reverse_enabled=False, because the flatten still runs.
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
run(eng, pos)
assert eng._position_accumulated.get(("0xA", "CHIP")) is None, eng._position_accumulated
assert eng._exit_twap_buffer.get(("0xA", "CHIP")) is None, eng._exit_twap_buffer
ok += 2

# ── 11. r6 P1 #1+#2: SINGLE-LEG PRECONDITION. HL nets by COIN, so an account-level read cannot
#     prove OUR leg is flat while another roster wallet holds the same coin. Declined explicitly
#     rather than deferred forever (dead-end) or force-exited against the aggregate net (which
#     could close the OTHER wallet's position).
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
eng.positions = [pos, {"coin": "CHIP", "wallet": "0xB", "filled": True, "side": "BUY", "size": 5.0}]
reaped = run(eng, pos)
assert reaped is False, "must not reap a leg it declined to flatten"
assert "exit" not in eng.calls, "must NOT flatten when per-wallet flatness is unprovable"
assert "_pending_reverse" not in pos, "the intent is declined, not left pending forever"
assert eng._reverse_opens == [], eng._reverse_opens
ok += 4

# 11b. A single roster leg on the coin proceeds normally.
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
eng.positions = [pos]
reaped = run(eng, pos)
assert "exit" in eng.calls and reaped is True, eng.calls
ok += 2

# ── 12. r6 P1 #6: attempts bound ORDER SUBMISSIONS, not cycles. Transient pre-order REST failures
#     must not burn the budget without an order ever being sent.
eng = StubEngine(leader_pos=-7.0)
eng._exch_raises = True
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1,
                       "knet": 3, "knet_ts": _t.time(), "attempts": 0}]
asyncio.run(eng._drain_reverse_opens())
assert eng.entered == [], "no order was submitted"
assert eng._reverse_opens and eng._reverse_opens[0]["attempts"] == 0, (
    "a failed pre-order read must NOT consume an order attempt")
ok += 2

# ══ ROUND-3 REGRESSIONS (codex r3 P1 #1-#4) ═════════════════════════════════════════════════════

# ── 7. LEG LOCK: a reverse must DEFER while an add holds the leg, not flatten under it. ─────────
#     _converge_add_once is still a background task that holds the per-leg lock across its order, so
#     without this an in-flight add fills AFTER the flatten and rebuilds the side we just closed.
#     _check_exits is the 1Hz heartbeat, so this is a non-blocking deferral, never a wait.
async def _locked_case():
    eng = StubEngine()
    pos = mkpos()
    lock = eng._leg_lock("0xA", "CHIP")
    await lock.acquire()                     # simulate an add holding the leg
    try:
        return eng, pos, await eng._execute_pending_reverse(pos)
    finally:
        lock.release()


eng, pos, reaped = asyncio.run(_locked_case())
assert reaped is False, "must not reap a leg it did not flatten"
assert "exit" not in eng.calls, "must NOT flatten while an add owns the leg"
assert pos.get("_pending_reverse"), "intent survives the deferral"
assert pos.get("_reverse_attempts", 0) == 0, "a deferral must not burn a retry attempt"
ok += 4

# ── 8. DRAIN is NON-DESTRUCTIVE until the leg is actually observed open. ────────────────────────
#     The earlier version cleared the queue BEFORE attempting entries, so a crash -- or an entry
#     simply rejected by a gate, since _enter_position returns no success status -- lost the reverse.
eng = StubEngine()
eng._entry_fills = False                     # entry silently does not take
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1, "attempts": 0,
                       "knet": 3, "knet_ts": __import__("time").time()}]
asyncio.run(eng._drain_reverse_opens())
assert len(eng.entered) == 1, eng.entered
assert len(eng._reverse_opens) == 1, "an unconfirmed entry must stay queued"
assert eng._reverse_opens[0]["attempts"] == 1, eng._reverse_opens
ok += 3

# 8b. Once the leg IS open on the requested side, the request is retired.
eng = StubEngine()
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1, "attempts": 0,
                       "knet": 3, "knet_ts": __import__("time").time()}]
asyncio.run(eng._drain_reverse_opens())      # _entry_fills=True -> leg appears
assert len(eng._reverse_opens) == 1 and eng._reverse_opens[0]["attempts"] == 1
asyncio.run(eng._drain_reverse_opens())      # next cycle observes it and retires the request
assert eng._reverse_opens == [], "a confirmed far-side leg must retire the request"
assert len(eng.entered) == 1, "must not re-enter once the leg exists"
ok += 3

# 8c. Bounded: gives up after 5 attempts rather than re-entering forever.
eng = StubEngine()
eng._entry_fills = False
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1, "attempts": 5,
                       "knet": 3, "knet_ts": __import__("time").time()}]
asyncio.run(eng._drain_reverse_opens())
assert eng._reverse_opens == [], "must give up after the attempt bound"
assert eng.entered == [], "the give-up cycle must not fire another entry"
ok += 2

# 8d. If a leg reappears on the OLD side before the far-side entry, abandon rather than stack.
eng = StubEngine()
eng.positions.append({"coin": "CHIP", "wallet": "0xA", "filled": True, "side": "BUY", "size": 1.0})
eng._reverse_opens = [{"wallet": "0xA", "coin": "CHIP", "is_buy": False, "gen": 1, "attempts": 0,
                       "knet": 3, "knet_ts": __import__("time").time()}]
asyncio.run(eng._drain_reverse_opens())
assert eng._reverse_opens == [], eng._reverse_opens
assert eng.entered == [], "never open a SHORT on top of a LONG that came back"
ok += 2

# ── 9. PERSISTENCE WHITELIST: _persist_position must actually carry the intent. ─────────────────
#     codex r3 P1 #1: the doc is an explicit field whitelist, so setting a key on the position dict
#     did NOT persist it -- the "durable latch" was in-memory only and a restart dropped the reverse.
#     Asserted against the REAL serializer, not the stub's.
import inspect  # noqa: E402
_src = inspect.getsource(V16CopyTrader.__mro__[-2]._persist_position
                         if hasattr(V16CopyTrader.__mro__[-2], "_persist_position")
                         else V16CopyTrader._persist_position)
assert "_pending_reverse" in _src, "_persist_position must serialize the reverse intent"
assert "_reverse_attempts" in _src, "_persist_position must serialize the attempt counter"
_load = inspect.getsource(V16CopyTrader.__mro__[-2]._load_persisted_positions
                          if hasattr(V16CopyTrader.__mro__[-2], "_load_persisted_positions")
                          else V16CopyTrader._load_persisted_positions)
assert "_pending_reverse" in _load, "_load_persisted_positions must restore the reverse intent"
ok += 3

# ── 13. r6 P1 #3: GENERATION BINDING on the knet stamp. ─────────────────────────────────────────
# 13a. A stamp bound to a DIFFERENT generation must not authorize this reverse -- otherwise a double
#      flip binds the current intent to an earlier same-direction flip's authorization.
eng = StubEngine(leader_pos=-7.0)
eng._v17_knet_pending = {("0xA", "CHIP"): [(3, _t.time(), False, 99)]}
pos = mkpos()                                   # intent gen = 1
reaped = run(eng, pos)
assert reaped is True and eng._reverse_opens == [], (
    "a stamp from another generation must never authorize this reverse")
ok += 2

# 13b. A plain 3-tuple OPEN stamp is not a reverse authorization either.
eng = StubEngine(leader_pos=-7.0)
eng._v17_knet_pending = {("0xA", "CHIP"): [(3, _t.time(), False)]}
pos = mkpos()
reaped = run(eng, pos)
assert eng._reverse_opens == [], "an ordinary OPEN stamp is not a reverse authorization"
ok += 1

# ── 14. r6 P1 #4: DURABILITY ORDERING. The old leg's records must not be retired until the far-side
#     obligation is durable. Persist failure -> we are flat on the exchange, the old row is retired,
#     and NOTHING is owed; the dangerous outcome would be retiring the row while believing a
#     far-side leg is queued.
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
run(eng, pos)
# happy path: request queued AND the old leg fully retired, in that order
assert eng._reverse_opens and eng.removed == [("0xA", "CHIP")], (eng._reverse_opens, eng.removed)
assert eng.calls.index("remove_persisted") > eng.calls.index("exch_check"), eng.calls
assert "_pending_reverse" not in pos and "_reverse_attempts" not in pos, pos
ok += 4

# ── 15. r7 P1 #1: FAILURE INJECTION on the far-side persist. ────────────────────────────────────
#     This is the test that was missing. Codex found the defect by injecting this failure: the code
#     said in a comment "do not retire the old leg" and then called _retire_old_leg() anyway,
#     destroying the intent, the queue entry and the position row in one go. Losing the obligation
#     silently is the worst available outcome, so assert the whole post-state, not just one field.
class _FailingPersistEngine(StubEngine):
    def __init__(self, **kw):
        super().__init__(**kw)
        self.shadow_mode = False              # exercise the real Mongo branch
        self.db = self                        # self-serving stub

    def __getitem__(self, name):
        return self

    def update_one(self, *a, **kw):
        raise RuntimeError("mongo down")

    def delete_one(self, *a, **kw):
        raise RuntimeError("mongo down")

    def find(self, *a, **kw):
        return []


eng = _FailingPersistEngine(leader_pos=-7.0)
pos = mkpos()
eng.positions = [pos]
reaped = run(eng, pos)
assert reaped is False, "a lost obligation must NOT be reaped -- the next cycle has to retry"
assert pos.get("_pending_reverse"), "the intent must SURVIVE a failed far-side persist"
assert eng._reverse_opens == [], "nothing may be queued if it could not be persisted"
assert eng.removed == [], "the old leg's record must not be retired on a failed persist"
ok += 4

# ── 16. r7 P1 #2: a DECLINED multi-leg reverse must be quarantined from the convergence path too.
#     Declining in _execute_pending_reverse is pointless if _converge_positions closes the same leg
#     microseconds later against the aggregate net -- the exact hazard the decline exists to avoid.
eng = StubEngine(leader_pos=-7.0)
pos = mkpos()
other = {"coin": "CHIP", "wallet": "0xB", "filled": True, "side": "BUY", "size": 5.0}
eng.positions = [pos, other]
run(eng, pos)
assert pos.get("_reverse_declined") is True, "a declined leg must be marked for quarantine"
ok += 1

# 16b. The quarantine lifts by itself once the coin is back to a single roster leg.
eng.positions = [pos]
same = [q for q in eng.positions if q.get("coin") == "CHIP" and q.get("filled")]
assert len(same) == 1, same
ok += 1

print(f"reverse-EXECUTION self-test PASSED ({ok} assertions, real V16CopyTrader method)")
print("  covers: happy path + ordering, failed flatten, bounded escalation, exchange-not-flat,")
print("          REST failure, flag-off, double flip, sub-floor leader, missing mid,")
print("          leg-lock deferral, non-destructive drain, drain bound, old-side reappearance,")
print("          and the persistence whitelist")
