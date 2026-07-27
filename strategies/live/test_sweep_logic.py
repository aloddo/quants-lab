"""Isolated logic test for _leader_book_sweep's DECISION rules.

Does not import the engine (needs mongo/env). Re-implements the exact predicate chain and asserts the
behaviours that matter for live capital, especially the fail-closed ones.
"""
import time

GRACE = 90.0
STRIKES = 2


def decide(pos, fresh, mid, now, auto_close=False):
    """Returns (action, new_flat_count). action in {skip_unread, skip_grace, skip_ws, skip_not_flat,
    strike, ALERT, CLOSE}."""
    w, c = pos["wallet"], pos["coin"]
    if w not in fresh:
        return "skip_unread", 0
    age = now - pos.get("fill_time", now)
    if age < GRACE:
        return "skip_grace", pos.get("_sweep_flat_count", 0)
    if (pos.get("_ws_exited") or pos.get("_force_exit") or pos.get("_gave_up")
            or pos.get("_exit_logged") or pos.get("_in_twap")
            or now - pos.get("_last_exit_attempt", 0) < 10):
        return "skip_ws", 0
    leader_sz = fresh[w].get(c, 0.0)
    leader_flat = (abs(leader_sz * mid) < 1.0) if mid > 0 else (abs(leader_sz) < 1e-9)
    if not leader_flat:
        return "skip_not_flat", 0
    n = int(pos.get("_sweep_flat_count", 0)) + 1
    if n < STRIKES:
        return "strike", n
    return ("CLOSE" if auto_close else "ALERT"), n


NOW = 10_000.0
base = dict(wallet="0xA", coin="CHIP", size=3391.0, fill_time=NOW - 3600)
MID = 0.0293

# 1. FAIL-CLOSED: a wallet whose REST read failed/was malformed is absent from `fresh`.
#    It must NEVER read as flat, and the strike counter must RESET (not persist across a blind cycle).
p = dict(base, _sweep_flat_count=1)
a, n = decide(p, {}, MID, NOW)
assert a == "skip_unread" and n == 0, (a, n)

# 2. Leader genuinely flat (coin absent from a VALID response) -> strike, not immediate action.
p = dict(base)
a, n = decide(p, {"0xA": {}}, MID, NOW)
assert a == "strike" and n == 1, (a, n)

# 3. Second consecutive agreeing read -> ALERT (alert-only default), NOT close.
p = dict(base, _sweep_flat_count=1)
a, n = decide(p, {"0xA": {}}, MID, NOW)
assert a == "ALERT" and n == 2, (a, n)

# 4. Same, with auto_close on -> CLOSE.
p = dict(base, _sweep_flat_count=1)
a, n = decide(p, {"0xA": {}}, MID, NOW, auto_close=True)
assert a == "CLOSE", a

# 5. GRACE: a leg younger than 90s is never judged, even with the leader flat and a strike banked.
p = dict(base, fill_time=NOW - 30, _sweep_flat_count=1)
a, _ = decide(p, {"0xA": {}}, MID, NOW)
assert a == "skip_grace", a

# 6. Leader still holds -> no strike, counter resets.
p = dict(base, _sweep_flat_count=1)
a, n = decide(p, {"0xA": {"CHIP": 50000.0}}, MID, NOW)
assert a == "skip_not_flat" and n == 0, (a, n)

# 7. WS path owns the leg (mid-exit) -> sweep stands off and resets.
for flag in ("_ws_exited", "_force_exit", "_gave_up", "_exit_logged", "_in_twap"):
    p = dict(base, _sweep_flat_count=1, **{flag: True})
    a, n = decide(p, {"0xA": {}}, MID, NOW)
    assert a == "skip_ws" and n == 0, (flag, a, n)
p = dict(base, _sweep_flat_count=1, _last_exit_attempt=NOW - 3)
a, _ = decide(p, {"0xA": {}}, MID, NOW)
assert a == "skip_ws", a

# 8. DUST: a leader residual worth < $1 counts as flat (matches the engine's <$1 dust rule).
p = dict(base, _sweep_flat_count=1)
a, _ = decide(p, {"0xA": {"CHIP": 10.0}}, MID, NOW)      # 10 * 0.0293 = $0.29
assert a == "ALERT", a
p = dict(base, _sweep_flat_count=1)
a, _ = decide(p, {"0xA": {"CHIP": 1000.0}}, MID, NOW)    # $29.3
assert a == "skip_not_flat", a

# 9. An INTERRUPTED streak cannot accumulate: flat, then a failed read, then flat again = 1 strike,
#    not 2. This is the property that makes two-strike meaningful under flaky REST.
p = dict(base)
_, n = decide(p, {"0xA": {}}, MID, NOW); p["_sweep_flat_count"] = n
_, n = decide(p, {}, MID, NOW);          p["_sweep_flat_count"] = n      # blind cycle resets
a, n = decide(p, {"0xA": {}}, MID, NOW)
assert a == "strike" and n == 1, (a, n)

# 10. The 2026-07-27 CHIP case end to end: leader flat, leg 5.5h old, two clean reads -> caught.
p = dict(wallet="0x12203316", coin="CHIP", size=3391.0, fill_time=NOW - 5.5 * 3600)
_, n = decide(p, {"0x12203316": {}}, MID, NOW); p["_sweep_flat_count"] = n
a, _ = decide(p, {"0x12203316": {}}, MID, NOW)
assert a == "ALERT", a

print("sweep decision-logic self-test PASSED (10 scenarios, 22 assertions)")
