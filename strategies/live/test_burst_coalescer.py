"""Isolated test of the burst-coalescer semantics (no engine import).

Models the real concurrency shape: _on_hl_trade is SYNCHRONOUS and processes an entire burst in one
loop iteration, so no spawned task has run when the last trade is handled.
"""
import asyncio

MIN_ORDER = 11.0


class Fake:
    """Mirrors the coalescer's state machine and the one order-sizing rule."""

    def __init__(self, our_usd, debounce=0.01, fill_s=0.0):
        self.inflight, self.dirty = set(), set()
        self.debounce = debounce
        self.fill_s = fill_s
        self.leader = {}          # key -> leader position (units)
        self.first = {}           # key -> leader's opening size
        self.our_usd = our_usd    # our current signed notional
        self.orders = []          # every order actually placed
        self.tasks = []

    # --- synchronous trade handler: this is the critical part ---
    def on_trade(self, key, leader_pos):
        self.leader[key] = leader_pos          # tracker updates ALWAYS, even when coalesced
        if key in self.inflight:
            self.dirty.add(key)
            return
        self.inflight.add(key)
        self.tasks.append(asyncio.ensure_future(self._converge_once(key)))

    async def _converge_once(self, key):
        try:
            for _ in range(3):
                await asyncio.sleep(self.debounce)
                self.dirty.discard(key)
                # recompute BOTH sides from CURRENT state
                tgt = 100.0 * (self.leader[key] / self.first[key])
                delta = tgt - self.our_usd
                if delta < MIN_ORDER:
                    return
                await asyncio.sleep(self.fill_s)  # order round-trip (real IOC took ~1.1s)
                self.orders.append(round(delta, 2))
                self.our_usd += delta            # our position now reflects the fill
                if key not in self.dirty:
                    return
        finally:
            self.inflight.discard(key)
            self.dirty.discard(key)


async def main():
    K = ("0x12203316", "CHIP")

    # 1. THE CHIP INCIDENT. Five adds in one synchronous burst. Old behaviour placed FIVE escalating
    #    orders ($981/$1520/$1587/$2611/$3530) because each read the same stale our_usd=$99.
    f = Fake(our_usd=99.0)
    f.first[K] = 2530.0
    for lp in (27334.0, 40968.0, 42670.0, 68568.0, 91827.0):
        f.on_trade(K, lp)
    await asyncio.gather(*f.tasks)
    assert len(f.orders) == 1, f"expected ONE coalesced order, got {f.orders}"
    # sized off the FINAL leader position: 100 * 91827/2530 = 3629.5, minus our 99
    assert abs(f.orders[0] - 3530.5) < 1.0, f.orders
    print(f"  1. five-add burst -> {len(f.orders)} order {f.orders} (was 5 escalating)")

    # 2. Coalescing is not single-shot: it must use the FINAL target, not the first burst member.
    #    A single-shot-on-first design would have produced ~$981 (the 10.8x rung).
    assert f.orders[0] > 3000, "single-shot bug: sized off an early burst member"

    # 3. Tracker is updated for EVERY trade even when the order is coalesced away -- otherwise the
    #    leader position would silently diverge.
    assert f.leader[K] == 91827.0

    # 4. State fully released after the burst.
    assert not f.inflight and not f.dirty

    # 5. DIRTY re-loop: an add landing DURING our order round-trip triggers exactly one more
    #    convergence, not a dropped signal. (An add arriving inside the DEBOUNCE window is
    #    correctly coalesced into the single order instead -- covered by test 1.)
    f2 = Fake(our_usd=100.0, debounce=0.01, fill_s=0.05)
    f2.first[K] = 1000.0
    f2.on_trade(K, 2000.0)                       # target 200 -> order 100
    await asyncio.sleep(0.03)                    # past debounce, DURING the order round-trip
    f2.on_trade(K, 5000.0)                       # target 500, arrives while in flight -> dirty
    await asyncio.gather(*f2.tasks)
    assert len(f2.orders) == 2, f2.orders
    assert abs(sum(f2.orders) - 400.0) < 1.0, f2.orders   # 100 -> 500 total
    print(f"  5. mid-flight add -> {len(f2.orders)} orders {f2.orders}, total converges to target")

    # 6. Deltas below the minimum order size place nothing.
    f3 = Fake(our_usd=100.0)
    f3.first[K] = 1000.0
    f3.on_trade(K, 1050.0)                       # target 105 -> delta 5 < 11
    await asyncio.gather(*f3.tasks)
    assert f3.orders == [], f3.orders
    print("  6. sub-minimum delta -> no order")

    # 7. Bounded: a leader adding continuously cannot spin the loop forever.
    f4 = Fake(our_usd=100.0, debounce=0.001)
    f4.first[K] = 1000.0
    f4.on_trade(K, 2000.0)
    for i in range(20):                          # keep marking dirty
        f4.dirty.add(K)
        f4.leader[K] = 3000.0 + i * 1000.0
        await asyncio.sleep(0.001)
    await asyncio.gather(*f4.tasks)
    assert len(f4.orders) <= 3, f"loop not bounded: {len(f4.orders)} orders"
    print(f"  7. continuous adds -> bounded at {len(f4.orders)} orders (cap 3)")

    print("burst-coalescer self-test PASSED")

asyncio.run(main())
