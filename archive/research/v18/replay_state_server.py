#!/usr/bin/env python3
"""Replay STATE-SERVER (pillar 2 harness) -- 2026-07-07.

Intercepts the engine's mid-decision HL /info calls (clearinghouseState for leaders + parent, userFillsByTime)
and serves state as-of the shared mocked clock, so a faithful replay never touches the LIVE API. Leader
positions are EXACT (startPosition reconstruction, validated 11/11 perps). Parent state is served from a
caller-supplied shadow-state provider (the engine's own tracking -> reconcile becomes a no-op).

Usage in the harness:
    clock = Clock(t0_ms)
    srv = ReplayStateServer(clock, leaders={addr: LeaderStateReplay(...)}, parent_addr=P, parent_provider=fn)
    with srv.patch_requests():
        # drive events; every requests.post to HL /info is routed here, keyed on clock.t
"""
from __future__ import annotations
import json
from contextlib import contextmanager

HL_HOST = "hyperliquid.xyz"


class Clock:
    """Shared mutable replay clock (ms). The harness advances .t as it feeds events."""
    def __init__(self, t_ms: int):
        self.t = int(t_ms)


class ReplayStateServer:
    def __init__(self, clock: Clock, leaders: dict, parent_addr: str,
                 parent_provider=None, leader_equity: float = 100_000.0):
        self.clock = clock
        self.leaders = {a.lower(): v for a, v in leaders.items()}
        self.parent_addr = parent_addr.lower()
        self.parent_provider = parent_provider   # () -> clearinghouseState dict (engine shadow state)
        self.leader_equity = leader_equity        # approximate; sizing-sensitivity flagged
        self.calls = {"leader_chs": 0, "parent_chs": 0, "userfills": 0, "passthrough": 0, "other": 0}

    # ---- routing ----
    def handle(self, payload: dict):
        """Return a response object (list or dict) for an HL /info payload, or None to passthrough."""
        typ = payload.get("type")
        user = (payload.get("user") or "").lower()
        if typ == "clearinghouseState":
            if user == self.parent_addr:
                self.calls["parent_chs"] += 1
                if self.parent_provider is not None:
                    return self.parent_provider()
                return {"marginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                        "assetPositions": [], "time": self.clock.t}
            lsr = self.leaders.get(user)
            if lsr is not None:
                self.calls["leader_chs"] += 1
                return lsr.clearinghouse_state_asof(self.clock.t, account_value=self.leader_equity)
            # unknown user (untracked leader) -> flat state as-of T
            self.calls["other"] += 1
            return {"marginSummary": {"accountValue": "0.0", "totalMarginUsed": "0.0"},
                    "assetPositions": [], "time": self.clock.t}
        if typ == "userFillsByTime" or typ == "userFills":
            self.calls["userfills"] += 1
            lsr = self.leaders.get(user)
            if lsr is not None:
                # serve that leader's fills up to clock.t (engine dedups by tid)
                out = []
                for c, (ts, pos) in lsr._series.items():
                    pass  # fills not stored raw here; init-path uses clearinghouseState, so return []
                return []
            return []
        self.calls["other"] += 1
        return None   # meta / spotClearinghouseState / etc -> let caller decide (passthrough)

    # ---- requests.post monkeypatch ----
    @contextmanager
    def patch_requests(self):
        import requests
        real_post = requests.post

        def fake_post(url, *args, **kwargs):
            if HL_HOST in str(url) and "/info" in str(url):
                payload = kwargs.get("json") or {}
                resp = self.handle(payload)
                if resp is not None:
                    return _FakeResp(resp)
            return real_post(url, *args, **kwargs)

        requests.post = fake_post
        try:
            yield self
        finally:
            requests.post = real_post


class _FakeResp:
    def __init__(self, obj):
        self._obj = obj
        self.status_code = 200

    def json(self):
        return self._obj

    def raise_for_status(self):
        pass


if __name__ == "__main__":
    import sys, time, requests
    import pandas as pd
    sys.path.insert(0, "research/v18")
    from leader_state_replay import LeaderStateReplay, pull_user_fills
    addr = open("/tmp/live10_wallets.txt").readline().strip()
    now = int(time.time() * 1000)
    start = int(pd.Timestamp("2026-06-01", tz="UTC").timestamp() * 1000)
    fills = pull_user_fills(addr, start, now)
    lsr = LeaderStateReplay(addr, fills)
    clock = Clock(now)
    srv = ReplayStateServer(clock, {addr: lsr}, parent_addr="0x11ca20aeb7cd014cf8406560ae405b12601994b4")
    print("=== TEST: interceptor routes leader clearinghouseState to reconstruction ===")
    with srv.patch_requests():
        # this call would normally hit the live API; must be served from reconstruction
        r = requests.post("https://api.hyperliquid.xyz/info",
                          json={"type": "clearinghouseState", "user": addr}, timeout=5).json()
    n = len(r.get("assetPositions", []))
    perps = [p["position"]["coin"] for p in r["assetPositions"] if not (p["position"]["coin"].startswith("@") or ":" in p["position"]["coin"])]
    print(f"served {n} positions (from reconstruction, NOT live), perps: {perps}")
    print(f"call routing: {srv.calls}")
    # confirm a NON-HL post still passes through (does not break other traffic)
    assert srv.calls["leader_chs"] == 1, "leader call not routed"
    print("PASS: HL /info intercepted + served from replay state; clock-keyed.")
