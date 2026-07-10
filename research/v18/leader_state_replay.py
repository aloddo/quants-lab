#!/usr/bin/env python3
"""Leader state-as-of-T reconstruction (pillar 2 replay state-server component) -- 2026-07-07.

The engine calls clearinghouseState(leader) mid-decision (NOT shadow-guarded) to refresh each leader's
positions + equity for proportional sizing / exit detection. For a faithful replay we must serve that call
with the leader's TRUE state as-of the mocked clock T, not the live API (which returns CURRENT state).

This reconstructs, per coin, the leader's position AFTER their last fill <= T, using HL's `startPosition`
field (position held JUST BEFORE each fill) + that fill's signed size -> exact, no drift. Returns a
clearinghouseState-shaped dict the engine can parse unchanged.

Validation (in __main__): reconstruct at T=now and compare to the live clearinghouseState -> must match.
"""
from __future__ import annotations
import time
import requests
import numpy as np

HL_API = "https://api.hyperliquid.xyz"


def pull_user_fills(addr: str, start_ms: int, end_ms: int):
    fills, s = [], start_ms
    for _ in range(60):
        r = None
        for a in range(6):
            try:
                r = requests.post(f"{HL_API}/info",
                                  json={"type": "userFillsByTime", "user": addr,
                                        "startTime": s, "endTime": end_ms}, timeout=25).json()
            except Exception:
                r = None
            if isinstance(r, list):
                break
            time.sleep(1.5 * (a + 1))
        if not isinstance(r, list) or not r:
            break
        fills += r
        if len(r) < 2000:
            break
        s = max(f["time"] for f in r) + 1
    return fills


class LeaderStateReplay:
    """Precompute per-coin (ts_ms, position_after) from fills; O(log n) as-of lookup."""

    def __init__(self, addr: str, fills: list):
        self.addr = addr.lower()
        self._series = {}  # coin -> (ts_array, pos_after_array)
        by_coin = {}
        for f in fills:
            c = f.get("coin")
            if not c or f.get("px") is None:
                continue
            by_coin.setdefault(c, []).append(f)
        for c, fs in by_coin.items():
            fs.sort(key=lambda x: x["time"])
            ts, pos = [], []
            for f in fs:
                sp = float(f.get("startPosition", 0) or 0)
                ssz = float(f["sz"]) * (1 if f["side"] == "B" else -1)
                ts.append(int(f["time"]))
                pos.append(sp + ssz)                 # position AFTER this fill
            self._series[c] = (np.asarray(ts, dtype="int64"), np.asarray(pos, dtype="float64"))

    def positions_asof(self, t_ms: int) -> dict:
        """coin -> signed position size held at t_ms (0 if flat/no fills yet)."""
        out = {}
        for c, (ts, pos) in self._series.items():
            i = int(np.searchsorted(ts, t_ms, side="right")) - 1
            if i >= 0:
                p = pos[i]
                if abs(p) > 1e-12:
                    out[c] = float(p)
        return out

    def clearinghouse_state_asof(self, t_ms: int, account_value: float = None) -> dict:
        """clearinghouseState-shaped dict the engine parses. account_value is approximate (served constant
        or caller-supplied); position sizes are EXACT from startPosition reconstruction."""
        aps = []
        for c, szi in self.positions_asof(t_ms).items():
            aps.append({"position": {"coin": c, "szi": str(szi), "entryPx": None,
                                     "unrealizedPnl": "0.0", "marginUsed": "0.0",
                                     "leverage": {"type": "cross", "value": 1}}})
        av = str(account_value) if account_value is not None else "0.0"
        return {"marginSummary": {"accountValue": av, "totalMarginUsed": "0.0"},
                "assetPositions": aps, "time": t_ms}


if __name__ == "__main__":
    import pandas as pd
    addr = open("/tmp/live10_wallets.txt").readline().strip()
    now_ms = int(time.time() * 1000)
    start_ms = int(pd.Timestamp("2026-06-01", tz="UTC").timestamp() * 1000)
    fills = pull_user_fills(addr, start_ms, now_ms)
    print(f"leader {addr[:12]}: {len(fills)} fills since Jun1")
    lsr = LeaderStateReplay(addr, fills)
    recon = lsr.positions_asof(now_ms)
    # live truth
    live = requests.post(f"{HL_API}/info", json={"type": "clearinghouseState", "user": addr}, timeout=15).json()
    live_pos = {p["position"]["coin"]: float(p["position"]["szi"]) for p in live.get("assetPositions", [])}
    print(f"reconstructed@now: {len(recon)} coins | live: {len(live_pos)} coins")
    # compare
    coins = set(recon) | set(live_pos)
    ok, mism = 0, []
    for c in coins:
        rv, lv = recon.get(c, 0.0), live_pos.get(c, 0.0)
        if abs(rv - lv) <= max(1e-6, 0.01 * abs(lv)):
            ok += 1
        else:
            mism.append((c, round(rv, 4), round(lv, 4)))
    print(f"MATCH: {ok}/{len(coins)} coins" + (f" | MISMATCH: {mism[:8]}" if mism else " | ALL MATCH"))
