#!/usr/bin/env python3
"""v26 causal trailing-14d fee tier engine (amendment codex #10, frozen).

One snapshot of the HL fee/tier schedule (app/data/research/v25/fee_snapshot_v26.json,
pulled via API BEFORE freeze, sha256 recorded in FREEZE-GRID.json). Applied CAUSALLY:
each sim day d uses the tier from the config's OWN trailing 14d simulated volume over
days d-14..d-1 (strictly prior days -- volume traded today never moves today's fee).
Starts at the base tier. FOLD BOUNDARIES RESET VOLUME TO ZERO: each fold is an
independent cold start (one engine instance per config x fold x scenario).

Rate mapping (decision D6, v26_common):
- BASE: snapshot tier rates x (1 - activeReferralDiscount). Base tier cross 0.00045 x
  0.96 = 4.32bps -- exactly the v25 BASE measured schedule; tiers upgrade causally.
- WORST: snapshot base-tier rates, NO referral discount, NO tier evolution (4.5bps
  taker, 1.5bps maker -- the v25 WORST taker number, now snapshot-sourced).
- HIP-3 (prefixed markets): multiplier 2.0 on the perp rate (same source as v25).
- maker rate = the tier's "add" rate (negative mm rebate tiers are NOT modeled; we are
  not a maker-fraction market maker).

The fixed 1.44/2.88 constants are REMOVED; every fee this harness charges comes from the
snapshot table through this engine.
"""
from __future__ import annotations

import json
from pathlib import Path

from v26_common import FEE_SNAPSHOT_PATH, MS_DAY, coin_is_hip3

HIP3_MULT = 2.0
TRAIL_DAYS = 14


def load_snapshot(path: Path = FEE_SNAPSHOT_PATH) -> dict:
    with open(path) as fh:
        return json.load(fh)


class FeeEngine:
    """Causal trailing-14d tier engine over ONE fold window for ONE config x scenario.

    record_volume(ts_ms, notional) accrues the config's own simulated fill volume onto
    its UTC day; rate(ts_ms, coin, maker) returns the fee FRACTION for a fill at ts
    using the tier implied by volume on the 14 strictly-prior days. tier_departed_base
    counts fills priced at a non-base tier (decision D5 assertion)."""

    def __init__(self, snapshot: dict, mode: str = "BASE"):
        assert mode in ("BASE", "WORST")
        self.mode = mode
        fs = snapshot["data"]["feeSchedule"]
        self.base_cross = float(fs["cross"])
        self.base_add = float(fs["add"])
        # vip tiers sorted by ascending 14d-volume cutoff; a tier applies when the
        # trailing 14d volume >= its ntlCutoff (highest such tier wins)
        self.tiers = sorted(
            [{"cutoff": float(t["ntlCutoff"]), "cross": float(t["cross"]),
              "add": float(t["add"])} for t in fs.get("tiers", {}).get("vip", [])],
            key=lambda t: t["cutoff"])
        ref = snapshot["data"].get("activeReferralDiscount") or 0.0
        self.discount = float(ref) if mode == "BASE" else 0.0
        self.tier_departed_base = 0
        self._vol: dict[int, float] = {}

    def reset(self):
        """Fold boundary: volume resets to zero (independent cold start)."""
        self._vol = {}
        self.tier_departed_base = 0

    def record_volume(self, ts_ms: int, notional: float):
        d = int(ts_ms // MS_DAY)
        self._vol[d] = self._vol.get(d, 0.0) + float(notional)

    def _vol14(self, day: int) -> float:
        return sum(self._vol.get(d, 0.0) for d in range(day - TRAIL_DAYS, day))

    def _tier_rates(self, ts_ms: int) -> tuple[float, float]:
        if self.mode == "WORST":
            return self.base_cross, self.base_add     # frozen: no tier evolution
        v = self._vol14(int(ts_ms // MS_DAY))
        cross, add = self.base_cross, self.base_add
        departed = False
        for t in self.tiers:
            if v >= t["cutoff"]:
                cross, add, departed = t["cross"], t["add"], True
        if departed:
            self.tier_departed_base += 1
        return cross, add

    def rate(self, ts_ms: int, coin: str, maker: bool) -> float:
        cross, add = self._tier_rates(ts_ms)
        r = (add if maker else cross) * (1.0 - self.discount)
        if coin_is_hip3(coin):
            r *= HIP3_MULT
        return r

    def base_taker_rate(self, coin: str) -> float:
        """Base-tier taker rate (trigger-geometry rate, decision D5)."""
        r = self.base_cross * (1.0 - self.discount)
        return r * HIP3_MULT if coin_is_hip3(coin) else r

    def base_maker_rate(self, coin: str) -> float:
        r = self.base_add * (1.0 - self.discount)
        return r * HIP3_MULT if coin_is_hip3(coin) else r
