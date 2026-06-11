#!/usr/bin/env python3
"""V16 per-coin concurrency -- empirical basis for max_coin_notional_pct (Alberto msg 9214).

For the LIVE cohort (config/copy_trader_wallets_v16.json), reconstruct their liquid-major round-trips
over the selection window and measure, at each entry: how many cohort positions were ALREADY open on
that same coin. cap K => entries arriving with >=K same-coin opens are BLOCKED. Report the blocked
share for K=2,3,4,6,8 and the concurrency distribution; pick K binding on <5% of entries.
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_HERE))

import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from _streaming_io import install_memory_guard
from select_cohort import load_wallet_fills, LIQUID

MAX_HOLD_MS = 172_800_000   # live 48h max-hold caps our position lifetime


def main():
    install_memory_guard(soft_gb=8.0, label="v16_conc")
    cfg = json.load(open(_REPO / "config" / "copy_trader_wallets_v16.json"))
    cohort = set(cfg["wallets"].keys())
    asof = pd.Timestamp(cfg["global"]["cohort_asof"])
    end_ms = int(asof.timestamp() * 1000)
    start_ms = end_ms - cfg["global"]["cohort_window_days"] * 86_400_000
    print(f"cohort {len(cohort)} wallets, window {pd.Timestamp(start_ms, unit='ms', tz='UTC')} -> {asof}")

    wf = load_wallet_fills(cohort, start_ms, end_ms)
    print(f"{len(wf)} cohort wallets with fills")

    # intervals per coin: (entry_ts, exit_ts) for liquid RTs, exit capped at entry+48h (live max_hold)
    by_coin = defaultdict(list)
    events = []   # (entry_ts, coin)
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        for c, dir_, ets, xts, *_ in roundtrips(fl):
            if c not in LIQUID or not (start_ms <= ets < end_ms):
                continue
            by_coin[c].append((ets, min(xts, ets + MAX_HOLD_MS)))
            events.append((ets, c))

    print(f"{len(events)} liquid round-trip entries across {len(by_coin)} coins")
    coin_share = pd.Series({c: len(v) for c, v in by_coin.items()}).sort_values(ascending=False)
    print("\nentries per coin:")
    for c, n in coin_share.items():
        print(f"  {c:>6}: {n:>5} ({n/len(events)*100:.0f}%)")

    # at each entry: count already-open same-coin intervals (strict: entered before, not yet exited)
    blocked = {k: 0 for k in (2, 3, 4, 6, 8)}
    conc_dist = defaultdict(int)
    for c, ivals in by_coin.items():
        ivals.sort()
        for i, (e, x) in enumerate(ivals):
            open_now = sum(1 for (e2, x2) in ivals if e2 < e and x2 > e)
            conc_dist[open_now] += 1
            for k in blocked:
                if open_now >= k:
                    blocked[k] += 1
    n = len(events)
    print("\nsame-coin concurrency at entry (cohort-wide):")
    for k in sorted(conc_dist):
        print(f"  {k} already open: {conc_dist[k]:>5} ({conc_dist[k]/n*100:.1f}%)")
    print("\nblocked share by per-coin position cap K:")
    for k, b in sorted(blocked.items()):
        print(f"  cap K={k} (notional ${k*100} at $100/trade): blocks {b} entries = {b/n*100:.1f}%")
    print("\nrecommendation: smallest K with blocked < 5%")


if __name__ == "__main__":
    main()
