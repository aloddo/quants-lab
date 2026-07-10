#!/usr/bin/env python3
"""live_cohort_freeze_classify.py -- FREEZE the copy-edge classification of the 100 LIVE cohort wallets
as-of now (codex step-1 directive: "freeze the rule now", then shadow-track forward).

For each live wallet, compute the copy-edge (our taker net, via execution_model) + turnover over the most
recent available data window (the frozen estimate). Classify copy+ / copy-. This list is the FROZEN rule;
forward shadow-tracking compares current-cohort vs copy+ vs excluded copy- on real live data going forward.
NO production change -- this only labels.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/live_cohort_freeze_classify.py
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path
import numpy as np, pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, slip_oneway

FEE_T = fee_rt(maker=False); CAP = 500.0 / 1e4
WIN_START = "2026-03-23"; WIN_END = "2026-05-23"   # most-recent ~2mo of data = frozen estimate window
LAT_MS = 2000


def _ms(d): return int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)


def main():
    set_latency_ms(LAT_MS)
    liquid = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())
    cfg = json.load(open("config/copy_trader_wallets_v17_expansion.json"))
    live = {w.lower() for w in cfg["wallets"]}
    lo, hi = _ms(WIN_START), _ms(WIN_END)
    print(f"FREEZE classify: {len(live)} live wallets, window {WIN_START}..{WIN_END}, liquid-only, taker.")
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(str(S.M02_ACTIONS))
    wf = defaultdict(list)
    for b in pf.iter_batches(batch_size=1_000_000, columns=["wallet", "coin", "ts", "signed_size", "price"]):
        d = b.to_pydict()
        for i in range(len(d["wallet"])):
            w = d["wallet"][i]; t = d["ts"][i]
            if w in live and lo <= t <= hi:
                wf[w].append((t, d["coin"][i], d["signed_size"][i], d["price"][i]))
    rows = []
    for w in live:
        fl = sorted(wf.get(w, []), key=lambda x: x[0]); rts = roundtrips(fl)
        nets = []
        for c, dir_, ets, xts, evw, xvw, g in rts:
            if c not in liquid:
                continue
            ent = S.mark_at(c, ets + LAT_MS); ex = S.mark_at(c, xts + LAT_MS)
            if ent is None or ex is None or ent <= 0:
                continue
            og = max(-CAP, min(CAP, dir_ * (ex - ent) / ent))
            nets.append(og - FEE_T - slip_oneway(c) * 2.0)
        n = len(nets)
        edge = float(np.mean(nets) * 1e4) if n else None
        rows.append({"wallet": w, "n_rt_liquid": n, "copy_edge_bps": edge,
                     "cls": ("copy+" if (edge is not None and edge > 0) else
                             "copy-" if edge is not None else "insufficient")})
    df = pd.DataFrame(rows).sort_values("copy_edge_bps", ascending=False, na_position="last")
    cp = (df.cls == "copy+").sum(); cn = (df.cls == "copy-").sum(); ins = (df.cls == "insufficient").sum()
    print(f"\nFROZEN CLASSIFICATION ({WIN_START}..{WIN_END}, >=1 liquid RT to classify):")
    print(f"  copy+: {cp}  | copy-: {cn}  | insufficient(no liquid RT in window): {ins}  | total {len(df)}")
    print(f"\n--- copy-NEGATIVE wallets (downweight/exclude candidates) ---")
    for r in df[df.cls == "copy-"].itertuples():
        print(f"  {r.wallet} edge {r.copy_edge_bps:+.1f}bps n={r.n_rt_liquid}")
    df.to_parquet("app/data/v15/live_cohort_freeze_classification.parquet")
    print(f"\nsaved app/data/v15/live_cohort_freeze_classification.parquet ({len(df)} wallets). FROZEN as-of {WIN_END}.")


if __name__ == "__main__":
    main()
