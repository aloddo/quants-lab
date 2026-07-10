#!/usr/bin/env python3
"""Aggregate the clean per-coin calendar-fold CV (copyA_cv.py output).

Answers the honest questions the rejected pack could not:
  - Of (wallet,coin) pairs SELECTED on IN-only evidence, what fraction persist OOS
    (have OOS trades AND positive OOS $)? vs the non-selected baseline (the real lift).
  - What is the clean per-coin roster and its OOS $/mo distribution?
Every number here is calendar-fold, IN-only-selected, per-coin, OOS-opened-after-cutoff.
"""
import json, sys
import numpy as np

JSONL = "app/data/copyA/cv_percoin.jsonl"


def load_pairs():
    pairs = []
    for line in open(JSONL):
        try:
            r = json.loads(line)
        except Exception:
            continue
        if r.get("status") != "ok":
            continue
        pairs.extend(r.get("pairs", []))
    return pairs


def main():
    pairs = load_pairs()
    sel = [p for p in pairs if p["selected"]]
    notsel = [p for p in pairs if not p["selected"]]
    # persistence requires the wallet actually traded the coin in OOS
    sel_active = [p for p in sel if p["oos_pos"] > 0]
    notsel_active = [p for p in notsel if p["oos_pos"] > 0]

    def posrate(g):
        return np.mean([p["oos_usd_mo"] > 0 for p in g]) if g else 0.0

    print(f"total (wallet,coin) pairs: {len(pairs)}")
    print(f"IN-selected pairs: {len(sel)}  (with OOS activity: {len(sel_active)})")
    print(f"non-selected pairs: {len(notsel)}  (with OOS activity: {len(notsel_active)})\n")
    if sel_active:
        print(f"SELECTED OOS-positive rate: {posrate(sel_active):.1%}  "
              f"median OOS $/mo: {np.median([p['oos_usd_mo'] for p in sel_active]):.2f}  "
              f"mean: {np.mean([p['oos_usd_mo'] for p in sel_active]):.2f}")
    if notsel_active:
        print(f"BASELINE (non-selected) OOS-positive rate: {posrate(notsel_active):.1%}  "
              f"median OOS $/mo: {np.median([p['oos_usd_mo'] for p in notsel_active]):.2f}")
    # clean roster: selected, OOS active, OOS positive, decent OOS consistency
    roster = [p for p in sel_active if p["oos_usd_mo"] > 0 and p["oos_posf"] >= 0.6 and p["oos_mo"] >= 2]
    roster.sort(key=lambda p: (p["oos_posf"], p["oos_usd_mo"]), reverse=True)
    print(f"\nCLEAN PER-COIN ROSTER (selected, OOS posf>=.6, >=2 OOS months, $>0): {len(roster)}\n")
    hdr = f"{'wallet':<44}{'coin':>6}{'S':>5}{'INpos':>6}{'INposf':>7}{'IN$/mo':>8}{'OOSpos':>7}{'OOSposf':>8}{'OOS$/mo':>8}"
    print(hdr); print("-" * len(hdr))
    for p in roster[:50]:
        print(f"{p['wallet']:<44}{p['coin']:>6}{p['S']:>5.0f}{p['in_pos']:>6}{p['in_posf']:>7.2f}"
              f"{p['in_usd_mo']:>8.2f}{p['oos_pos']:>7}{p['oos_posf']:>8.2f}{p['oos_usd_mo']:>8.2f}")
    if roster:
        tot = sum(p["oos_usd_mo"] for p in roster)
        print(f"\nroster OOS $/mo sum (all {len(roster)}): ${tot:.0f}; "
              f"top-10 sum: ${sum(p['oos_usd_mo'] for p in roster[:10]):.0f}")
        # coin concentration
        from collections import Counter
        cc = Counter(p["coin"] for p in roster)
        print("coin mix (top):", dict(cc.most_common(8)))


if __name__ == "__main__":
    main()
