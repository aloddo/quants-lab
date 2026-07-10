#!/usr/bin/env python3
"""mae_bag_measure.py -- ABSOLUTE, non-circular bag-holding measure (Alberto 2026-07-07 voice, GO).

The p90-vs-own-history trigger is circular: a chronic bag-holder's own p90 is inflated BY the bags.
The un-launderable measure is MAXIMUM ADVERSE EXCURSION (MAE): how deep underwater each position goes
DURING the hold (absolute %, entry-relative, direction-signed), plus TIME-UNDERWATER (fraction of the
hold spent in the red past a threshold). Cross-sectional, not self-referenced: a disciplined leader's
positions rarely excurse past -X%; a bag-holder's routinely do, whether or not they eventually recover.

REUSES research/v15/leadlag_clean_rank_sim.mark_at (page-cached per-coin asof marks). Streams per
journey (scalar accumulation, no giant DataFrame) -> memory-safe.

Read-only. Codex-gated for any live use. Run:
  ~/miniforge3/envs/quants-lab/bin/python research/v16/mae_bag_measure.py [--smoke]
"""
from __future__ import annotations
import json, sys
import numpy as np, pandas as pd

sys.path.insert(0, "research/v15")
from leadlag_clean_rank_sim import mark_at

JOURNEYS = "app/data/v15/m02_journeys.parquet"
LIVE10 = ["0x36c097864a03c7f0215c0d43165a734152a12e0b", "0x6f83ab8890ed38bf38a31010aa9a5e9ca743bfad",
          "0xe46eafafb60af2eea3a59768106a9342aec59ec3", "0x1404109f8cd4a79a0447365edbb7a13acd0b2f27",
          "0x760ec8576c2dc5dba2655f7b948c0689b02b6cb0", "0x36a60294f8b77e8ebe2ee32f3d3697952a379514",
          "0x03d8c9ce2a103a0094acc96520cf5eb87f85270c", "0xccf595171e2e56655fb4d386b7424da16be69d42",
          "0x5a5ec18fcf9db025d24c3674dd48ff40d5305204", "0x8c364082b2d8151ef4e06f6b6cef395030c9bc00"]

HOUR = 3_600_000
MAX_SAMPLES = 40           # cap samples/journey to bound compute
UW_THRESH = -0.05          # "underwater" = entry-relative return past -5%
DEEP_MAE = -0.20           # a position that excursed past -20% = a real bag


def journey_mae(coin, side, entry_ts, exit_ts):
    """Signed entry-relative return path over [entry, exit]; return (mae, frac_underwater, n)."""
    e = mark_at(coin, int(entry_ts))
    if not e or e <= 0:
        return None
    span = max(int(exit_ts) - int(entry_ts), 0)
    step = int(min(max(span / MAX_SAMPLES, 5 * 60_000), HOUR)) or HOUR
    long = (side == "long")
    worst = 0.0; uw = 0; n = 0
    t = int(entry_ts)
    while t <= int(exit_ts):
        m = mark_at(coin, t)
        if m and m > 0:
            r = (m - e) / e if long else (e - m) / e   # >0 favorable, <0 adverse
            if r < worst:
                worst = r
            if r <= UW_THRESH:
                uw += 1
            n += 1
        t += step
    if n == 0:
        return None
    return worst, uw / n, n


def main():
    smoke = "--smoke" in sys.argv
    universe = LIVE10
    uni_file = None
    for i, a in enumerate(sys.argv):
        if a == "--universe-file" and i + 1 < len(sys.argv):
            uni_file = sys.argv[i + 1]
    if uni_file:
        universe = [l.strip().lower() for l in open(uni_file) if l.strip() and not l.startswith("#")]
    elif "--candidates" in sys.argv:
        # 13 OOS-holds cohort candidates (truncated prefixes -> resolve from journeys)
        pref = ["0xb567367a97986d", "0xa55573fc0ba35d", "0xd2efdde0def642", "0x08e881cb053a76",
                "0x5c3f8fdc2c99cd", "0x83c4c5a492d77e", "0xffdb2d4eb40e3b", "0x8aa077f5998d23",
                "0x70f2470004a760", "0xb13dfc88a37e32", "0xaf266b453d153c", "0x25554a80781ee6",
                "0x1f15d5bb38f0d3"]
        allw = set(pd.read_parquet(JOURNEYS, columns=["wallet"])["wallet"].unique())
        universe = []
        for p in pref:
            hits = [w for w in allw if w.startswith(p)]
            if len(hits) == 1:
                universe.append(hits[0])
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "max_position_notional",
            "net_realized_pnl", "liq_closed", "open_at_window_end", "duration_h"]
    j = pd.read_parquet(JOURNEYS, columns=cols, filters=[("wallet", "in", universe)])
    j = j[(j.max_position_notional > 10) & (~j.open_at_window_end)].copy()
    if smoke:
        j = j.groupby("wallet").head(20)
    print(f"journeys: {len(j)} closed, {j.coin.nunique()} coins", flush=True)

    rows = []
    per = {w: {"maes": [], "uws": [], "n_j": 0, "n_cov": 0} for w in universe}
    for r in j.itertuples(index=False):
        per[r.wallet]["n_j"] += 1
        res = journey_mae(r.coin, r.side, r.entry_ts, r.exit_ts)
        if res is None:
            continue
        mae, fuw, _n = res
        per[r.wallet]["n_cov"] += 1
        per[r.wallet]["maes"].append(mae)
        per[r.wallet]["uws"].append(fuw)

    print(f"\n{'leader':12s} {'nJ':>4s} {'cov':>4s} {'medMAE%':>8s} {'p90MAE%':>8s} {'worstMAE%':>10s} "
          f"{'%jrnys>-20%':>11s} {'medTimeUW%':>11s}", flush=True)
    out = {}
    for w in universe:
        d = per[w]
        maes = np.array(d["maes"]); uws = np.array(d["uws"])
        if len(maes) == 0:
            print(f"{w[:12]:12s} {d['n_j']:4d} {0:4d}  (no mark coverage)", flush=True); continue
        med_mae = np.median(maes) * 100
        p90_mae = np.percentile(maes, 10) * 100      # deep-tail (10th pct of signed = deepest excursions)
        worst = maes.min() * 100
        frac_deep = float((maes <= DEEP_MAE).mean()) * 100
        med_uw = np.median(uws) * 100
        out[w] = {"n_j": d["n_j"], "cov": d["n_cov"], "med_mae_pct": round(med_mae, 1),
                  "p90_mae_pct": round(p90_mae, 1), "worst_mae_pct": round(worst, 1),
                  "pct_journeys_deep": round(frac_deep, 1), "med_time_uw_pct": round(med_uw, 1)}
        print(f"{w[:12]:12s} {d['n_j']:4d} {d['n_cov']:4d} {med_mae:8.1f} {p90_mae:8.1f} {worst:10.1f} "
              f"{frac_deep:11.1f} {med_uw:11.1f}", flush=True)

    outpath = ("/tmp/mae_bag_measure_pass400.json" if uni_file else
               "/tmp/mae_bag_measure_candidates.json" if "--candidates" in sys.argv else
               "/tmp/mae_bag_measure.json")
    json.dump(out, open(outpath, "w"), indent=2)
    print("\nCOLS: medMAE=typical worst-drawdown-during-hold; p90MAE=deep-tail excursion; "
          "%jrnys>-20% = share of positions that went past -20% underwater (the bag rate); "
          "medTimeUW = typical fraction of hold spent past -5%. ABSOLUTE, cross-sectional, not self-normalized.")
    print(f"wrote {outpath}")


if __name__ == "__main__":
    main()
