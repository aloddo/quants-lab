#!/usr/bin/env python3
"""RESULT (2026-07-06, run on gate1_v4 cohort May17-Jun1): finding #6 REFUTED.
  raw mean +103.5bps n=111 (9 wallets, 14 days) | IID 5th +62.9 | CLUSTER-by-wallet 5th +62.4 (eff_n=5)
  | CLUSTER-by-day 5th +70.3 (eff_n=10). Edge survives clustering; CI is NOT an iid artifact. Holdout
  trip mix is diversified (HYPE 35 / LIT 11 / ETH 9 / GRASS 6...), the 98%-HYPE is a LIVE-positioning
  stat, not holdout composition. Output: app/data/copyA/holdout_trips_labeled.csv.

Fable finding #6: the +94.5bps/trip holdout CI is overstated because trips are
clustered (7/10 wallets ~98% same-side HYPE). An iid bootstrap treats n=120 clustered
trips as 120 independent obs and shrinks the CI artificially.

This regenerates the holdout trips PER TRIP with (wallet, day, coin, net_bps) labels
using the SAME machinery as revalidate_api_execmodel (roundtrips + execution_model,
per-coin slippage + real taker fee, ALL coins), then compares:
  - iid bootstrap (resample trips)
  - CLUSTER bootstrap by wallet (resample whole wallets)
  - CLUSTER bootstrap by day (resample whole UTC days)
Reports mean, 5th pct, P(mean>0), and effective n for each.

READ-ONLY. Pulls HL API for the live cohort over the holdout window (bounded, backoff).
"""
import sys, json, argparse
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, apply_entry, apply_exit, set_slip_default_bps
from revalidate_api_execmodel import pull_api  # reuse the integrity-gated API pull

LIQ = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())
FEE_T = fee_rt(maker=False)
CAP = 500.0 / 1e4


def trip_rows(w, rts, lo, hi, lat):
    """Per-trip net bps rows for one wallet within [lo,hi)."""
    rows = []
    for c, dir_, ets, xts, evw, xvw, g in rts:
        if not (lo <= ets < hi):
            continue
        em = S.mark_at(c, ets + lat); xm = S.mark_at(c, xts + lat)
        if em is None or xm is None or em <= 0:
            continue
        ef = apply_entry(c, em, dir_ > 0); xf = apply_exit(c, xm, dir_ > 0)
        og = max(-CAP, min(CAP, dir_ * (xf - ef) / ef))
        net_bps = (og - FEE_T) * 1e4
        day = pd.Timestamp(ets, unit="ms", tz="UTC").strftime("%Y-%m-%d")
        rows.append({"wallet": w[:10], "coin": c, "day": day,
                     "side": ("L" if dir_ > 0 else "S"), "net_bps": net_bps})
    return rows


def boot(vals, groups, n=20000, seed=0):
    """Bootstrap mean. If groups given -> resample whole groups (cluster). Returns (mean, p5, p_pos)."""
    rng = np.random.default_rng(seed)
    vals = np.asarray(vals, float)
    means = np.empty(n)
    if groups is None:
        for i in range(n):
            means[i] = rng.choice(vals, size=len(vals), replace=True).mean()
    else:
        gkeys = list(groups.keys())
        gvals = [np.asarray(groups[k], float) for k in gkeys]
        for i in range(n):
            pick = rng.integers(0, len(gkeys), size=len(gkeys))
            means[i] = np.concatenate([gvals[j] for j in pick]).mean()
    return float(means.mean()), float(np.percentile(means, 5)), float((means > 0).mean())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--start", default="2026-05-17")
    ap.add_argument("--end", default="2026-06-01")
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--slip-default", type=float, default=0.5)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--out", default="app/data/copyA/holdout_trips_labeled.csv")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000); lat = args.latency_s * 1000
    set_slip_default_bps(args.slip_default)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, end = ms(args.start), ms(args.end)
    import time as _t
    wlist = [l.strip().lower() for l in open(args.wallets_file) if l.strip() and not l.startswith('#')]

    all_rows = []
    for wi, w in enumerate(wlist):
        if wi and args.sleep:
            _t.sleep(args.sleep)
        try:
            t, integ = pull_api(w, start, end)
        except RuntimeError as e:
            print(f"{w[:10]} PULL-FAILED: {e}", file=sys.stderr); continue
        rts = roundtrips(t)
        rows = trip_rows(w, rts, start, end, lat)
        all_rows += rows
        print(f"{w[:10]}: {len(rows)} holdout trips (integ {integ:.2f})", file=sys.stderr)

    if not all_rows:
        print("NO TRIPS in holdout window.", file=sys.stderr); sys.exit(1)
    df = pd.DataFrame(all_rows)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)

    vals = df["net_bps"].values
    by_wallet = {k: g["net_bps"].values for k, g in df.groupby("wallet")}
    by_day = {k: g["net_bps"].values for k, g in df.groupby("day")}

    def eff_n(groups):
        # Kish effective n from cluster sizes: (sum n_g)^2 / sum n_g^2
        sizes = np.array([len(v) for v in groups.values()], float)
        return float(sizes.sum() ** 2 / (sizes ** 2).sum())

    print("\n" + "=" * 72)
    print(f"HOLDOUT CLUSTER BOOTSTRAP  {args.start}..{args.end}  n_trips={len(df)}")
    print(f"  raw mean net = {vals.mean():+.1f} bps | wallets={df.wallet.nunique()} days={df.day.nunique()}")
    m, p5, pp = boot(vals, None)
    print(f"  IID bootstrap:         mean {m:+.1f}  5th pct {p5:+.1f}  P(mean>0) {pp:.3f}  eff_n={len(df)}")
    m, p5, pp = boot(vals, by_wallet)
    print(f"  CLUSTER by WALLET:     mean {m:+.1f}  5th pct {p5:+.1f}  P(mean>0) {pp:.3f}  eff_n={eff_n(by_wallet):.0f}")
    m, p5, pp = boot(vals, by_day)
    print(f"  CLUSTER by DAY:        mean {m:+.1f}  5th pct {p5:+.1f}  P(mean>0) {pp:.3f}  eff_n={eff_n(by_day):.0f}")
    print("=" * 72)
    print("Top coins by trip count:")
    print(df.groupby("coin").agg(n=("net_bps", "size"), mean_bps=("net_bps", "mean")).sort_values("n", ascending=False).head(8).to_string())


if __name__ == "__main__":
    main()
