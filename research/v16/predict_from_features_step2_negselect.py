"""
predict-from-features step 2 -- NEGATIVE-selection gate (codex-suggested, 2026-06-28).

Question (codex): instead of picking winners (step1 FAILED), can the same AS-OF features reliably
EXCLUDE bad wallets while preserving enough flow? i.e. does dropping the predicted-worst X% raise the
KEPT pool's after-cost OOS ROE vs the full pool AND vs random exclusion, on a frozen holdout?

Prior: falsification activity-neutralized residual bottom was -6.9% vs top -1.3% -> an AVOID signal may exist.

Reuses step1 panel + model. Walk-forward, frozen holdout fold 8. Pre-registered decision rule below.
PRE-REG (frozen here before scoring): NEGATIVE-selection PASSES iff, on holdout fold 8 AND median(folds 5-8),
for at least one exclusion fraction in {0.2,0.33,0.5} that KEEPS >=50% flow:
  (1) kept-pool MEAN roe_engine > full-pool mean AND kept-pool MEDIAN >= full-pool median;
  (2) kept-pool mean beats RANDOM-exclusion kept-pool mean (same fraction);
  (3) the improvement is directionally consistent (not a single-fold artifact).
FAIL (any missing) -> negative selection adds no reliable value -> closes the in-domain copy residual.
"""
import numpy as np
import pandas as pd
from predict_from_features_step1 import build_panel, fit_predict, LOCKED_FEATURES

RNG = np.random.default_rng(7)
FRACS = [0.20, 0.33, 0.50]

def kept_stats(roe, keep_mask):
    k = roe[keep_mask]
    return dict(n=int(keep_mask.sum()), mean=float(np.mean(k)), med=float(np.median(k)),
                posfrac=float(np.mean(k > 0)))

def main():
    df = build_panel()
    print(f"panel {df.shape}")
    print(f"{'fold':>4} {'frac':>5} {'full_mean':>10} {'full_med':>9} {'kept_mean':>10} {'kept_med':>9} "
          f"{'rand_mean':>10} {'kept_n':>7} {'beats_full':>11} {'beats_rand':>11}")
    agg = {f: [] for f in FRACS}
    for k in range(4, 9):
        train = df[df.fold_id < k].copy()
        test = df[df.fold_id == k].copy()
        roe = test["roe_engine"].values
        n = len(roe)
        pred = fit_predict(train, test, "hgb")  # higher pred = better expected ROE
        order_badfirst = np.argsort(pred)  # ascending: worst predicted first
        full_mean, full_med = float(np.mean(roe)), float(np.median(roe))
        for frac in FRACS:
            ndrop = int(round(frac * n))
            drop_idx = set(order_badfirst[:ndrop].tolist())
            keep = np.array([i not in drop_idx for i in range(n)])
            ks = kept_stats(roe, keep)
            # random exclusion baseline (avg of 200 draws)
            rand_means = []
            for _ in range(200):
                ridx = RNG.choice(n, size=n - ndrop, replace=False)
                rand_means.append(np.mean(roe[ridx]))
            rand_mean = float(np.mean(rand_means))
            bf = ks["mean"] > full_mean and ks["med"] >= full_med
            br = ks["mean"] > rand_mean
            agg[frac].append(dict(fold=k, full_mean=full_mean, full_med=full_med, **ks,
                                  rand_mean=rand_mean, beats_full=bf, beats_rand=br))
            tag = " <-- HOLDOUT" if k == 8 else ""
            print(f"{k:>4} {frac:>5.2f} {full_mean:>+10.4f} {full_med:>+9.4f} {ks['mean']:>+10.4f} "
                  f"{ks['med']:>+9.4f} {rand_mean:>+10.4f} {ks['n']:>7} {str(bf):>11} {str(br):>11}{tag}")
        print()

    print("=== PRE-REGISTERED DECISION (negative-selection gate) ===")
    any_pass = False
    for frac in FRACS:
        rows = agg[frac]
        h8 = [r for r in rows if r["fold"] == 58 or r["fold"] == 8][0]
        f58 = [r for r in rows if r["fold"] >= 5]
        med_kept = float(np.median([r["mean"] for r in f58]))
        med_full = float(np.median([r["full_mean"] for r in f58]))
        med_rand = float(np.median([r["rand_mean"] for r in f58]))
        flow_ok = (1 - frac) >= 0.50
        c1 = h8["beats_full"] and (med_kept > med_full)
        c2 = h8["beats_rand"] and (med_kept > med_rand)
        c3 = sum(r["beats_full"] for r in f58) >= 3  # majority of folds 5-8
        verdict = c1 and c2 and c3 and flow_ok
        any_pass = any_pass or verdict
        print(f"  frac={frac:.2f} flow_kept={1-frac:.0%} | holdout8 kept_mean {h8['mean']:+.4f} vs full "
              f"{h8['full_mean']:+.4f} vs rand {h8['rand_mean']:+.4f} | med(5-8) kept {med_kept:+.4f} "
              f"full {med_full:+.4f} rand {med_rand:+.4f} | C1 {c1} C2 {c2} C3 {c3} -> {'PASS' if verdict else 'fail'}")
    print(f"\n  VERDICT: {'PASS -> negative-selection has value (escalate + codex)' if any_pass else 'FAIL -> negative selection adds no reliable value'}")

if __name__ == "__main__":
    main()
