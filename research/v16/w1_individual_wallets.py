"""
W1 -- individual-wallet case study ("find ONE", Alberto). Different question than the failed cohort averages:
is there even ONE wallet with a robust, large copyable edge (costed delayed-entry markout) that survives our
execution OOS? Anti-overfit: train/test split by date, require sample size, compare top-train OOS-positive rate
vs base rate (multiple-testing control), require BOTH train+test positive AND large AND consistent across test
sub-windows. Best-case slip (0.5bps) so a failure is definitive.
"""
import numpy as np, pandas as pd
import sys; sys.path.insert(0, "research/v16")
import markout_study as M

SLIP = 0.0005          # best-case (majors-grade); if no individual survives even here, definitive
MIN_TR = 30; MIN_TE = 15
PRIMARY = "mk_4h"

def main():
    df = M.build_priced(min_hold_h=2.0, max_addon=20, coins_keep=None, min_notional=200, slip_oneway=SLIP)
    cut = df.entry_ts.quantile(0.70)
    tr, te = df[df.entry_ts <= cut], df[df.entry_ts > cut]
    a = tr.groupby("wallet").agg(tr_mk=(PRIMARY,"mean"), tr_med=(PRIMARY,"median"), n_tr=(PRIMARY,"size"))
    b = te.groupby("wallet").agg(te_mk=(PRIMARY,"mean"), te_med=(PRIMARY,"median"), n_te=(PRIMARY,"size"))
    m = a.join(b, how="inner")
    m = m[(m.n_tr >= MIN_TR) & (m.n_te >= MIN_TE)]
    print(f"eligible wallets (n_tr>=30 & n_te>=15): {len(m)}")
    base_pos = (m.te_mk > 0).mean()
    print(f"BASE RATE: fraction of eligible wallets with OOS test markout > 0: {base_pos:.3f}")

    # multiple-testing control: do TOP-train wallets beat the base OOS-positive rate?
    for q, lbl in [(0.90,"top decile"), (0.95,"top 5%"), (0.99,"top 1%")]:
        thr = m.tr_mk.quantile(q); top = m[m.tr_mk >= thr]
        print(f"  {lbl} by train markout (n={len(top)}): OOS-positive rate {(top.te_mk>0).mean():.3f} | "
              f"OOS mean {top.te_mk.mean()*1e4:+.1f}bps | OOS median {top.te_mk.median()*1e4:+.1f}bps")

    # the literal "find ONE": top 20 by train markout, show OOS
    print("\n=== TOP 20 wallets by TRAIN costed mk_4h -- do they survive OOS? ===")
    top20 = m.sort_values("tr_mk", ascending=False).head(20)
    print(f"{'wallet':>12} {'tr_mk_bps':>9} {'n_tr':>5} {'te_mk_bps':>9} {'te_med_bps':>10} {'n_te':>5} {'OOS+?':>6}")
    for w, r in top20.iterrows():
        print(f"{w[:10]:>12} {r.tr_mk*1e4:>+9.1f} {int(r.n_tr):>5} {r.te_mk*1e4:>+9.1f} {r.te_med*1e4:>+10.1f} {int(r.n_te):>5} {str(r.te_mk>0):>6}")

    # robust survivors: train>+10bps AND test>+10bps AND test median>0 AND n_te>=20
    surv = m[(m.tr_mk > 0.0010) & (m.te_mk > 0.0010) & (m.te_med > 0) & (m.n_te >= 20)]
    print(f"\n=== ROBUST SURVIVORS (train>+10bps & test>+10bps & test-median>0 & n_te>=20): {len(surv)} ===")
    if len(surv):
        print(surv.sort_values("te_mk", ascending=False).head(15).assign(
            tr_bps=lambda x:(x.tr_mk*1e4).round(1), te_bps=lambda x:(x.te_mk*1e4).round(1))[
            ["tr_bps","n_tr","te_bps","n_te"]].to_string())
    # expected survivors by chance: base_pos^2-ish over len(m) -- contextualize
    exp_chance = len(m) * base_pos * 0.5   # rough: P(test>0)~base, plus the >10bps/median cuts
    print(f"\n  (context: ~{len(m)} eligible; if OOS were pure luck, ~{base_pos:.2f} are OOS-positive by chance;")
    print(f"   robust-survivor count {len(surv)} must MEANINGFULLY exceed a luck-only expectation to be real.)")
    print(f"  VERDICT: {'candidate copyable individuals EXIST -> live-shadow (Alberto discretion)' if len(surv) >= 5 and (m[m.tr_mk>=m.tr_mk.quantile(0.95)].te_mk>0).mean() > base_pos + 0.1 else 'NO robust individual edge beyond chance -> copy dead at individual level too'}")

if __name__ == "__main__":
    main()
