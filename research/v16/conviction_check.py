"""
Conviction-filter check (Alberto: "copy LESS, only when sure"). Does conditioning trades on CONVICTION
(big size vs the wallet's own norm; multi-wallet consensus) raise the costed markout enough to clear our cost,
even though the average trade does not? Canonical execution_model slip (incl impact -- the honest cost).
Causal: size_z uses the wallet's PRIOR trades only. If the high-conviction slice clears cost -> the unlock.
"""
import numpy as np, pandas as pd
import sys; sys.path.insert(0, "research/v16"); sys.path.insert(0, "research/v15")
import markout_study as M, execution_model as EX

def main():
    # gross markout (slip 0), then subtract CANONICAL per-coin RT cost (fee + 2x per-coin slip incl impact)
    df = M.build_priced(min_hold_h=2.0, max_addon=20, coins_keep=None, min_notional=200, slip_oneway=0.0)
    df = df.sort_values("entry_ts").reset_index(drop=True)
    slip = df.coin.map(lambda c: EX.slip_oneway(c)).astype(float)
    df["costed"] = df["mk_4h"] - 2*slip                       # mk_4h already has FEE_RT removed
    df["calib"] = df.coin.map(lambda c: EX.slip_oneway(c) < 0.001)  # liquid/calibrated coins

    # CONVICTION 1: size z vs wallet's TRAILING mean notional (causal, expanding, shifted)
    df["w_trail_notional"] = df.groupby("wallet")["notional"].transform(
        lambda s: s.shift(1).expanding(min_periods=3).mean())
    df["size_z"] = df["notional"] / df["w_trail_notional"]
    d = df[df.size_z.notna()].copy()

    print("=== costed mk_4h (CANONICAL slip incl impact) by SIZE-CONVICTION bucket ===")
    d["sz_bucket"] = pd.qcut(d.size_z.rank(method="first"), 10, labels=False)
    g = d.groupby("sz_bucket").agg(mk=("costed","mean"), med=("costed","median"),
                                   pos=("costed", lambda x:(x>0).mean()), n=("costed","size"))
    g["mk"]*=1e4; g["med"]*=1e4
    print(g.round(2).to_string())
    print(f"\n  overall costed mk_4h: mean {d.costed.mean()*1e4:+.1f}bps median {d.costed.median()*1e4:+.1f}bps")

    # the literal "copy less": top conviction slices
    print("\n=== ULTRA-SELECTIVE (copy less): top conviction trades, costed markout ===")
    for pct in [0.20, 0.10, 0.05, 0.01]:
        thr = d.size_z.quantile(1-pct); top = d[d.size_z >= thr]
        # also require liquid (calibrated) coin -- "only when sure"
        topl = top[top.calib]
        print(f"  top {pct*100:4.1f}% by size_z (n={len(top):5d}): costed mean {top.costed.mean()*1e4:+6.1f}bps med {top.costed.median()*1e4:+6.1f} pos {(top.costed>0).mean():.3f} | "
              f"LIQUID-only (n={len(topl):5d}): mean {topl.costed.mean()*1e4:+6.1f}bps med {topl.costed.median()*1e4:+6.1f}")

    # CONVICTION 2: consensus (other entries same coin+side within +/-30min)
    print("\n=== CONSENSUS check (same coin+side within 30min) ===")
    d2 = d.sort_values("entry_ts").copy()
    # approximate consensus count per trade via merge_asof self-join window
    d2["t"] = d2.entry_ts
    res = []
    for (coin, side), grp in d2.groupby(["coin","side"]):
        ts = grp.t.values
        # count entries within 30min before each
        cnt = np.searchsorted(ts, ts, "right") - np.searchsorted(ts, ts - 30*60*1000, "left") - 1
        res.append(pd.DataFrame({"idx": grp.index, "consensus": cnt}))
    cc = pd.concat(res).set_index("idx")
    d2 = d2.join(cc)
    for thr in [0, 1, 2, 4]:
        sub = d2[d2.consensus >= thr]
        print(f"  consensus>={thr} (n={len(sub):6d}): costed mean {sub.costed.mean()*1e4:+6.1f}bps med {sub.costed.median()*1e4:+6.1f} pos {(sub.costed>0).mean():.3f}")

    print("\n  NOTE: if no high-conviction slice clears 0 (costed mean & median > 0), copying-less does NOT escape the cost wall.")

if __name__ == "__main__":
    main()
