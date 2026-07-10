"""
Mechanics-pass eval vs the codex HARD pre-registered bar (copy-rebuild/2026-06-28-mechanics-pass-prereg).
Compares markout cohort WITH per-seat DD stop (follower_trail=0.08) vs WITHOUT, conditioned on ACTIVE seats.
BAR (all required to CONTINUE, else REPOINT): OOS active-seat median ROE > +0.5% AND mean > +0.5%;
median maxDD <= 6.2%; worst-decile improves; robust (both WF + holdout). Active = >=3 round-trips AND
>= $500 notional turnover in the test window (pre-registered).
"""
import numpy as np, pandas as pd
from pathlib import Path
WS = Path("app/data/v15/weekly_spike")
MIN_RT = 3
MIN_TURN = 500.0

def load(d):
    s = pd.read_parquet(WS/d/"m07_summary.parquet")
    s["active"] = (s.n_round_trips >= MIN_RT) & (s.notional_traded >= MIN_TURN)
    return s

def stats(s, lbl, oos=False):
    d = s[s.fold_id>=16] if oos else s
    a = d[d.active]
    r = a.roe_engine.values
    print(f"  {lbl:34s} active={len(a):5d}/{len(d):5d} | median {np.median(r)*100:+6.2f}% mean {r.mean()*100:+6.2f}% "
          f"pos {np.mean(r>0):.3f} | maxDD(med) {np.median(a.max_dd)*100:5.2f}% | worstdec {np.percentile(r,10)*100:+6.2f}%")
    return dict(med=np.median(r), mean=r.mean(), dd=np.median(a.max_dd), wd=np.percentile(r,10), n=len(a))

def main():
    base = load("m07_markout_cohort")        # no stop
    mech = load("m07_markout_mech_t08")       # +DD stop
    print("=== ALL folds (active seats: >=3 RT & >=$500 turnover) ===")
    stats(base, "markout cohort (no stop)")
    stats(mech, "markout + DD-stop 8%")
    print("=== HOLDOUT OOS (folds>=16) ===")
    b_o = stats(base, "markout cohort (no stop) OOS", oos=True)
    m_o = stats(mech, "markout + DD-stop 8% OOS", oos=True)
    m_all = stats(mech, "markout + DD-stop 8% ALLfold")

    print("\n=== CODEX HARD BAR (active seats) ===")
    primary = (m_o["med"] > 0.005) and (m_o["mean"] > 0.005) and (m_all["med"] > 0.005) and (m_all["mean"] > 0.005)
    risk = (m_o["dd"] <= 0.062) and (m_o["wd"] > b_o["wd"])
    print(f"  PRIMARY OOS+ALL median&mean > +0.5%: {primary}  (OOS med {m_o['med']*100:+.2f}% mean {m_o['mean']*100:+.2f}%)")
    print(f"  RISK maxDD<=6.2% & worstdec improves: {risk}  (DD {m_o['dd']*100:.2f}% wd {m_o['wd']*100:+.2f}% vs base {b_o['wd']*100:+.2f}%)")
    verdict = "PASS probation -> continue (netting refine + frontier + codex)" if (primary and risk) else "FAIL -> REPOINT off copy with finality"
    print(f"\n  VERDICT: {verdict}")

if __name__ == "__main__":
    main()
