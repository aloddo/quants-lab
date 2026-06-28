"""
Weekly-cadence FEASIBILITY screen (2026-06-28) -- de-risk Alberto's fork (shorter-cadence vs repoint).

The 2-week costed engine FAILS (copy-the-wallets falsified, 4 ways). Before proposing/building a WEEKLY
engine, cheaply screen: at WEEKLY granularity, (a) what is wallet week-to-week overlap (vs ~2% at 2wk), and
(b) does week-N trailing copy-edge predict week-(N+1) forward copy-edge?

Data: app/data/v15/copy_edge_label_panel_causal.parquet (weekly per-wallet, copy_net_pnl ALREADY costed via
the causal label: latency 1.86s + slip + fees + funding). NOTE: this panel is an OPTIMISTIC UPPER BOUND vs
the M7 engine (no capacity caps / concurrent-position limits / sleeve mechanics / bankroll). So:
  - weekly top-decile forward edge <= 0  => shorter cadence is DEAD even optimistically -> recommend REPOINT.
  - weekly top-decile forward edge >> 0  => MIGHT survive the engine -> worth Alberto greenlighting a real build.
This is a screen, NOT a green light.
"""
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

MIN_NOTIONAL = 5000.0   # require >= $5k copied gross to compute a non-noise weekly edge
P = "app/data/v15/copy_edge_label_panel_causal.parquet"

def main():
    p = pd.read_parquet(P)
    p = p[p.copied_gross_notional >= MIN_NOTIONAL].copy()
    p["copy_edge"] = p.copy_net_pnl / p.copied_gross_notional
    p = p[np.isfinite(p.copy_edge)]
    weeks = sorted(p.week.unique())
    print(f"panel rows (>= ${MIN_NOTIONAL:.0f} gross): {len(p)}, weeks {weeks[0]}-{weeks[-1]}")

    # ---- (a) week-to-week wallet overlap (persistence of the ACTIVE/liquid population) ----
    print("\n=== WEEKLY wallet overlap (active & liquid both weeks / active week N) ===")
    ov = []
    by_week = {w: set(p[p.week == w].wallet) for w in weeks}
    for w in weeks[:-1]:
        a, b = by_week[w], by_week[w + 1]
        if a:
            ov.append(len(a & b) / len(a))
    print(f"  median week-to-week overlap: {np.median(ov):.3f}  (range {min(ov):.3f}-{max(ov):.3f})")
    print(f"  (compare: 2-week M5 eligibility overlap was ~0.02)")

    # ---- (b) weekly persistence: trailing week-N edge -> forward week-(N+1) edge, by decile ----
    print("\n=== WEEKLY persistence: rank by week-N copy_edge -> forward week-(N+1) copy_edge ===")
    rows = []
    for w in weeks[:-1]:
        cur = p[p.week == w][["wallet", "copy_edge", "copied_gross_notional"]].rename(
            columns={"copy_edge": "trail", "copied_gross_notional": "trail_gross"})
        nxt = p[p.week == w + 1][["wallet", "copy_edge", "copied_gross_notional"]].rename(
            columns={"copy_edge": "fwd", "copied_gross_notional": "fwd_gross"})
        m = cur.merge(nxt, on="wallet", how="inner")
        if len(m) >= 30:
            m["sel_week"] = w
            rows.append(m)
    panel = pd.concat(rows, ignore_index=True)
    print(f"  paired (wallet present both weeks) observations: {len(panel)}")

    # pooled decile by trailing edge; dollar-weight forward by fwd_gross
    panel["dec"] = pd.qcut(panel.trail.rank(method="first"), 10, labels=False)
    def dw(g):
        return np.average(g.fwd, weights=g.fwd_gross)
    dec = panel.groupby("dec").apply(lambda g: pd.Series({
        "fwd_mean_bps": g.fwd.mean() * 1e4,
        "fwd_med_bps": g.fwd.median() * 1e4,
        "fwd_dollarwtd_bps": dw(g) * 1e4,
        "fwd_posfrac": (g.fwd > 0).mean(),
        "n": len(g),
    }))
    print(dec.round(2).to_string())
    sp = spearmanr(panel.trail, panel.fwd).correlation
    top = dec.loc[9]; bot = dec.loc[0]
    print(f"\n  Spearman(trail, fwd) = {sp:+.4f}")
    print(f"  TOP decile fwd: mean {top.fwd_mean_bps:+.1f}bps | median {top.fwd_med_bps:+.1f}bps | "
          f"dollar-wtd {top.fwd_dollarwtd_bps:+.1f}bps | posfrac {top.fwd_posfrac:.2f}")
    print(f"  BOT decile fwd: mean {bot.fwd_mean_bps:+.1f}bps | dollar-wtd {bot.fwd_dollarwtd_bps:+.1f}bps")
    overall = panel.fwd.mean() * 1e4
    print(f"  overall fwd mean {overall:+.1f}bps")

    # ---- screen verdict ----
    print("\n=== FEASIBILITY SCREEN (optimistic upper bound; NOT a green light) ===")
    viable = top.fwd_dollarwtd_bps > 0 and top.fwd_mean_bps > 0 and sp > 0.05
    print(f"  top-decile forward edge clearly positive (dollar-wtd & mean) AND spearman>0.05? {viable}")
    if viable:
        print("  => shorter cadence MIGHT survive the costed engine -> worth Alberto greenlighting a real")
        print("     weekly M7 build (the panel is optimistic; engine will haircut capacity/sleeve/bankroll).")
    else:
        print("  => even the OPTIMISTIC weekly panel shows ~zero/negative top-decile forward edge ->")
        print("     shorter cadence is DEAD before engine costs -> recommend REPOINT off copy.")

if __name__ == "__main__":
    main()
