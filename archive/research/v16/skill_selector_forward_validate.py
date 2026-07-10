#!/usr/bin/env python
"""
skill_selector_forward_validate.py -- build the skill-ranked leader selector and FORWARD-VALIDATE it beats
the current PnL-ranked cohort out-of-sample (Alberto 2026-06-14).

Broad universe: app/data/v15/m02_journeys.parquet (17,947 wallets, 7.7M journeys, Dec25-May26).
copy-edge proxy per journey = net_realized_pnl / max_position_notional (~ our net price capture).

Design (leak-free, global time cutoff):
- TRAIN = journeys with entry < CUTOFF; FORWARD = entry >= CUTOFF (out of sample).
- Eligibility: >= MIN_TRAIN journeys in train AND >= MIN_FWD in forward (so we can score the forward edge).
- TRAIN scores per wallet: mean_ret, sharpe(=mean/std), win_rate, max_consec_dd, n, liq_rate.
- COHORTS (train-ranked, top-K):
    PnL    : rank by SUM(realized $)         <- proxy for the current 'top decile by PnL' (the variance pick)
    RETmean: rank by mean return             <- naive return selection
    SKILL  : rank by z(win_rate)+z(sharpe)+z(-max_dd)  (NO raw return)  <- pre-registered skill rule
- FORWARD edge: equal-weight the cohort's wallets; each wallet's forward mean_ret; report cohort mean,
  net-of-cost (minus RT_COST), forward hit-rate, forward sharpe. Compare cohorts + a RANDOM baseline.
- Robustness: sweep K (50/100/200). Report whether SKILL > PnL forward, net of cost.

Memory-safe: loads only needed columns; groupby aggregation (bounded, not a per-row fanout).
Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/skill_selector_forward_validate.py
"""
import numpy as np
import pandas as pd

CUTOFF = "2026-04-01"          # train < cutoff, forward >= cutoff
MIN_TRAIN = 20
MIN_FWD = 10
RT_COST_BPS = 11.0            # our round-trip taker fee + avg slip (both cohorts pay the same)
KS = [50, 100, 200]


def max_consec_dd(returns):
    """Max drawdown of the cumulative return curve (in return units)."""
    eq = np.cumsum(returns)
    peak = np.maximum.accumulate(eq)
    return float((peak - eq).max()) if len(eq) else 0.0


def main():
    cols = ["wallet", "entry_ts", "realized_pnl", "net_realized_pnl", "max_position_notional", "liq_closed"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[j.max_position_notional > 10].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")
    cutoff = pd.Timestamp(CUTOFF)
    tr = j[j.t < cutoff]
    fw = j[j.t >= cutoff]
    print(f"universe wallets={j.wallet.nunique()} | train journeys={len(tr)} (<{CUTOFF}) | forward={len(fw)}")

    # train scores
    def agg(g):
        r = g["ret"].to_numpy()
        return pd.Series({
            "n": len(r), "mean_ret": r.mean(), "sum_pnl": g["realized_pnl"].sum(),
            "sharpe": r.mean() / (r.std() + 1e-9), "win": (r > 0).mean(),
            "maxdd": max_consec_dd(r), "liq": g["liq_closed"].mean(),
        })
    ts = tr.groupby("wallet").apply(agg, include_groups=False)
    fwm = fw.groupby("wallet")["ret"].agg(["mean", "std", "count"]).rename(
        columns={"mean": "fwd_mean", "std": "fwd_std", "count": "fwd_n"})
    s = ts.join(fwm, how="inner")
    s = s[(s.n >= MIN_TRAIN) & (s.fwd_n >= MIN_FWD)].copy()
    print(f"eligible wallets (>={MIN_TRAIN} train, >={MIN_FWD} fwd): {len(s)}")

    # z-scores for skill rank
    def z(x):
        return (x - x.mean()) / (x.std() + 1e-9)
    s["skill_score"] = z(s["win"]) + z(s["sharpe"]) + z(-s["maxdd"])
    s["pnl_score"] = s["sum_pnl"]
    s["retmean_score"] = s["mean_ret"]

    pooled_fwd = s["fwd_mean"].mean() * 1e4
    print(f"\npooled forward edge (all eligible, the null): {pooled_fwd:+.1f}bps gross | "
          f"{pooled_fwd - RT_COST_BPS:+.1f}bps net")

    print(f"\n{'K':>5}{'method':>10}{'fwd_gross':>11}{'fwd_NET':>9}{'fwd_hit%':>9}{'fwd_sharpe':>11}{'liq%':>7}")
    results = {}
    for K in KS:
        for method, col in [("PnL", "pnl_score"), ("RETmean", "retmean_score"), ("SKILL", "skill_score")]:
            top = s.nlargest(K, col)
            fg = top["fwd_mean"].mean() * 1e4
            net = fg - RT_COST_BPS
            hit = (top["fwd_mean"] > 0).mean() * 100
            shp = top["fwd_mean"].mean() / (top["fwd_mean"].std() + 1e-9)
            liq = top["liq"].mean() * 100
            results[(K, method)] = net
            print(f"{K:>5}{method:>10}{fg:>11.1f}{net:>9.1f}{hit:>9.0f}{shp:>11.2f}{liq:>7.1f}")
        # random baseline at this K (mean of 20 draws)
        rng = np.arange(len(s))
        draws = []
        for seed in range(20):
            idx = (rng * 2654435761 + seed * 40503) % len(s)
            draws.append(s.iloc[np.unique(idx)[:K]]["fwd_mean"].mean() * 1e4)
        print(f"{K:>5}{'random':>10}{np.mean(draws):>11.1f}{np.mean(draws)-RT_COST_BPS:>9.1f}{'':>9}{'':>11}{'':>7}")

    print("\n=== VERDICT ===")
    for K in KS:
        sk, pn = results[(K, "SKILL")], results[(K, "PnL")]
        win = "SKILL > PnL" if sk > pn else "PnL >= SKILL"
        print(f"  K={K}: SKILL net {sk:+.1f}bps vs PnL net {pn:+.1f}bps -> {win} (edge {sk-pn:+.1f}bps)")
    print("\nIf SKILL beats PnL net-of-cost across K -> deploy skill-ranked cohort. That is the real edge.")


if __name__ == "__main__":
    main()
