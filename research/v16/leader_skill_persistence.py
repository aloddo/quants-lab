#!/usr/bin/env python
"""
leader_skill_persistence.py -- THE first-principles test (Alberto 2026-06-14):
does a leader's PAST risk-adjusted skill predict their FORWARD copy-edge? If yes, copy-trading has a real,
selectable edge (and scaling capital is legitimate). If no, copying top wallets is variance-harvesting and
no amount of capital/size/breadth fixes it.

Method (per-wallet train/forward split, leak-free):
- Universe: all cohort journeys (m02_cohort_journeys). Per-journey return_frac = net_realized_pnl /
  max_position_notional = the leader's realized return on the position ~ the price-capture WE would copy.
- For each wallet with enough journeys: split at the wallet's MEDIAN entry time. train = earlier half,
  forward = later half. Require >= MIN_N journeys in each half.
- Train skill metrics: mean_ret, sharpe (mean/std), hit_rate, n.
- Forward outcome: forward mean_ret (and forward sharpe).
- Persistence: Spearman corr(train_skill, forward_ret); decile/tercile spread (top-train-skill forward mean
  vs bottom); and the matched-null (overall forward drift) to see if skill BEATS just being in the market.

Verdict: persistence EXISTS if top-train-skill wallets have materially higher forward edge than bottom AND
than the pooled mean, with corr > 0 robustly. NULL if forward edge is flat across train-skill (= variance).

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/leader_skill_persistence.py
"""
import json
import numpy as np
import pandas as pd

MIN_N = 15   # min journeys per half for a wallet to be testable


def main():
    d = json.load(open("config/copy_trader_wallets_v17_expansion.json"))
    universe = set(d["global"]["coin_whitelist"]) | set(d["global"]["expansion"]["coins"])
    j = pd.read_parquet("app/data/v16/m02_cohort_journeys.parquet")
    print(f"journeys total: {len(j)} | wallets: {j.wallet.nunique()}")
    # use ALL coins for power (report universe-only too)
    j = j[j.max_position_notional > 10].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    # sane bounds (drop data errors)
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = j["entry_ts"].astype("float64")

    rows = []
    for w, g in j.groupby("wallet"):
        if len(g) < 2 * MIN_N:
            continue
        g = g.sort_values("t")
        mid = g["t"].median()
        tr = g[g.t <= mid]["ret"]
        fw = g[g.t > mid]["ret"]
        if len(tr) < MIN_N or len(fw) < MIN_N:
            continue
        rows.append({
            "wallet": w,
            "train_mean_bps": tr.mean() * 1e4,
            "train_sharpe": tr.mean() / (tr.std() + 1e-9),
            "train_hit": (tr > 0).mean(),
            "train_n": len(tr),
            "fwd_mean_bps": fw.mean() * 1e4,
            "fwd_sharpe": fw.mean() / (fw.std() + 1e-9),
            "fwd_n": len(fw),
        })
    p = pd.DataFrame(rows)
    print(f"testable wallets (>= {MIN_N} journeys each half): {len(p)}")
    if len(p) < 10:
        print("too few wallets to test persistence robustly.")
        return

    pooled_fwd = p["fwd_mean_bps"].mean()
    print(f"\npooled forward mean edge (the null / 'just be in the market'): {pooled_fwd:+.1f} bps")

    def spear(a, b):
        return pd.Series(a).rank().corr(pd.Series(b).rank())

    print("\n=== PERSISTENCE: does TRAIN skill predict FORWARD edge? (Spearman rank corr) ===")
    for metric in ["train_mean_bps", "train_sharpe", "train_hit"]:
        c = spear(p[metric], p["fwd_mean_bps"])
        print(f"  corr({metric:<16}, fwd_mean_bps) = {c:+.3f}")

    print("\n=== TERCILE SPREAD by train_sharpe (the decisive read) ===")
    p["terc"] = pd.qcut(p["train_sharpe"], 3, labels=["bottom", "mid", "top"])
    g = p.groupby("terc", observed=True).agg(
        n=("wallet", "size"), train_sharpe=("train_sharpe", "mean"),
        fwd_mean_bps=("fwd_mean_bps", "mean"), fwd_hit=("fwd_sharpe", lambda s: (s > 0).mean()))
    print(g.to_string())
    top = g.loc["top", "fwd_mean_bps"]
    bot = g.loc["bottom", "fwd_mean_bps"]
    print(f"\n  top-skill forward edge {top:+.1f}bps vs bottom {bot:+.1f}bps  -> spread {top - bot:+.1f}bps")
    print(f"  top-skill vs pooled-null {pooled_fwd:+.1f}: {'BEATS null' if top > pooled_fwd + 2 else 'does NOT beat null'}")

    print("\n=== VERDICT ===")
    csharpe = spear(p["train_sharpe"], p["fwd_mean_bps"])
    if csharpe > 0.15 and (top - bot) > 5 and top > pooled_fwd + 2:
        print("PERSISTENCE PRESENT: past skill predicts forward edge -> select on skill -> real edge.")
    elif csharpe < 0.1 and abs(top - bot) < 5:
        print("NO PERSISTENCE: forward edge ~flat across train-skill -> top-wallet copying is VARIANCE.")
        print("  => the edge is not in WHO-by-PnL; need a different signal (setup-level, or skill metric")
        print("     that IS persistent). Scaling size/capital cannot fix a non-edge.")
    else:
        print("WEAK/AMBIGUOUS persistence -- not robustly selectable. Treat as marginal.")
    print(f"\n(corr train_sharpe->fwd = {csharpe:+.3f}; n={len(p)} wallets; MIN_N={MIN_N}/half)")


if __name__ == "__main__":
    main()
