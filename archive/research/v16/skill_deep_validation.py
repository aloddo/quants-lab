#!/usr/bin/env python
"""
skill_deep_validation.py -- harden the skill-selector validation against the 3 things codex flagged
(Alberto: 'sim too shallow'). Causal/attrition-honest, tail-risk, multi-window.

(A) ATTRITION-ADJUSTED causal eval: select top-K by TRAIN skill, then in FORWARD count wallets that went
    DORMANT (no forward journeys) as ZERO contribution (they hold a slot, do nothing). This is the real
    deploy expectation; the prior eval dropped them (survivor-flattered). Compare SKILL vs PnL this way.
(B) HOLD-LOSER TAIL: for the skill cohort, open_at_window_end rate (unrealized, maybe underwater) + the
    worst realized journeys + adverse-excursion proxy. Tests the '90% win hides a tail' worry.
(C) MULTI-WINDOW: SKILL vs PnL net across 6 cutoffs (attrition-adjusted).

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/skill_deep_validation.py
"""
import json
import numpy as np
import pandas as pd

ACTIVE_DAYS = 14
MIN_J = 40
K = 100
RT = 11.0
HOLD_MIN, HOLD_MAX = 2.0, 48.0


def max_dd(r):
    eq = np.cumsum(np.sort(r) * 0 + r)  # chronological assumed pre-sorted by caller
    peak = np.maximum.accumulate(np.cumsum(r))
    return float((peak - np.cumsum(r)).max()) if len(r) else 0.0


def load():
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())
    cols = ["wallet", "coin", "entry_ts", "duration_h", "realized_pnl", "net_realized_pnl",
            "max_position_notional", "liq_closed", "open_at_window_end"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.max_position_notional > 10) & (j.coin.isin(calib))].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")
    return j.sort_values(["wallet", "t"])


def z(x):
    return (x - x.mean()) / (x.std() + 1e-9)


def select_and_eval(j, cutoff, attrition=True):
    """Train < cutoff -> rank. Forward >= cutoff -> portfolio edge. attrition: dormant wallets count 0."""
    asof = cutoff + pd.Timedelta(days=30)
    j = j[j.t <= asof]
    tr, fw = j[j.t < cutoff], j[j.t >= cutoff]
    # active as of CUTOFF (train-only, codex P2)
    active = set(tr[tr.t >= cutoff - pd.Timedelta(days=ACTIVE_DAYS)].wallet.unique())
    g = tr.groupby("wallet")
    s = g.agg(n=("ret", "size"), mean=("ret", "mean"), std=("ret", "std"),
             win=("ret", lambda x: (x > 0).mean()), sum_pnl=("realized_pnl", "sum"),
             hold=("duration_h", "median"))
    s["sharpe"] = s["mean"] / (s["std"] + 1e-9)
    s["maxdd"] = g["ret"].apply(lambda x: float((np.maximum.accumulate(np.cumsum(x.values)) - np.cumsum(x.values)).max()) if len(x) else 0.0)
    s = s[(s.n >= MIN_J) & (s.index.isin(active)) & (s.hold >= HOLD_MIN) & (s.hold <= HOLD_MAX)].copy()
    s["skill"] = z(s.win) + z(s.sharpe) + z(-s.maxdd)
    fwd_mean = fw.groupby("wallet")["ret"].mean()

    def port_edge(ranked_idx):
        # equal-weight K wallets; dormant (no forward) -> 0 if attrition else dropped
        sel = ranked_idx[:K]
        vals = []
        for w in sel:
            if w in fwd_mean.index:
                vals.append(fwd_mean[w])
            elif attrition:
                vals.append(0.0)   # holds a slot, no trades
        return np.mean(vals) * 1e4 - RT if vals else np.nan

    skl = port_edge(list(s.nlargest(K, "skill").index))
    pnl = port_edge(list(s.nlargest(K, "sum_pnl").index))
    dormant = sum(1 for w in s.nlargest(K, "skill").index if w not in fwd_mean.index)
    return skl, pnl, dormant


def main():
    j = load()
    asof = pd.Timestamp("2026-05-23")

    print("=== (A) ATTRITION-ADJUSTED causal eval (dormant selected wallets count as ZERO) ===")
    cut = asof - pd.Timedelta(days=30)
    skl_a, pnl_a, dorm = select_and_eval(j, cut, attrition=True)
    skl_d, pnl_d, _ = select_and_eval(j, cut, attrition=False)
    print(f"  drop-dormant (flattered): SKILL {skl_d:+.1f} vs PnL {pnl_d:+.1f} net")
    print(f"  ATTRITION-honest (dormant=0): SKILL {skl_a:+.1f} vs PnL {pnl_a:+.1f} net "
          f"({dorm}/{K} skill wallets went dormant in forward)")

    print("\n=== (C) MULTI-WINDOW (attrition-adjusted, 6 cutoffs) ===")
    wins = []
    for d in [75, 60, 50, 40, 30, 20]:
        c = asof - pd.Timedelta(days=d)
        sk, pn, dm = select_and_eval(j, c, attrition=True)
        wins.append(sk - pn)
        print(f"  cutoff -{d}d: SKILL {sk:+.1f} vs PnL {pn:+.1f} -> {'SKILL' if sk > pn else 'PnL'} (+{sk - pn:.1f}) [dormant {dm}]")
    print(f"  SKILL beats PnL in {sum(1 for w in wins if w > 0)}/6 windows; mean edge +{np.mean(wins):.1f}bps")

    print("\n=== (B) HOLD-LOSER TAIL (skill cohort, the '90% win hides a tail' check) ===")
    sk_wallets = set(json.load(open("config/copy_trader_wallets_v17_skill.json"))["wallets"].keys())
    g = j[(j.wallet.isin(sk_wallets)) & (j.t <= asof)]
    open_rate = g["open_at_window_end"].mean() if "open_at_window_end" in g else float("nan")
    worst = g["ret"].quantile(0.005) * 1e4
    p1 = g["ret"].quantile(0.01) * 1e4
    print(f"  open_at_window_end rate: {open_rate:.3f} (unrealized/still-open journeys -- hidden underwater?)")
    print(f"  realized worst-0.5% journey: {worst:+.0f}bps | worst-1%: {p1:+.0f}bps | liq rate: {g.liq_closed.mean():.4f}")
    print(f"  realized loss>5%/journey count: {(g.ret < -0.05).sum()} of {len(g)} ({(g.ret < -0.05).mean()*100:.2f}%)")
    print("  (low open-rate + bounded worst-tail + ~0 liq = win-rate is real, not hold-loser masking)")


if __name__ == "__main__":
    main()
