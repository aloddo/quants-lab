#!/usr/bin/env python3
"""V16 OVERLAY SWEEP -- find the widest protective (SL, trail) that preserves the validated edge.

overlay_oos.py FAILED with the engine defaults (sl -400, trail 150/75): fold1 faithful +24.9bps ->
overlay -1.9bps (28% of trades trail-truncated). The edge lives in multi-hour right tails; a 75bps
giveback exits on intraday noise. Rule #7 mandates a trailing TP -- this sweep finds the widest config
whose OOS cost is ~zero, so the trail is a CATASTROPHE FLOOR, not an edge tax.

One pass: per TEST trade walk 1-min marks ONCE -> pnl path -> evaluate every (sl, activate, trail)
config on the path. Identical selection/pricing to overlay_oos.py (execution_model, taker, 500bps clip
on mark exits).

Run: python research/v16/overlay_sweep.py
"""
from __future__ import annotations
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_HERE))

import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, apply_entry, apply_exit, set_latency_ms
from _streaming_io import install_memory_guard
from select_cohort import load_wallet_fills, edge, LIQUID, CAP, MIN_COHORT, MAX_COHORT

FEE_T = fee_rt(maker=False)
MARK_STEP_MS = 60_000
WALK_HOLD_S = 604_800            # walk paths to 7d; per-config hold caps truncate
LAT = 2_000
MIN_RT = 15

FOLDS = [
    ("fold1", "2025-12-01", "2026-03-15", "2026-05-17"),
    ("fold2", "2025-12-15", "2026-04-15", "2026-05-23"),
]

H48, H7D = 172_800, 604_800
# (name, hold_s, sl_bps, trail_activate_bps, trail_bps); None = layer disabled.
# v2 (post-bug): hold cap is now a SWEPT dimension -- v1 baked 48h into every config including
# "faithful", which silently cost ~19bps median in fold1 (15% of trades hit 48h; the edge has a long-
# hold tail). faithful_7d FIRST so the in-loop cost column is correct.
CONFIGS = [
    ("faithful_7d", H7D, None, None, None),
    ("faithful_48h", H48, None, None, None),
    ("engine_default_48h", H48, -400.0, 150.0, 75.0),
    ("sl800_tr300_150_48h", H48, -800.0, 300.0, 150.0),
    ("sl800_tr300_150_7d", H7D, -800.0, 300.0, 150.0),
    ("sl800_tr600_300_7d", H7D, -800.0, 600.0, 300.0),
    ("sl800_notrail_7d", H7D, -800.0, None, None),
    ("sl1500_tr600_300_7d", H7D, -1500.0, 600.0, 300.0),
    ("sl1500_tr1000_500_7d", H7D, -1500.0, 1000.0, 500.0),
    ("sl1500_notrail_7d", H7D, -1500.0, None, None),
    ("sl2500_notrail_7d", H7D, -2500.0, None, None),
    # sprint v3 (2026-06-11): hold-cap sweep between 48h (kills edge) and 7d (shipped).
    # Motivated by sprint_decompose: >3d-hold trades are NEGATIVE both folds (fold2 wmed -34.6),
    # while 1-3d trades are strongly positive -- 72h/96h caps may trim the toxic tail only.
    ("sl1500_tr600_300_72h", 259_200, -1500.0, 600.0, 300.0),
    ("sl1500_tr600_300_96h", 345_600, -1500.0, 600.0, 300.0),
    ("sl1500_tr600_300_5d", 432_000, -1500.0, 600.0, 300.0),
]


def trade_path(coin, dir_, ets, xts):
    """Walk 1-min marks ONCE to min(leader exit, 7d). Returns (entry_px, [(t, pnl_bps, mark)...],
    leader_deadline_ms)."""
    m0 = S.mark_at(coin, ets + LAT)
    if m0 is None or m0 <= 0:
        return None
    entry_px = apply_entry(coin, m0, dir_ > 0)
    deadline = min(xts + LAT, ets + LAT + WALK_HOLD_S * 1000)
    path = []
    t = ets + LAT + MARK_STEP_MS
    while t < deadline:
        m = S.mark_at(coin, t)
        if m is not None and m > 0:
            path.append((t, dir_ * (m - entry_px) / entry_px * 1e4, m))
        t += MARK_STEP_MS
    m_end = S.mark_at(coin, deadline)
    if m_end is None or m_end <= 0:
        return None
    return entry_px, path, deadline, m_end


def eval_config(entry_px, path, leader_deadline, m_end, coin, dir_, ets, hold_s, sl, act, tr):
    """Net bps for one (hold, sl, trail) config on a precomputed path."""
    hold_deadline = ets + LAT + hold_s * 1000
    exit_mark, hit = None, None
    peak = 0.0
    last_m_before_hold = None
    for t, pnl, m in path:
        if t >= hold_deadline:
            break
        last_m_before_hold = m
        peak = max(peak, pnl)
        if sl is not None and pnl <= sl:
            exit_mark, hit = m, "sl"
            break
        if act is not None and peak >= act and (peak - pnl) >= tr:
            exit_mark, hit = m, "trail"
            break
    if exit_mark is None:
        if leader_deadline <= hold_deadline:
            exit_mark, hit = m_end, "leader_close"
        else:
            exit_mark, hit = last_m_before_hold, "max_hold"
            if exit_mark is None:
                exit_mark, hit = m_end, "leader_close"   # no path point before cap; degenerate, use end
    exit_px = apply_exit(coin, exit_mark, dir_ > 0)
    g = dir_ * (exit_px - entry_px) / entry_px
    if hit in ("leader_close", "max_hold"):
        g = max(-CAP, min(CAP, g))
    return g * 1e4 - FEE_T * 1e4, hit


def main():
    install_memory_guard(soft_gb=12.0, label="v16_sweep")
    set_latency_ms(LAT)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    uni = set(l.strip().lower() for l in open(S._DATA / "m01_nonerroring_wallets.txt")
              if l.strip() and not l.startswith("#"))

    summary = []
    for fname, f_start, f_split, f_end in FOLDS:
        start, split, end = ms(f_start), ms(f_split), ms(f_end)
        print(f"\n=== {fname}: train {f_start}->{f_split}, test {f_split}->{f_end} ===")
        wf = load_wallet_fills(uni, start, end)
        rows = []
        for w, fl in wf.items():
            fl.sort(key=lambda x: x[0])
            rts = roundtrips(fl)
            tr_e, trn, _ = edge(rts, start, split, LAT, FEE_T)
            ten = sum(1 for c, d_, e_, x_, *_ in rts if split <= e_ < end and c in LIQUID)
            if tr_e is not None and trn >= MIN_RT and ten >= MIN_RT:
                rows.append({"wallet": w, "train_taker": tr_e})
        df = pd.DataFrame(rows).sort_values("train_taker", ascending=False).reset_index(drop=True)
        cohort = df.head(min(max(MIN_COHORT, len(df) // 10), MAX_COHORT))
        print(f"  cohort {len(cohort)} of {len(df)} rankable")

        # per config: {wallet: [nets]}; one path walk per trade
        per_cfg = {name: {} for name, *_ in CONFIGS}
        reasons = {name: {} for name, *_ in CONFIGS}
        for w in cohort.wallet:
            for c, dir_, ets, xts, evw, xvw, g in roundtrips(wf[w]):
                if not (split <= ets < end) or c not in LIQUID:
                    continue
                tp = trade_path(c, dir_, ets, xts)
                if tp is None:
                    continue
                entry_px, path, leader_deadline, m_end = tp
                for name, hold_s, sl, act, tr in CONFIGS:
                    net, hit = eval_config(entry_px, path, leader_deadline, m_end, c, dir_, ets,
                                           hold_s, sl, act, tr)
                    per_cfg[name].setdefault(w, []).append(net)
                    reasons[name][hit] = reasons[name].get(hit, 0) + 1
        base_med = None
        print(f"  {'config':>22} | {'median':>8} {'mean':>8} | {'cost':>7} | %>0 | exits(sl/trail/hold)")
        for name, hold_s, sl, act, tr in CONFIGS:
            pw = pd.Series({w: np.mean(v) for w, v in per_cfg[name].items() if len(v) >= MIN_RT})
            med, mean = pw.median(), pw.mean()
            if name == "faithful_7d":
                base_med = med
            r = reasons[name]; tot = sum(r.values()) or 1
            print(f"  {name:>22} | {med:>+8.2f} {mean:>+8.2f} | "
                  f"{(med - (base_med if base_med is not None else med)):>+7.2f} | "
                  f"{(pw > 0).mean()*100:>3.0f} | "
                  f"{r.get('sl',0)/tot*100:.0f}%/{r.get('trail',0)/tot*100:.0f}%/{r.get('max_hold',0)/tot*100:.0f}%")
            summary.append({"fold": fname, "config": name, "median": med, "mean": mean,
                            "pct_pos": (pw > 0).mean()})
        del wf, per_cfg
    sdf = pd.DataFrame(summary)
    sdf.to_parquet(_REPO / "app" / "data" / "v16" / "overlay_sweep.parquet")
    print("\n=== CROSS-FOLD (median bps by config) ===")
    piv = sdf.pivot(index="config", columns="fold", values="median")
    base = piv.loc["faithful_7d"]
    piv["cost_f1"] = piv["fold1"] - base["fold1"]
    piv["cost_f2"] = piv["fold2"] - base["fold2"]
    print(piv.round(2).to_string())
    ok = piv[(piv.fold1 > 0) & (piv.fold2 > 0) & (piv.cost_f1 > -3) & (piv.cost_f2 > -3)]
    print(f"\nconfigs passing (both folds >0, cost >-3bps both): {list(ok.index)}")


if __name__ == "__main__":
    main()
