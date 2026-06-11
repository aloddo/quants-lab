#!/usr/bin/env python3
"""V16 OVERLAY OOS -- does the validated edge survive the LIVE engine's risk overlay?

The fidelity_oos validation (+8-9bps OOS taker median) replayed FAITHFUL copies: enter at leader entry
mark + 2s, exit at leader exit mark + 2s, no risk overlay. The LIVE V16 engine adds (rule #7 + safety):
    SL hard floor (sl_bps), trailing TP (trail_activate_bps / trail_bps), max-hold force close.
A protective overlay can DEGRADE a validated edge (truncates winners via trail, realizes losers via SL).
This script answers, OOS and per fold: overlay edge vs faithful edge on the SAME selected cohort.

Protocol (mirrors the validated procedure exactly; the ONLY new element is the exit overlay):
  per fold: select top-decile by TRAIN faithful taker edge (liquid, cap, min_rt -- identical to
  select_cohort.py) -> replay each TEST round-trip TWICE:
    A. FAITHFUL: exit at leader exit mark + latency (the validated baseline)
    B. OVERLAY:  walk 1-min marks from entry; exit at first of SL / trail / max_hold / leader close
  Both priced identically: entry/exit through execution_model.apply_entry/apply_exit + taker fee.
  Per-trade net capped +/-500bps (validated convention) for the FAITHFUL leg and for the overlay leg's
  leader-close exits; overlay SL/trail exits are bounded by construction.

PASS (codex gate input): overlay median edge > 0 on both folds AND within ~3bps of faithful median
(or better). FAIL -> tune trail params and re-run; go-live blocks until PASS.

Run: python research/v16/overlay_oos.py
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_HERE))

import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, apply_entry, apply_exit, set_latency_ms, calibrated_share
from _streaming_io import install_memory_guard
from select_cohort import load_wallet_fills, edge, LIQUID, CAP, MIN_COHORT, MAX_COHORT

FEE_T = fee_rt(maker=False)
MARK_STEP_MS = 60_000

# V16 live overlay params -- MUST match config/copy_trader_wallets_v16.json defaults
SL_BPS = -400.0
TRAIL_ACTIVATE_BPS = 150.0
TRAIL_BPS = 75.0
MAX_HOLD_S = 172_800

# Folds: fold1 = the validated fidelity_oos defaults; fold2 = later split through m02 end.
FOLDS = [
    ("fold1", "2025-12-01", "2026-03-15", "2026-05-17"),
    ("fold2", "2025-12-15", "2026-04-15", "2026-05-23"),
]


def overlay_trade(coin: str, dir_: int, ets: int, xts: int, lat: int):
    """Replay ONE test round-trip under the V16 exit overlay. Returns (net_bps, exit_reason) or None.
    Engine semantics (hl_copy_trader_v15 exit layers): pnl in bps vs entry fill; SL: pnl <= SL_BPS;
    trail: peak >= activate AND peak - pnl >= trail_dist; max_hold force close; else leader close."""
    mark0 = S.mark_at(coin, ets + lat)
    if mark0 is None or mark0 <= 0:
        return None
    entry_px = apply_entry(coin, mark0, dir_ > 0)
    deadline = min(xts + lat, ets + lat + MAX_HOLD_S * 1000)
    reason = "leader_close" if deadline == xts + lat else "max_hold"
    exit_mark = None
    peak = 0.0
    t = ets + lat + MARK_STEP_MS
    while t < deadline:
        m = S.mark_at(coin, t)
        if m is not None and m > 0:
            pnl = dir_ * (m - entry_px) / entry_px * 1e4    # bps vs entry fill (pre exit-cost, like engine mid-pnl)
            peak = max(peak, pnl)
            if pnl <= SL_BPS:
                exit_mark, reason = m, "sl"
                break
            if peak >= TRAIL_ACTIVATE_BPS and (peak - pnl) >= TRAIL_BPS:
                exit_mark, reason = m, "trail"
                break
        t += MARK_STEP_MS
    if exit_mark is None:
        exit_mark = S.mark_at(coin, deadline)
        if exit_mark is None or exit_mark <= 0:
            return None
    exit_px = apply_exit(coin, exit_mark, dir_ > 0)
    gross = dir_ * (exit_px - entry_px) / entry_px
    if reason in ("leader_close", "max_hold"):
        gross = max(-CAP, min(CAP, gross))                  # validated clip on mark-based exits
    return (gross * 1e4 - FEE_T * 1e4, reason)


def faithful_trade(coin: str, dir_: int, ets: int, xts: int, lat: int):
    m0 = S.mark_at(coin, ets + lat); m1 = S.mark_at(coin, xts + lat)
    if m0 is None or m1 is None or m0 <= 0:
        return None
    e = apply_entry(coin, m0, dir_ > 0); x = apply_exit(coin, m1, dir_ > 0)
    g = max(-CAP, min(CAP, dir_ * (x - e) / e))
    return g * 1e4 - FEE_T * 1e4


def main():
    install_memory_guard(soft_gb=12.0, label="v16_overlay")
    ap = argparse.ArgumentParser()
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-rt", type=int, default=15)
    ap.add_argument("--universe-file", default=str(S._DATA / "m01_nonerroring_wallets.txt"))
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    lat = args.latency_s * 1000
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    uni = set(l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#"))

    overall = []
    for fname, f_start, f_split, f_end in FOLDS:
        start, split, end = ms(f_start), ms(f_split), ms(f_end)
        print(f"\n=== {fname}: train {f_start}->{f_split}, test {f_split}->{f_end} ===")
        wf = load_wallet_fills(uni, start, end)
        print(f"  {len(wf)} wallets with fills")

        # TRAIN selection (identical to the validated procedure / select_cohort.py).
        # Memory: do NOT cache round-trips for all 18k wallets; recompute for the <=100 cohort below.
        rows = []
        for w, fl in wf.items():
            fl.sort(key=lambda x: x[0])
            rts = roundtrips(fl)
            tr, trn, _ = edge(rts, start, split, lat, FEE_T)
            ten_check = sum(1 for c, d_, e_, x_, *_ in rts if split <= e_ < end and c in LIQUID)
            if tr is not None and trn >= args.min_rt and ten_check >= args.min_rt:
                rows.append({"wallet": w, "train_taker": tr, "train_n": trn})
        df = pd.DataFrame(rows).sort_values("train_taker", ascending=False).reset_index(drop=True)
        dec = max(MIN_COHORT, len(df) // 10)
        cohort = df.head(min(dec, MAX_COHORT))
        print(f"  {len(df)} rankable; cohort {len(cohort)} (train taker median {cohort.train_taker.median():+.1f}bps)")

        # TEST replay: faithful vs overlay, per wallet
        per_wallet = []
        reasons_all = {}
        for w in cohort.wallet:
            f_nets, o_nets = [], []
            for c, dir_, ets, xts, evw, xvw, g in roundtrips(wf[w]):
                if not (split <= ets < end) or c not in LIQUID:
                    continue
                fv = faithful_trade(c, dir_, ets, xts, lat)
                ov = overlay_trade(c, dir_, ets, xts, lat)
                if fv is None or ov is None:
                    continue
                f_nets.append(fv); o_nets.append(ov[0])
                reasons_all[ov[1]] = reasons_all.get(ov[1], 0) + 1
            if len(f_nets) >= args.min_rt:
                per_wallet.append({"wallet": w, "faithful": np.mean(f_nets), "overlay": np.mean(o_nets),
                                   "n": len(f_nets)})
        pw = pd.DataFrame(per_wallet)
        fm, om = pw.faithful.median(), pw.overlay.median()
        n_tr = int(pw.n.sum())
        print(f"  TEST ({len(pw)} wallets, {n_tr} trades): faithful median {fm:+.2f}bps | "
              f"OVERLAY median {om:+.2f}bps | delta {om - fm:+.2f}bps")
        print(f"  means: faithful {pw.faithful.mean():+.2f} | overlay {pw.overlay.mean():+.2f} | "
              f"overlay>0 wallets: {(pw.overlay > 0).mean()*100:.0f}%")
        tot = sum(reasons_all.values())
        print(f"  exit reasons: " + ", ".join(f"{k} {v} ({v/tot*100:.0f}%)" for k, v in sorted(reasons_all.items())))
        overall.append({"fold": fname, "faithful_med": fm, "overlay_med": om, "wallets": len(pw), "trades": n_tr})
        pw.to_parquet(_REPO / "app" / "data" / "v16" / f"overlay_{fname}.parquet")

    cs, nc, nd = calibrated_share()
    print(f"\ncalibrated slippage share: {cs:.0f}% ({nc} calib / {nd} default lookups)")
    print("\n=== VERDICT ===")
    ok = all(o["overlay_med"] > 0 and (o["overlay_med"] - o["faithful_med"]) > -3.0 for o in overall)
    for o in overall:
        print(f"  {o['fold']}: overlay {o['overlay_med']:+.2f} vs faithful {o['faithful_med']:+.2f} "
              f"({o['wallets']} wallets, {o['trades']} trades)")
    print("PASS: overlay preserves the validated edge" if ok else
          "FAIL: overlay degrades the edge -- tune trail/SL before go-live")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
