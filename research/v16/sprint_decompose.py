#!/usr/bin/env python3
"""SPRINT 2026-06-11: per-trade decomposition of the V16 edge (existing folds, existing m02).

Goal: find WHERE the validated edge concentrates, to design a STRONGER strategy (Alberto directive:
50-100%/mo ROE target). One m02 pass per fold -> per-trade parquet with conditioning features:

  fold, wallet, rank (train_taker rank within selected cohort), train_taker_bps, train_n,
  coin, dir, entry_ts, exit_ts (leader), hold_h, leader_open_notional,
  k_same (cohort wallets already holding same coin+side at our entry, top-100),
  k_opp  (same coin, opposite side), k30_same (k_same among rank<=30),
  faithful_net_bps @ latency 2s (validated baseline),
  fl_500/fl_1000/fl_5000 (latency sensitivity),
  ov_net_bps + ov_reason (SHIPPED overlay sl-1500/trail 600-300/hold 7d),
  hour_utc, dow.

Selection protocol identical to the validated procedure (engine_replay.py / overlay_oos.py):
train_taker edge on TRAIN window, liquid-only, cap 500bps, min 15 train RTs + 15 test entries,
cohort = top decile floored 30 capped 100. Pricing via canonical execution_model (BINDING).

Run: python research/v16/sprint_decompose.py        (~10-15 min both folds)
Out: app/data/v16/sprint_trades.parquet
"""
from __future__ import annotations
import sys
from collections import defaultdict
from datetime import datetime, timezone
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
from _streaming_io import ShardedParquetWriter, install_memory_guard
from select_cohort import load_wallet_fills, edge, LIQUID, CAP, MIN_COHORT, MAX_COHORT

FEE_T = fee_rt(maker=False)
MARK_STEP_MS = 60_000
LAT = 2_000                       # validated latency
LAT_VARIANTS = (500, 1_000, 5_000)

# SHIPPED overlay (config/copy_trader_wallets_v16.json defaults) -- the live exit family
SL_BPS = -1500.0
TRAIL_ACTIVATE_BPS = 600.0
TRAIL_BPS = 300.0
MAX_HOLD_S = 604_800              # 7d

FOLDS = [
    ("fold1", "2025-12-01", "2026-03-15", "2026-05-17"),
    ("fold2", "2025-12-15", "2026-04-15", "2026-05-23"),
]


def faithful_net(coin, dir_, ets, xts, lat):
    m0 = S.mark_at(coin, ets + lat); m1 = S.mark_at(coin, xts + lat)
    if m0 is None or m1 is None or m0 <= 0 or m1 is None:
        return None
    e = apply_entry(coin, m0, dir_ > 0); x = apply_exit(coin, m1, dir_ > 0)
    g = max(-CAP, min(CAP, dir_ * (x - e) / e))
    return g * 1e4 - FEE_T * 1e4


def overlay_net(coin, dir_, ets, xts, lat, ride_hold_ms=None):
    """Shipped overlay replay (copy of overlay_oos.overlay_trade with SHIPPED params).

    ride_hold_ms (V17.1 ride): when set, IGNORE the leader close (xts) and hold to
    ets+lat+ride_hold_ms (still 7d-capped), with the SAME trailing stop. None = shipped
    behavior, byte-identical. Returns (net_bps, reason, exit_ts_ms)."""
    mark0 = S.mark_at(coin, ets + lat)
    if mark0 is None or mark0 <= 0:
        return None
    entry_px = apply_entry(coin, mark0, dir_ > 0)
    if ride_hold_ms is not None:
        deadline = min(ets + lat + ride_hold_ms, ets + lat + MAX_HOLD_S * 1000)
        reason = "ride_cap" if deadline == ets + lat + ride_hold_ms else "max_hold"
    else:
        deadline = min(xts + lat, ets + lat + MAX_HOLD_S * 1000)
        reason = "leader_close" if deadline == xts + lat else "max_hold"
    exit_mark = None
    exit_t = deadline
    peak = 0.0
    t = ets + lat + MARK_STEP_MS
    while t < deadline:
        m = S.mark_at(coin, t)
        if m is not None and m > 0:
            pnl = dir_ * (m - entry_px) / entry_px * 1e4
            peak = max(peak, pnl)
            if pnl <= SL_BPS:
                exit_mark, reason, exit_t = m, "sl", t
                break
            if peak >= TRAIL_ACTIVATE_BPS and (peak - pnl) >= TRAIL_BPS:
                exit_mark, reason, exit_t = m, "trail", t
                break
        t += MARK_STEP_MS
    if exit_mark is None:
        exit_mark = S.mark_at(coin, deadline)
        if exit_mark is None or exit_mark <= 0:
            return None
    exit_px = apply_exit(coin, exit_mark, dir_ > 0)
    gross = dir_ * (exit_px - entry_px) / entry_px
    if reason in ("leader_close", "max_hold", "ride_cap"):
        gross = max(-CAP, min(CAP, gross))
    return (gross * 1e4 - FEE_T * 1e4, reason, exit_t)


def run_fold(writer, fname, f_start, f_split, f_end, uni):
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end = ms(f_start), ms(f_split), ms(f_end)
    print(f"\n=== {fname}: train {f_start}->{f_split} | test {f_split}->{f_end} ===", flush=True)
    wf = load_wallet_fills(uni, start, end)
    print(f"  {len(wf)} wallets with fills", flush=True)

    # TRAIN selection -- identical to validated procedure
    rows = []
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        rts = roundtrips(fl)
        tr, trn, _ = edge(rts, start, split, LAT, FEE_T)
        ten = sum(1 for c, d_, e_, x_, *_ in rts if split <= e_ < end and c in LIQUID)
        if tr is not None and trn >= 15 and ten >= 15:
            rows.append({"wallet": w, "train_taker": tr, "train_n": trn})
    df = pd.DataFrame(rows).sort_values("train_taker", ascending=False).reset_index(drop=True)
    cohort = df.head(min(max(MIN_COHORT, len(df) // 10), MAX_COHORT)).copy()
    cohort["rank"] = np.arange(1, len(cohort) + 1)
    print(f"  cohort {len(cohort)} of {len(df)} rankable "
          f"(train med {cohort.train_taker.median():+.1f}bps)", flush=True)
    rank_of = dict(zip(cohort.wallet, cohort["rank"]))
    traint_of = dict(zip(cohort.wallet, cohort.train_taker))
    trainn_of = dict(zip(cohort.wallet, cohort.train_n))

    # PASS 1: collect cohort TEST round-trips + leader opening notionals
    trades = []   # (w, c, dir_, ets, xts)
    open_notional = {}
    for w in cohort.wallet:
        fl = wf[w]
        nmap = defaultdict(float)
        for t, c, ssz, px in fl:
            nmap[(c, t)] += abs(float(ssz)) * float(px)
        for c, dir_, ets, xts, evw, xvw, g in roundtrips(fl):
            if not (split <= ets < end) or c not in LIQUID:
                continue
            trades.append((w, c, int(dir_), int(ets), int(xts)))
            open_notional[(w, c, ets)] = nmap.get((c, ets), 0.0)
    del wf
    print(f"  {len(trades)} cohort TEST round-trips", flush=True)

    # consensus K: vectorized per coin+side interval overlap (among top-100 cohort)
    bycoin = defaultdict(list)
    for i, (w, c, dir_, ets, xts) in enumerate(trades):
        bycoin[c].append(i)
    k_same = np.zeros(len(trades), dtype=np.int32)
    k_opp = np.zeros(len(trades), dtype=np.int32)
    k30_same = np.zeros(len(trades), dtype=np.int32)
    for c, idxs in bycoin.items():
        arr = np.array([(trades[i][3], trades[i][4], trades[i][2],
                         rank_of[trades[i][0]], hash(trades[i][0]) & 0x7FFFFFFF)
                        for i in idxs], dtype=np.int64)
        E, X, D, R, W = arr[:, 0], arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4]
        for j, i in enumerate(idxs):
            t0, d0, w0 = E[j], D[j], W[j]
            live = (E <= t0) & (X > t0) & (W != w0)
            k_same[i] = int(np.sum(live & (D == d0)))
            k_opp[i] = int(np.sum(live & (D != d0)))
            k30_same[i] = int(np.sum(live & (D == d0) & (R <= 30)))

    # PASS 2: price every trade (faithful @ latencies + shipped overlay) and stream out
    n_out = 0
    for i, (w, c, dir_, ets, xts) in enumerate(trades):
        fl2 = faithful_net(c, dir_, ets, xts, LAT)
        ov = overlay_net(c, dir_, ets, xts, LAT)
        if fl2 is None or ov is None:
            continue
        row = {
            "fold": fname, "wallet": w, "rank": int(rank_of[w]),
            "train_taker": float(traint_of[w]), "train_n": int(trainn_of[w]),
            "coin": c, "dir": dir_, "entry_ts": ets, "exit_ts": xts,
            "hold_h": (xts - ets) / 3_600_000.0,
            "leader_open_notional": float(open_notional.get((w, c, ets), 0.0)),
            "k_same": int(k_same[i]), "k_opp": int(k_opp[i]), "k30_same": int(k30_same[i]),
            "faithful_bps": fl2, "ov_bps": ov[0], "ov_reason": ov[1],
            "hour_utc": datetime.fromtimestamp(ets / 1000, tz=timezone.utc).hour,
            "dow": datetime.fromtimestamp(ets / 1000, tz=timezone.utc).weekday(),
        }
        for lv in LAT_VARIANTS:
            v = faithful_net(c, dir_, ets, xts, lv)
            row[f"fl_{lv}"] = v if v is not None else np.nan
        writer.add_many([row])
        n_out += 1
    print(f"  {n_out} trades priced + written", flush=True)


def main():
    install_memory_guard(soft_gb=12.0, label="sprint_decompose")
    set_latency_ms(LAT)
    uni = set(l.strip().lower() for l in open(S._DATA / "m01_nonerroring_wallets.txt")
              if l.strip() and not l.startswith("#"))
    out = _REPO / "app" / "data" / "v16" / "sprint_trades.parquet"
    writer = ShardedParquetWriter(out, flush_rows=50_000)
    for f in FOLDS:
        run_fold(writer, *f, uni)
    n = writer.close()
    cs, nc, nd = calibrated_share()
    print(f"\nDONE {n} rows -> {out}")
    print(f"calibrated slippage share: {cs:.0f}% ({nc} calib / {nd} default)")


if __name__ == "__main__":
    main()
