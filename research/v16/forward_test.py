#!/usr/bin/env python3
"""SPRINT H13: TRUE FORWARD TEST of the SHIPPED V16 cohort on unseen post-selection data.

The shipped cohort was selected asof 2026-05-23 (trailing 75d). m02 ends 05-23. This script:
  1. Loads wallet fills DIRECTLY from app/data/hl_s3_fills_v2/ daily parquets (the m02 SOURCE),
     so freshly downloaded days extend the window with zero rebuild.
  2. --validate: recompute the train edge per shipped-cohort wallet on the selection window
     (75d ending 05-23, latency 2s, taker) from the fills loader and compare to the SHIPPED
     trail_taker_bps in config -- proves the loader is equivalent to the m02 path.
  3. --forward: replay 05-23 -> --end on the shipped cohort (faithful + shipped overlay,
     latency variants, consensus-K), emitting the SAME per-trade schema as sprint_decompose
     (fold = 'forward') -> analyze with sprint_analysis.py --in <out> --days N.

Marks: assetctx npy (extended by scripts/extract_asset_ctx_marks.py through --end).
Execution: canonical execution_model (BINDING).

Run:
  python research/v16/forward_test.py --validate
  python research/v16/forward_test.py --forward --end 2026-06-10
"""
from __future__ import annotations
import argparse, json, sys
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
from execution_model import fee_rt, set_latency_ms, calibrated_share
from _streaming_io import ShardedParquetWriter, install_memory_guard
from select_cohort import edge, LIQUID, CAP
from sprint_decompose import faithful_net, overlay_net, LAT, LAT_VARIANTS

RIDE_HOLD_MS = 72 * 3_600_000     # V17.1 ride: hold high-knet entries to 72h (matches engine default)

FEE_T = fee_rt(maker=False)
FILLS_DIR = _REPO / "app" / "data" / "hl_s3_fills_v2"
CFG = json.load(open(_REPO / "config" / "copy_trader_wallets_v16.json"))


def load_fills_daily(wallets: set, start_ms: int, end_ms: int) -> dict:
    """{wallet: [(ts, coin, signed_size, price), ...]} from fills_v2 daily parquets."""
    import pyarrow.parquet as pq
    d0 = pd.Timestamp(start_ms, unit="ms", tz="UTC").strftime("%Y%m%d")
    d1 = pd.Timestamp(end_ms, unit="ms", tz="UTC").strftime("%Y%m%d")
    wf = defaultdict(list)
    files = sorted(p for p in FILLS_DIR.glob("2*.parquet") if d0 <= p.stem <= d1)
    for p in files:
        tb = pq.read_table(p, columns=["wallet", "coin", "side", "size", "price", "time"])
        d = tb.to_pydict()
        for i in range(len(d["wallet"])):
            w = d["wallet"][i]
            if w not in wallets:
                continue
            t = int(d["time"][i])
            if not (start_ms <= t <= end_ms):
                continue
            sz = float(d["size"][i])
            ssz = sz if d["side"][i] == "B" else -sz
            wf[w].append((t, d["coin"][i], ssz, float(d["price"][i])))
    for w in wf:
        wf[w].sort(key=lambda x: x[0])
    print(f"  loaded {len(files)} day files [{files[0].stem}..{files[-1].stem}], "
          f"{sum(len(v) for v in wf.values())} fills for {len(wf)} wallets", flush=True)
    return wf


def validate():
    """Reproduce the shipped train ranking from the fills loader (loader equivalence proof)."""
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    sel_end = ms("2026-05-23")
    sel_start = sel_end - 75 * 86_400_000
    cohort = CFG["wallets"]
    wf = load_fills_daily(set(cohort), sel_start - 14 * 86_400_000, sel_end)  # 2w warmup pre-window
    rows = []
    for w, meta in cohort.items():
        fl = wf.get(w, [])
        if not fl:
            rows.append({"wallet": w, "shipped": meta["trail_taker_bps"], "recomputed": np.nan, "n_rt": 0})
            continue
        tr, trn, _ = edge(roundtrips(fl), sel_start, sel_end, LAT, FEE_T)
        rows.append({"wallet": w, "shipped": meta["trail_taker_bps"],
                     "recomputed": tr if tr is not None else np.nan, "n_rt": trn})
    df = pd.DataFrame(rows)
    df["diff"] = df.recomputed - df.shipped
    ok = df.dropna(subset=["recomputed"])
    print(f"\nVALIDATE: {len(ok)}/{len(df)} wallets recomputed")
    print(f"  |diff| quantiles (bps): {ok['diff'].abs().quantile([0.5, 0.9, 0.99]).round(3).tolist()}")
    print(f"  within 1bps: {(ok['diff'].abs() < 1.0).mean()*100:.0f}% | within 5bps: {(ok['diff'].abs() < 5.0).mean()*100:.0f}%")
    print(f"  rank corr (spearman): {ok[['shipped','recomputed']].corr(method='spearman').iloc[0,1]:.4f}")
    bad = ok[ok["diff"].abs() > 10].sort_values("diff")
    if len(bad):
        print(f"  WALLETS OFF >10bps ({len(bad)}):\n{bad.head(10).round(2).to_string()}")
    df.to_parquet(_REPO / "app" / "data" / "v16" / "forward_validate.parquet")
    return (ok["diff"].abs() < 5.0).mean() > 0.9


def forward(end_str: str, m02_slice: str | None = None, ride_knet: int | None = None):
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    split = ms("2026-05-23")
    end = ms(end_str) + 86_399_000          # inclusive end-of-day
    cohort = CFG["wallets"]
    rank_of = {w: m["rank"] for w, m in cohort.items()}
    # warmup from 2025-12-01 (validated fold convention) for RT position context
    if m02_slice:
        # VALIDATED PATH: cohort-only m02 slice built by v15_m02_journey_trace (same machinery
        # as the selection of record). The fills-native loader FAILED equivalence (rank corr
        # 0.43) -- see /tmp/forward_validate.log, lesson 2026-06-11.
        import select_cohort as SC
        SC.M02 = Path(m02_slice)
        wf = SC.load_wallet_fills(set(cohort), ms("2025-12-01"), end)
        print(f"  m02-slice loader: {m02_slice}", flush=True)
    else:
        wf = load_fills_daily(set(cohort), ms("2025-12-01"), end)

    trades = []
    open_notional = {}
    for w in cohort:
        fl = wf.get(w, [])
        if not fl:
            continue
        nmap = defaultdict(float)
        for t, c, ssz, px in fl:
            nmap[(c, t)] += abs(ssz) * px
        for c, dir_, ets, xts, evw, xvw, g in roundtrips(fl):
            if not (split <= ets < end) or c not in LIQUID:
                continue
            trades.append((w, c, int(dir_), int(ets), int(xts)))
            open_notional[(w, c, ets)] = nmap.get((c, ets), 0.0)
    print(f"  {len(trades)} forward round-trips (entries {pd.Timestamp(split, unit='ms', tz='UTC').date()}"
          f" -> {end_str})", flush=True)

    # consensus K (same conventions as sprint_decompose)
    bycoin = defaultdict(list)
    for i, tr in enumerate(trades):
        bycoin[tr[1]].append(i)
    k_same = np.zeros(len(trades), dtype=np.int32)
    k_opp = np.zeros(len(trades), dtype=np.int32)
    k30 = np.zeros(len(trades), dtype=np.int32)
    for c, idxs in bycoin.items():
        arr = np.array([(trades[i][3], trades[i][4], trades[i][2],
                         rank_of[trades[i][0]], hash(trades[i][0]) & 0x7FFFFFFF)
                        for i in idxs], dtype=np.int64)
        E, X, D, R, W = arr.T
        for j, i in enumerate(idxs):
            live = (E <= E[j]) & (X > E[j]) & (W != W[j])
            k_same[i] = int(np.sum(live & (D == D[j])))
            k_opp[i] = int(np.sum(live & (D != D[j])))
            k30[i] = int(np.sum(live & (D == D[j]) & (R <= 30)))

    out = _REPO / "app" / "data" / "v16" / (
        "forward_trades.parquet" if ride_knet is None
        else f"forward_trades_ride_knet{ride_knet}.parquet")
    writer = ShardedParquetWriter(out, flush_rows=50_000)
    n_out, n_skip, n_ride = 0, 0, 0
    for i, (w, c, dir_, ets, xts) in enumerate(trades):
        knet = int(k_same[i]) - int(k_opp[i])
        is_ride = ride_knet is not None and knet >= ride_knet
        fl2 = faithful_net(c, dir_, ets, xts, LAT)
        ov = overlay_net(c, dir_, ets, xts, LAT, ride_hold_ms=(RIDE_HOLD_MS if is_ride else None))
        if fl2 is None or ov is None:
            n_skip += 1
            continue
        row = {"fold": "forward", "wallet": w, "rank": int(rank_of[w]),
               "train_taker": float(cohort[w]["trail_taker_bps"]), "train_n": int(cohort[w]["trail_n_rt"]),
               "coin": c, "dir": dir_, "entry_ts": ets, "exit_ts": xts,
               "hold_h": (xts - ets) / 3_600_000.0,
               "leader_open_notional": float(open_notional.get((w, c, ets), 0.0)),
               "k_same": int(k_same[i]), "k_opp": int(k_opp[i]), "k30_same": int(k30[i]),
               "faithful_bps": fl2, "ov_bps": ov[0], "ov_reason": ov[1],
               "hour_utc": datetime.fromtimestamp(ets / 1000, tz=timezone.utc).hour,
               "dow": datetime.fromtimestamp(ets / 1000, tz=timezone.utc).weekday()}
        for lv in LAT_VARIANTS:
            v = faithful_net(c, dir_, ets, xts, lv)
            row[f"fl_{lv}"] = v if v is not None else np.nan
        if ride_knet is not None:   # ride columns ONLY in ride mode -> default output byte-identical
            row["knet"] = knet
            row["ride"] = bool(is_ride)
            row["ride_exit_ts"] = int(ov[2])
            row["actual_hold_h"] = (int(ov[2]) - ets) / 3_600_000.0
            n_ride += int(is_ride)
        writer.add_many([row])
        n_out += 1
    n = writer.close()
    cs, nc, nd = calibrated_share()
    _rmsg = "" if ride_knet is None else f" | ride knet>={ride_knet}: {n_ride} rode of {n_out}"
    print(f"  {n_out} priced ({n_skip} skipped, no mark) -> {out}{_rmsg}")
    print(f"  calibrated slip share: {cs:.0f}%")
    days = (end - split) / 86_400_000
    print(f"\nANALYZE: python research/v16/sprint_analysis.py --in {out} --days {days:.1f}")


def main():
    install_memory_guard(soft_gb=12.0, label="forward_test")
    set_latency_ms(LAT)
    import os
    _md = os.environ.get("V16_SPRINT_MARKS_DIR")
    if _md:
        S.ASSETCTX_DIR = Path(_md)   # sprint marks (assetctx + tape bridge) for the forward window
        print(f"marks dir override: {_md}")
    ap = argparse.ArgumentParser()
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--forward", action="store_true")
    ap.add_argument("--end", default="2026-06-10")
    ap.add_argument("--m02-slice", default=None, help="cohort-only m02 actions parquet (validated path)")
    ap.add_argument("--ride-knet", type=int, default=None,
                    help="V17.1 ride: entries with knet>=N ignore the leader close and hold to 72h cap")
    args = ap.parse_args()
    if args.validate:
        ok = validate()
        print("VALIDATE:", "PASS" if ok else "FAIL")
        sys.exit(0 if ok else 1)
    if args.forward:
        forward(args.end, args.m02_slice, args.ride_knet)


if __name__ == "__main__":
    main()
