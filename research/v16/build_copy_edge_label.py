#!/usr/bin/env python3
"""WHO-selection step 1 (codex-agreed): build the COPY-EDGE LABEL panel.

The correct target is OUR copy PnL after execution (latency 1.86s + per-coin slippage + real HL fees +
funding), NOT the leader's PnL. This builds, per wallet per weekly window, the copy edge of journeys
ENTERED in that window, priced through the canonical execution_model. Output = the (wallet, week) label
panel that downstream feature-ranking + persistence tests consume.

Memory-safe (CLAUDE.md Key Rule 8): one compact in-RAM columnar load (~0.5GB), processed COIN-BY-COIN
(bounded), streaming the panel out. install_memory_guard backstop. No per-row Mongo; marks from the
page-cached ohlc_cache .npy (refreshed to 06-24).

Pricing is the SINGLE source of truth: research/v15/execution_model.py. No hardcoded slip/fee/latency.

Usage: python research/v16/build_copy_edge_label.py [--sample-coins N] [--out PATH]
"""
import argparse, sys, time, urllib.parse as ulib
from pathlib import Path
import numpy as np
import pyarrow.parquet as pq
import pyarrow as pa

V15 = Path(__file__).resolve().parent.parent / "v15"
sys.path.insert(0, str(V15))
from execution_model import apply_entry, apply_exit, fee_rt, gross_return, set_latency_ms, calibrated_share  # noqa
try:
    from _streaming_io import install_memory_guard
except Exception:
    def install_memory_guard(*a, **k):
        pass

DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
JOURNEYS = DATA / "m02_journeys.parquet"
OHLC = DATA / "ohlc_cache"
LAT_MS = 1860  # measured median copy latency (copy-rebuild/2026-06-24-measured-copy-latency)
WEEK_MS = 7 * 24 * 3600 * 1000


def _ohlc(coin):
    p = OHLC / f"{ulib.quote(coin, safe='')}.npy"
    if not p.exists():
        return None
    a = np.load(p, mmap_mode="r")
    if a.shape[1] == 0:
        return None
    return np.asarray(a[0]), np.asarray(a[4])  # minute(ms), close


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample-coins", type=int, default=0, help="0=all; else first N coins (smoke)")
    ap.add_argument("--out", default=str(DATA / "copy_edge_label_panel.parquet"))
    ap.add_argument("--slip-default", type=float, default=0.0,
                    help="override DEFAULT_SLIP_BPS for the uncalibrated tail (codex sensitivity)")
    args = ap.parse_args()
    install_memory_guard(soft_gb=12, label="copyedge")
    set_latency_ms(LAT_MS)
    if args.slip_default > 0:
        from execution_model import set_slip_default_bps
        set_slip_default_bps(args.slip_default)
        print(f"[slip] DEFAULT_SLIP_BPS override -> {args.slip_default} bps one-way")
    t0 = time.time()

    # 1) compact columnar load (bounded): only fields needed for pricing + windowing
    cols = ["wallet", "coin", "side", "entry_ts", "exit_ts", "max_position_notional",
            "net_realized_pnl", "funding_net", "open_at_window_end"]
    pf = pq.ParquetFile(JOURNEYS)
    wallets, coins, sides, ets, xts, notl, lnet, fund, openend = ([] for _ in range(9))
    for b in pf.iter_batches(batch_size=500_000, columns=cols):
        d = b.to_pydict()
        wallets += d["wallet"]; coins += d["coin"]; sides += d["side"]
        ets += d["entry_ts"]; xts += d["exit_ts"]; notl += d["max_position_notional"]
        lnet += d["net_realized_pnl"]; fund += d["funding_net"]; openend += d["open_at_window_end"]
    n = len(wallets)
    ets = np.asarray(ets, dtype="int64")
    xts = np.asarray([(-1 if (x is None) else x) for x in xts], dtype="float64")
    notl = np.abs(np.asarray([(0.0 if v is None else v) for v in notl], dtype="float64"))
    lnet = np.asarray([(0.0 if v is None else v) for v in lnet], dtype="float64")
    fund = np.asarray([(0.0 if v is None else v) for v in fund], dtype="float64")
    is_long = np.asarray([1 if s == "long" else 0 for s in sides], dtype="int8")
    openend = np.asarray([1 if v else 0 for v in openend], dtype="int8")
    t_min = int(ets.min())
    week = ((ets - t_min) // WEEK_MS).astype("int32")
    print(f"[load] {n:,} journeys in {time.time()-t0:.0f}s")

    # integer-code coins for grouping
    ucoins = sorted(set(coins))
    if args.sample_coins:
        ucoins = ucoins[:args.sample_coins]
    coin_arr = np.asarray(coins, dtype=object)

    # 2) per-coin vectorized pricing -> our copy net pnl per journey.
    # CAUSAL (codex gate 2026-06-27): each journey is attributed to its ENTRY week and valued at
    # min(exit+lat, ENTRY-WEEK-END). NEVER value past the entry-week boundary (the old dataset-end MTM
    # was a hard look-ahead leak). Exit slip + exit fee + funding charged ONLY if the journey actually
    # closed within its entry week; otherwise it is a mark-to-market at week end (entry costs only).
    from execution_model import slip_oneway as _slip
    copy_pnl = np.full(n, np.nan, dtype="float64")
    week_end_ms = (t_min + (week.astype("int64") + 1) * WEEK_MS)  # entry-week boundary per journey
    INF = np.iinfo("int64").max
    priced = 0; skipped = 0
    for coin in ucoins:
        oh = _ohlc(coin)
        idxs = np.where(coin_arr == coin)[0]
        if oh is None or len(idxs) == 0:
            skipped += len(idxs); continue
        mins, clo = oh
        e_ts = ets[idxs] + LAT_MS
        x_raw = xts[idxs]
        x_exit = np.where(x_raw < 0, INF, (x_raw + LAT_MS)).astype("int64")  # open -> INF
        wend = week_end_ms[idxs]
        val_ts = np.minimum(x_exit, wend)                 # causal cap at entry-week end
        closed_in_win = x_exit <= wend                    # truly closed within entry week
        ei = np.searchsorted(mins, e_ts, side="right") - 1
        vi = np.searchsorted(mins, val_ts, side="right") - 1
        ok = (ei >= 0) & (vi >= 0) & (ei < len(mins)) & (vi < len(mins))
        em = np.where(ok, clo[np.clip(ei, 0, len(mins) - 1)], np.nan)
        vm = np.where(ok, clo[np.clip(vi, 0, len(mins) - 1)], np.nan)
        good = ok & (em > 0) & (vm > 0)
        sub = idxs[good]
        if len(sub) == 0:
            skipped += len(idxs); continue
        longs = is_long[sub] == 1
        cw = closed_in_win[good]
        slip = _slip(coin)
        half_fee = fee_rt() / 2.0
        emg = em[good]; vmg = vm[good]
        entry_px = np.where(longs, emg * (1 + slip), emg * (1 - slip))   # entry always crosses spread
        # exit crosses spread ONLY if actually closed in-window; else it is a mark (no exit slip)
        exit_px = np.where(cw, np.where(longs, vmg * (1 - slip), vmg * (1 + slip)), vmg)
        gross = np.where(longs, exit_px / entry_px - 1.0, entry_px / exit_px - 1.0)
        cost = half_fee + np.where(cw, half_fee, 0.0)      # entry fee always; exit fee only if closed
        nt = notl[sub]
        fy = np.where((nt > 0) & cw, fund[sub] / nt, 0.0)  # funding only if closed in-window (else unknown)
        copy_pnl[sub] = (gross - cost + fy) * nt
        priced += len(sub); skipped += (len(idxs) - len(sub))
    print(f"[price] priced {priced:,} | skipped {skipped:,} | calib_share {calibrated_share()[0]:.1f}% "
          f"| {time.time()-t0:.0f}s")

    # 3) aggregate -> (wallet, week) panel + whole-history per-wallet (vectorized, pandas)
    import pandas as pd
    valid = ~np.isnan(copy_pnl)
    df = pd.DataFrame({
        "wallet": np.asarray(wallets, dtype=object)[valid],
        "week": week[valid],
        "copy_net_pnl": copy_pnl[valid],
        "copied_gross_notional": notl[valid],
        "leader_net_pnl": lnet[valid],
    })
    panel = (df.groupby(["wallet", "week"], sort=False)
               .agg(copy_net_pnl=("copy_net_pnl", "sum"),
                    copied_gross_notional=("copied_gross_notional", "sum"),
                    leader_net_pnl=("leader_net_pnl", "sum"),
                    n_journeys=("copy_net_pnl", "size")).reset_index())
    pq.write_table(pa.Table.from_pandas(panel, preserve_index=False), args.out)
    print(f"[panel] {len(panel):,} (wallet,week) rows -> {args.out}")

    # 4) whole-history existence read (>=20 priced journeys)
    whole = (df.groupby("wallet", sort=False)
               .agg(cp=("copy_net_pnl", "sum"), gn=("copied_gross_notional", "sum"),
                    n=("copy_net_pnl", "size")))
    whole = whole[whole["n"] >= 20]
    edge = (whole["cp"] / whole["gn"].where(whole["gn"] > 0)).to_numpy()
    print(f"[existence] wallets>=20 priced: {len(whole)} | copy-edge bps p10/50/90 = "
          f"{np.round(np.nanpercentile(edge,[10,50,90])*1e4,1)} | %positive {100*np.nanmean(edge>0):.0f}%")
    print(f"[done] {time.time()-t0:.0f}s")


if __name__ == "__main__":
    main()
