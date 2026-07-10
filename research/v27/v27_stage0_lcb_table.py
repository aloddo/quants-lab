#!/usr/bin/env python3
"""v27 Stage 0 helper: precompute the (wallet x boundary) R2-LCB table in parallel.

Same frozen code path per call (v25_r2_lcb.wallet_lcb, frozen constants, seed 42),
same strict half-open boundary slicing as v27_stage0._rank_entities — only the outer
loop is parallelized over wallets (each worker handles a wallet chunk and computes all
16 boundaries: b3 plus k=4..18). Output: lcb_table.parquet with
(wallet, boundary_k, lcb_bps, n_trips). Memory: workers load only their wallets' trips
via a shared read-only parquet + row-group pushdown is unnecessary at this size
(r2trips 128MB); each worker reads the pruned columns once.

Run: python research/v27/v27_stage0_lcb_table.py --procs 8
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "research" / "v25"))
sys.path.insert(0, str(REPO / "research" / "v15"))

from _streaming_io import install_memory_guard  # noqa: E402

OUT_DIR = REPO / "app" / "data" / "research" / "v27"
GRID_START_MS = 1765670400000
MS_DAY = 86_400_000
BLOCK_MS = 10 * MS_DAY
R2_MIN_TRIPS_RANKABLE = 50
BOUNDARIES = [GRID_START_MS + k * BLOCK_MS for k in range(3, 18)]  # b3..b17 (rank asofs)
BOUNDARY_KS = list(range(3, 18))

_TRIPS = None  # per-worker global


def _init_worker():
    global _TRIPS
    install_memory_guard()
    t = pd.read_parquet(OUT_DIR / "r2trips_full.parquet",
                        columns=["wallet", "exit_fill_ts_last", "net_bps", "terminal"])
    t = t[~t["terminal"].astype(bool)]
    per = t.groupby("wallet")["wallet"].transform("size")
    t = t[per >= R2_MIN_TRIPS_RANKABLE]        # bound worker memory to candidates only
    _TRIPS = {w: g.sort_values("exit_fill_ts_last")[["net_bps", "exit_fill_ts_last"]]
              .reset_index(drop=True) for w, g in t.groupby("wallet", sort=False)}


def _work(wallets: list) -> list:
    from v25_r2_lcb import wallet_lcb
    rows = []
    for w in wallets:
        g = _TRIPS.get(w)
        if g is None:
            continue
        ex = g["exit_fill_ts_last"].to_numpy()
        for k, b in zip(BOUNDARY_KS, BOUNDARIES):
            n = int(np.searchsorted(ex, b, side="left"))   # strict <
            if n < R2_MIN_TRIPS_RANKABLE:
                continue
            r = wallet_lcb(g.iloc[:n], GRID_START_MS, b)
            if not np.isnan(r["lcb_bps"]):
                rows.append({"wallet": w, "boundary_k": k,
                             "lcb_bps": float(r["lcb_bps"]),
                             "n_trips": int(r["n_trips"])})
    return rows


def main():
    install_memory_guard()
    ap = argparse.ArgumentParser()
    ap.add_argument("--procs", type=int, default=8)
    a = ap.parse_args()
    t = pd.read_parquet(OUT_DIR / "r2trips_full.parquet", columns=["wallet", "terminal"])
    per = t[~t["terminal"].astype(bool)].groupby("wallet").size()
    cand = sorted(per[per >= R2_MIN_TRIPS_RANKABLE].index)  # >=50 over full window =>
    # superset of every boundary's rankable set (trip counts only grow with k)
    print(f"candidates: {len(cand)} wallets x {len(BOUNDARY_KS)} boundaries", flush=True)
    chunks = [cand[i::a.procs * 4] for i in range(a.procs * 4)]
    t0 = time.time()
    rows = []
    with Pool(a.procs, initializer=_init_worker) as pool:
        for i, out in enumerate(pool.imap_unordered(_work, chunks)):
            rows.extend(out)
            print(f"  chunk {i+1}/{len(chunks)} done, {len(rows)} rows, "
                  f"{time.time()-t0:.0f}s", flush=True)
    df = pd.DataFrame(rows)
    df.to_parquet(OUT_DIR / "lcb_table.parquet", index=False)
    print(f"done: {len(df)} rows -> lcb_table.parquet in {time.time()-t0:.0f}s", flush=True)


if __name__ == "__main__":
    main()
