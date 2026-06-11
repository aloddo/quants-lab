#!/usr/bin/env python3
"""SPRINT 2026-06-11: partition hl_s3_fills_v2 day files -> per-wallet shards for the V16 cohort.

m01.load_wallet_fills prefers app/data/hl_s3_fills_v2_by_wallet/<wallet>.parquet; without a shard it
re-scans ALL day files PER WALLET (the reason the 18k m02 run took hours). For the 100-wallet cohort
m02 slice, one streaming pass over the day files -> 100 shards makes the m02 build take minutes.

Schema: identical to the day files (m01 expects the full enriched schema).
Safety: writes ONLY wallets given in --wallets-json (the V16 cohort config); existing shards for
other wallets are untouched. Re-run safe (overwrites cohort shards atomically).

Run: python scripts/sprint_partition_cohort.py [--end 20260612]
"""
from __future__ import annotations
import argparse, json, sys
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path("/Users/hermes/quants-lab")
DAY_DIR = REPO / "app" / "data" / "hl_s3_fills_v2"
OUT_DIR = REPO / "app" / "data" / "hl_s3_fills_v2_by_wallet"
sys.path.insert(0, str(REPO / "research" / "v15"))
from _streaming_io import install_memory_guard


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-json", default=str(REPO / "config" / "copy_trader_wallets_v16.json"))
    ap.add_argument("--start", default="20251101")
    ap.add_argument("--end", default="20260612")
    args = ap.parse_args()
    install_memory_guard(soft_gb=12.0, label="sprint_partition")

    cfg = json.load(open(args.wallets_json))
    wallets = set(w.lower() for w in cfg["wallets"])
    print(f"{len(wallets)} cohort wallets")

    files = sorted(p for p in DAY_DIR.glob("2*.parquet") if args.start <= p.stem <= args.end)
    print(f"{len(files)} day files [{files[0].stem}..{files[-1].stem}]")
    batches = defaultdict(list)
    n_rows = 0
    for i, p in enumerate(files):
        t = pq.read_table(p)
        if t.num_rows == 0:
            continue
        # vectorized filter on wallet column
        import pyarrow.compute as pc
        mask = pc.is_in(t.column("wallet"), value_set=pa.array(list(wallets)))
        ft = t.filter(mask)
        if ft.num_rows == 0:
            continue
        # split by wallet within the (small) filtered table
        wcol = ft.column("wallet").to_pylist()
        idx_by_w = defaultdict(list)
        for j, w in enumerate(wcol):
            idx_by_w[w].append(j)
        for w, idxs in idx_by_w.items():
            batches[w].append(ft.take(idxs))
        n_rows += ft.num_rows
        if (i + 1) % 40 == 0:
            print(f"  [{i+1}/{len(files)}] {n_rows:,} cohort rows accumulated", flush=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for w, tabs in batches.items():
        t = pa.concat_tables(tabs)
        # sort by time for clean downstream consumption
        t = t.sort_by("time")
        tmp = OUT_DIR / f"{w}.parquet.tmp"
        pq.write_table(t, tmp, compression="snappy")
        tmp.replace(OUT_DIR / f"{w}.parquet")
    print(f"DONE: {len(batches)} shards written ({n_rows:,} rows) -> {OUT_DIR}")
    missing = wallets - set(batches)
    if missing:
        print(f"NOTE: {len(missing)} cohort wallets had zero fills in range")


if __name__ == "__main__":
    main()
