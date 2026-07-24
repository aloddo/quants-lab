#!/usr/bin/env python
"""Reshape the m01 ledger monolith (ledger_20k.parquet) into the hot store's day-partitioned schema
so the hot ledger store gains the pre-2026-06-09 deep history (the fills store already has its deep
history hardlinked; ledger never got that). NO redownload: the history already exists locally.

Byte-identical-by-construction: m01.load_wallet_ledger's REAL source is this monolith (per-wallet JSON
caches are empty), and it reconstructs delta as {type, token, + _LEDGER_DELTA_FIELDS}. We reproduce that
EXACT reconstruction into hot-schema `delta_json`, so hl_fills_io.load_grouped_ledger over the reshaped
day-files yields the same consumed projection m01 returns. Validation (separate) must confirm.

Writes ONLY days < 20260609 (the hot store already owns 2026-06-09+ from S3). Memory-safe: reads only
needed columns, streams per-day writes.
"""
import sys, json, math, glob
from pathlib import Path
import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab/research/v15")
from _streaming_io import install_memory_guard  # noqa: E402

MONOLITH = Path("/Users/hermes/quants-lab/app/data/raw/ledger/ledger_20k.parquet")
HOT_LEDGER_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_ledger_hot")
CUTOFF_DAY = "20260609"  # hot store owns this day onward; reshape strictly-before

# EXACT copy of m01._LEDGER_DELTA_FIELDS (v15_m01_equity_reconstruct.py)
LEDGER_DELTA_FIELDS = (
    "usdc", "usdcValue", "amount", "fee", "user", "destination",
    "sourceDex", "destinationDex", "toPerp", "netWithdrawnUsd", "operation", "dex",
)
READ_COLS = ["wallet", "time", "hash", "coin", "type"] + list(LEDGER_DELTA_FIELDS)


def _is_missing(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


def _native(v):
    # numpy scalar -> python native; leave python types as-is (matches how consumers float()/str() them)
    if hasattr(v, "item"):
        try:
            return v.item()
        except Exception:  # noqa: BLE001
            return v
    return v


def build_delta(row) -> dict:
    """Replicate m01.load_wallet_ledger's monolith-fallback delta reconstruction exactly."""
    delta = {"type": str(row["type"])}
    coin = row.get("coin")
    if not _is_missing(coin) and str(coin) != "":
        delta["token"] = str(coin)
    for fld in LEDGER_DELTA_FIELDS:
        v = row.get(fld)
        if _is_missing(v):
            continue
        delta[fld] = _native(v)
    return delta


def main():
    install_memory_guard(soft_gb=6.0, label="ledger-reshape")
    import pyarrow.parquet as pq
    avail_cols = set(pq.ParquetFile(str(MONOLITH)).schema_arrow.names)
    read = [c for c in READ_COLS if c in avail_cols]
    missing = [c for c in READ_COLS if c not in avail_cols]
    print(f"[reshape] reading {len(read)} cols; monolith-missing {missing}")
    df = pd.read_parquet(MONOLITH, columns=read)
    df["time"] = df["time"].astype("int64")
    df["wallet"] = df["wallet"].astype(str).str.lower()
    df["day"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.strftime("%Y%m%d")
    df = df[df["day"] < CUTOFF_DAY]
    print(f"[reshape] {len(df)} pre-{CUTOFF_DAY} rows across {df['day'].nunique()} days")
    HOT_LEDGER_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    for day, g in df.groupby("day", sort=True):
        outp = HOT_LEDGER_DIR / f"{day}.parquet"
        if outp.exists():
            print(f"[reshape] SKIP {day} (hot file already exists)")
            continue
        recs = g.to_dict("records")
        rows = []
        for r in recs:
            delta = build_delta(r)
            usdc = delta.get("usdc", "")
            rows.append({
                "wallet": r["wallet"],
                "time": int(r["time"]),
                "type": str(r["type"]),
                "usdc": "" if _is_missing(usdc) else str(usdc),
                "delta_json": json.dumps(delta, separators=(",", ":")),
                "hash": "" if _is_missing(r.get("hash")) else str(r.get("hash")),
                "source": "m01_monolith_reshape",
            })
        out = pd.DataFrame(rows, columns=["wallet", "time", "type", "usdc", "delta_json", "hash", "source"])
        out.to_parquet(outp, index=False)
        written += 1
    print(f"[reshape] wrote {written} day-files -> {HOT_LEDGER_DIR}")
    print("RESHAPE_DONE")


if __name__ == "__main__":
    main()
