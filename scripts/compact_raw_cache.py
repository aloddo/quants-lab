#!/usr/bin/env python3
"""Compact a flat per-wallet-per-window JSON raw cache (funding or ledger) into ONE deduped parquet,
trimmed to a date window. Memory-safe: processes wallet-by-wallet (duplication is within a wallet's
overlapping files), so peak RAM = one wallet's events. Deletes NOTHING -- writes a new parquet only.

Each source file = JSON array of delta events: {"time":ms, "hash":..., "delta":{...}}.
Dedupe key = (wallet, time, hash, coin/token). Trim: keep events with LO <= time < HI.

Usage:
  python scripts/compact_raw_cache.py --src <flatdir> --out <parquet> [--limit N] [--report-only]
"""
import os, sys, json, argparse, datetime as dt
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "research", "v15"))
from _streaming_io import ShardedParquetWriter  # memory-safe (CLAUDE.md Rule 8)

LO = 1764547200000  # 2025-12-01 00:00 UTC
HI = 1782921600000  # 2026-07-01 00:00 UTC (end of 2026-06-30)

def wallet_of(fname):
    i = fname.find("_")
    return fname[:i] if i > 0 else fname

def flush_wallet(events, writer, stats):
    """Dedupe one wallet's events, trim to window, write."""
    seen = set(); out = []
    for ev in events:
        t = ev.get("time")
        if t is None or not (LO <= t < HI):
            stats["out_window"] += 1; continue
        d = ev.get("delta", {}) or {}
        coin = d.get("coin") or d.get("token") or ""
        k = (ev.get("_w"), t, ev.get("hash"), coin)
        if k in seen:
            stats["dup"] += 1; continue
        seen.add(k)
        row = {"wallet": ev.get("_w"), "time": t, "hash": ev.get("hash"), "coin": coin}
        for kk, vv in d.items():
            if kk not in ("coin", "token"):
                row[kk] = vv
        out.append(row)
    writer.add_many(out)
    stats["kept"] += len(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--limit", type=int, default=0, help="smoke: process only first N files")
    ap.add_argument("--report-only", action="store_true", help="scan+dedupe stats, do not write parquet")
    a = ap.parse_args()

    names = sorted(f for f in os.listdir(a.src) if f.endswith(".json"))
    if a.limit:
        names = names[:a.limit]
    print(f"src={a.src} files={len(names):,}")

    writer = None if a.report_only else ShardedParquetWriter(a.out, flush_rows=500_000)
    stats = {"files": 0, "raw_events": 0, "kept": 0, "dup": 0, "out_window": 0}
    cur_w = None; buf = []
    for fn in names:
        w = wallet_of(fn)
        if w != cur_w and cur_w is not None:
            if a.report_only:
                _rep_flush(buf, stats)
            else:
                flush_wallet(buf, writer, stats)
            buf = []
        cur_w = w
        try:
            with open(os.path.join(a.src, fn)) as fh:
                arr = json.load(fh)
        except Exception:
            continue
        stats["files"] += 1
        for ev in (arr or []):
            ev["_w"] = w
            buf.append(ev); stats["raw_events"] += 1
    if buf:
        if a.report_only: _rep_flush(buf, stats)
        else: flush_wallet(buf, writer, stats)

    rows = 0 if a.report_only else writer.close()
    print(f"files_read={stats['files']:,} raw_events={stats['raw_events']:,}")
    print(f"kept(unique,in-window)={stats['kept']:,} dup_dropped={stats['dup']:,} out_of_window={stats['out_window']:,}")
    if not a.report_only:
        sz = os.path.getsize(a.out) if os.path.exists(a.out) else 0
        print(f"OUT parquet={a.out} rows={rows:,} size={sz/1e6:.1f}MB")

def _rep_flush(events, stats):
    seen = set()
    for ev in events:
        t = ev.get("time")
        if t is None or not (LO <= t < HI): stats["out_window"] += 1; continue
        d = ev.get("delta", {}) or {}
        coin = d.get("coin") or d.get("token") or ""
        k = (ev.get("_w"), t, ev.get("hash"), coin)
        if k in seen: stats["dup"] += 1; continue
        seen.add(k); stats["kept"] += 1

if __name__ == "__main__":
    main()
