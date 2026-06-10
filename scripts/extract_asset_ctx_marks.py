#!/usr/bin/env python3
"""Extract HL EXACT main-dex MARK price from the S3 archive asset_ctxs -> per-coin .npy series.

Alberto 2026-06-04 "reconcile to the cent": HL computes accountValue on the MARK price, not the last
trade. asset_ctxs (s3://hyperliquid-archive/asset_ctxs/YYYYMMDD.csv.lz4, requester-pays) carries the
EXACT per-minute mark_px for every MAIN-dex coin (~229 coins, 1-min). This is the precise mark M1 must
use for main coins (exotic HIP-3 coins use setOracle -> hyperliquid_oracle, separate).

STREAMING + CRASH-SAFE (rule 8, mandatory): each day is downloaded, parsed, and FLUSHED to its own
parquet shard (app/data/v15/assetctx_marks_daily/YYYYMMDD.parquet) immediately, then freed. Only ONE
day (~330k rows) is ever in RAM during download. Resumable: existing day-shards are skipped. A separate
bounded `consolidate()` reads the shards and writes per-coin (minute_ms, mark_px) .npy for M1.get_mark.

Usage:
  python scripts/extract_asset_ctx_marks.py --start 2025-12-01 --end 2026-05-24            # download shards
  python scripts/extract_asset_ctx_marks.py --consolidate                                  # shards -> per-coin npy
  python scripts/extract_asset_ctx_marks.py --start ... --end ... --consolidate            # both
"""
import argparse, csv, io, logging, os, sys, datetime as dt, glob
from pathlib import Path
import boto3, lz4.frame, numpy as np, pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "research", "v15"))
try:
    from _streaming_io import install_memory_guard
except Exception:
    def install_memory_guard(*a, **k): pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
log = logging.getLogger("assetctx")

BUCKET = "hyperliquid-archive"
DAILY = Path("/Users/hermes/quants-lab/app/data/v15/assetctx_marks_daily")
OUT = Path("/Users/hermes/quants-lab/app/data/v15/assetctx_marks")
import urllib.parse as _ulib


def download(start: str, end: str):
    """Stream each day's mark_px to its OWN parquet shard, flush, free. Bounded to 1 day RAM. Resumable."""
    DAILY.mkdir(parents=True, exist_ok=True)
    s3 = boto3.client("s3")
    d = dt.datetime.strptime(start, "%Y-%m-%d").date()
    d1 = dt.datetime.strptime(end, "%Y-%m-%d").date()
    while d <= d1:
        shard = DAILY / f"{d.strftime('%Y%m%d')}.parquet"
        if shard.exists():
            log.info(f"  {d}: shard exists, skip"); d += dt.timedelta(days=1); continue
        key = f"asset_ctxs/{d.strftime('%Y%m%d')}.csv.lz4"
        try:
            o = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")
            data = lz4.frame.decompress(o["Body"].read()).decode("utf-8", "replace")
        except Exception as e:
            log.warning(f"  {key}: {str(e)[:60]} (skip)"); d += dt.timedelta(days=1); continue
        ts_l, coin_l, mk_l = [], [], []
        for r in csv.DictReader(io.StringIO(data)):
            mk = r.get("mark_px")
            if not mk:
                continue
            try:
                ts = int(dt.datetime.strptime(r["time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc).timestamp() * 1000)
                v = float(mk)
            except (ValueError, TypeError):
                continue
            ts_l.append(ts); coin_l.append(r["coin"]); mk_l.append(v)
        df = pd.DataFrame({"ts_ms": np.asarray(ts_l, dtype="int64"), "coin": coin_l,
                           "mark": np.asarray(mk_l, dtype="float64")})
        tmp = shard.with_suffix(".parquet.tmp")
        df.to_parquet(tmp, index=False)
        tmp.replace(shard)
        log.info(f"  {d} -> {len(df)} rows flushed to {shard.name}")
        del df, ts_l, coin_l, mk_l, data  # free immediately
        d += dt.timedelta(days=1)


def consolidate():
    """Per-coin (minute_ms, mark) .npy from day-shards. Bounded: streams shards, one pyarrow column
    read per shard; groups in a dict then writes per coin. Peak ~ full dataset arrays (~900MB-1.3GB),
    under the 12GB guard. (If ever larger: switch to per-coin pass with pyarrow dataset filters.)"""
    OUT.mkdir(parents=True, exist_ok=True)
    shards = sorted(glob.glob(str(DAILY / "*.parquet")))
    if not shards:
        log.warning("no day-shards to consolidate"); return
    acc: dict[str, list] = {}  # coin -> [df, df, ...] (chronological)
    for sh in shards:
        df = pd.read_parquet(sh, columns=["ts_ms", "coin", "mark"])
        for coin, g in df.groupby("coin"):
            acc.setdefault(coin, []).append(g[["ts_ms", "mark"]].to_numpy())
        del df
    for coin, parts in acc.items():
        m = np.vstack(parts)
        order = np.argsort(m[:, 0].astype("int64"))
        ts_a = m[:, 0].astype("int64")[order]; px_a = m[:, 1].astype("float64")[order]
        arr = np.vstack([ts_a.astype("float64"), px_a])
        p = OUT / f"{_ulib.quote(coin, safe='')}.npy"
        tmp = p.with_name(f"{p.name}.{os.getpid()}.tmp")
        with open(tmp, "wb") as fh:
            np.save(fh, arr)
        tmp.replace(p)
    log.info(f"consolidate: {len(acc)} coins -> {OUT}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start"); ap.add_argument("--end")
    ap.add_argument("--consolidate", action="store_true")
    args = ap.parse_args()
    install_memory_guard("assetctx")
    if args.start and args.end:
        download(args.start, args.end)
    if args.consolidate or not (args.start and args.end):
        consolidate()
    log.info("DONE")


if __name__ == "__main__":
    main()
