#!/usr/bin/env python3
"""Daily hot-parquet -> Mongo candle sync (Fable-approved plan 2026-08-08; findings/quant/
2026-08-07-mongo-candle-freeze-silently-truncated-selection-evidence).

Keeps `hyperliquid_candles` fed from app/data/hl_s3_candles_1m_hot so the m07 mark source can
never silently freeze again. Wired as the final step of scripts/hl_s3_fills_daily_refresh.sh.

Write contract (B1/B2 — the collection has BOTH (pair,interval,timestamp_utc) and
(coin,interval,timestamp_utc) UNIQUE indexes; the API loader (backfill_hyperliquid_history)
upserts pair-keyed with $set and MUST retain overwrite precedence):
  - every op filters on the COIN unique index;
  - amend:  UpdateOne({coin,interval,ts, source: "s3_node_fills_by_block_1m"}, {$set: OHLCV})
            -> corrects partial minutes when the hot day-files are rewritten (--rewrite-lookback-days),
            and by construction never touches API-sourced docs (source: "hl_api" / absent);
  - insert: UpdateOne(key, {$setOnInsert: doc}, upsert=True) with pair=coin_to_pair(coin) and
            source from the parquet -> first write of a minute; never overwrites anything.
Cache invalidation (B4/B5): coins with upserted+modified > 0 get their derived ohlc_cache .npy
DELETED (lazy rebuild by the next consumer's build_ohlc_cache preamble). A sidecar journal makes
this crash-safe: candidates are journaled BEFORE each bulk_write and cleared only after deletion;
leftover journals are processed at startup (over-delete acceptable, silent under-delete not).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.parse as _ulib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from pymongo import MongoClient, UpdateOne

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "data_pipeline"))
from backfill_hyperliquid_history import coin_to_pair  # noqa: E402  (B1: never hand-roll the map)

logger = logging.getLogger("hl_candles_mongo_sync")
DEFAULT_CANDLES_DIR = REPO / "app" / "data" / "hl_s3_candles_1m_hot"
OHLC_CACHE_DIR = REPO / "app" / "data" / "v15" / "ohlc_cache"
JOURNAL = OHLC_CACHE_DIR / ".mongo_sync_invalidate_journal.json"
RECON_SOURCE = "s3_node_fills_by_block_1m"
STALE_DAYS = 2
BATCH = 10_000


def _cache_path(coin: str) -> Path:
    # identical derivation to v15_m07_engine (quote(coin, safe='') — ':'/'#' coins included)
    return OHLC_CACHE_DIR / f"{_ulib.quote(coin, safe='')}.npy"


def _invalidate(coins: set[str], dry_run: bool) -> int:
    n = 0
    for c in sorted(coins):
        p = _cache_path(c)
        if p.exists():
            if not dry_run:
                p.unlink()
            n += 1
    return n


def _write_journal(coins: set) -> None:
    """codex 2026-08-10 #1: write atomically (tmp + fsync + replace). A torn journal read as an
    empty set is exactly the silent under-invalidation B5 exists to prevent."""
    JOURNAL.parent.mkdir(parents=True, exist_ok=True)
    tmp = JOURNAL.with_suffix(JOURNAL.suffix + f".{os.getpid()}.tmp")
    try:
        with open(tmp, "w") as fh:
            json.dump(sorted(coins), fh)
            fh.flush()
            os.fsync(fh.fileno())
        tmp.replace(JOURNAL)
    finally:
        # codex 2026-08-10 #1b: never leak the temp file if dump/flush/fsync/replace raises.
        # (After a successful replace the path is gone; missing_ok covers both cases.)
        tmp.unlink(missing_ok=True)


def _process_leftover_journal(dry_run: bool) -> None:
    if not JOURNAL.exists():
        return
    try:
        coins = set(json.loads(JOURNAL.read_text()))
    except Exception as exc:
        # FAIL CLOSED (codex #1): an unreadable journal means an unknown set of caches may be
        # stale. Deleting it would silently bless them. Refuse and make a human look.
        raise SystemExit(
            f"MARK-CACHE JOURNAL UNREADABLE ({JOURNAL}): {exc}. An unknown set of ohlc caches may be "
            f"stale after a crashed sync. Inspect the file, or delete the caches it covers manually "
            f"(worst case: rm the whole ohlc_cache dir, it is DERIVED and rebuilds), then remove it.")
    if coins:
        n = _invalidate(coins, dry_run)
        logger.warning("leftover journal from a crashed run: invalidated %d caches (%d coins)",
                       n, len(coins))
    if not dry_run:
        JOURNAL.unlink()


def normalize_heal(col, dry_run: bool) -> int:
    """B3 one-shot: heal-era reconstructed docs were written with pair=<coin> and NO source field.
    Server-side per-coin update_many (rides the coin index; Fable note 1). Touches pair/source
    only — never OHLCV — so no cache invalidation is needed."""
    total = 0
    for coin in col.distinct("coin"):
        # codex 2026-08-10 #2: match the heal-era shape EXACTLY (pair == coin AND no source field).
        # The old "any pair not ending -USDT" predicate could relabel a malformed/legacy API doc as
        # reconstructed, making it amendable by the S3 path and inverting API precedence.
        flt = {"coin": coin, "pair": coin, "source": {"$exists": False}}
        if dry_run:
            total += col.count_documents(flt)
            continue
        res = col.update_many(flt, {"$set": {"pair": coin_to_pair(coin), "source": RECON_SOURCE}})
        total += res.modified_count
    logger.info("normalize-heal: %s %d docs", "would touch" if dry_run else "normalized", total)
    return total


def sync(candles_dir: Path, mongo_uri: str, days: int, dry_run: bool,
         db: str = "quants_lab", collection: str = "hyperliquid_candles") -> dict:
    col = MongoClient(mongo_uri)[db][collection]
    _process_leftover_journal(dry_run)

    files = sorted(candles_dir.glob("20??????.parquet"))
    if not files:
        raise SystemExit(f"no hot candle files in {candles_dir}")
    # Staleness on the newest FILENAME date (mtime is useless — rewrites touch old files daily).
    newest = datetime.strptime(files[-1].stem, "%Y%m%d").replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - newest > timedelta(days=STALE_DAYS + 1):
        logger.error("hot candle store STALE: newest file %s is > %d days old — upstream refresh "
                     "is broken; refusing to report a healthy sync", files[-1].name, STALE_DAYS)
        raise SystemExit(2)

    touched: set[str] = set()
    n_ins = n_mod = 0
    for f in files[-days:]:
        df = pd.read_parquet(f)
        day_coins = set(df["coin"].unique())
        if not dry_run:
            _write_journal(touched | day_coins)          # B5: journal (atomically) BEFORE writes
        day_ins = day_mod = 0
        # per-coin batches (Fable note 3: BulkWriteResult counts are aggregate-only; the
        # invalidation trigger needs per-coin attribution)
        for coin, g in df.groupby("coin", sort=True):
            ops = []
            c_ins = c_mod = 0
            pair = coin_to_pair(coin)
            for r in g.itertuples(index=False):
                key = {"coin": r.coin, "interval": "1m", "timestamp_utc": int(r.timestamp_utc)}
                ohlcv = {"open": float(r.open), "high": float(r.high), "low": float(r.low),
                         "close": float(r.close), "volume": float(r.volume),
                         "n_trades": int(r.n_trades)}
                ops.append(UpdateOne({**key, "source": RECON_SOURCE}, {"$set": ohlcv}))   # amend
                ops.append(UpdateOne(key, {"$setOnInsert": {**key, "pair": pair,
                                                            "source": getattr(r, "source", RECON_SOURCE),
                                                            **ohlcv}}, upsert=True))     # insert
                if len(ops) >= BATCH:
                    if not dry_run:
                        res = col.bulk_write(ops, ordered=False)
                        c_ins += res.upserted_count; c_mod += res.modified_count
                    ops = []
            if ops and not dry_run:
                res = col.bulk_write(ops, ordered=False)
                c_ins += res.upserted_count; c_mod += res.modified_count
            # NOTE: an amend that sets identical values counts modified=0 in MongoDB -> no
            # spurious daily invalidation of every coin; only real changes/new minutes invalidate.
            if c_ins + c_mod > 0:
                touched.add(coin)
            day_ins += c_ins; day_mod += c_mod
        n_ins += day_ins; n_mod += day_mod
        logger.info("%s: %d inserted, %d amended%s", f.name, day_ins, day_mod,
                    " [DRY-RUN: 0 written]" if dry_run else "")
    n_del = _invalidate(touched, dry_run)
    if not dry_run and JOURNAL.exists():
        JOURNAL.unlink()
    logger.info("sync done: %d inserted, %d amended, %d caches invalidated (%d coins touched)%s",
                n_ins, n_mod, n_del, len(touched), " [DRY-RUN]" if dry_run else "")
    return {"inserted": n_ins, "amended": n_mod, "caches_invalidated": n_del,
            "coins_touched": len(touched)}


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--days", type=int, default=3,
                    help="how many trailing day-files to sync (match --rewrite-lookback-days)")
    ap.add_argument("--candles-dir", default=str(DEFAULT_CANDLES_DIR))
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--dry-run", action="store_true",
                    help="count everything, write/delete/journal NOTHING")
    ap.add_argument("--normalize-heal", action="store_true",
                    help="B3 one-shot: fix heal-era docs (pair=<coin>, no source) in place")
    ap.add_argument("--db", default="quants_lab")
    ap.add_argument("--collection", default="hyperliquid_candles")
    args = ap.parse_args()
    if args.normalize_heal:
        col = MongoClient(args.mongo_uri)[args.db][args.collection]
        normalize_heal(col, args.dry_run)
        return 0
    sync(Path(args.candles_dir), args.mongo_uri, args.days, args.dry_run,
         db=args.db, collection=args.collection)
    return 0


if __name__ == "__main__":
    sys.exit(main())
