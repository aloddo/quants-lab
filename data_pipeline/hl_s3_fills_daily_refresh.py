#!/usr/bin/env python3
"""Daily incremental Hyperliquid S3 fills -> filtered parquet refresh.

This maintains a bounded "hot" fills store for research and leader audits:

  app/data/hl_s3_fills_v2_hot/{YYYYMMDD}.parquet

It can also reconstruct all-market 1m candles from the same S3 download pass:

  app/data/hl_s3_candles_1m_hot/{YYYYMMDD}.parquet

Design:
  - requester-pays S3 source, no Hyperliquid REST calls
  - filters on a configured wallet universe while streaming each hour
  - aggregates candles from the full all-market stream before wallet filtering
  - never persists raw .lz4 objects
  - rewrites a small recent lookback window to catch late S3 publication
  - optional retention is available but disabled by default
  - manifest records availability, row counts, bytes downloaded, and freshness

The older historical archive under app/data/hl_s3_fills_v2 is left untouched.
"""
from __future__ import annotations

import argparse
import json
import logging
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import lz4.frame
import pandas as pd
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

LOG = logging.getLogger("hl_s3_daily")

REPO = Path(__file__).resolve().parents[1]
BUCKET = "hl-mainnet-node-data"
PREFIX = "node_fills_by_block/hourly"
DEFAULT_OUT = REPO / "app" / "data" / "hl_s3_fills_v2_hot"
DEFAULT_CANDLES_OUT = REPO / "app" / "data" / "hl_s3_candles_1m_hot"
DEFAULT_MANIFEST = REPO / "app" / "data" / "hl_s3_fills_v2_hot_manifest.json"

PARQUET_COLUMNS = [
    "wallet",
    "coin",
    "side",
    "size",
    "price",
    "time",
    "dir",
    "closedPnl",
    "startPosition",
    "fee",
    "feeToken",
    "builderFee",
    "deployerFee",
    "crossed",
    "hash",
    "oid",
    "tid",
    "cloid",
    "twapId",
    "builder",
    "notional",
    "source",
]

CANDLE_COLUMNS = [
    "coin",
    "timestamp_utc",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "n_trades",
    "source",
]


def parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def day_id(d: date) -> str:
    return d.strftime("%Y%m%d")


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "days": {}}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {"version": 1, "days": {}}
    data.setdefault("version", 1)
    data.setdefault("days", {})
    return data


def save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


def load_wallets(paths: list[str]) -> set[str]:
    wallets: set[str] = set()
    for raw in paths:
        path = Path(raw)
        if not path.is_absolute():
            path = REPO / path
        if not path.exists():
            LOG.warning("wallet source missing: %s", path)
            continue
        if path.suffix == ".parquet":
            df = pd.read_parquet(path, columns=["wallet"])
            vals = df["wallet"].dropna().astype(str)
        elif path.suffix == ".csv":
            df = pd.read_csv(path, usecols=["wallet"])
            vals = df["wallet"].dropna().astype(str)
        elif path.suffix == ".json":
            vals = pd.Series(wallets_from_json(path))
        else:
            vals = pd.Series([x.strip() for x in path.read_text().splitlines()])
        wallets.update(w.lower() for w in vals if isinstance(w, str) and w.lower().startswith("0x"))
    if not wallets:
        raise SystemExit("no wallets loaded; provide at least one valid --wallet-source")
    return wallets


def wallets_from_json(path: Path) -> list[str]:
    def walk(x: Any) -> list[str]:
        out: list[str] = []
        if isinstance(x, dict):
            for k, v in x.items():
                if k.lower() in {"wallet", "address", "leader", "user"} and isinstance(v, str):
                    out.append(v)
                out.extend(walk(v))
        elif isinstance(x, list):
            for v in x:
                out.extend(walk(v))
        elif isinstance(x, str) and x.lower().startswith("0x") and len(x) >= 40:
            out.append(x)
        return out

    return walk(json.loads(path.read_text()))


def existing_latest_day(out_dir: Path) -> date | None:
    days = []
    for p in out_dir.glob("20??????.parquet"):
        try:
            days.append(datetime.strptime(p.stem, "%Y%m%d").date())
        except ValueError:
            continue
    return max(days) if days else None


def plan_days(args: argparse.Namespace) -> list[date]:
    today_utc = datetime.now(timezone.utc).date()
    default_end = today_utc - timedelta(days=args.publish_lag_days)
    end = parse_day(args.end) if args.end else default_end

    if args.start:
        start = parse_day(args.start)
    else:
        latest = existing_latest_day(Path(args.out_dir))
        if latest:
            start = latest - timedelta(days=max(args.rewrite_lookback_days - 1, 0))
        else:
            start = end - timedelta(days=args.bootstrap_days - 1)

    if start > end:
        return []
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def _update_candle_bucket(
    buckets: dict[tuple[str, int], dict[str, Any]],
    *,
    coin: str,
    minute_ms: int,
    price: float,
    size: float,
    order_key: tuple[int, int],
) -> None:
    key = (coin, minute_ms)
    b = buckets.get(key)
    if b is None:
        buckets[key] = {
            "coin": coin,
            "timestamp_utc": minute_ms,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": size,
            "n_trades": 1,
            "_open_key": order_key,
            "_close_key": order_key,
        }
        return
    if order_key < b["_open_key"]:
        b["open"] = price
        b["_open_key"] = order_key
    if order_key >= b["_close_key"]:
        b["close"] = price
        b["_close_key"] = order_key
    b["high"] = max(float(b["high"]), price)
    b["low"] = min(float(b["low"]), price)
    b["volume"] = float(b["volume"]) + size
    b["n_trades"] = int(b["n_trades"]) + 1


def fetch_hour(
    s3,
    day: str,
    hour: int,
    wallets_lc: set[str],
    build_candles: bool,
) -> tuple[list[dict[str, Any]], dict[tuple[str, int], dict[str, Any]], int, bool]:
    key = f"{PREFIX}/{day}/{hour}.lz4"
    try:
        resp = s3.get_object(Bucket=BUCKET, Key=key, RequestPayer="requester")
        raw = resp["Body"].read()
    except ClientError as exc:
        code = (exc.response or {}).get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return [], {}, 0, False
        raise

    fills: list[dict[str, Any]] = []
    candle_buckets: dict[tuple[str, int], dict[str, Any]] = {}
    data = lz4.frame.decompress(raw)
    for line in data.split(b"\n"):
        if not line:
            continue
        try:
            block = json.loads(line)
        except Exception:
            continue
        for ev in block.get("events", []):
            if not isinstance(ev, list) or len(ev) != 2:
                continue
            wallet, fd = ev
            if not isinstance(wallet, str) or not isinstance(fd, dict):
                continue
            if build_candles and fd.get("side") == "B":
                try:
                    coin = str(fd.get("coin", ""))
                    price = float(fd.get("px", 0) or 0)
                    size = float(fd.get("sz", 0) or 0)
                    time_ms = int(fd.get("time", 0) or 0)
                    if coin and price > 0 and size > 0 and time_ms > 0:
                        minute_ms = (time_ms // 60_000) * 60_000
                        tid = int(fd.get("tid", 0) or 0)
                        _update_candle_bucket(
                            candle_buckets,
                            coin=coin,
                            minute_ms=minute_ms,
                            price=price,
                            size=size,
                            order_key=(time_ms, tid),
                        )
                except Exception:
                    pass
            w_lc = wallet.lower()
            if w_lc not in wallets_lc:
                continue
            try:
                size = fd.get("sz", "0")
                price = fd.get("px", "0")
                fills.append(
                    {
                        "wallet": w_lc,
                        "coin": fd.get("coin", ""),
                        "side": fd.get("side", ""),
                        "size": size,
                        "price": price,
                        "time": int(fd.get("time", 0)),
                        "dir": fd.get("dir", ""),
                        "closedPnl": fd.get("closedPnl", "0"),
                        "startPosition": fd.get("startPosition", "0"),
                        "fee": fd.get("fee", "0"),
                        "feeToken": fd.get("feeToken", ""),
                        "builderFee": fd.get("builderFee", "0"),
                        "deployerFee": fd.get("deployerFee", "0"),
                        "crossed": bool(fd.get("crossed", False)),
                        "hash": fd.get("hash", ""),
                        "oid": int(fd.get("oid", 0)),
                        "tid": int(fd.get("tid", 0)),
                        "cloid": fd.get("cloid", "") or "",
                        "twapId": fd.get("twapId") if fd.get("twapId") is not None else 0,
                        "builder": fd.get("builder", "") or "",
                    }
                )
            except Exception:
                continue
    return fills, candle_buckets, len(raw), True


def merge_candle_buckets(
    dst: dict[tuple[str, int], dict[str, Any]],
    src: dict[tuple[str, int], dict[str, Any]],
) -> None:
    for key, s in src.items():
        d = dst.get(key)
        if d is None:
            dst[key] = dict(s)
            continue
        if s["_open_key"] < d["_open_key"]:
            d["open"] = s["open"]
            d["_open_key"] = s["_open_key"]
        if s["_close_key"] >= d["_close_key"]:
            d["close"] = s["close"]
            d["_close_key"] = s["_close_key"]
        d["high"] = max(float(d["high"]), float(s["high"]))
        d["low"] = min(float(d["low"]), float(s["low"]))
        d["volume"] = float(d["volume"]) + float(s["volume"])
        d["n_trades"] = int(d["n_trades"]) + int(s["n_trades"])


def write_day(out_dir: Path, day: str, rows: list[dict[str, Any]], hours_found: int, dry_run: bool) -> dict[str, Any]:
    out = out_dir / f"{day}.parquet"
    if hours_found == 0:
        return {"status": "missing", "rows": 0, "path": str(out), "written": False}

    if rows:
        df = pd.DataFrame(rows)
        df["notional"] = df["size"].astype(float) * df["price"].astype(float)
        df["source"] = "s3_node_fills_by_block_v2_hot"
        df = df[PARQUET_COLUMNS].sort_values(["wallet", "time", "tid"], kind="mergesort")
    else:
        df = pd.DataFrame(columns=PARQUET_COLUMNS)

    if dry_run:
        return {"status": "dry_run", "rows": int(len(df)), "path": str(out), "written": False}

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    tmp.replace(out)
    return {"status": "ok", "rows": int(len(df)), "path": str(out), "written": True}


def write_candles_day(
    out_dir: Path,
    day: str,
    buckets: dict[tuple[str, int], dict[str, Any]],
    hours_found: int,
    dry_run: bool,
) -> dict[str, Any]:
    out = out_dir / f"{day}.parquet"
    if hours_found == 0:
        return {"status": "missing", "rows": 0, "path": str(out), "written": False}

    rows = []
    for b in buckets.values():
        rows.append(
            {
                "coin": b["coin"],
                "timestamp_utc": int(b["timestamp_utc"]),
                "open": float(b["open"]),
                "high": float(b["high"]),
                "low": float(b["low"]),
                "close": float(b["close"]),
                "volume": float(b["volume"]),
                "n_trades": int(b["n_trades"]),
                "source": "s3_node_fills_by_block_1m",
            }
        )
    df = pd.DataFrame(rows, columns=CANDLE_COLUMNS)
    if not df.empty:
        df = df.sort_values(["coin", "timestamp_utc"], kind="mergesort")

    if dry_run:
        return {"status": "dry_run", "rows": int(len(df)), "path": str(out), "written": False}

    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    tmp.replace(out)
    return {"status": "ok", "rows": int(len(df)), "path": str(out), "written": True}


def refresh_day(s3, d: date, wallets_lc: set[str], args: argparse.Namespace) -> dict[str, Any]:
    ds = day_id(d)
    t0 = time.time()
    rows: list[dict[str, Any]] = []
    candle_buckets: dict[tuple[str, int], dict[str, Any]] = {}
    bytes_downloaded = 0
    hours_found = 0

    with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
        futures = {
            ex.submit(fetch_hour, s3, ds, h, wallets_lc, not args.no_candles): h
            for h in range(24)
        }
        for fut in as_completed(futures):
            fills, hour_candles, n_bytes, found = fut.result()
            rows.extend(fills)
            merge_candle_buckets(candle_buckets, hour_candles)
            bytes_downloaded += n_bytes
            hours_found += int(found)

    # A partially published/read day is not a valid daily partition. In particular, do not
    # atomically replace an already-complete rewrite-lookback file with a 23-hour retry. A wholly
    # absent day remains distinguishable as `missing` below, but the caller treats that planned day
    # as an incomplete run and exits non-zero.
    if 0 < hours_found < 24:
        raise RuntimeError(
            f"{ds}: incomplete S3 fills day ({hours_found}/24 hourly objects); "
            "refusing to write a partial fills/candles partition"
        )

    write = write_day(Path(args.out_dir), ds, rows, hours_found, args.dry_run)
    candles = (
        {"status": "disabled", "rows": 0, "path": None, "written": False}
        if args.no_candles
        else write_candles_day(Path(args.candles_out_dir), ds, candle_buckets, hours_found, args.dry_run)
    )
    elapsed = time.time() - t0
    return {
        "day": ds,
        "status": write["status"],
        "rows": write["rows"],
        "candle_status": candles["status"],
        "candle_rows": candles["rows"],
        "candle_path": candles["path"],
        "hours_found": hours_found,
        "bytes_downloaded": bytes_downloaded,
        "wallets_filter_n": len(wallets_lc),
        "path": write["path"],
        "written": write["written"],
        "updated_utc": iso_now(),
        "elapsed_s": elapsed,
    }


def prune_hot_store(out_dir: Path, manifest: dict[str, Any], retention_days: int, dry_run: bool) -> list[str]:
    if retention_days <= 0:
        return []
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=retention_days)
    removed = []
    for p in out_dir.glob("20??????.parquet"):
        try:
            d = datetime.strptime(p.stem, "%Y%m%d").date()
        except ValueError:
            continue
        if d >= cutoff:
            continue
        removed.append(str(p))
        if not dry_run:
            p.unlink()
            manifest.get("days", {}).pop(p.stem, None)
    return removed


def disk_summary(out_dir: Path) -> dict[str, Any]:
    files = sorted(out_dir.glob("20??????.parquet"))
    size = sum(p.stat().st_size for p in files)
    return {
        "files": len(files),
        "size_gb": size / 1e9,
        "first_day": files[0].stem if files else None,
        "last_day": files[-1].stem if files else None,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", stream=sys.stdout)
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet-source", action="append", default=[], help="parquet/csv/txt/json with wallet column or addresses")
    ap.add_argument("--out-dir", default=str(DEFAULT_OUT))
    ap.add_argument("--candles-out-dir", default=str(DEFAULT_CANDLES_OUT))
    ap.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    ap.add_argument("--start", help="YYYY-MM-DD. Default: latest existing minus lookback, else bootstrap window")
    ap.add_argument("--end", help="YYYY-MM-DD. Default: today UTC minus publish lag")
    ap.add_argument("--publish-lag-days", type=int, default=1)
    ap.add_argument("--rewrite-lookback-days", type=int, default=3)
    ap.add_argument("--bootstrap-days", type=int, default=14)
    ap.add_argument("--retention-days", type=int, default=0, help="0 disables pruning; never deletes old data by default")
    ap.add_argument("--n-workers", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-prune", action="store_true")
    ap.add_argument("--no-candles", action="store_true", help="disable all-market 1m candle reconstruction")
    args = ap.parse_args()

    if not args.wallet_source:
        args.wallet_source = [
            "app/data/v13/equity_universe_20k.parquet",
            "config/copy_trader_wallets_gate1_v4.json",
        ]

    wallets_lc = load_wallets(args.wallet_source)
    days = plan_days(args)
    LOG.info("wallets=%d days=%s out=%s dry_run=%s", len(wallets_lc), ",".join(day_id(d) for d in days), args.out_dir, args.dry_run)
    if not days:
        LOG.info("nothing to do")
        return 0

    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        config=BotoConfig(
            connect_timeout=10,
            read_timeout=45,
            retries={"max_attempts": 6, "mode": "adaptive"},
            max_pool_connections=max(8, args.n_workers + 2),
        ),
    )

    manifest_path = Path(args.manifest)
    manifest = load_manifest(manifest_path)
    manifest["last_run_started_utc"] = iso_now()
    manifest["out_dir"] = str(Path(args.out_dir))
    manifest["candles_out_dir"] = str(Path(args.candles_out_dir))
    manifest["wallet_sources"] = args.wallet_source
    manifest["wallets_filter_n"] = len(wallets_lc)

    failed = 0
    for d in days:
        try:
            row = refresh_day(s3, d, wallets_lc, args)
            manifest["days"][row["day"]] = row
            LOG.info(
                "%s status=%s rows=%s candles=%s hours=%s dl=%.2fMB elapsed=%.1fs",
                row["day"],
                row["status"],
                row["rows"],
                row["candle_rows"],
                row["hours_found"],
                row["bytes_downloaded"] / 1e6,
                row["elapsed_s"],
            )
            if row["status"] not in {"ok", "dry_run"}:
                failed += 1
                LOG.error("planned day incomplete: %s status=%s", row["day"], row["status"])
        except Exception as exc:
            failed += 1
            ds = day_id(d)
            manifest["days"][ds] = {
                "day": ds,
                "status": "error",
                "error": str(exc),
                "updated_utc": iso_now(),
            }
            LOG.exception("day failed: %s", ds)
        save_manifest(manifest_path, manifest)

    pruned = [] if args.no_prune else prune_hot_store(Path(args.out_dir), manifest, args.retention_days, args.dry_run)
    manifest["last_run_finished_utc"] = iso_now()
    manifest["last_run_failed_days"] = failed
    manifest["last_pruned_files"] = pruned
    manifest["disk"] = disk_summary(Path(args.out_dir))
    manifest["candles_disk"] = disk_summary(Path(args.candles_out_dir))
    save_manifest(manifest_path, manifest)

    if pruned:
        LOG.info("pruned hot-store files: %d", len(pruned))
    LOG.info("disk=%s", manifest["disk"])
    if not args.no_candles:
        LOG.info("candles_disk=%s", manifest["candles_disk"])
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
