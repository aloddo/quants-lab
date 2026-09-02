#!/usr/bin/env python3
"""Daily HL FUNDING + LEDGER refresh for the 20K-wallet universe, from the S3 node archive.

Sibling to hl_s3_fills_daily_refresh.py (fills + 1m candles). Alberto 2026-07-09: "revive funding and
ledger from Hyperliquid, at least for those 20K wallets, in the SAME pipeline and cron as candles and fills."

Source: s3://hl-mainnet-node-data/misc_events_by_block/hourly/YYYYMMDD/H.lz4 (requester-pays, per-hour LZ4
JSONL of block events). Reuses the battle-tested parser research/v13/v13_s3_ledger_downloader.stream_one_hour
(LedgerUpdate.users[] + Funding.deltas[]) and the fills-pipeline orchestration helpers (wallet load, day
plan, manifest, disk summary) -- no duplication, no live API (no 429 co-tenant risk), Rule-8 streaming
(each hour is decompressed in memory, parsed, discarded; only per-day filtered rows are held).

Outputs (per day, atomic .tmp->rename):
  app/data/hl_s3_funding_hot/{YYYYMMDD}.parquet  cols: wallet,time,coin,usdc,szi,fundingRate,nSamples,hash,source
  app/data/hl_s3_ledger_hot/{YYYYMMDD}.parquet   cols: wallet,time,type,usdc,delta_json,hash,source
Manifest: app/data/hl_s3_misc_hot_manifest.json
"""
from __future__ import annotations
import argparse, json, logging, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas as pd
from botocore.config import Config as BotoConfig

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "data_pipeline"))
sys.path.insert(0, str(REPO / "research" / "v13"))

# Reuse fills-pipeline orchestration (single source of truth for wallet load / day plan / manifest).
from hl_s3_fills_daily_refresh import (  # noqa: E402
    load_wallets, plan_days, day_id, iso_now, load_manifest, save_manifest, disk_summary,
    existing_latest_day, parse_day,
)
# Reuse the battle-tested misc-events fetch+parse (explicit ok/error status per hour) + Funding/Ledger shapes.
from v13_s3_ledger_downloader import _fetch_and_parse_hour, BUCKET, PREFIX  # noqa: E402
sys.path.insert(0, str(REPO / "research" / "v15"))
try:
    from _streaming_io import install_memory_guard  # noqa: E402
except Exception:  # keep the cron robust if the helper moves
    def install_memory_guard(*a, **k):  # type: ignore
        return None


def _is_missing_404(err: str) -> bool:
    """A genuinely-absent hour object (vs a real outage) -- treat as 'not published', not a day error."""
    e = (err or "").lower()
    return "nosuchkey" in e or "404" in e or "notfound" in e

LOG = logging.getLogger("hl_s3_misc_daily")

DEFAULT_FUNDING_OUT = REPO / "app" / "data" / "hl_s3_funding_hot"
DEFAULT_LEDGER_OUT = REPO / "app" / "data" / "hl_s3_ledger_hot"
DEFAULT_MANIFEST = REPO / "app" / "data" / "hl_s3_misc_hot_manifest.json"

FUNDING_COLUMNS = ["wallet", "time", "coin", "usdc", "szi", "fundingRate", "nSamples", "hash", "source"]
LEDGER_COLUMNS = ["wallet", "time", "type", "usdc", "delta_json", "hash", "source"]


def day_bounds_ms(d: date) -> tuple[int, int]:
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000) - 1


def _funding_row(u_lc: str, entry: dict) -> dict[str, Any]:
    delta = entry.get("delta", {}) or {}
    return {
        "wallet": u_lc, "time": int(entry["time"]), "coin": delta.get("coin", ""),
        "usdc": str(delta.get("usdc", "0")), "szi": str(delta.get("szi", "0")),
        "fundingRate": str(delta.get("fundingRate", "0")), "nSamples": int(delta.get("nSamples", 1) or 1),
        "hash": entry.get("hash", ""), "source": "s3_misc_events_by_block_funding",
    }


def _ledger_row(u_lc: str, entry: dict) -> dict[str, Any]:
    delta = entry.get("delta", {}) or {}
    return {
        "wallet": u_lc, "time": int(entry["time"]),
        "type": (delta.get("type", "") if isinstance(delta, dict) else ""),
        "usdc": str(delta.get("usdc", "")) if isinstance(delta, dict) else "",
        "delta_json": json.dumps(delta, separators=(",", ":"), default=str),
        "hash": entry.get("hash", ""), "source": "s3_misc_events_by_block_ledger",
    }


def fetch_hour(s3, day: str, hour: int, wallets_lc: set[str], lo_ms: int, hi_ms: int):
    """Fetch+parse one misc-events hour with EXPLICIT status (codex P1: don't conflate outage with empty).
    Returns (funding_rows, ledger_rows, ok, missing404, err). ok=True => the S3 object existed and parsed."""
    ledger, funding, _dl, ok, err = _fetch_and_parse_hour(s3, day, hour, wallets_lc, lo_ms, hi_ms, ledger_only=False)
    if not ok:
        return [], [], False, _is_missing_404(err), err
    funding_rows = [_funding_row(u, e) for u, e in funding]
    ledger_rows = [_ledger_row(u, e) for u, e in ledger]
    return funding_rows, ledger_rows, True, False, None


def _dedup(rows: list[dict[str, Any]], key_cols: list[str]) -> list[dict[str, Any]]:
    """Codex P2: guard against the same block/event appearing in adjacent hourly files."""
    seen: set = set()
    out: list[dict[str, Any]] = []
    for r in rows:
        k = tuple(r.get(c) for c in key_cols)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def _write_day(out_dir: Path, day: str, rows: list[dict[str, Any]], cols: list[str],
               hours_ok: int, dry_run: bool) -> dict[str, Any]:
    out = out_dir / f"{day}.parquet"
    if hours_ok == 0:
        # No S3 object existed for any hour -> genuinely not published yet (NOT an outage; outages raise upstream).
        return {"status": "missing", "rows": 0, "path": str(out), "written": False}
    # hours_ok > 0: the day exists. Write parquet even if 0 matching rows (distinct from 'missing').
    df = pd.DataFrame(rows, columns=cols) if rows else pd.DataFrame(columns=cols)
    if not df.empty:
        df = df.sort_values(["wallet", "time"], kind="mergesort")
    if dry_run:
        return {"status": "dry_run", "rows": int(len(df)), "path": str(out), "written": False}
    out_dir.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False, compression="snappy")
    tmp.replace(out)
    return {"status": "ok", "rows": int(len(df)), "path": str(out), "written": True}


def refresh_day(s3, d: date, wallets_lc: set[str], args: argparse.Namespace) -> dict[str, Any]:
    ds = day_id(d)
    lo_ms, hi_ms = day_bounds_ms(d)
    t0 = time.time()
    funding_rows: list[dict[str, Any]] = []
    ledger_rows: list[dict[str, Any]] = []
    hours_ok = hours_missing = 0
    real_errors: list[str] = []
    with ThreadPoolExecutor(max_workers=args.n_workers) as ex:
        futures = {ex.submit(fetch_hour, s3, ds, h, wallets_lc, lo_ms, hi_ms): h for h in range(24)}
        for fut in as_completed(futures):
            fr, lr, ok, missing404, err = fut.result()
            if ok:
                hours_ok += 1
                funding_rows.extend(fr)
                ledger_rows.extend(lr)
            elif missing404:
                hours_missing += 1
            else:
                real_errors.append(err or "unknown")
    # A real outage/permission/read error must NEVER produce or overwrite a daily partition, even
    # when the other 23 hours succeeded. Likewise, a partly published day is incomplete. Preserve a
    # prior complete rewrite-lookback file and retry the whole day on the next run.
    if real_errors:
        raise RuntimeError(
            f"{ds}: {len(real_errors)} hour(s) failed with non-404 errors "
            f"({hours_ok}/24 ok): {real_errors[:3]}"
        )
    if 0 < hours_ok < 24:
        raise RuntimeError(
            f"{ds}: incomplete S3 misc day ({hours_ok}/24 hourly objects, "
            f"{hours_missing} missing); refusing to write partial funding/ledger partitions"
        )
    funding_rows = _dedup(funding_rows, ["wallet", "time", "hash", "coin", "usdc", "szi", "fundingRate"])
    ledger_rows = _dedup(ledger_rows, ["wallet", "time", "hash", "delta_json"])
    fw = _write_day(Path(args.funding_out_dir), ds, funding_rows, FUNDING_COLUMNS, hours_ok, args.dry_run)
    lw = _write_day(Path(args.ledger_out_dir), ds, ledger_rows, LEDGER_COLUMNS, hours_ok, args.dry_run)
    return {
        "day": ds, "status": fw["status"], "funding_status": fw["status"], "ledger_status": lw["status"],
        "funding_rows": fw["rows"], "ledger_rows": lw["rows"],
        "hours_ok": hours_ok, "hours_missing": hours_missing, "hours_error": len(real_errors),
        "wallets_filter_n": len(wallets_lc),
        "funding_path": fw["path"], "ledger_path": lw["path"],
        "written": fw["written"], "updated_utc": iso_now(), "elapsed_s": time.time() - t0,
    }


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                        stream=sys.stdout, force=True)  # codex P2: override the parser module's import-time basicConfig
    install_memory_guard(soft_gb=8.0, label="hl_s3_misc_daily")  # codex P1: abort LOUD, not silent SIGKILL (Rule 8)
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallet-source", action="append", default=[])
    ap.add_argument("--funding-out-dir", default=str(DEFAULT_FUNDING_OUT))
    ap.add_argument("--ledger-out-dir", default=str(DEFAULT_LEDGER_OUT))
    ap.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    ap.add_argument("--start", help="YYYY-MM-DD")
    ap.add_argument("--end", help="YYYY-MM-DD. Default: today UTC minus publish lag")
    ap.add_argument("--publish-lag-days", type=int, default=1)
    ap.add_argument("--rewrite-lookback-days", type=int, default=3)
    ap.add_argument("--bootstrap-days", type=int, default=14)
    ap.add_argument("--n-workers", type=int, default=3)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-prune", action="store_true", help="accepted for cron symmetry; this job never prunes")
    args = ap.parse_args()

    if not args.wallet_source:
        args.wallet_source = ["app/data/v13/equity_universe_20k.parquet", "config/copy_trader_wallets_gate1_v4.json"]

    # Codex P1: plan off the MORE-BEHIND of the two stores so ledger can never be permanently skipped when
    # funding is current (plan_days keys off existing_latest_day(args.out_dir)).
    if args.start:
        args.out_dir = args.funding_out_dir
    else:
        lf = existing_latest_day(Path(args.funding_out_dir))
        ll = existing_latest_day(Path(args.ledger_out_dir))
        if lf and ll:
            args.out_dir = args.funding_out_dir if lf <= ll else args.ledger_out_dir
        else:
            args.out_dir = args.funding_out_dir  # bootstrap: at least one dir empty -> full bootstrap window
    wallets_lc = load_wallets(args.wallet_source)
    days = plan_days(args)
    LOG.info("wallets=%d days=%s funding_out=%s ledger_out=%s dry_run=%s",
             len(wallets_lc), ",".join(day_id(d) for d in days), args.funding_out_dir, args.ledger_out_dir, args.dry_run)
    if not days:
        LOG.info("nothing to do")
        return 0

    s3 = boto3.client("s3", region_name="us-east-1", config=BotoConfig(
        connect_timeout=10, read_timeout=45, retries={"max_attempts": 6, "mode": "adaptive"},
        max_pool_connections=max(8, args.n_workers + 2)))

    manifest_path = Path(args.manifest)
    manifest = load_manifest(manifest_path)
    manifest["last_run_started_utc"] = iso_now()
    manifest["funding_out_dir"] = str(Path(args.funding_out_dir))
    manifest["ledger_out_dir"] = str(Path(args.ledger_out_dir))
    manifest["wallet_sources"] = args.wallet_source
    manifest["wallets_filter_n"] = len(wallets_lc)

    failed = 0
    for d in days:
        try:
            row = refresh_day(s3, d, wallets_lc, args)
            manifest["days"][row["day"]] = row
            LOG.info("%s status=%s funding=%s ledger=%s hours_ok=%s missing=%s err=%s elapsed=%.1fs",
                     row["day"], row["status"], row["funding_rows"], row["ledger_rows"],
                     row["hours_ok"], row["hours_missing"], row["hours_error"], row["elapsed_s"])
            if row["status"] not in {"ok", "dry_run"}:
                failed += 1
                LOG.error("planned day incomplete: %s status=%s", row["day"], row["status"])
        except Exception as exc:
            failed += 1
            ds = day_id(d)
            manifest["days"][ds] = {"day": ds, "status": "error", "error": str(exc), "updated_utc": iso_now()}
            LOG.exception("day failed: %s", ds)
        save_manifest(manifest_path, manifest)

    manifest["last_run_finished_utc"] = iso_now()
    manifest["last_run_failed_days"] = failed
    manifest["funding_disk"] = disk_summary(Path(args.funding_out_dir))
    manifest["ledger_disk"] = disk_summary(Path(args.ledger_out_dir))
    save_manifest(manifest_path, manifest)
    LOG.info("funding_disk=%s ledger_disk=%s", manifest["funding_disk"], manifest["ledger_disk"])
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
