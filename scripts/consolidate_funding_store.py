#!/usr/bin/env python3
"""Consolidate funding into the ONE store (Alberto TG11307, 2026-07-14).

Fills were drop-in (v2 + hot are both day-partitioned, identical schema -> hardlink).
Funding is NOT: the deep-history store `app/data/v13/funding_cache` is PER-WALLET files
`<wallet>_<t0ms>_<t1ms>.parquet` with cols (time_ms, coin, usdc_signed), while the hot store
`app/data/hl_s3_funding_hot` is DAY-partitioned `YYYYMMDD.parquet` with cols
(wallet, time, coin, usdc, ...). So this reformats the per-wallet legacy funding into the SAME
day-partitioned shape the single store (and hl_fills_io.load_wallet_funding) expects.

- Reads every funding_cache file (wallet from filename), renames time_ms->time, usdc_signed->usdc.
- Writes ONE `YYYYMMDD.parquet` per day into hl_s3_funding_hot, cols [wallet, time, coin, usdc]
  (the 4 the IO layer requires; extra hot cols are optional and absent here).
- SKIPS any day-file that ALREADY exists in hot (Jun 9+ overlap) -> never overwrites the fresh store.
- funding_cache spans Dec1..May22; hot spans Jun9..Jul13 => a known funding GAP May23..Jun8 remains
  (17 days, minor cashflow term; flagged, not backfilled here to avoid re-download).
- Reads nothing that mutates the source; DELETES nothing (Rule 7/15). Idempotent.

Memory: funding_cache is ~237MB on disk; read in file-batches into per-day buffers, flush completed
buffer set once at the end (all days known only after full scan). Peak bounded ~1-2GB; memory guard on.
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "research" / "v15"))
try:
    from _streaming_io import install_memory_guard  # type: ignore
except Exception:  # noqa: BLE001
    def install_memory_guard(*a, **k):  # fallback no-op
        return None

FC_DIR = Path("/Users/hermes/quants-lab/app/data/v13/funding_cache")
HOT_FUNDING_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_funding_hot")


def _wallet_from_name(p: str) -> str:
    return Path(p).name.split("_", 1)[0].lower()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="process only first N files (smoke)")
    args = ap.parse_args()
    install_memory_guard(soft_gb=6.0, label="funding_consolidate")

    files = sorted(glob.glob(str(FC_DIR / "*.parquet")))
    if args.limit:
        files = files[: args.limit]
    print(f"funding_cache files: {len(files)}")

    existing_days = {Path(p).stem for p in glob.glob(str(HOT_FUNDING_DIR / "*.parquet"))}
    print(f"hot funding days already present (skip): {len(existing_days)}")

    # Accumulate per-day frames. Small data; hold row lists keyed by day-string.
    per_day: dict[str, list[pd.DataFrame]] = {}
    n_rows = 0
    for i, fp in enumerate(files):
        try:
            df = pd.read_parquet(fp, columns=["time_ms", "coin", "usdc_signed"])
        except Exception as e:  # noqa: BLE001
            print(f"  skip {Path(fp).name}: {e!r}")
            continue
        if df.empty:
            continue
        df["wallet"] = _wallet_from_name(fp)
        df = df.rename(columns={"time_ms": "time", "usdc_signed": "usdc"})
        df["time"] = df["time"].astype("int64")
        df["day"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.strftime("%Y%m%d")
        for day, g in df.groupby("day"):
            if day in existing_days:
                continue  # never overwrite the fresh hot store
            per_day.setdefault(day, []).append(g[["wallet", "time", "coin", "usdc"]])
        n_rows += len(df)
        if (i + 1) % 3000 == 0:
            print(f"  scanned {i+1}/{len(files)} files, {n_rows:,} rows, {len(per_day)} days buffered")

    days = sorted(per_day)
    print(f"days to write: {len(days)}  {days[0] if days else '-'}..{days[-1] if days else '-'}")
    if args.dry_run:
        print("DRY RUN — no writes")
        return

    written = 0
    for day in days:
        out = pd.concat(per_day[day], ignore_index=True)
        # dedup identical (wallet,time,coin) funding rows (overlapping per-wallet range files)
        out = out.drop_duplicates(subset=["wallet", "time", "coin"])
        dst = HOT_FUNDING_DIR / f"{day}.parquet"
        if dst.exists():
            continue
        out.to_parquet(dst, index=False)
        written += 1
    print(f"written day-files: {written}")
    allf = sorted(glob.glob(str(HOT_FUNDING_DIR / "*.parquet")))
    stems = [Path(p).stem for p in allf]
    print(f"consolidated funding store: {len(allf)} days  {stems[0]}..{stems[-1]}")


if __name__ == "__main__":
    main()
