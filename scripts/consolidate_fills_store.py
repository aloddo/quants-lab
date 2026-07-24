#!/usr/bin/env python3
"""Consolidate fills into ONE store (Alberto TG11307, 2026-07-14).

The deep-history v2 store (app/data/hl_s3_fills_v2, day-partitioned, 2025-07-27..2026-06-25)
and the hot store (app/data/hl_s3_fills_v2_hot, 2026-06-09..today) are byte-schema-identical
day-partitioned parquet. This makes the HOT dir the single canonical store by HARDLINKING every
v2 day-file that is NOT already present in hot (i.e. the non-overlap history 2025-07-27..2026-06-08).

- Hardlink (same filesystem, same inode): zero extra disk, instant, no re-download.
- Overlap days (06-09..06-25) already exist in hot -> KEPT AS-IS (hot is the refreshed canonical
  version); v2's copy is NOT linked over them. Downstream dedup (order_wallet_fills_causally tid/row)
  would make it safe either way, but we simply skip to avoid touching existing files.
- v2 stays fully intact as an archive (RAW DATA NEVER DELETES, Rule 7/15). This script only ADDS
  hardlinks into hot; it deletes nothing.

Idempotent: re-running skips days already linked. Verifies date continuity + counts at the end.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

V2 = Path("/Users/hermes/quants-lab/app/data/hl_s3_fills_v2")
HOT = Path("/Users/hermes/quants-lab/app/data/hl_s3_fills_v2_hot")


def _days(d: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for p in d.glob("*.parquet"):
        s = p.stem
        if len(s) == 8 and s.isdigit():
            out[s] = p
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    v2 = _days(V2)
    hot = _days(HOT)
    print(f"v2:  {len(v2)} days  {min(v2) if v2 else '-'}..{max(v2) if v2 else '-'}")
    print(f"hot: {len(hot)} days  {min(hot) if hot else '-'}..{max(hot) if hot else '-'}")

    to_link = sorted(set(v2) - set(hot))
    print(f"to link (v2 days absent from hot): {len(to_link)}  "
          f"{to_link[0] if to_link else '-'}..{to_link[-1] if to_link else '-'}")

    linked = 0
    for day in to_link:
        src = v2[day]
        dst = HOT / src.name
        if dst.exists():
            continue
        if args.dry_run:
            linked += 1
            continue
        os.link(src, dst)  # hardlink; same FS, same inode
        linked += 1
    print(f"{'WOULD link' if args.dry_run else 'linked'}: {linked}")

    if not args.dry_run:
        merged = _days(HOT)
        ds = sorted(merged)
        # continuity check
        from datetime import datetime, timedelta
        gaps = []
        for a, b in zip(ds, ds[1:]):
            da = datetime.strptime(a, "%Y%m%d")
            db = datetime.strptime(b, "%Y%m%d")
            if (db - da) != timedelta(days=1):
                gaps.append((a, b, (db - da).days - 1))
        print(f"consolidated store: {len(merged)} days  {ds[0]}..{ds[-1]}")
        if gaps:
            print(f"WARNING: {len(gaps)} date gap(s): {gaps[:10]}")
        else:
            print("continuity: OK (no gaps)")


if __name__ == "__main__":
    main()
