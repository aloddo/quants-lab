#!/usr/bin/env python3
"""ONE-TIME fills wallet-shard via DuckDB: read the 11GB day-partitioned hot fills store ONCE and re-partition
by wallet, so M2 reads each wallet's fills as a direct fast lookup instead of re-scanning all 252 day-files
(11GB) per batch -- the O(store) re-read that made full-universe M2 crawl (0 batches in 44min) because the
11GB store does not fit the ~6GB page cache on the fleet-loaded box.

DuckDB does the 18k-way partition OUT-OF-CORE with a hard memory_limit (spills to disk, cannot OOM) + threads
(uses CPU) -- exactly "push RAM+CPU with a safeguard". Also run WRAPPED in mem_safe_run as a second backstop.

Byte-identity: this only re-partitions raw rows + applies the SAME time-window filter as load_wallet_fills;
the sharded LOADER applies the identical _normalize_fills_df + order_wallet_fills_causally, so per-wallet output
is identical (validated by gate before M2 uses it).

  scripts/mem_safe_run.sh --floor-gb 6 --label fillshard -- python scripts/build_fills_wallet_shard.py \
      --start 2025-12-01 --end 2026-05-23 --mem-limit-gb 4 --threads 4
"""
import argparse, os, shutil, sys, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "research" / "v15"))
import duckdb
import pyarrow.parquet as pq
import hl_fills_io as fio

OUT = Path(__file__).resolve().parents[1] / "app" / "data" / "v15" / "m2_fills_wallet_shards"


def _day_ms(d: str) -> int:
    import pandas as pd
    return int(pd.Timestamp(f"{d[:4]}-{d[4:6]}-{d[6:]}", tz="UTC").timestamp() * 1000)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--mem-limit-gb", type=float, default=4.0)
    ap.add_argument("--threads", type=int, default=4)
    a = ap.parse_args()
    t0 = _day_ms(a.start.replace("-", "")); t1 = _day_ms(a.end.replace("-", "")) + 86_400_000 - 1

    # columns present across the store (union), intersected with the M2 core fill columns
    sample = sorted(__import__("glob").glob(str(fio.HOT_FILLS_DIR / "*.parquet")))[-1]
    avail = set(pq.ParquetFile(sample).schema_arrow.names)
    cols = [c for c in fio._FILLS_COLS if c in avail]
    collist = ", ".join(f'"{c}"' for c in cols)

    tmp = OUT.with_name(OUT.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    t = time.time()
    spill = str(OUT.parent / ".duckdb_spill")
    shutil.rmtree(spill, ignore_errors=True); os.makedirs(spill, exist_ok=True)
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{a.mem_limit_gb}GB'")   # HARD cap
    con.execute(f"SET temp_directory='{spill}'")            # REQUIRED so DuckDB SPILLS to disk (cannot OOM)
    con.execute(f"SET max_temp_directory_size='200GB'")
    con.execute(f"SET threads={a.threads}")
    con.execute(f"SET preserve_insertion_order=false")      # allow streaming out-of-core partition
    src = str(fio.HOT_FILLS_DIR / "*.parquet")
    # ORDER BY wallet -> DuckDB writes partitions SEQUENTIALLY (one wallet at a time -> tiny write buffer,
    # no 18k-partition buffer blowup). The sort itself is spillable to temp_directory, so it stays under
    # memory_limit. This is what lets an 18k-way partition run in ~3.5GB.
    con.execute(f"""
        COPY (SELECT {collist} FROM read_parquet('{src}', union_by_name=true)
              WHERE "time" BETWEEN {t0} AND {t1}
              ORDER BY "wallet")
        TO '{tmp}' (FORMAT PARQUET, PARTITION_BY ("wallet"), OVERWRITE_OR_IGNORE 1,
                    FILENAME_PATTERN 'fills_{{i}}')
    """)
    con.close()
    if OUT.exists():
        shutil.rmtree(OUT.with_name(OUT.name + ".old"), ignore_errors=True)
        os.replace(OUT, OUT.with_name(OUT.name + ".old"))
    os.replace(tmp, OUT)
    (OUT / "._complete").write_text(f"{a.start}..{a.end}")
    n_parts = sum(1 for _ in OUT.glob("wallet=*"))
    print(f"DONE fills wallet-shard: {n_parts} wallet partitions -> {OUT} | {(time.time()-t)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
