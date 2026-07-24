"""Bridge: materialize the CANONICAL daily journeys store -> the flat m02_journeys.parquet
that the selection funnel (m03/m05) reads.

WHY: m03/m05 read app/data/v15/m02_journeys.parquet, a FROZEN legacy snapshot (2025-12-01..2026-05-23,
mtime 2026-07-17). The canonical, self-updating source is app/data/v15/m02_journeys_daily/closed/
(run-partitioned, current to ~2026-07-14). This bridge reduces the daily store to its CURRENT active
view and writes the flat file, so folds/eligibility see fresh (June-July) data.

SEMANTICS = byte-equivalent to data_pipeline/m02_journeys_daily.load_active_closed():
  per journey_uid keep the row with the highest run_id; within a run active=True (fresh) beats
  active=False (tombstone); then keep only active rows. EXCLUDE the *.wallets.parquet TOUCHED
  sidecars (no journey_uid/active/run_id -> would corrupt the reduce; codex P1).
Implemented in DuckDB (out-of-core, spills to disk) instead of pandas pd.concat of ~19.8M rows,
which OOM-crashes the RAM-tight box. Memory-safe by construction (streaming-IO mandate 2026-05-31).

Output schema = the 29 columns m02_journeys.parquet carries. The daily store is a superset except
`entry_fill_ord` (UNUSED downstream: zero refs in m03/m04/m05); synthesized = entry_fill_seq for a
schema-identical flat file. Writes to a *.v2 temp then verifies BEFORE the caller swaps -- never
clobbers the legacy copy before its replacement is proven.

Usage: python scripts/bridge_journeys_daily_to_flat.py [--out PATH] [--store DIR]
"""
from __future__ import annotations

import argparse
import glob
import os
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
DEFAULT_STORE = REPO / "app" / "data" / "v15" / "m02_journeys_daily" / "closed"
DEFAULT_OUT = REPO / "app" / "data" / "v15" / "m02_journeys.parquet"

# The exact 29-column schema of the legacy flat m02_journeys.parquet.
SCHEMA_COLS = [
    "wallet", "coin", "journey_id", "side", "entry_ts", "exit_ts", "peak_ts",
    "duration_h", "n_entry_fills", "n_addon_fills", "n_trim_fills", "n_exit_fills",
    "n_reverse_fills", "n_carry_in_seeds", "max_position_notional", "realized_pnl",
    "fees", "funding_net", "net_realized_pnl", "journey_class", "liq_closed",
    "open_at_window_end", "carry_in_status", "lifecycle_valid", "state_discontinuity",
    "entry_fill_seq", "entry_fill_tid", "entry_fill_ord", "stream_replay_valid",
]


def build(store: Path, out: Path) -> Path:
    parts = sorted(
        p for p in glob.glob(str(store / "run_*.parquet"))
        if not p.endswith(".wallets.parquet")
    )
    if not parts:
        sys.exit(f"no journey partitions under {store}")
    tmp = out.with_suffix(".v2.parquet")
    # Column availability: daily store lacks entry_fill_ord -> synthesize from entry_fill_seq.
    # Select the current active view (highest run_id per uid; active beats tombstone within a run).
    select_cols = ",\n        ".join(
        "entry_fill_seq AS entry_fill_ord" if c == "entry_fill_ord" else c
        for c in SCHEMA_COLS
    )
    con = duckdb.connect()
    # bound DuckDB's memory + let it spill to a temp dir (out-of-core, no OOM on the tight box)
    con.execute("PRAGMA memory_limit='4GB'")
    con.execute(f"PRAGMA temp_directory='{tmp.parent}/.duckdb_spill'")
    con.execute("PRAGMA threads=3")
    filelist = ",".join(f"'{p}'" for p in parts)
    con.execute(f"""
        COPY (
            SELECT
                {select_cols}
            FROM (
                SELECT *,
                    row_number() OVER (
                        PARTITION BY journey_uid
                        ORDER BY run_id DESC, active DESC
                    ) AS _rn
                FROM read_parquet([{filelist}])
            )
            WHERE _rn = 1 AND active = TRUE
        ) TO '{tmp}' (FORMAT parquet)
    """)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
    rng = con.execute(
        f"SELECT min(entry_ts), max(exit_ts) FROM read_parquet('{tmp}')"
    ).fetchone()
    con.close()
    print(f"[bridge] parts={len(parts)} -> {tmp.name} rows={n:,}")
    print(f"[bridge] entry_ts min={rng[0]}  exit_ts max={rng[1]}")
    print(f"[bridge] wrote {tmp} ({tmp.stat().st_size/1e6:.0f}MB). "
          f"Verify, then swap over {out.name}.")
    return tmp


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--store", default=str(DEFAULT_STORE))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()
    build(Path(args.store), Path(args.out))
