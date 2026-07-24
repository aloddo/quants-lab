"""Bridge: build the m03 ACTIONS index (wallet, ts, stream_replay_valid) from the CANONICAL
fresh fills hot store, so m03 fold-activity covers June/July instead of the frozen May-23
legacy m02_actions.parquet.

SCOPE (deliberately minimal + honest): m03 reads actions with ONLY columns
[wallet, ts, stream_replay_valid] (v15_m03_fold_geometry.py:405). When journeys are COMPLETE
(they are — the fresh bridged journeys), m03 derives the eligibility-driving `active` flag from
JOURNEYS, not actions (line 295: active = n_journeys>=1). Actions feed only auxiliary counts
(n_actions, first/last_action_ts, total_actions). So an actions index = one row per FILL event
(fills ARE the action stream) is faithful for the recency rebuild.

INTERIM vs DURABLE: this is the interim source for the M2-M5 recency rebuild. The DURABLE canonical
fix (state 2026-07-13: "persist the daily worker's already-computed actions") is a followup — the
M6/M7 backtest engine needs the FULL 29-col action stream, which this lite index does NOT provide.
M6+ (selection) stays frozen; this unblocks m03/m04/m05 only. stream_replay_valid is set TRUE (every
recorded fill is a real event); the journey-side stream_replay_valid filter (correct, from the trace)
still gates the `active` flag, so this approximation touches only auxiliary stats. Codex-gated.

Memory-safe (streaming-IO mandate): DuckDB out-of-core COPY, bounded memory_limit, no pandas concat.

Usage: python scripts/bridge_actions_lite_from_fills.py [--fills DIR] [--out PATH]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parents[1]
DEFAULT_FILLS = REPO / "app" / "data" / "hl_s3_fills_v2_hot"
DEFAULT_OUT = REPO / "app" / "data" / "v15" / "m02_actions.v2.parquet"


def build(fills: Path, out: Path) -> Path:
    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='4GB'")
    con.execute(f"PRAGMA temp_directory='{out.parent}/.duckdb_spill'")
    con.execute("PRAGMA threads=3")
    con.execute(f"""
        COPY (
            SELECT
                wallet,
                time AS ts,
                TRUE AS stream_replay_valid
            FROM read_parquet('{fills}/*.parquet')
            WHERE wallet IS NOT NULL AND time IS NOT NULL
        ) TO '{out}' (FORMAT parquet)
    """)
    n, lo, hi = con.execute(
        f"SELECT count(*), min(ts), max(ts) FROM read_parquet('{out}')"
    ).fetchone()
    con.close()
    print(f"[actions-lite] {out.name} rows={n:,} ts_min={lo} ts_max={hi} "
          f"({out.stat().st_size/1e6:.0f}MB)")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fills", default=str(DEFAULT_FILLS))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args()
    build(Path(args.fills), Path(args.out))
