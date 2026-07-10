#!/usr/bin/env python3
"""Live-mark source for OOS/selection: reads the real-time 1m mark store
(app/data/hl_mark_1m_hot, collected by data_pipeline/hl_live_mark_collector.py from HL metaAndAssetCtxs).
This is the EXACT oracle mark, captured continuously -> for any window on/after the collector started it
replaces the ~10-day-lagging asset_ctxs archive. Same asof-backward contract as leadlag_clean_rank_sim.mark_at
and candle_marks.candle_mark_at, so forward_oos_hot can swap sources cleanly (--mark-source live).

Coverage begins 2026-07-09; does NOT retro-cover the 06-30..07-08 gap (we didn't record it then). Read-only.
"""
from __future__ import annotations
import glob
from bisect import bisect_right
from pathlib import Path
import numpy as np, pandas as pd

REPO = Path(__file__).resolve().parent.parent.parent
MARK_DIR = REPO / "app" / "data" / "hl_mark_1m_hot"
MARK_MAX_AGE_MS = 5 * 60_000   # a mark older than 5m => no coverage (poll cadence is 1m)

_idx: dict[str, tuple[np.ndarray, np.ndarray] | None] = {}
_field = "markPx"


def load(field: str = "markPx") -> None:
    """Build per-coin (ts_ms, price) asof index from the live-mark parquets. field in {markPx,oraclePx,midPx}."""
    global _field
    _field = field
    _idx.clear()
    files = sorted(glob.glob(str(MARK_DIR / "20??????.parquet")))
    if not files:
        return
    frames = [pd.read_parquet(f, columns=["coin", "timestamp_utc", field]) for f in files]
    allm = pd.concat(frames, ignore_index=True).dropna(subset=["coin", "timestamp_utc", field])
    allm["timestamp_utc"] = allm["timestamp_utc"].astype("int64")
    allm[field] = pd.to_numeric(allm[field], errors="coerce")
    allm = allm.dropna(subset=[field]).sort_values(["coin", "timestamp_utc"], kind="mergesort")
    for coin, g in allm.groupby("coin", sort=False):
        _idx[coin] = (g["timestamp_utc"].to_numpy(), g[field].to_numpy(dtype="float64"))


def live_mark_at(coin: str, ts_ms: int):
    if not _idx:
        load()
    m = _idx.get(coin)
    if m is None:
        return None
    ts, px = m
    i = bisect_right(ts, ts_ms) - 1
    if i < 0:
        return None
    if ts_ms - int(ts[i]) > MARK_MAX_AGE_MS:
        return None
    return float(px[i])


def coverage():
    if not _idx:
        load()
    rows = []
    for c, v in _idx.items():
        if v is None or len(v[0]) == 0:
            continue
        rows.append({"coin": c, "n": len(v[0]),
                     "first": pd.Timestamp(int(v[0][0]), unit="ms", tz="UTC"),
                     "last": pd.Timestamp(int(v[0][-1]), unit="ms", tz="UTC")})
    return pd.DataFrame(rows)


if __name__ == "__main__":
    load()
    df = coverage()
    if len(df):
        print(f"live marks: {len(df)} coins | window {df['first'].min()} .. {df['last'].max()} | median n {int(df['n'].median())}")
    else:
        print("no live marks yet")
