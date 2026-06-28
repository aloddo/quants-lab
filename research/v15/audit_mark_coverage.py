#!/usr/bin/env python3
"""Mark coverage + staleness AUDIT across the 2026-05-24 reconstruction boundary.

Codex marks-gate (projects/quant/copy-rebuild/2026-06-24-codex-marks-gate) required PROOF that the
out-of-sample window (gap 05-24->06-24, incl. the V17-live holdout) has clean marks before any sweep
number is trusted, AND that the 15min candle staleness cap (CANDLE_MAX_AGE_MS) does not over-flag thin
coins. This computes, per coin and ACTION-WEIGHTED by fill notional:
  - candle minute-coverage in a PRE-gap window vs the GAP window (dense vs sparse?)
  - staleness-cap HIT rate: for each fill instant, is the nearest prior 1m candle older than the cap
    (-> get_mark returns None -> unmarkable)? Weighted by notional = the share of exposure we cannot
    mark within the cap. Pre/post 05-24 comparison = the discontinuity test.

Read-only. No writes. Memory-safe (per-day streaming over fills; per-coin candle asof from M01 cache).

Usage:
    python research/v15/audit_mark_coverage.py [--pre-start 2026-05-01 --pre-end 2026-05-23 \
        --gap-start 2026-05-24 --gap-end 2026-06-24] [--top 25]
"""
from __future__ import annotations

import argparse
import glob
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m01_equity_reconstruct as m01  # noqa: E402

FILLS_DIR = Path("/Users/hermes/quants-lab/app/data/hl_s3_fills_v2")
CAP_MS = m01.CANDLE_MAX_AGE_MS


def _day_ms(s: str) -> int:
    return int(datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()) * 1000


def audit_window(start: str, end: str, stats: dict):
    """Stream v2 fills day-by-day in [start,end); accumulate per-coin notional + unmarkable notional
    (nearest prior 1m candle older than CAP_MS at the fill instant)."""
    s_ms, e_ms = _day_ms(start), _day_ms(end)
    for fp in sorted(glob.glob(str(FILLS_DIR / "*.parquet"))):
        day = Path(fp).stem
        try:
            d_ms = int(datetime.strptime(day, "%Y%m%d").replace(tzinfo=timezone.utc).timestamp()) * 1000
        except ValueError:
            continue
        if not (s_ms <= d_ms < e_ms):
            continue
        df = pd.read_parquet(fp, columns=["coin", "time", "price", "size", "side"])
        df = df[df["side"] == "B"]
        for c in ("price", "size"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["coin", "time", "price", "size"])
        for coin, g in df.groupby("coin", sort=False):
            mins, _ = m01._coin_series(coin)
            notional = (g["price"] * g["size"]).to_numpy()
            ts = g["time"].astype("int64").to_numpy()
            tot = float(notional.sum())
            stats[coin]["notional"] += tot
            stats[coin]["n"] += len(g)
            if mins.size == 0:
                stats[coin]["unmarkable_notional"] += tot  # no candles at all
                continue
            minute_key = (ts // 60_000) * 60_000
            idx = np.searchsorted(mins, minute_key, side="right") - 1
            ok = idx >= 0
            age = np.where(ok, minute_key - mins[np.clip(idx, 0, mins.size - 1)], CAP_MS + 1)
            stale = (~ok) | (age > CAP_MS)
            stats[coin]["unmarkable_notional"] += float(notional[stale].sum())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pre-start", default="2026-05-01")
    ap.add_argument("--pre-end", default="2026-05-24")   # exclusive
    ap.add_argument("--gap-start", default="2026-05-24")
    ap.add_argument("--gap-end", default="2026-06-25")   # exclusive
    ap.add_argument("--top", type=int, default=25)
    args = ap.parse_args()

    pre: dict = defaultdict(lambda: defaultdict(float))
    gap: dict = defaultdict(lambda: defaultdict(float))
    print(f"PRE  window {args.pre_start}..{args.pre_end}  |  GAP window {args.gap_start}..{args.gap_end}")
    print(f"staleness cap = {CAP_MS/60000:.0f}min\n")
    audit_window(args.pre_start, args.pre_end, pre)
    audit_window(args.gap_start, args.gap_end, gap)

    def share(s):
        tot = sum(v["notional"] for v in s.values())
        unm = sum(v["unmarkable_notional"] for v in s.values())
        return tot, unm, (100 * unm / tot if tot else 0.0)

    pt, pu, pp = share(pre)
    gt, gu, gp = share(gap)
    print(f"ACTION-WEIGHTED UNMARKABLE (>cap-stale or no candle):")
    print(f"  PRE-gap:  {pp:5.2f}% of ${pt/1e6:.1f}M notional")
    print(f"  GAP:      {gp:5.2f}% of ${gt/1e6:.1f}M notional")
    print(f"  DISCONTINUITY: gap-unmarkable {gp:.2f}% vs pre {pp:.2f}% (delta {gp-pp:+.2f}pp)\n")

    # worst coins in the gap by unmarkable notional
    rows = sorted(gap.items(), key=lambda kv: kv[1]["unmarkable_notional"], reverse=True)
    print(f"Top {args.top} gap coins by UNMARKABLE notional:")
    print(f"  {'coin':14s} {'gap_notional$':>14s} {'unmark%':>8s} {'n_fills':>8s}")
    for coin, v in rows[:args.top]:
        unmp = 100 * v["unmarkable_notional"] / v["notional"] if v["notional"] else 0
        print(f"  {coin:14s} {v['notional']:>14,.0f} {unmp:>7.1f}% {int(v['n']):>8d}")
    print("\nVERDICT GUIDE: if GAP action-weighted unmarkable ~= PRE (small delta) and the worst coins are "
          "immaterial notional, the OOS window is clean. A large gap-vs-pre delta concentrated in material "
          "coins => quarantine those coins or widen the cap (codex marks-gate option A hard-gate).")


if __name__ == "__main__":
    main()
