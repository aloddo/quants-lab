#!/usr/bin/env python
"""nb_pct_series_build.py -- H-NB-FILTER card, Step 2 (agent G, 2026-06-11).

Per-coin HOURLY causal NB percentile series for the V17 replay folds, built by
REUSING the position-true machinery of v18_raw_rebuild.py (codex-gated C1/C2/C6):

  - slice: app/data/v18/cohort_fills_slice.parquet (cache hit; FAIL if absent)
  - cohorts: sprint fold1 -> v18 fold_A book, sprint fold2 -> fold_B (per the card)
  - NB(t): build_fold_books (C1 seeding + C6 position-true walk), hourly samples nb_h
  - pct[h]: midrank of nb_h[h] vs the trailing PCT_WINDOW_H (720) COMPLETED hourly
    samples STRICTLY BEFORE hour h (nb_h[h-720:h]), requiring PCT_MINW_H (168) --
    the same causality convention as v18 causal_pct (codex C2), applied on the grid.

Output: app/data/v18/nb_pct_series.parquet (coin, fold, hour_ts, nb, pct)
        fold labels are the REPLAY fold names (fold1/fold2) for direct engine joins.

Run: python research/v18/nb_pct_series_build.py
"""
from __future__ import annotations

import sys

import numpy as np
import pandas as pd

REPO = "/Users/hermes/quants-lab"
sys.path.insert(0, f"{REPO}/research/v18")

from v18_raw_rebuild import (  # noqa: E402  (reuse, not reimplementation)
    COINS, DAY_MS, FOLDS, HOUR_MS, PCT_MINW_H, PCT_WINDOW_H, PRE_WINDOW_D,
    build_fold_books, cohort_universe, load_slice, ms,
)

OUT_PQ = f"{REPO}/app/data/v18/nb_pct_series.parquet"
FOLD_MAP = {"fold_A": "fold1", "fold_B": "fold2"}  # v18 book -> replay fold label


def hourly_pct(nb_h: np.ndarray) -> np.ndarray:
    """Causal midrank percentile of each hourly NB sample vs its own trailing window.
    Denominator = nb_h[max(0, h-720):h] (completed samples STRICTLY before h),
    min 168 samples, midrank (left+right)/(2n) -- identical convention to
    v18_raw_rebuild.causal_pct, evaluated on the hourly grid itself."""
    n = len(nb_h)
    pct = np.full(n, np.nan)
    for h in range(PCT_MINW_H, n):
        lo = max(0, h - PCT_WINDOW_H)
        sw = np.sort(nb_h[lo:h])
        v = nb_h[h]
        lft = int(np.searchsorted(sw, v, side="left"))
        rgt = int(np.searchsorted(sw, v, side="right"))
        pct[h] = (lft + rgt) / (2.0 * len(sw))
    return pct


def main():
    fold_wallets, uni = cohort_universe()
    sl = load_slice(False, None, uni)
    assert len(sl) == 320_637, f"slice cache changed: {len(sl)} rows (expected 320637)"

    rows = []
    for fold, cfg in FOLDS.items():
        test0, end = ms(cfg["test_start"]), ms(cfg["end"])
        grid0 = test0 - PRE_WINDOW_D * DAY_MS
        books, seed = build_fold_books(sl, set(fold_wallets[fold]), grid0, test0, end)
        lbl = FOLD_MAP[fold]
        for coin in COINS:
            b = books[coin]
            pct = hourly_pct(b.nb_h)
            n_ok = int(np.isfinite(pct).sum())
            rows.append(pd.DataFrame({
                "coin": coin, "fold": lbl,
                "hour_ts": b.hours.astype(np.int64),
                "nb": b.nb_h.astype(np.int32), "pct": pct,
            }))
            print(f"{lbl} {coin}: hours={len(b.hours)} pct_valid={n_ok} "
                  f"first_valid={pd.to_datetime(b.hours[PCT_MINW_H], unit='ms')} "
                  f"nb[min,max]=[{b.nb_h.min()},{b.nb_h.max()}]")
        print(f"{lbl}: grid {pd.to_datetime(grid0, unit='ms')} .. {pd.to_datetime(end, unit='ms')} "
              f"(test from {cfg['test_start']}); seed backfilled={seed['backfilled_n']} "
              f"seen_after={seed['seen_after_n']}")

    out = pd.concat(rows, ignore_index=True)
    out.to_parquet(OUT_PQ, index=False)
    print(f"\nwrote {len(out)} rows -> {OUT_PQ}")
    cov = out.groupby("fold")["hour_ts"].agg(["min", "max", "count"])
    cov["min"] = pd.to_datetime(cov["min"], unit="ms")
    cov["max"] = pd.to_datetime(cov["max"], unit="ms")
    print(cov.to_string())


if __name__ == "__main__":
    main()
