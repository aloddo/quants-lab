#!/usr/bin/env python3
"""v25 R2: objective-aligned LCB rule (spec-frozen).

Per eligible wallet: simulate fixed-$150 FIRST_CLOSE cold-start copy net bps on TRAIN via
the shared sim core (v25_portfolio_sim.simulate_wallet_trips). Require >= 50 realized
copyable trips. Rank by the one-sided 90% LCB of mean net bps via STATIONARY block
bootstrap over train CALENDAR DATES (block length 5d, 2000 resamples, seed 42, frozen).
Top 25 entities (one wallet per entity; entity rank = its best wallet's LCB; ties broken
by lexicographically smaller wallet).

The bootstrap resamples the train calendar-date axis (all dates in [train_start, asof)),
carrying each selected date's trips with multiplicity; FROZEN trip-to-date assignment:
a trip belongs to its EXIT date (UTC). A resample with zero trips contributes nothing
and is redrawn-safe (skipped in the percentile with value = NaN -> treated as -inf,
conservative).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from v25_common import (MS_DAY, R2_BLOCK_DAYS, R2_LCB_LEVEL, R2_MIN_TRIPS, R2_RESAMPLES,
                        R2_SEED, TOP_N_ENTITIES)
from v25_bootstrap import stationary_bootstrap_indices


def wallet_lcb(trips: pd.DataFrame, train_start_ms: int, asof_ms: int,
               block_days: int = R2_BLOCK_DAYS, n_resamples: int = R2_RESAMPLES,
               seed: int = R2_SEED, level: float = R2_LCB_LEVEL) -> dict:
    """One-sided `level` LCB of mean net bps for ONE wallet's realized trips.
    trips: DataFrame with net_bps + exit_fill_ts_last, terminal excluded by caller.
    FROZEN: a trip belongs to its EXIT date (UTC) for date-block bootstrap purposes."""
    n_days = int((asof_ms - train_start_ms) // MS_DAY)
    if n_days <= 0 or trips.empty:
        return {"lcb_bps": float("nan"), "mean_bps": float("nan"), "n_trips": int(len(trips))}
    day_idx = ((trips["exit_fill_ts_last"].to_numpy() - train_start_ms) // MS_DAY).astype(int)
    day_idx = np.clip(day_idx, 0, n_days - 1)
    bps = trips["net_bps"].to_numpy(dtype="float64")
    # bucket trips by train day: day -> (sum, count)
    sums = np.zeros(n_days)
    cnts = np.zeros(n_days, dtype="int64")
    np.add.at(sums, day_idx, bps)
    np.add.at(cnts, day_idx, 1)
    rng = np.random.default_rng(seed)
    means = np.empty(n_resamples)
    for b in range(n_resamples):
        idx = stationary_bootstrap_indices(n_days, block_days, rng)
        c = cnts[idx].sum()
        means[b] = (sums[idx].sum() / c) if c > 0 else np.nan
    means = np.where(np.isnan(means), -np.inf, means)   # zero-trip resample: conservative
    # method="lower": no interpolation across -inf sentinels (nan-safe), conservative
    lcb = float(np.quantile(means, 1.0 - level, method="lower"))
    return {"lcb_bps": lcb, "mean_bps": float(bps.mean()), "n_trips": int(len(bps))}


def score_r2(wallet_trip_frames: pd.DataFrame, entity_map: dict, train_start_ms: int,
             asof_ms: int, top_n: int = TOP_N_ENTITIES) -> tuple:
    """wallet_trip_frames: concatenated per-wallet TRAIN sim trips (columns: wallet,
    net_bps, exit_fill_ts_last, terminal) for ELIGIBLE wallets only.
    Returns (roster DataFrame, scored DataFrame, diagnostics)."""
    diag = {}
    t = wallet_trip_frames[~wallet_trip_frames["terminal"].astype(bool)]
    rows = []
    for w, g in t.groupby("wallet"):
        if len(g) < R2_MIN_TRIPS:
            continue
        r = wallet_lcb(g, train_start_ms, asof_ms)
        r["wallet"] = w
        rows.append(r)
    diag["n_wallets_min_trips"] = len(rows)
    scored = pd.DataFrame(rows)
    if scored.empty:
        return pd.DataFrame(), scored, diag
    scored = scored[np.isfinite(scored["lcb_bps"])]
    scored = scored.iloc[np.lexsort((scored["wallet"].to_numpy(),
                                     -scored["lcb_bps"].to_numpy()))].reset_index(drop=True)
    roster_rows = []
    seen = set()
    for _, r in scored.iterrows():
        ent = entity_map.get(r["wallet"], r["wallet"])
        if ent in seen:
            continue
        seen.add(ent)
        roster_rows.append({"wallet": r["wallet"], "entity": ent,
                            "rank": len(roster_rows) + 1, "score": float(r["lcb_bps"]),
                            "mean_bps": float(r["mean_bps"]), "n_trips": int(r["n_trips"])})
        if len(roster_rows) >= top_n:
            break
    return pd.DataFrame(roster_rows), scored, diag
