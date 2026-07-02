#!/usr/bin/env python3
"""v26 primary estimand + familywise corrections + K-scaling criteria (codex #4/#5/#6).

Primary estimand per config (frozen): mean daily EXCESS return on deployed capital =
    ($PnL_d - 10bps x admitted_entry_notional_d) / avg_gross_d
with avg_gross_d = time-weighted average gross notional over the day's 1m marks (the
per-mark DD clock provides the weights) and admitted_entry_notional_d = sum of entry
notionals FILLED that day. Zero-exposure days (avg_gross_d = 0) are EXCLUDED; a config
must retain >= 30 nonzero-exposure test days across folds. Pass = adjusted one-sided
LCB(mean daily excess) > 0 (the hurdle lives inside the estimand).

K-scaling delta translation (decision D8, documented): "LCB bar delta x M" is an
ESTIMAND SHIFT -- the tested series uses M x 10bps inside the numerator, i.e. pass
requires LCB(base estimand) > (M - 1) x 10bps x admitted_d / avg_gross_d per day in
excess-return units. Pass = adjusted LCB(shifted series) > 0 uniformly.

Corrections (frozen): joint max-statistic over ALL RUN configs with SHARED bootstrap
draws, 100,000 resamples of 7d stationary blocks on aligned calendar days (fold
boundaries respected: each fold segment resampled independently, one draw per
(resample, segment) applied to EVERY config). Fallback (same frozen trigger as v25: any
exception or non-finite LCB): Holm-Bonferroni alpha 0.05 one-sided over the exact
non-pruned family size, 200,000 resamples; per-config p-value on the RECENTERED null
(null mean_b = boot_mean_b - observed mean), one-sided p = (1 + #null means >= observed
mean) / (B + 1) (plus-one); runtime failures enter the family with p = 1. If the
fallback also fails, the batch has NO winner (fail closed).

A config's excess series is defined only on ITS nonzero-exposure days: a drawn day that
is zero-exposure for config r contributes to neither numerator nor denominator of r's
resampled mean (masked-mean bootstrap over the aligned day axis).
"""
from __future__ import annotations

import numpy as np

from v25_bootstrap import stationary_bootstrap_indices
from v26_common import (GRID_BLOCK_DAYS, GRID_FAMILYWISE_LEVEL, GRID_RESAMPLES,
                        GRID_SEED, HOLM_RESAMPLES, HURDLE_FRAC, MIN_NONZERO_DAYS)


def daily_excess(pnl: np.ndarray, admitted: np.ndarray, gross: np.ndarray,
                 delta_mult: float = 1.0) -> np.ndarray:
    """Aligned daily excess-return series; NaN on zero-exposure days (excluded)."""
    pnl = np.asarray(pnl, dtype="float64")
    admitted = np.asarray(admitted, dtype="float64")
    gross = np.asarray(gross, dtype="float64")
    out = np.full(pnl.shape, np.nan)
    nz = gross > 0
    out[nz] = (pnl[nz] - delta_mult * HURDLE_FRAC * admitted[nz]) / gross[nz]
    return out


def _draw_weights(seg_lens: list[int], n_resamples: int, block_days: float, seed: int,
                  chunk: int):
    """Yield (start, count-weight matrix W [total_days x chunk]) of SHARED stationary
    block draws: one day-index draw per (resample, fold segment), applied to every
    config (this is what makes the max statistic joint)."""
    rng = np.random.default_rng(seed)
    total = sum(seg_lens)
    offs = np.cumsum([0] + list(seg_lens))
    b = 0
    while b < n_resamples:
        m = min(chunk, n_resamples - b)
        W = np.zeros((total, m), dtype="float64")
        for c in range(m):
            for s, n in enumerate(seg_lens):
                if n == 0:
                    continue
                idx = stationary_bootstrap_indices(n, block_days, rng)
                np.add.at(W[:, c], offs[s] + idx, 1.0)
        yield b, W
        b += m


def _masked_boot_means(M: np.ndarray, W: np.ndarray):
    """Resampled masked means: M [cfg x day] with NaN on excluded days; W [day x draws]
    day-count weights. Returns [cfg x draws] (NaN when a draw hits zero valid days)."""
    A = np.nan_to_num(M, nan=0.0)
    mask = np.isfinite(M).astype("float64")
    num = A @ W
    den = mask @ W
    with np.errstate(invalid="ignore", divide="ignore"):
        out = num / den
    out[den == 0] = np.nan
    return out


def joint_maxstat(M: np.ndarray, seg_lens: list[int],
                  n_resamples: int = GRID_RESAMPLES, seed: int = GRID_SEED,
                  block_days: float = GRID_BLOCK_DAYS,
                  level: float = GRID_FAMILYWISE_LEVEL, chunk: int = 2000) -> dict:
    """THE method: joint max-statistic familywise LCBs with SHARED draws.
    M: [n_cfg x total_days] shifted excess series (NaN = excluded day). Returns
    {'c_maxstat', 'mean', 'lcb'} with lcb = mean - c per config. Raises on any
    internal exception (the caller owns the frozen fallback trigger)."""
    n_cfg = M.shape[0]
    hat = np.nanmean(np.where(np.isfinite(M), M, np.nan), axis=1)
    d_parts = []
    for _b0, W in _draw_weights(seg_lens, n_resamples, block_days, seed, chunk):
        boot = _masked_boot_means(M, W)                       # [cfg x draws]
        dev = hat[:, None] - boot
        dev = np.where(np.isfinite(dev), dev, -np.inf)        # failed draw: no support
        d_parts.append(dev.max(axis=0))
        del boot, dev, W
    d = np.concatenate(d_parts)
    finite = np.isfinite(d)
    if not finite.any():
        raise RuntimeError("joint_maxstat: no finite max-deviation draws")
    c = float(np.quantile(d[finite], level))
    lcb = hat - c
    if not np.isfinite(lcb[np.isfinite(hat)]).all():
        raise RuntimeError("joint_maxstat: non-finite LCB")
    return {"method": "joint_maxstat", "c_maxstat": c, "mean": hat, "lcb": lcb,
            "n_resamples": n_resamples}


def holm_fallback(M: np.ndarray, seg_lens: list[int], family_size: int,
                  n_resamples: int = HOLM_RESAMPLES, seed: int = GRID_SEED,
                  block_days: float = GRID_BLOCK_DAYS, alpha: float = 0.05,
                  chunk: int = 1000) -> dict:
    """FROZEN fallback: Holm-Bonferroni over the EXACT non-pruned family size.
    M rows = configs WITH a series; family_size >= M.shape[0] (runtime failures carry
    p = 1 and are appended by the caller). Per-config recentered plus-one p; the
    reported per-config LCB (winner ordering) = the alpha/family_size one-sided
    percentile-method LCB of that config's own bootstrap means (v25 bonferroni_lcb
    convention). Returns {'mean', 'p_raw', 'lcb'}."""
    n_cfg = M.shape[0]
    hat = np.nanmean(np.where(np.isfinite(M), M, np.nan), axis=1)
    ge_count = np.zeros(n_cfg, dtype="int64")
    boot_all = np.full((n_cfg, n_resamples), np.nan)
    for b0, W in _draw_weights(seg_lens, n_resamples, block_days, seed, chunk):
        boot = _masked_boot_means(M, W)
        # recentered null: null_mean_b = boot_b - hat; one-sided #(null >= hat)
        ge_count += np.nansum((boot - hat[:, None]) >= hat[:, None], axis=1)
        boot_all[:, b0:b0 + boot.shape[1]] = boot
        del boot, W
    p_raw = (1.0 + ge_count) / (n_resamples + 1.0)
    q = alpha / max(family_size, 1)
    lcb = np.array([float(np.nanquantile(boot_all[i], q))
                    if np.isfinite(boot_all[i]).any() else float("nan")
                    for i in range(n_cfg)])
    return {"method": "holm_fallback", "mean": hat, "p_raw": p_raw, "lcb": lcb,
            "n_resamples": n_resamples, "family_size": family_size}


def holm_adjust(p_raw: np.ndarray, family_size: int) -> np.ndarray:
    """Holm step-down adjusted p-values over a family of family_size hypotheses (the
    supplied p_raw may be fewer rows; the missing members carry p = 1 and can only make
    the adjustment MORE conservative for smaller p's -- family never shrinks)."""
    p = np.asarray(p_raw, dtype="float64")
    order = np.argsort(p, kind="mergesort")
    adj = np.empty_like(p)
    running = 0.0
    m = max(family_size, p.size)
    for rank, i in enumerate(order):
        running = max(running, (m - rank) * p[i])
        adj[i] = min(running, 1.0)
    return adj


def nonzero_day_count(M_row: np.ndarray) -> int:
    return int(np.isfinite(M_row).sum())


def sample_ok(M_row: np.ndarray, min_days: int = MIN_NONZERO_DAYS) -> bool:
    return nonzero_day_count(M_row) >= min_days
