#!/usr/bin/env python3
"""v25 stationary block bootstrap + familywise adjustment + hurdle (spec-frozen).

Primary statistic: the rule's portfolio daily $ PnL series, all folds concatenated, FOLD
BOUNDARIES RESPECTED in blocking (blocks never cross a fold boundary: each fold segment is
resampled independently with the stationary bootstrap, then concatenated).

- Stationary bootstrap (Politis-Romano): geometric block lengths, mean = block_days,
  circular wrap within the segment. 7d primary, 14d robustness. 10000 resamples, seed 42.
- Familywise adjustment across the TWO rules: joint max-statistic with SHARED resample
  draws (same RNG stream of segment indices applied to every rule), one-sided 95%
  familywise LCB: LCB_r = mean_r - c where c = 95th percentile over resamples of
  max_r (mean_r - boot_mean_rb). THE method. Bonferroni fallback (bonferroni_lcb):
  per-rule 97.5% one-sided LCB (2.5th percentile of that rule's own bootstrap means),
  used IFF the joint bootstrap raises ANY exception or returns a non-finite LCB; the
  trigger event is written to manifest.json BEFORE the fallback runs (orchestrator).
- single_rule_lcb: holdout evaluator statistic (one-sided 95%, single rule, no
  familywise adjustment; 7d blocks, 10000 resamples, seed 42, frozen).
- hurdle_daily_$ = (10 / 10000) x 150 x (total realized trips across folds / total test
  calendar days)   [frozen formula, blocker #1]
"""
from __future__ import annotations

import numpy as np

from v25_common import (DELTA_BPS, ORDER_USD, PORT_BLOCK_DAYS, PORT_RESAMPLES, PORT_SEED)


def hurdle_daily_usd(total_realized_trips: int, total_test_days: float,
                     delta_bps: float = DELTA_BPS, order_usd: float = ORDER_USD) -> float:
    """FROZEN: hurdle_daily_$ = (delta/1e4) x $150 x (trips / test calendar days)."""
    if total_test_days <= 0:
        return float("inf")
    return (delta_bps / 1e4) * order_usd * (total_realized_trips / total_test_days)


def stationary_bootstrap_indices(n: int, mean_block: float, rng: np.random.Generator) -> np.ndarray:
    """One stationary-bootstrap resample of indices 0..n-1 (Politis-Romano 1994): start
    points uniform, block length geometric with mean `mean_block`, circular wrap."""
    if n <= 0:
        return np.empty(0, dtype="int64")
    p = 1.0 / max(float(mean_block), 1.0)
    out = np.empty(n, dtype="int64")
    i = 0
    while i < n:
        start = int(rng.integers(0, n))
        blk = int(rng.geometric(p))
        blk = min(blk, n - i)
        out[i:i + blk] = (start + np.arange(blk)) % n
        i += blk
    return out


def joint_lcb(rule_segments: dict[str, list[np.ndarray]],
              block_days: float = PORT_BLOCK_DAYS,
              n_resamples: int = PORT_RESAMPLES,
              seed: int = PORT_SEED,
              familywise_level: float = 0.95) -> dict:
    """Familywise one-sided LCBs of the mean daily $ PnL for every rule.

    rule_segments: {rule_name: [fold1 daily array, fold2 daily array, ...]}. All rules
    MUST cover the same folds with the same per-fold day counts (same test calendar), so
    ONE set of per-(resample, segment) day-index draws is SHARED across rules -- this is
    what makes the max statistic joint (it preserves the cross-rule dependence). Raises
    if segment lengths differ across rules.

    Returns {rule: {mean, lcb_maxstat, lcb_bonferroni, n_days}} + {'c_maxstat': c}."""
    rules = sorted(rule_segments.keys())
    seg_lens = None
    for r in rules:
        lens = [s.size for s in rule_segments[r]]
        if seg_lens is None:
            seg_lens = lens
        elif lens != seg_lens:
            raise ValueError(f"joint_lcb: segment lengths differ across rules "
                             f"({rules[0]}={seg_lens} vs {r}={lens}); shared draws impossible")
    seg_lens = seg_lens or []
    means = {r: (np.concatenate([s for s in rule_segments[r]]) if rule_segments[r] else
                 np.empty(0)) for r in rules}
    hat = {r: (float(means[r].mean()) if means[r].size else float("nan")) for r in rules}
    boot = {r: np.empty(n_resamples) for r in rules}
    rng = np.random.default_rng(seed)
    n_total = sum(seg_lens)
    for b in range(n_resamples):
        # ONE draw of day indices per segment, applied to EVERY rule (joint/shared)
        seg_idx = [stationary_bootstrap_indices(n, block_days, rng) for n in seg_lens]
        for r in rules:
            if n_total == 0:
                boot[r][b] = float("nan")
                continue
            tot = 0.0
            for seg, idx in zip(rule_segments[r], seg_idx):
                if idx.size:
                    tot += float(seg[idx].sum())
            boot[r][b] = tot / n_total
    d = np.full(n_resamples, -np.inf)
    for r in rules:
        if means[r].size:
            d = np.maximum(d, hat[r] - boot[r])
    c = float(np.nanquantile(d, familywise_level)) if np.isfinite(d).any() else float("nan")
    out = {"c_maxstat": c, "method": "joint_maxstat", "rules": {}}
    alpha_bonf = (1.0 - familywise_level) / max(len(rules), 1)
    for r in rules:
        if not means[r].size:
            out["rules"][r] = {"mean_daily_usd": float("nan"), "lcb_maxstat": float("nan"),
                               "lcb_bonferroni": float("nan"), "n_days": 0}
            continue
        out["rules"][r] = {
            "mean_daily_usd": hat[r],
            "lcb_maxstat": hat[r] - c,
            "lcb_bonferroni": float(np.nanquantile(boot[r], alpha_bonf)),
            "n_days": int(means[r].size),
        }
    return out


def _per_rule_boot_means(segments: list[np.ndarray], block_days: float,
                         n_resamples: int, seed: int) -> np.ndarray:
    """Independent per-rule stationary bootstrap of the mean daily $ PnL (own RNG)."""
    seg_lens = [s.size for s in segments]
    n_total = sum(seg_lens)
    boot = np.full(n_resamples, np.nan)
    if n_total == 0:
        return boot
    rng = np.random.default_rng(seed)
    for b in range(n_resamples):
        tot = 0.0
        for seg in segments:
            if seg.size:
                idx = stationary_bootstrap_indices(seg.size, block_days, rng)
                tot += float(seg[idx].sum())
        boot[b] = tot / n_total
    return boot


def bonferroni_lcb(rule_segments: dict[str, list[np.ndarray]],
                   block_days: float = PORT_BLOCK_DAYS,
                   n_resamples: int = PORT_RESAMPLES,
                   seed: int = PORT_SEED,
                   familywise_level: float = 0.95) -> dict:
    """FROZEN Bonferroni fallback: per-rule 97.5% one-sided LCB (2.5th percentile of the
    rule's OWN independent bootstrap means). Only run after the trigger event has been
    written to manifest.json (orchestrator responsibility). Same return shape as
    joint_lcb: {'method', 'rules': {rule: {mean_daily_usd, lcb_maxstat, n_days}}} where
    lcb_maxstat carries the ADJUSTED LCB used by the pass criteria."""
    rules = sorted(rule_segments.keys())
    alpha = (1.0 - familywise_level) / max(len(rules), 1)
    out = {"method": "bonferroni_fallback", "alpha_per_rule": alpha, "rules": {}}
    for r in rules:
        concat = (np.concatenate(rule_segments[r]) if rule_segments[r] else np.empty(0))
        if not concat.size:
            out["rules"][r] = {"mean_daily_usd": float("nan"), "lcb_maxstat": float("nan"),
                               "n_days": 0}
            continue
        boot = _per_rule_boot_means(rule_segments[r], block_days, n_resamples, seed)
        lcb = (float(np.nanquantile(boot, alpha))
               if np.isfinite(boot).any() else float("nan"))
        out["rules"][r] = {"mean_daily_usd": float(concat.mean()), "lcb_maxstat": lcb,
                           "n_days": int(concat.size)}
    return out


def single_rule_lcb(segments: list[np.ndarray],
                    block_days: float = PORT_BLOCK_DAYS,
                    n_resamples: int = PORT_RESAMPLES,
                    seed: int = PORT_SEED,
                    level: float = 0.95) -> dict:
    """Holdout statistic (frozen): one-sided `level` LCB of the mean daily $ PnL for a
    SINGLE rule (no familywise adjustment); stationary block bootstrap, 7d blocks,
    10000 resamples, seed 42."""
    concat = np.concatenate(segments) if segments else np.empty(0)
    if not concat.size:
        return {"mean_daily_usd": float("nan"), "lcb": float("nan"), "n_days": 0}
    boot = _per_rule_boot_means(segments, block_days, n_resamples, seed)
    lcb = (float(np.nanquantile(boot, 1.0 - level))
           if np.isfinite(boot).any() else float("nan"))
    return {"mean_daily_usd": float(concat.mean()), "lcb": lcb, "n_days": int(concat.size)}
