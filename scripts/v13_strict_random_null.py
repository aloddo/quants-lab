#!/usr/bin/env python3
"""V13 Module 11 — Strict Random Null.

Per spec: projects/quant/v13/modules/11-strict-random-null

For each fold, draw N=300 random wallet pools of size K_target from STAGE 3 eligible
universe. Run Module 08 portfolio sim on each. p95 of agg-Sharpe distribution = the
benchmark Module 10 Gate G6 requires the ranked strategy to exceed.

V11 lacked this check. V13 won't ship without it.

CORE API:
- compute_random_null_for_fold(fold_n, eligible_pool, params, K_target, fold_window,
                                portfolio_sim_fn, n_pools=300)
- aggregate_random_null_across_folds(per_fold_random)

The portfolio_sim_fn is INJECTED (Module 08); Module 11 is just the sampling + aggregation
harness. This makes Module 11 testable in isolation with a stub sim function.
"""
from __future__ import annotations

import hashlib
import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Optional

import numpy as np

logger = logging.getLogger("v13_strict_random_null")

# === Constants (Module 11 spec) ===
N_RANDOM_POOLS_DEFAULT = 300
SEED_BASE = 42
P95_QUANTILE = 0.95
PARALLEL_WORKERS_DEFAULT = 4


def _params_hash(params: dict) -> int:
    """Stable integer hash for params dict (for seed determinism)."""
    s = json.dumps(params, sort_keys=True)
    h = hashlib.md5(s.encode()).hexdigest()
    return int(h[:8], 16) % 1_000_000


def compute_random_null_for_fold(
    fold_n: int,
    eligible_pool: list[str],
    params: dict,
    K_target: int,
    fold_window: tuple,
    portfolio_sim_fn: Callable[[list[str], dict, tuple], dict],
    n_pools: int = N_RANDOM_POOLS_DEFAULT,
    parallel_workers: int = PARALLEL_WORKERS_DEFAULT,
    seed_base: int = SEED_BASE,
) -> dict:
    """Draw n_pools random wallet pools, run portfolio_sim_fn on each, compute p95 Sharpe.

    Returns:
        {
            sharpes: list[float] (NaN-filtered),
            p95_sharpe: float (or -inf if pool too small),
            median_sharpe, n_trials, params, warning: Optional[str]
        }
    """
    # codex m11 r1 fix #3: de-duplicate eligible_pool to prevent same wallet sampled
    # multiple times into a "K-size" pool that's really K-D unique wallets.
    eligible_pool = list(dict.fromkeys(eligible_pool))  # preserve order, dedup
    if len(eligible_pool) < K_target:
        # codex m11 r5 fix: include sharpes_dense + n_pools so the early-return contract
        # matches the normal path. Callers (aggregate_random_null_across_folds) expect
        # these keys on every fold result.
        return {
            "sharpes": [],
            "sharpes_dense": [None] * n_pools,
            "n_pools": n_pools,
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "params": params,
            "warning": f"pool_too_small ({len(eligible_pool)} < K_target={K_target})",
        }

    base_seed = seed_base + fold_n + _params_hash(params)
    pool_arr = np.array(eligible_pool)

    def _producer_sharpe_is_valid(v):
        """codex m11 r11 fix: producer-side type guard. Reject bool (subclass of int),
        non-numeric, NaN, Inf, complex. Mirrors aggregator's _is_valid_dense_element so
        the producer can't emit a value the aggregator would reject — would otherwise
        let bools coerce to 1.0/0.0, strings cast via float(), and inf survive past NaN
        filter."""
        if isinstance(v, bool):
            return False
        if not isinstance(v, (int, float)) or isinstance(v, complex):
            return False
        try:
            return bool(np.isfinite(v))
        except (TypeError, ValueError):
            return False

    def run_one(trial_idx: int):
        """codex m11 r1 fix #1: return (trial_idx, sharpe) so parallel collection preserves order."""
        try:
            local_rng = np.random.default_rng(base_seed + trial_idx)
            idx = local_rng.choice(len(pool_arr), size=K_target, replace=False)
            random_pool = pool_arr[idx].tolist()
            out = portfolio_sim_fn(random_pool, params, fold_window)
            raw = out.get("summary", {}).get("sharpe")
            if not _producer_sharpe_is_valid(raw):
                return (trial_idx, None)
            return (trial_idx, float(raw))
        except Exception as e:
            logger.warning(f"random null trial {trial_idx} failed: {e}")
            return (trial_idx, None)

    # codex m11 r1 fix #1: index-keyed collection → deterministic order regardless of parallel completion order.
    results_by_idx: dict[int, Optional[float]] = {}
    if parallel_workers > 1:
        with ThreadPoolExecutor(max_workers=parallel_workers) as ex:
            futures = [ex.submit(run_one, i) for i in range(n_pools)]
            for f in as_completed(futures):
                idx, val = f.result()
                results_by_idx[idx] = val
    else:
        for i in range(n_pools):
            idx, val = run_one(i)
            results_by_idx[idx] = val

    # codex m11 r2 fix: preserve trial-index alignment via DENSE list (None for failures).
    # Aggregation across folds must align by trial_idx, NOT by compressed list position.
    sharpes_dense: list = []  # length = n_pools, entries are float or None
    for i in range(n_pools):
        v = results_by_idx.get(i)
        if v is None or (isinstance(v, float) and np.isnan(v)):
            sharpes_dense.append(None)
        else:
            sharpes_dense.append(v)

    # Backward-compat compressed list (drops None/NaN); used for per-fold p95.
    sharpes = [s for s in sharpes_dense if s is not None]
    sharpes_arr = np.array(sharpes)
    if len(sharpes_arr) == 0:
        return {
            "sharpes": [],
            "sharpes_dense": sharpes_dense,  # codex m11 r3 fix: expose dense for aggregation
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "n_pools": n_pools,
            "params": params,
            "warning": "all_trials_nan_or_failed",
        }

    return {
        "sharpes": sharpes_arr.tolist(),         # compressed, backward-compat
        "sharpes_dense": sharpes_dense,          # codex m11 r3 fix: length-n_pools with None placeholders
        "p95_sharpe": float(np.quantile(sharpes_arr, P95_QUANTILE)),
        "median_sharpe": float(np.quantile(sharpes_arr, 0.5)),
        "n_trials": len(sharpes_arr),
        "n_pools": n_pools,
        "params": params,
        "warning": None,
    }


def aggregate_random_null_across_folds(per_fold_random: dict[int, dict]) -> dict:
    """Aggregate per-fold random Sharpes into a strategy-level null.

    v0 simplification (per Module 11 spec): mean per-fold Sharpe across trials.
    v0.1 may concat daily returns + recompute one Sharpe per trial across folds.

    codex m11 r1 fix #2: if ANY fold has 0 trials (pool_too_small / all-failed), aggregate
    REFUSES to silently drop it. Returns -inf p95 + warning so Module 10 G6 fails. Module 11
    is supposed to surface incomplete random-null coverage, not hide it.

    Returns {p95_sharpe, median_sharpe, n_trials, fold_count, skipped_folds, warning}.
    """
    if not per_fold_random:
        return {"p95_sharpe": float("-inf"), "median_sharpe": float("nan"),
                "n_trials": 0, "fold_count": 0, "skipped_folds": [], "warning": "no_folds"}

    # codex m11 r8 fix: STRICT contract validation MUST run FIRST. The previous order let
    # a fold missing required fields (`sharpes_dense` / `n_pools`) but with an empty
    # `sharpes: []` slip past as a `folds_without_trials` warning instead of the required
    # `missing_required_fields` warning. Module 10's G6 fails closed either way, but the
    # warning text drives diagnostics and the spec promises the strict-contract warning.
    sorted_folds = sorted(per_fold_random.keys())
    missing_dense = [f for f in sorted_folds if "sharpes_dense" not in per_fold_random[f]]
    missing_npools = [f for f in sorted_folds if "n_pools" not in per_fold_random[f]]
    if missing_dense or missing_npools:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": (
                f"missing_required_fields: missing_dense={missing_dense} "
                f"missing_n_pools={missing_npools}"
            ),
        }
    # codex m11 r9 fix: TYPE validation. Strict contract requires sharpes_dense to be a list
    # and n_pools to be a non-negative int. None / wrong types must fail closed (not crash
    # on len() or sort).
    malformed_dense = [
        f for f in sorted_folds
        if not isinstance(per_fold_random[f]["sharpes_dense"], list)
    ]
    malformed_npools = [
        f for f in sorted_folds
        if not isinstance(per_fold_random[f]["n_pools"], int)
        or isinstance(per_fold_random[f]["n_pools"], bool)  # bool subclass of int
        or per_fold_random[f]["n_pools"] < 0
    ]
    if malformed_dense or malformed_npools:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": (
                f"malformed_required_fields: dense_not_list={malformed_dense} "
                f"n_pools_invalid={malformed_npools}"
            ),
        }
    # codex m11 r10 fix: ELEMENT-level type validation on dense lists. Each element must be
    # None or a non-bool numeric (int/float). Strings, lists, bools, NaN, Inf are invalid.
    # Without this, aggregation silently drops bad entries and the trial count gets quietly
    # smaller than what the strict-N=300 contract requires. Bools (subclass of int) would
    # aggregate as 1.0/0.0 — also rejected here.
    def _is_valid_dense_element(v):
        if v is None:
            return True
        if isinstance(v, bool):  # bool subclass of int — reject explicitly
            return False
        if isinstance(v, (int, float)) and not isinstance(v, complex):
            try:
                return bool(np.isfinite(v))
            except (TypeError, ValueError):
                return False
        return False
    malformed_dense_elements = []
    for f in sorted_folds:
        d = per_fold_random[f]["sharpes_dense"]
        for idx, v in enumerate(d):
            if not _is_valid_dense_element(v):
                malformed_dense_elements.append((f, idx, type(v).__name__))
                break  # one bad element per fold is enough to identify
    if malformed_dense_elements:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": (
                f"malformed_required_fields: dense_element_invalid={malformed_dense_elements}"
            ),
        }

    skipped = [f for f, pfr in per_fold_random.items() if not pfr.get("sharpes")]
    if skipped:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(per_fold_random),
            "skipped_folds": skipped,
            "warning": f"folds_without_trials: {skipped}",
        }

    # codex m11 r3 fix: aggregate via DENSE list aligned by trial_idx (not compressed list position).
    # By r8: missing_dense + missing_npools already rejected above; safe to index directly.
    dense_lists = [per_fold_random[f]["sharpes_dense"] for f in sorted_folds]
    declared_n_pools = [per_fold_random[f]["n_pools"] for f in sorted_folds]
    # codex m11 r6 fix: STRICT cross-fold n_pools validation. Spec requires fixed N=300 per
    # fold. If declared n_pools differ across folds, or if dense list lengths differ across
    # folds, the null coverage is INCOMPLETE — return -inf and a warning so Module 10's
    # G6 fails closed. Without this, mismatched coverage silently passes via the small
    # overlapping subset of trial indices.
    if declared_n_pools and len(set(declared_n_pools)) > 1:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": f"n_pools_mismatch: declared={sorted(set(declared_n_pools))}",
        }
    dense_lens = {len(d) for d in dense_lists}
    if len(dense_lens) > 1:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": f"sharpes_dense_length_mismatch: lengths={sorted(dense_lens)}",
        }
    n_pools_canon = max((len(d) for d in dense_lists), default=0)
    # If declared n_pools provided AND consistent, additionally enforce it matches dense length.
    if declared_n_pools and declared_n_pools[0] != n_pools_canon:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": 0,
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": f"n_pools_declared_vs_dense_mismatch: declared={declared_n_pools[0]} dense={n_pools_canon}",
        }
    if n_pools_canon == 0:
        return {"p95_sharpe": float("-inf"), "median_sharpe": float("nan"),
                "n_trials": 0, "fold_count": len(per_fold_random), "skipped_folds": [], "warning": "no_trials"}

    agg = []
    for trial_idx in range(n_pools_canon):
        # Collect per-fold value at trial_idx; skip folds where this trial failed (None)
        per_fold_i = []
        for d in dense_lists:
            if trial_idx < len(d) and d[trial_idx] is not None:
                v = d[trial_idx]
                if isinstance(v, (int, float)) and np.isfinite(v):
                    per_fold_i.append(float(v))
        # Only aggregate if ALL folds had a valid value for this trial (strict alignment)
        if len(per_fold_i) == len(dense_lists):
            agg.append(float(np.mean(per_fold_i)))
    if not agg:
        return {"p95_sharpe": float("-inf"), "median_sharpe": float("nan"),
                "n_trials": 0, "fold_count": len(sorted_folds), "skipped_folds": [],
                "warning": "no_trials_with_all_folds_valid"}
    # codex m11 r12 fix: STRICT coverage requirement. If fewer than 95% of n_pools trials
    # are aligned-valid, the null distribution is too sparse for a meaningful p95. The spec
    # requires N=300 — accepting n_trials=1 means a strategy can beat a single random pool
    # and pass G6, which is meaningless. Threshold 95% gives small tolerance for rare
    # producer failures while still enforcing meaningful coverage. Below threshold → fail
    # closed (-inf) so Module 10 G6 rejects.
    coverage_pct = len(agg) / n_pools_canon if n_pools_canon > 0 else 0.0
    coverage_warning = None
    if coverage_pct < 0.95:
        return {
            "p95_sharpe": float("-inf"),
            "median_sharpe": float("nan"),
            "n_trials": len(agg),
            "fold_count": len(sorted_folds),
            "skipped_folds": [],
            "warning": (
                f"insufficient_coverage: {len(agg)}/{n_pools_canon} "
                f"({coverage_pct*100:.1f}%) < 95% threshold"
            ),
        }
    if coverage_pct < 1.0:
        coverage_warning = (
            f"partial_coverage: {len(agg)}/{n_pools_canon} ({coverage_pct*100:.1f}%)"
        )
    agg_arr = np.array(agg)
    return {
        "p95_sharpe": float(np.quantile(agg_arr, P95_QUANTILE)),
        "median_sharpe": float(np.quantile(agg_arr, 0.5)),
        "n_trials": len(agg_arr),
        "fold_count": len(sorted_folds),
        "skipped_folds": [],
        "warning": coverage_warning,
    }
