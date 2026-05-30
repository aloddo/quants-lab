#!/usr/bin/env python3
"""V13 Module 09 — Walk-Forward Folds calendar + regime tagging.

Per spec: projects/quant/v13/modules/09-walk-forward-folds

8-fold rolling walk-forward calendar: 30/15/15 day train/val/test, 15-day step.

WHY 30/15/15: train enough to fit ranking + params; val to select; test to evaluate OOS.
60-day window per fold (30+15+15). 15-day step is recent-enough to test latest market state.

Regime REPORTING (not stratification):
- BTC trend 30d at test_start: BULL (>+10%), NEUTRAL (±10%), BEAR (<-10%)
- HL perp index trend 30d at test_start: same buckets
- Volatility (BTC DVOL at test_start): HIGH (>p66), MID, LOW (<p33) of training-window DVOL

Anti-correlation greedy pool selection (Section 6.1):
- Sort eligible wallets by copy_score desc.
- Take top M = 3 × K_target (positive scores only).
- Compute pairwise Pearson on daily copied-return series.
- Greedy fill K_target accepting next wallet if max correlation with selected < threshold.
- If len(selected) < K_target / 2 → ANTI_CORR_PRUNED flag.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import numpy as np


# === Constants (Module 09 spec) ===
TRAIN_DAYS = 30
VAL_DAYS = 15
TEST_DAYS = 15
STEP_DAYS = 15
DEFAULT_N_FOLDS = 8


@dataclass
class Fold:
    n: int
    train_start: date
    train_end: date
    val_start: date
    val_end: date
    test_start: date
    test_end: date
    regime_tags: Optional[dict] = None   # populated after market data lookup


def build_folds(window_start: date, n_folds: int = DEFAULT_N_FOLDS) -> list[Fold]:
    """Build n_folds rolling walk-forward folds starting at window_start.

    Per spec: window_start = train_start of fold 1. Each fold:
        train: [start, start+30d)
        val:   [start+30d, start+45d)
        test:  [start+45d, start+60d)
    Next fold's start = previous start + 15 days (STEP_DAYS).

    Returns list of n_folds Fold objects.
    """
    folds = []
    for i in range(n_folds):
        s = window_start + timedelta(days=i * STEP_DAYS)
        folds.append(Fold(
            n=i + 1,
            train_start=s,
            train_end=s + timedelta(days=TRAIN_DAYS) - timedelta(days=1),
            val_start=s + timedelta(days=TRAIN_DAYS),
            val_end=s + timedelta(days=TRAIN_DAYS + VAL_DAYS) - timedelta(days=1),
            test_start=s + timedelta(days=TRAIN_DAYS + VAL_DAYS),
            test_end=s + timedelta(days=TRAIN_DAYS + VAL_DAYS + TEST_DAYS) - timedelta(days=1),
        ))
    return folds


def _is_valid_positive(v) -> bool:
    """codex m09 r2 fix: shared finite-positive check (rejects NaN/Inf/bool/non-numeric)."""
    if v is None or isinstance(v, bool):
        return False
    if not isinstance(v, (int, float)):
        return False
    if not np.isfinite(v):
        return False
    return v > 0


def classify_btc_trend(btc_price_at_test_start, btc_price_30d_ago) -> str:
    """BTC trend 30d at test_start: BULL (>+10%), NEUTRAL (±10%), BEAR (<-10%).
    codex m09 r2 fix: reject NaN/Inf inputs as UNKNOWN.
    """
    if not _is_valid_positive(btc_price_30d_ago) or not _is_valid_positive(btc_price_at_test_start):
        return "UNKNOWN"
    pct = (btc_price_at_test_start - btc_price_30d_ago) / btc_price_30d_ago
    if pct > 0.10:
        return "BULL"
    if pct < -0.10:
        return "BEAR"
    return "NEUTRAL"


def classify_volatility(dvol_at_test_start, train_window_dvol_series: list[float]) -> str:
    """Volatility bucket: HIGH (>p66 of train DVOL), MID, LOW (<p33).
    codex m09 r2 fix: reject NaN/Inf dvol as UNKNOWN.
    """
    if not _is_valid_positive(dvol_at_test_start):
        return "UNKNOWN"
    if not train_window_dvol_series:
        return "UNKNOWN"
    arr = np.array([v for v in train_window_dvol_series if _is_valid_positive(v)])
    if len(arr) < 5:
        return "UNKNOWN"
    p33, p66 = np.quantile(arr, [0.33, 0.66])
    if dvol_at_test_start > p66:
        return "HIGH"
    if dvol_at_test_start < p33:
        return "LOW"
    return "MID"


def tag_fold_regime(fold: Fold, market_data_fn) -> dict:
    """Populate fold.regime_tags via market_data_fn callable.

    market_data_fn(date) → {btc_price, hl_perp_price, btc_dvol}.
    Returns the regime tags dict (also mutates fold.regime_tags).

    codex m09 r1 CRITICAL fix: 30d lookback for trend uses test_start - 30 days, NOT
    train_start (which is 45d before test_start). Prior code mis-tagged every fold.
    codex m09 r1 fix: missing/None market data → return "UNKNOWN" not falsely BEAR/LOW.
    """
    test_start_md = market_data_fn(fold.test_start)
    # Spec: BTC trend 30d at test_start (NOT 45d via train_start).
    lookback_date = fold.test_start - timedelta(days=30)
    lookback_md = market_data_fn(lookback_date)

    def _safe_get(md_dict, key):
        """codex m09 r2 fix: reject NaN/Inf explicitly (np.isfinite). NaN was passing as
        a valid float and producing falsely-NEUTRAL regime labels."""
        v = md_dict.get(key) if md_dict else None
        if v is None or isinstance(v, bool):
            return None
        if not isinstance(v, (int, float)):
            return None
        if not np.isfinite(v):
            return None
        if v <= 0:
            return None
        return float(v)

    btc_now = _safe_get(test_start_md, "btc_price")
    btc_old = _safe_get(lookback_md, "btc_price")
    btc_trend = classify_btc_trend(btc_now, btc_old) if (btc_now is not None and btc_old is not None) else "UNKNOWN"

    hl_now = _safe_get(test_start_md, "hl_perp_price")
    hl_old = _safe_get(lookback_md, "hl_perp_price")
    hl_trend = classify_btc_trend(hl_now, hl_old) if (hl_now is not None and hl_old is not None) else "UNKNOWN"

    # Build train-window DVOL series
    dvol_series = []
    d = fold.train_start
    while d <= fold.train_end:
        md = market_data_fn(d)
        dvol = _safe_get(md, "btc_dvol")
        if dvol is not None:
            dvol_series.append(dvol)
        d = d + timedelta(days=1)

    dvol_now = _safe_get(test_start_md, "btc_dvol")
    if dvol_now is None:
        vol_bucket = "UNKNOWN"
    else:
        vol_bucket = classify_volatility(dvol_now, dvol_series)

    tags = {"btc_trend": btc_trend, "hl_trend": hl_trend, "vol": vol_bucket}
    fold.regime_tags = tags
    return tags


# === Anti-correlation greedy pool selection ===

def anti_corr_greedy_fill(
    wallets_with_scores: dict[str, float],
    daily_returns_by_wallet: dict[str, np.ndarray],
    threshold: float,
    K_target: int,
    multiplier: int = 3,
    candidate_multiplier: int = None,
) -> tuple[list[str], bool]:
    """Greedy fill K_target wallets accepting next if max correlation with selected < threshold.

    Per Module 09 spec:
    1. Take top M = multiplier × K_target wallets with positive scores.
    2. Top-score wallet always selected first.
    3. For each subsequent candidate (in score desc order), accept if max pairwise Pearson
       (signed, not abs) with all already-selected < threshold.
    4. If final |selected| < K_target / 2 → ANTI_CORR_PRUNED flag = True.

    codex m09 r1 fixes:
    - Use SIGNED correlation (was abs() — wrongly rejected NEGATIVELY-correlated wallets that
      should pass anti-corr per spec).
    - Accept both `multiplier` (spec name) and `candidate_multiplier` (back-compat) kwargs.

    Returns (selected_wallet_list, anti_corr_pruned_flag).
    """
    # codex m09 r1 fix: accept either kwarg name
    if candidate_multiplier is not None:
        multiplier = candidate_multiplier
    M = multiplier * K_target
    # Sort by score desc; positive scores only
    candidates = sorted(
        [(w, s) for w, s in wallets_with_scores.items() if s > 0],
        key=lambda x: -x[1],
    )[:M]
    if not candidates:
        return [], True  # no positives, definitely pruned

    selected = [candidates[0][0]]
    for w, _s in candidates[1:]:
        if len(selected) >= K_target:
            break
        if w not in daily_returns_by_wallet:
            continue
        w_returns = daily_returns_by_wallet[w]
        max_corr = -float("inf")  # tracking SIGNED max
        for s_w in selected:
            if s_w not in daily_returns_by_wallet:
                continue
            s_returns = daily_returns_by_wallet[s_w]
            n = min(len(w_returns), len(s_returns))
            if n < 5:
                continue
            corr = float(np.corrcoef(w_returns[:n], s_returns[:n])[0, 1])
            if np.isnan(corr):
                corr = 0.0
            # codex m09 r1 fix: SIGNED correlation, not abs. Negative corr → diversifying → keep.
            max_corr = max(max_corr, corr)
        # If no valid correlation computed, treat as 0 (accept by default)
        if max_corr == -float("inf"):
            max_corr = 0.0
        if max_corr < threshold:
            selected.append(w)

    pruned = len(selected) < K_target / 2
    return selected, pruned
