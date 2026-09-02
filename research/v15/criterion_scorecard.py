#!/usr/bin/env python3
"""Walk-forward criterion scorecard over the parity-census as-of-07-13 run.

For each candidate wallet-selection criterion, measure whether its AS-OF value (computed from
folds <= k-1 only) PREDICTS next-fold parity copy returns (fold-k seat conservative_roe).
Read-only analysis over existing artifacts -- no engine changes, no experiment.sh stage.
Output: a scorecard table + provenance JSON the quant-engineer sends to Alberto before any
reselection.

INPUT ALLOWLIST (fence, binding): ONLY
  R/m03_folds.parquet, R/m06a_shortlist.parquet, R/m07_test/m07_summary.parquet,
  R/m07_test/m07_positions.parquet, and Mongo hyperliquid_candles BTC 1m closes.
The run dir also contains post-2026-07-13 HOLDOUT material (forward_oos.parquet,
frozen10_fresh/, gap2_fullmirror/) -- NEVER glob the run dir for inputs; touching holdout files
is a fence violation. Every loaded artifact asserts max relevant timestamp <= 2026-07-13 23:59 UTC.
Forbidden criterion inputs: m06a active_test_folds, source_6m_roe_full (never read -- loads are
column-projected). The per-fold m06a entity->wallet mapping is used AS-IS (no re-resolution).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as sps

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
from _streaming_io import install_memory_guard  # noqa: E402

REPO_ROOT = _HERE.parents[1]
DEFAULT_RUN_DIR = REPO_ROOT / "app" / "data" / "v15" / "experiments" / "parity_census_asof0713"
DEFAULT_MONGO_URI = "mongodb://localhost:27017"

# Timestamp fence (pin 6): 2026-07-13 23:59 UTC in epoch ms.
FENCE_MS = int(pd.Timestamp("2026-07-13T23:59:00Z").value // 1_000_000)

EVAL_FOLDS = tuple(range(4, 13))  # k = 4..12 (9 observations, dof 8)

CRITERIA = (
    "C1_boot_p",
    "C2_regime_agree",
    "C3_trajectory",
    "C4_share_gt_1m",
    "C4_share_gt_5m",
    "C4_share_gt_30m",
    "C5_breadth",
    "C5_neg_hhi",
    "C6_decay_winshare",
)
C4_FAMILY = ("C4_share_gt_1m", "C4_share_gt_5m", "C4_share_gt_30m")
C4_THRESH_MIN = {"C4_share_gt_1m": 1.0, "C4_share_gt_5m": 5.0, "C4_share_gt_30m": 30.0}
C6_HALF_LIFE_FOLDS = 2.0

PREREGISTRATION = """\
================================================================================
PRE-REGISTRATION -- V15 WALK-FORWARD CRITERION SCORECARD (frozen before computation)
================================================================================
Run basis: parity census as-of 2026-07-13 (folds 1..12, 14d non-overlapping test windows
2026-01-26 .. 2026-07-13). Timestamp fence: every loaded artifact must have max relevant
timestamp <= 2026-07-13 23:59 UTC.
Inputs (allowlist): m03_folds.parquet; m06a_shortlist.parquet (per-fold entity->primary_wallet
mapping, used as-is, no re-resolution); m07_test/m07_summary.parquet; m07_test/
m07_positions.parquet; Mongo hyperliquid_candles BTC 1m closes (fold regime labels only).
Forbidden criterion inputs: m06a active_test_folds, source_6m_roe_full.

PANEL. Eval folds k = 4..12 (9 observations, dof 8). A row is (wallet, k) where the wallet
(primary_wallet) has >= 1 seat in folds <= k-1 AND a seat in fold k. X (criterion value) is
computed from folds <= k-1 ONLY. y_primary = fold-k seat conservative_roe; y_sens = roe_engine.
Zero-fill seats (n_fills = 0, y ~ 0) are KEPT and their share is reported. Deciles are assigned
WITHIN-fold, then averaged across folds. Prior-fold seat 'roe' on the X side = conservative_roe
(same definition as y_primary).

CRITERIA (9 pre-registered columns; expected IC sign + for all):
 C1_boot_p        : one-sided bootstrap p of H0 mean(prior-fold per-journey r_i) <= 0, pooled over
                    the wallet's prior-fold positions (transplant of cycle-1's _boot_p_mean_gt:
                    n_boot=4000, seed=12345, margin=0, H0-centered resampling; p=1.0 for n<5).
                    X = -p (higher = better). Journey window ends <= fold-k test_start by
                    construction (prior folds only).
 C2_regime_agree  : sign-agreement of the wallet's mean seat roe across BTC-up vs BTC-down prior
                    folds: X = sign(mean roe | up-folds) + sign(mean roe | down-folds), in
                    {-2,-1,0,+1,+2} (+2 = profitable in BOTH regimes; agreement-in-loss scores
                    lowest, consistent with the declared + direction). NA unless >= 2 prior folds
                    in EACH regime bucket. Fold regime = sign of BTC return over the fold's test
                    window (last 1m close before test_end_excl vs close at test_start; up if > 0
                    else down).
 C3_trajectory    : OLS slope of the wallet's fold roe vs fold index over folds <= k-1.
                    NA unless >= 3 prior folds with a seat.
 C4_share_gt_{1m,5m,30m} : signed share of prior-fold realized PnL (realized_pnl_after_cost)
                    earned by positions with holding duration > T: X = sum(pnl | dur > T) /
                    sum(|pnl|), durations (exit_ts - entry_ts) clamped below at 0 (clock jitter,
                    min observed -1.0m). NA if sum(|pnl|) = 0 (incl. no prior positions). ONE
                    pre-registered family: reported individually; the FAMILY passes only if all
                    three agree in sign.
 C5_breadth       : count of distinct coins with positive cumulative prior-fold PnL (0 if no
                    prior positions).
 C5_neg_hhi       : X = -HHI of per-coin |cumulative PnL| concentration (shares of
                    |sum pnl per coin|; higher = better = less concentrated). NA if all-zero.
 C6_decay_winshare: exp-decay-weighted share of positive prior folds, weight 0.5^((k-1-f)/2)
                    (half-life 2 folds). NA if < 2 prior folds.

SCORING. Per criterion per eval fold k: cross-sectional Spearman rank-IC(X, y). Reported per
criterion: mean IC, plain t, Newey-West lag-2 t (across the 9 fold ICs), sign-count (/9),
top-decile mean y, bottom-decile mean y, coverage n. PASS iff (1) one-sided NW t >= 2 in the
declared direction on y_primary; (2) BH q <= 0.10 within the 9-column family (multiplicity
penalty PINNED at 9 even when a column's p is undefined); (3) IC in the declared direction in
>= 6/9 folds; (4) verdict unchanged under y_sens. Tiers: PASS / WEAK (clears (1) only) / FAIL.
Wrong-sign significance = FAIL. Degenerate-fold handling (pre-specified): a fold contributes NO
IC when its cross-sectional IC is undefined (constant y or constant/structurally-NA X, e.g. a
fold whose seats all have n_fills=0 and hence constant y, or C2 before both regimes have >= 2
prior folds); t statistics then use the observed fold-IC series with dof = n_obs - 1, the
sign-count denominator stays 9, and every such fold is reported loudly in the DATA NOTES.
Each criterion is reported on its own coverage AND on the intersection panel (rows where ALL
criteria are defined); the dial proposal reads from the intersection.
================================================================================
"""

CRITERION_SPEC_SHA256 = hashlib.sha256(PREREGISTRATION.encode("utf-8")).hexdigest()

Y_DEFINITIONS = {
    "y_primary": "m07_test/m07_summary.conservative_roe of the (wallet, fold-k) seat",
    "y_sens": "m07_test/m07_summary.roe_engine of the (wallet, fold-k) seat",
}


# --------------------------------------------------------------------------- #
# Fence + loaders (input allowlist -- pin 6)
# --------------------------------------------------------------------------- #
ALLOWLISTED_INPUTS = (
    "m03_folds.parquet",
    "m06a_shortlist.parquet",
    "m07_test/m07_summary.parquet",
    "m07_test/m07_positions.parquet",
)
HOLDOUT_LOCATIONS = ("forward_oos.parquet", "frozen10_fresh", "gap2_fullmirror")


def _resolve_run_file(run_dir: Path, rel: str) -> Path:
    """Resolve a run-dir file to its REAL path and verify identity: the fully resolved path must
    equal resolved(run_dir)/rel -- no symlinked file or intermediate directory may redirect the
    read anywhere else -- and must not be inside any holdout location."""
    base = Path(run_dir).resolve(strict=True)
    real = (Path(run_dir) / rel).resolve(strict=True)
    expected = base / rel
    if real != expected:
        raise ValueError(
            f"ALLOWLIST VIOLATION: {Path(run_dir) / rel} resolves to {real}, not the allowlisted "
            f"artifact {expected} -- symlinked/relocated inputs are refused (holdout smuggling "
            f"guard)")
    for h in HOLDOUT_LOCATIONS:
        hp = base / h
        if real == hp or hp in real.parents:
            raise ValueError(f"ALLOWLIST VIOLATION: {real} is holdout material ({h})")
    return real


def _resolve_allowlisted(run_dir: Path, rel: str) -> Path:
    """Resolve an allowlisted artifact to its REAL path and verify identity. A filename alone is
    not a fence: an allowlisted name could be a symlink into the holdout material."""
    if rel not in ALLOWLISTED_INPUTS:
        raise ValueError(f"ALLOWLIST VIOLATION: {rel!r} is not an allowlisted input")
    return _resolve_run_file(run_dir, rel)


def _assert_fence(what: str, max_ms) -> None:
    """Every loaded artifact must have max relevant timestamp <= 2026-07-13 23:59 UTC."""
    if max_ms is None:
        raise ValueError(f"fence check for {what}: no timestamp available (empty artifact?)")
    max_ms = int(max_ms)
    if max_ms > FENCE_MS:
        raise ValueError(
            f"FENCE VIOLATION: {what} max relevant timestamp {max_ms} "
            f"({pd.Timestamp(max_ms, unit='ms', tz='UTC')}) exceeds the 2026-07-13 23:59 UTC "
            f"fence ({FENCE_MS}). Holdout material must never enter the scorecard.")


def _fence_series(what: str, ms_values) -> dict:
    """Fail-closed fence over a whole timestamp column: empty or null timestamps REFUSE (a null
    row cannot prove it is pre-fence), then the max is checked. Returns {min_ms, max_ms}."""
    s = pd.Series(ms_values)
    if len(s) == 0:
        raise ValueError(f"FENCE VIOLATION: {what} is empty -- cannot prove pre-fence "
                         f"(fail closed)")
    n_null = int(s.isna().sum())
    if n_null:
        raise ValueError(f"FENCE VIOLATION: {what} has {n_null} null timestamps -- null rows "
                         f"cannot prove they are pre-fence (fail closed)")
    mx = int(s.max())
    _assert_fence(what, mx)
    return {"min_ms": int(s.min()), "max_ms": mx}


def _dt_ms(s: pd.Series) -> pd.Series:
    """datetime64 column -> epoch-ms int64 (caller must have already null-checked)."""
    return pd.to_datetime(s).astype("datetime64[ms]").astype("int64")


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _input_meta(path: Path, columns, ts_extrema: dict) -> dict:
    return {"path": str(path), "sha256": _sha256_file(path), "bytes": path.stat().st_size,
            "columns_read": list(columns), "ts_extrema_ms": ts_extrema}


def load_folds(run_dir: Path) -> pd.DataFrame:
    path = _resolve_allowlisted(run_dir, "m03_folds.parquet")
    cols = ["fold_id", "test_start", "test_end_excl"]
    df = pd.read_parquet(path, columns=cols)
    if sorted(df["fold_id"].tolist()) != list(range(1, 13)):
        raise ValueError(f"m03_folds: expected folds 1..12, got {sorted(df['fold_id'].tolist())}")
    if df["test_start"].isna().any() or df["test_end_excl"].isna().any():
        raise ValueError("FENCE VIOLATION: m03_folds has null test window timestamps "
                         "(fail closed)")
    if not (df["test_start"] < df["test_end_excl"]).all():
        raise ValueError("m03_folds: test_start >= test_end_excl (window ordering broken)")
    ext = {"test_start": _fence_series("m03_folds.test_start", _dt_ms(df["test_start"])),
           "test_end_excl": _fence_series("m03_folds.test_end_excl",
                                          _dt_ms(df["test_end_excl"]))}
    df = df.sort_values("fold_id").reset_index(drop=True)
    df.attrs["input_meta"] = _input_meta(path, cols, ext)
    return df


def load_mapping(run_dir: Path) -> pd.DataFrame:
    """Per-fold entity->primary_wallet mapping from m06a, used AS-IS. Column-projected: the
    forbidden criterion inputs (active_test_folds, source_6m_roe_full) are never read."""
    path = _resolve_allowlisted(run_dir, "m06a_shortlist.parquet")
    cols = ["entity_id", "primary_wallet", "fold_id", "in_shortlist", "as_of_ms"]
    df = pd.read_parquet(path, columns=cols)
    ext = {"as_of_ms": _fence_series("m06a_shortlist.as_of_ms", df["as_of_ms"])}
    df = df[df["in_shortlist"]].drop(columns=["in_shortlist", "as_of_ms"]).reset_index(drop=True)
    if df.duplicated(["entity_id", "fold_id"]).any():
        raise ValueError("m06a_shortlist: duplicate (entity_id, fold_id) rows in shortlist")
    multi = df.groupby(["fold_id", "primary_wallet"])["entity_id"].nunique()
    if (multi > 1).any():
        raise ValueError("m06a_shortlist: a primary_wallet maps to >1 entity within a fold; "
                         "the (wallet, fold) seat is no longer unique -- refusing to score")
    df.attrs["input_meta"] = _input_meta(path, cols, ext)
    return df


def load_summary(run_dir: Path, extra_cols: tuple = ()) -> pd.DataFrame:
    path = _resolve_allowlisted(run_dir, "m07_test/m07_summary.parquet")
    cols = ["entity_id", "fold_id", "conservative_roe", "roe_engine", "n_fills",
            "window_end_ms", *extra_cols]
    df = pd.read_parquet(path, columns=cols)
    ext = {"window_end_ms": _fence_series("m07_summary.window_end_ms", df["window_end_ms"])}
    df = df.drop(columns=["window_end_ms"])
    df.attrs["input_meta"] = _input_meta(path, cols, ext)
    return df


def load_positions(run_dir: Path) -> pd.DataFrame:
    path = _resolve_allowlisted(run_dir, "m07_test/m07_positions.parquet")
    cols = ["entity_id", "fold_id", "coin", "entry_ts", "exit_ts",
            "realized_pnl_after_cost", "r_i"]
    df = pd.read_parquet(path, columns=cols)
    ext = {"entry_ts": _fence_series("m07_positions.entry_ts", df["entry_ts"]),
           "exit_ts": _fence_series("m07_positions.exit_ts", df["exit_ts"])}
    df.attrs["input_meta"] = _input_meta(path, cols, ext)
    return df


def load_btc_regimes(mongo_uri: str, folds: pd.DataFrame,
                     collection=None) -> tuple[dict[int, str], list[dict]]:
    """Fold regime label from Mongo BTC 1m closes: BTC return over the fold's test window;
    'up' if > 0 else 'down'. Returns (labels, per-fold boundary observations for provenance).
    `collection` is injectable for tests (fence checks apply either way)."""
    if collection is None:
        from pymongo import MongoClient
        collection = MongoClient(mongo_uri)["quants_lab"]["hyperliquid_candles"]

    def close_at(ms: int) -> tuple[float, int]:
        if ms > FENCE_MS:
            raise ValueError(f"FENCE VIOLATION: BTC close requested at {ms} > fence {FENCE_MS}")
        doc = collection.find_one(
            {"coin": "BTC", "interval": "1m",
             "timestamp_utc": {"$lte": ms, "$gte": ms - 86_400_000}},
            sort=[("timestamp_utc", -1)])
        if doc is None:
            raise ValueError(f"hyperliquid_candles: no BTC 1m close at-or-before {ms}")
        _assert_fence("hyperliquid_candles.timestamp_utc", doc["timestamp_utc"])
        return float(doc["close"]), int(doc["timestamp_utc"])

    regimes: dict[int, str] = {}
    observations: list[dict] = []
    for r in folds.itertuples():
        start_ms = int(pd.Timestamp(r.test_start).value // 1_000_000)
        end_ms = int(pd.Timestamp(r.test_end_excl).value // 1_000_000) - 60_000
        c0, t0 = close_at(start_ms)
        c1, t1 = close_at(end_ms)
        ret = c1 / c0 - 1.0
        regimes[int(r.fold_id)] = "up" if ret > 0.0 else "down"
        observations.append({"fold_id": int(r.fold_id), "start_query_ms": start_ms,
                             "end_query_ms": end_ms, "start_ts_used": t0, "end_ts_used": t1,
                             "start_close": c0, "end_close": c1, "ret": ret,
                             "regime": regimes[int(r.fold_id)]})
    return regimes, observations


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def _boot_p_mean_gt(x: np.ndarray, margin: float, n_boot: int = 4000, seed: int = 12345,
                    chunk_elems: int = 8_000_000) -> float:
    """One-sided bootstrap p-value for H0: mean(x) <= margin. TRANSPLANT of cycle-1's statistic
    (research/v15/v15_m06b_ranking.py::_boot_p_mean_gt): resamples the H0-centered data (shift so
    mean == margin) and returns P(bootstrap mean >= observed mean); p = 1.0 for n < 5 or
    obs <= margin. Only change: the (n_boot, n) index matrix is drawn in row chunks of at most
    `chunk_elems` elements to bound RAM -- numpy Generator.integers consumes the bit stream
    sequentially in C order, so chunked draws are element-identical to the single call
    (asserted in tests/v15/test_criterion_scorecard.py)."""
    x = np.asarray(x, dtype="float64")
    x = x[np.isfinite(x)]
    n = x.size
    if n < 5:
        return 1.0
    obs = float(x.mean())
    if obs <= margin:
        return 1.0
    rng = np.random.default_rng(seed)
    null = x - obs + margin  # H0: mean == margin
    rows_per_chunk = max(1, int(chunk_elems // n))
    n_ge = 0
    done = 0
    while done < n_boot:
        rows = min(rows_per_chunk, n_boot - done)
        idx = rng.integers(0, n, size=(rows, n))
        n_ge += int((null[idx].mean(axis=1) >= obs).sum())
        done += rows
    return n_ge / n_boot


def nw_tstat(ics, lag: int = 2) -> float:
    """t-stat of the mean of a short time series with Newey-West (Bartlett) HAC variance,
    truncation `lag`. Used across the per-fold IC series (lag 2)."""
    x = np.asarray(ics, dtype="float64")
    x = x[np.isfinite(x)]
    n = x.size
    if n < 2:
        return float("nan")
    m = float(x.mean())
    e = x - m
    var = float(np.dot(e, e)) / n
    for j in range(1, min(lag, n - 1) + 1):
        gamma_j = float(np.dot(e[j:], e[:-j])) / n
        var += 2.0 * (1.0 - j / (lag + 1.0)) * gamma_j
    if var <= 0.0:
        return math.copysign(math.inf, m) if m != 0.0 else 0.0
    return m / math.sqrt(var / n)


def plain_tstat(ics) -> float:
    x = np.asarray(ics, dtype="float64")
    x = x[np.isfinite(x)]
    if x.size < 2:
        return float("nan")
    sd = float(x.std(ddof=1))
    if sd == 0.0:
        return math.copysign(math.inf, x.mean()) if x.mean() != 0.0 else 0.0
    return float(x.mean() / (sd / math.sqrt(x.size)))


def bh_q(pvals: pd.Series, family_size: int | None = None) -> pd.Series:
    """Benjamini-Hochberg step-up q-values. When `family_size` is given (production: the 9
    pre-registered columns) the multiplicity penalty m is PINNED to it -- a criterion whose p is
    undefined gets q=NaN but can never shrink the family's penalty below the declared size.
    Without `family_size`, m falls back to the number of finite p-values."""
    p = pvals.astype(float)
    mask = p.notna()
    n_finite = int(mask.sum())
    if family_size is not None and n_finite > family_size:
        raise ValueError(f"bh_q: {n_finite} finite p-values exceed the declared family size "
                         f"{family_size}")
    m = family_size if family_size is not None else n_finite
    q = pd.Series(np.nan, index=p.index)
    if n_finite == 0:
        return q
    sub = p[mask].sort_values()
    ranks = np.arange(1, n_finite + 1)
    raw = sub.to_numpy() * m / ranks
    adj = np.minimum.accumulate(raw[::-1])[::-1]
    q.loc[sub.index] = np.clip(adj, 0.0, 1.0)
    return q


# --------------------------------------------------------------------------- #
# Panel + criteria
# --------------------------------------------------------------------------- #
def build_seats(summary: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """One row per (primary_wallet, fold) seat: y columns + n_fills. Every m07 seat must map."""
    seats = summary.merge(mapping[["entity_id", "fold_id", "primary_wallet"]],
                          on=["entity_id", "fold_id"], how="left", validate="one_to_one")
    if seats["primary_wallet"].isna().any():
        n = int(seats["primary_wallet"].isna().sum())
        raise ValueError(f"{n} m07 seats have no m06a shortlist wallet mapping")
    return seats


def attach_wallets_to_positions(positions: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    pos = positions.merge(mapping[["entity_id", "fold_id", "primary_wallet"]],
                          on=["entity_id", "fold_id"], how="left")
    if pos["primary_wallet"].isna().any():
        n = int(pos["primary_wallet"].isna().sum())
        raise ValueError(f"{n} m07 positions have no m06a shortlist wallet mapping")
    dur_min = (pos["exit_ts"] - pos["entry_ts"]) / 60_000.0
    pos["dur_min"] = dur_min.clip(lower=0.0)  # clamp clock jitter (min observed -1.0m)
    pos["neg_dur"] = dur_min < 0.0
    pos["abs_pnl"] = pos["realized_pnl_after_cost"].abs()
    return pos


def _c1_boot_series(r_by_wallet: pd.core.groupby.SeriesGroupBy) -> pd.Series:
    out = {}
    for wallet, vals in r_by_wallet:
        out[wallet] = -_boot_p_mean_gt(vals.to_numpy(dtype="float64"), 0.0)
    return pd.Series(out, dtype="float64")


def compute_panel(seats: pd.DataFrame, positions: pd.DataFrame, regimes: dict[int, str],
                  eval_folds=EVAL_FOLDS) -> pd.DataFrame:
    """Panel rows (wallet, k) with y_primary / y_sens / zero_fill + the 9 criterion columns,
    each computed from folds <= k-1 ONLY."""
    seats = seats.copy()
    seats["regime"] = seats["fold_id"].map(regimes)
    first_fold = seats.groupby("primary_wallet")["fold_id"].min()

    rows = []
    for k in eval_folds:
        cur = seats[seats["fold_id"] == k]
        cur = cur[cur["primary_wallet"].map(first_fold) < k]  # >=1 prior-fold seat
        if cur.empty:
            continue
        wallets = pd.Index(cur["primary_wallet"].unique(), name="primary_wallet")

        prior = seats[(seats["fold_id"] < k) & seats["primary_wallet"].isin(wallets)].copy()
        prior["fy"] = prior["fold_id"] * prior["conservative_roe"]
        prior["f2"] = prior["fold_id"].astype("float64") ** 2

        # C3 trajectory: OLS slope of roe vs fold index (closed form), NA unless >= 3 prior folds.
        agg = prior.groupby("primary_wallet").agg(
            n_prior=("fold_id", "size"), mf=("fold_id", "mean"),
            my=("conservative_roe", "mean"), mfy=("fy", "mean"), mf2=("f2", "mean"))
        var_f = agg["mf2"] - agg["mf"] ** 2
        c3 = (agg["mfy"] - agg["mf"] * agg["my"]) / var_f.where(var_f > 0)
        c3 = c3.where(agg["n_prior"] >= 3)

        # C2 regime sign-agreement: NA unless >= 2 prior folds in EACH bucket.
        reg = prior.groupby(["primary_wallet", "regime"])["conservative_roe"].agg(["size", "mean"])
        reg = reg.unstack("regime")
        n_up = reg.get(("size", "up"), pd.Series(dtype=float)).reindex(wallets).fillna(0)
        n_dn = reg.get(("size", "down"), pd.Series(dtype=float)).reindex(wallets).fillna(0)
        mu_up = reg.get(("mean", "up"), pd.Series(dtype=float)).reindex(wallets)
        mu_dn = reg.get(("mean", "down"), pd.Series(dtype=float)).reindex(wallets)
        c2 = (np.sign(mu_up) + np.sign(mu_dn)).where((n_up >= 2) & (n_dn >= 2))

        # C6 exp-decay-weighted share of positive prior folds (half-life 2), NA if < 2 prior folds.
        w = 0.5 ** ((k - 1 - prior["fold_id"]) / C6_HALF_LIFE_FOLDS)
        c6_df = pd.DataFrame({"primary_wallet": prior["primary_wallet"], "w": w,
                              "win_w": w * (prior["conservative_roe"] > 0)})
        c6_g = c6_df.groupby("primary_wallet")[["w", "win_w"]].sum()
        c6 = (c6_g["win_w"] / c6_g["w"]).where(agg["n_prior"].reindex(c6_g.index) >= 2)

        # Position-derived criteria over prior-fold positions.
        pp = positions[(positions["fold_id"] <= k - 1)
                       & positions["primary_wallet"].isin(wallets)]
        pg = pp.groupby("primary_wallet")
        c1 = _c1_boot_series(pg["r_i"])
        abs_pnl = pg["abs_pnl"].sum()
        c4 = {}
        for name, t_min in C4_THRESH_MIN.items():
            num = pp.loc[pp["dur_min"] > t_min].groupby("primary_wallet")[
                "realized_pnl_after_cost"].sum().reindex(abs_pnl.index).fillna(0.0)
            c4[name] = (num / abs_pnl.where(abs_pnl > 0)).astype(float)
        coin_pnl = pp.groupby(["primary_wallet", "coin"], observed=True)[
            "realized_pnl_after_cost"].sum()
        c5_breadth = (coin_pnl > 0).groupby("primary_wallet").sum().astype(float)
        abs_coin = coin_pnl.abs()
        tot_abs_coin = abs_coin.groupby("primary_wallet").sum()
        hhi = ((abs_coin / tot_abs_coin.reindex(abs_coin.index.get_level_values(0)).values) ** 2
               ).groupby("primary_wallet").sum()
        c5_hhi = (-hhi).where(tot_abs_coin > 0)

        out = pd.DataFrame({
            "primary_wallet": cur["primary_wallet"].to_numpy(),
            "fold_id": k,
            "y_primary": cur["conservative_roe"].to_numpy(dtype="float64"),
            "y_sens": cur["roe_engine"].to_numpy(dtype="float64"),
            "zero_fill": (cur["n_fills"] == 0).to_numpy(),
            "n_prior_folds": agg["n_prior"].reindex(cur["primary_wallet"]).to_numpy(),
        })
        wl = cur["primary_wallet"]
        out["C1_boot_p"] = c1.reindex(wl).to_numpy()
        out["C1_boot_p"] = out["C1_boot_p"].fillna(-1.0)  # no prior positions -> p = 1.0 (n < 5)
        out["C2_regime_agree"] = c2.reindex(wl).to_numpy()
        out["C3_trajectory"] = c3.reindex(wl).to_numpy()
        for name in C4_FAMILY:
            out[name] = c4[name].reindex(wl).to_numpy()
        out["C5_breadth"] = c5_breadth.reindex(wl).fillna(0.0).to_numpy()  # no positions -> 0
        out["C5_neg_hhi"] = c5_hhi.reindex(wl).to_numpy()
        out["C6_decay_winshare"] = c6.reindex(wl).to_numpy()
        rows.append(out)

    return pd.concat(rows, ignore_index=True)


# --------------------------------------------------------------------------- #
# Scoring
# --------------------------------------------------------------------------- #
def score_family(panel: pd.DataFrame, criteria=CRITERIA, eval_folds=EVAL_FOLDS,
                 y_col: str = "y_primary", min_sign_folds: int = 6,
                 min_fold_rows: int = 3, min_decile_rows: int = 10) -> pd.DataFrame:
    """Per-criterion walk-forward stats on `panel` against `y_col`. One BH family = `criteria`."""
    recs = []
    undefined_map: dict[str, dict[int, str]] = {}
    for c in criteria:
        fold_ics, tops, bots, cov = {}, [], [], 0
        undefined: dict[int, str] = {}
        for k in eval_folds:
            sub = panel[(panel["fold_id"] == k)
                        & panel[c].notna() & panel[y_col].notna()]
            n = len(sub)
            ic = np.nan
            if n == 0:
                undefined[int(k)] = "no_rows"
            elif n < min_fold_rows:
                undefined[int(k)] = "n_lt_min_rows"
            elif sub[c].nunique() <= 1:
                undefined[int(k)] = "constant_X"
            elif sub[y_col].nunique() <= 1:
                undefined[int(k)] = "constant_y"
            else:
                ic = float(sps.spearmanr(sub[c], sub[y_col]).statistic)
                if np.isnan(ic):
                    undefined[int(k)] = "ic_nan"
            fold_ics[int(k)] = None if np.isnan(ic) else ic
            cov += n
            if n >= min_decile_rows:
                r = sub[c].rank(method="first")
                dec = np.ceil(r * 10.0 / n).astype(int).clip(1, 10)
                tops.append(float(sub.loc[dec == 10, y_col].mean()))
                bots.append(float(sub.loc[dec == 1, y_col].mean()))
        ics = np.array([v for v in fold_ics.values() if v is not None], dtype="float64")
        n_ic = ics.size
        nw_t = nw_tstat(ics, lag=2)
        p_1s = float(sps.t.sf(nw_t, df=n_ic - 1)) if (n_ic >= 2 and np.isfinite(nw_t)) else (
            0.0 if nw_t == math.inf else 1.0 if nw_t == -math.inf else np.nan)
        recs.append({
            "criterion": c,
            "coverage_n": int(cov),
            "n_fold_ics": int(n_ic),
            "mean_ic": float(ics.mean()) if n_ic else np.nan,
            "t_plain": plain_tstat(ics),
            "nw_t": nw_t,
            "p_1s": p_1s,
            "sign_count": int((ics > 0).sum()),
            "n_eval_folds": len(eval_folds),
            "top_decile_y": float(np.mean(tops)) if tops else np.nan,
            "bottom_decile_y": float(np.mean(bots)) if bots else np.nan,
            "fold_ics": json.dumps(fold_ics),
        })
        if undefined:
            undefined_map[c] = undefined
    df = pd.DataFrame(recs).set_index("criterion")
    # Frozen reporting rule: EVERY undefined fold-IC (with its reason) is surfaced -- main()
    # prints these in DATA NOTES and records them in provenance.
    df.attrs["fold_ic_undefined"] = undefined_map
    df["q_bh"] = bh_q(df["p_1s"], family_size=len(criteria))
    df["gate1_nw"] = df["nw_t"] >= 2.0
    df["gate2_bh"] = df["q_bh"] <= 0.10
    df["gate3_sign"] = df["sign_count"] >= min_sign_folds
    df["g123"] = df["gate1_nw"] & df["gate2_bh"] & df["gate3_sign"]
    return df


def assign_tiers(primary: pd.DataFrame, sens: pd.DataFrame) -> pd.DataFrame:
    """PASS iff gates (1)-(3) on y_primary AND the same verdict under y_sens (gate 4).
    WEAK = clears gate (1) only. Wrong-sign significance (NW t <= -2) = FAIL."""
    out = primary.copy()
    out["nw_t_sens"] = sens["nw_t"]
    out["q_bh_sens"] = sens["q_bh"]
    out["sign_count_sens"] = sens["sign_count"]
    out["g123_sens"] = sens["g123"]
    out["gate4_sens_unchanged"] = out["g123"] == out["g123_sens"]
    tiers = []
    for _, r in out.iterrows():
        if np.isfinite(r["nw_t"]) and r["nw_t"] <= -2.0:
            tiers.append("FAIL")  # wrong-sign significance
        elif bool(r["g123"]) and bool(r["gate4_sens_unchanged"]):
            tiers.append("PASS")
        elif bool(r["gate1_nw"]):
            tiers.append("WEAK")
        else:
            tiers.append("FAIL")
    out["tier"] = tiers
    return out


def c4_family_verdict(scored: pd.DataFrame) -> dict:
    """The C4 family passes only if all three members agree in sign (and each member PASSes)."""
    mean_ics = {c: scored.loc[c, "mean_ic"] for c in C4_FAMILY}
    signs = {c: np.sign(v) if np.isfinite(v) else 0.0 for c, v in mean_ics.items()}
    agree = len({s for s in signs.values()}) == 1 and 0.0 not in signs.values()
    all_pass = all(scored.loc[c, "tier"] == "PASS" for c in C4_FAMILY)
    return {"mean_ics": {c: (None if not np.isfinite(v) else round(float(v), 4))
                         for c, v in mean_ics.items()},
            "sign_agreement": bool(agree),
            "family_pass": bool(agree and all_pass)}


# --------------------------------------------------------------------------- #
# PRELIMINARY cohort (--rank-latest)
# --------------------------------------------------------------------------- #
PRELIM_LABEL = ("PRELIMINARY -- evidence through 2026-07-13; selection NOT frozen; "
                "no capital decision")
LATEST_EVAL_FOLD = 12   # X from folds <= 11: the maximal CLEAN prior-fold set (fold-12
#                         outcomes are degenerate in the artifact; positions end at fold 11)
VETO_FILE = "hard_gates_report.csv"          # cycle-1 standing hard gates (wallet-level)
VETO_FLAG_COLS = ("uw_add_ok", "mae_p90_ok", "liq_ok")  # martingale / MAE / liquidation vetoes
RANK_CRITERIA = ("C6_decay_winshare", "C2_regime_agree")      # PASS tier: rank by these
FILTER_CRITERIA = ("C1_boot_p", "C3_trajectory", "C4_share_gt_1m")  # PASS tier: median filter


def load_vetoes(run_dir: Path) -> tuple[pd.DataFrame | None, dict]:
    """Standing wallet-level hard-veto flags from the cycle-1 hard-gates report, reused AS-IS
    (martingale/underwater-add, MAE, liquidation). Returns (flags keyed by wallet, meta).
    Missing artifact -> (None, {available: False}); wallets absent from the report are NOT
    vetoed (no data) and are marked gate_data=False."""
    if not (Path(run_dir) / VETO_FILE).exists():
        return None, {"available": False, "file": VETO_FILE}
    path = _resolve_run_file(run_dir, VETO_FILE)
    df = pd.read_csv(path, usecols=["wallet", *VETO_FLAG_COLS])
    df["wallet"] = df["wallet"].str.lower()
    df = df.drop_duplicates("wallet").rename(columns={"wallet": "primary_wallet"})
    meta = {"available": True, "file": VETO_FILE, "path": str(path),
            "sha256": _sha256_file(path), "n_wallets": int(len(df)),
            "flags": list(VETO_FLAG_COLS)}
    return df, meta


def prior_fold_stats(seats: pd.DataFrame, upto_fold: int) -> pd.DataFrame:
    """Per-wallet stats over prior folds <= upto_fold: active-fold count (n_fills > 0) and
    total conservative PnL (if the column is present)."""
    prior = seats[seats["fold_id"] <= upto_fold].copy()
    prior["active"] = prior["n_fills"] > 0
    agg = {"n_prior_active_folds": ("active", "sum")}
    if "conservative_pnl_total" in prior.columns:
        agg["prior_conservative_pnl"] = ("conservative_pnl_total", "sum")
    out = prior.groupby("primary_wallet").agg(**agg)
    out["n_prior_active_folds"] = out["n_prior_active_folds"].astype(int)
    return out


def build_preliminary_cohort(panel: pd.DataFrame, vetoes: pd.DataFrame | None = None,
                             top_n: int | None = 20, min_active_folds: int = 3
                             ) -> tuple[pd.DataFrame, dict]:
    """Apply the validated scorecard verdict to the latest as-of criterion values.
    Universe = the panel rows passed in (latest eval fold). Steps, in order:
      1. require >= `min_active_folds` prior ACTIVE folds (n_fills > 0);
      2. require the rank criteria (C6, C2) defined;
      3. median filter: each of C1/C3/C4@1m must be >= its within-universe median
         (medians computed on the post-step-2 universe; declared direction is + for all);
      4. standing hard vetoes: a wallet with ANY flag explicitly False is excluded;
         wallets absent from the gate report are kept and marked gate_data=False;
      5. rank by the AVERAGE of within-universe ranks of C6 and C2 (higher value = better
         rank), tie-broken by C6 then prior PnL then wallet; keep top_n."""
    meta: dict = {"universe": int(len(panel)), "min_active_folds": min_active_folds}
    u = panel[panel["n_prior_active_folds"] >= min_active_folds]
    meta["after_active_filter"] = int(len(u))
    u = u[u[list(RANK_CRITERIA)].notna().all(axis=1)]
    meta["after_rank_defined"] = int(len(u))
    medians = {c: float(u[c].median()) for c in FILTER_CRITERIA}
    meta["filter_medians"] = medians
    for c in FILTER_CRITERIA:
        u = u[u[c].notna() & (u[c] >= medians[c])]
    meta["after_median_filter"] = int(len(u))
    u = u.copy()
    if vetoes is not None:
        u = u.merge(vetoes, on="primary_wallet", how="left")
        flag = u[list(VETO_FLAG_COLS)]
        u["gate_data"] = flag.notna().all(axis=1)
        u["vetoed"] = (flag == False).any(axis=1)  # noqa: E712 -- NaN (no data) is NOT a veto
        meta["gate_data_coverage"] = int(u["gate_data"].sum())
        meta["vetoed_excluded"] = int(u["vetoed"].sum())
        u = u[~u["vetoed"]]
    else:
        u["gate_data"] = False
        u["vetoed"] = False
        meta["gate_data_coverage"] = 0
        meta["vetoed_excluded"] = 0
    meta["after_vetoes"] = int(len(u))
    u["rank_c6"] = u["C6_decay_winshare"].rank(ascending=False, method="average")
    u["rank_c2"] = u["C2_regime_agree"].rank(ascending=False, method="average")
    u["combined_rank"] = (u["rank_c6"] + u["rank_c2"]) / 2.0
    sort_cols = ["combined_rank", "C6_decay_winshare", "prior_conservative_pnl",
                 "primary_wallet"]
    sort_asc = [True, False, False, True]
    have = [c in u.columns for c in sort_cols]
    u = u.sort_values([c for c, h in zip(sort_cols, have) if h],
                      ascending=[a for a, h in zip(sort_asc, have) if h])
    cohort = (u if top_n is None else u.head(top_n)).reset_index(drop=True)
    cohort.insert(0, "rank", np.arange(1, len(cohort) + 1))
    cohort["label"] = "PRELIMINARY"
    meta["top_n"] = int(len(cohort))
    return cohort, meta


_COHORT_COLS = ["rank", "primary_wallet", "combined_rank", "C6_decay_winshare",
                "C2_regime_agree", "C1_boot_p", "C3_trajectory", "C4_share_gt_1m",
                "prior_conservative_pnl", "n_prior_folds", "n_prior_active_folds",
                "gate_data", "vetoed", "label"]


def format_cohort_text(cohort: pd.DataFrame, meta: dict) -> str:
    hdr = (f"{'#':>2} {'wallet':<42} {'cmbR':>6} {'C6':>6} {'C2':>4} {'C1_p':>7} "
           f"{'C3_slope':>9} {'C4@1m':>7} {'priorPnL':>11} {'nF':>3} {'nAct':>4} gates")
    lines = [
        "=" * len(hdr),
        f"PRELIMINARY COHORT -- {PRELIM_LABEL}",
        f"X as-of eval fold {LATEST_EVAL_FOLD} (criteria from folds <= {LATEST_EVAL_FOLD - 1});"
        f" universe {meta['universe']} -> active>={meta['min_active_folds']}:"
        f" {meta['after_active_filter']} -> C6/C2 defined: {meta['after_rank_defined']}"
        f" -> median filter C1/C3/C4@1m: {meta['after_median_filter']}"
        f" -> vetoes ({meta['vetoed_excluded']} excluded,"
        f" gate data on {meta['gate_data_coverage']}): {meta['after_vetoes']}",
        "=" * len(hdr), hdr, "-" * len(hdr)]
    for r in cohort.itertuples():
        gates = "ok" if (r.gate_data and not r.vetoed) else "no_gate_data"
        pnl = getattr(r, "prior_conservative_pnl", float("nan"))
        lines.append(
            f"{r.rank:>2} {r.primary_wallet:<42} {r.combined_rank:>6.1f} "
            f"{r.C6_decay_winshare:>6.3f} {int(r.C2_regime_agree):>4} "
            f"{-r.C1_boot_p:>7.4f} {r.C3_trajectory:>9.5f} {r.C4_share_gt_1m:>7.3f} "
            f"{pnl:>11.0f} {int(r.n_prior_folds):>3} {int(r.n_prior_active_folds):>4} {gates}")
    lines.append("-" * len(hdr))
    lines.append(f"NOTE: {PRELIM_LABEL}")
    return "\n".join(lines)


def _load_latest_inputs(run_dir: Path, mongo_uri: str):
    """Shared load pipeline for the latest-fold panel (X from folds <= LATEST_EVAL_FOLD-1)."""
    folds = load_folds(run_dir)
    mapping = load_mapping(run_dir)
    summary = load_summary(run_dir, extra_cols=("conservative_pnl_total",))
    positions = load_positions(run_dir)
    regimes, mongo_obs = load_btc_regimes(mongo_uri, folds)
    input_meta = {"m03_folds": folds.attrs["input_meta"],
                  "m06a_shortlist": mapping.attrs["input_meta"],
                  "m07_summary": summary.attrs["input_meta"],
                  "m07_positions": positions.attrs["input_meta"]}
    seats = build_seats(summary, mapping)
    pos = attach_wallets_to_positions(positions, mapping)
    panel = compute_panel(seats, pos, regimes, eval_folds=(LATEST_EVAL_FOLD,))
    stats = prior_fold_stats(seats, LATEST_EVAL_FOLD - 1)
    panel = panel.merge(stats, left_on="primary_wallet", right_index=True, how="left")
    panel["n_prior_active_folds"] = panel["n_prior_active_folds"].fillna(0).astype(int)
    return panel, input_meta, regimes, mongo_obs


def run_rank_latest(run_dir: Path, out_dir: Path, mongo_uri: str) -> int:
    """--rank-latest: PRELIMINARY cohort from the latest as-of criterion values, applying the
    validated scorecard verdict (rank by C6+C2; filter C1/C3/C4@1m above-median; standing hard
    vetoes; >= 3 prior active folds)."""
    print(f"\n*** {PRELIM_LABEL} ***\n", flush=True)
    panel, input_meta, regimes, mongo_obs = _load_latest_inputs(run_dir, mongo_uri)
    vetoes, veto_meta = load_vetoes(run_dir)
    cohort, meta = build_preliminary_cohort(panel, vetoes)
    text = format_cohort_text(cohort, meta)
    print(text)
    out_dir.mkdir(parents=True, exist_ok=True)
    cohort[[c for c in _COHORT_COLS if c in cohort.columns]].to_csv(
        out_dir / "preliminary_cohort.csv", index=False)
    (out_dir / "preliminary_cohort.txt").write_text(text + "\n")
    provenance = {
        "status": "complete", "label": PRELIM_LABEL,
        "created_utc": pd.Timestamp.now("UTC").isoformat(),
        "module": "research/v15/criterion_scorecard.py",
        "module_sha256": _sha256_file(Path(__file__).resolve()),
        "git": _git_state(), "versions": _versions(),
        "criterion_spec_sha256": CRITERION_SPEC_SHA256,
        "run_dir": str(run_dir), "inputs": input_meta, "vetoes": veto_meta,
        "latest_eval_fold": LATEST_EVAL_FOLD,
        "verdict_basis": {"rank": list(RANK_CRITERIA), "filter": list(FILTER_CRITERIA),
                          "combine": "average of within-universe ranks",
                          "min_active_folds": meta["min_active_folds"]},
        "funnel": meta,
        "cohort_wallets": cohort["primary_wallet"].tolist(),
        "regime_labels": {int(k): v for k, v in regimes.items()},
        "mongo_observations_sha256": hashlib.sha256(
            json.dumps(mongo_obs, sort_keys=True).encode("utf-8")).hexdigest(),
    }
    (out_dir / "preliminary_cohort_provenance.json").write_text(
        json.dumps(provenance, indent=2))
    print(f"\nwrote {out_dir}/preliminary_cohort.csv, preliminary_cohort.txt, "
          f"preliminary_cohort_provenance.json")
    return 0


# --------------------------------------------------------------------------- #
# Behavior screen (--behavior-screen) for the PRELIMINARY cohort
# --------------------------------------------------------------------------- #
JULY_PANEL_DEFAULT = REPO_ROOT / "app" / "data" / "v15" / "census20k_20260728" / \
    "profile_panel.parquet"          # cycle-1 LEADER panel (roster_freeze_provenance.json)
FRESH_PANEL_REL = "profile_fresh/profile_panel.parquet"  # REPLICA panel inside the run dir
JULY_COLS = ["primary_wallet", "fold_id", "n_pos", "mean_underwater_add", "mae_p90",
             "liq_rate", "frac_long"]
FRESH_COLS = ["primary_wallet", "fold_id", "n_pos", "median_hold_h", "frac_long"]
BEHAVIOR_GATES = ("uw_add", "leader_mae_p90", "leader_liq", "long_share", "latency_ratio")


def screen_wallet_behavior(wallets: list[str], july: pd.DataFrame,
                           fresh: pd.DataFrame) -> pd.DataFrame:
    """LEADER-tier behavior gates applied EXACTLY as research/v15/build_roster_freeze.py does
    (its own leader_tier/replica_tier functions + its L0 thresholds): n_pos-weighted
    mean_underwater_add <= 0.20, liq_rate <= 0.5%, mae_p90 <= 0.15, two-sided long-share
    0.25-0.75 (leader; replica fallback), latency_ratio = 4s/median_hold <= 2%.
    NA fails CLOSED and is flagged 'NA_fail', distinct from a measured 'fail'."""
    import build_roster_freeze as brf
    lv = dict(brf.LADDER[0][1])  # L0 thresholds
    empty_journeys = pd.DataFrame({c: pd.Series(dtype="object") for c in
                                   ("wallet", "side", "n_addon_fills", "liq_closed",
                                    "net_realized_pnl")})
    july = july.copy()
    fresh = fresh.copy()
    july["primary_wallet"] = july["primary_wallet"].str.lower()
    fresh["primary_wallet"] = fresh["primary_wallet"].str.lower()
    rows = []
    for w in [x.lower() for x in wallets]:
        lt = brf.leader_tier(july, empty_journeys, w)
        rt = brf.replica_tier(fresh, w)
        long_share = lt["leader_long"] if lt["leader_long"] == lt["leader_long"] \
            else rt["replica_long"]
        vals = {"uw_add": lt["uw_add"], "leader_mae_p90": lt["leader_mae_p90"],
                "leader_liq": lt["leader_liq"], "long_share": long_share,
                "latency_ratio": rt["latency_ratio"]}
        checks = {"uw_add": vals["uw_add"] <= lv["uw"],
                  "leader_mae_p90": vals["leader_mae_p90"] <= brf.MAE_LEADER_MAX,
                  "leader_liq": vals["leader_liq"] <= lv["liq"],
                  "long_share": lv["lo"] <= vals["long_share"] <= lv["hi"],
                  "latency_ratio": vals["latency_ratio"] <= brf.LATENCY_MAX}
        jw, fw = july[july["primary_wallet"] == w], fresh[fresh["primary_wallet"] == w]
        row = {"primary_wallet": w, "leader_src": lt["leader_src"],
               "replica_hold_h": rt["replica_hold_h"],
               "n_pos_july": int(jw["n_pos"].sum()), "n_folds_july": int(len(jw)),
               "n_pos_fresh": int(fw["n_pos"].sum()), "n_folds_fresh": int(len(fw)),
               **vals}
        for g in BEHAVIOR_GATES:
            row[f"{g}_gate"] = ("NA_fail" if vals[g] != vals[g]
                                else ("pass" if bool(checks[g]) else "fail"))
        statuses = [row[f"{g}_gate"] for g in BEHAVIOR_GATES]
        row["verdict"] = ("PASS" if all(s == "pass" for s in statuses)
                          else "VETO_MEASURED" if "fail" in statuses
                          else "VETO_NA_CLOSED")
        rows.append(row)
    return pd.DataFrame(rows)


def format_behavior_screen_text(df: pd.DataFrame) -> str:
    hdr = (f"{'#':>2} {'wallet':<42} {'uw_add':>7} {'mae_p90':>8} {'liq':>7} {'longSh':>7} "
           f"{'latRat':>7} {'hold_h':>7} {'nPosJ':>6} gates(uw/mae/liq/2s/lat)  verdict")
    lines = ["=" * len(hdr),
             f"COHORT BEHAVIOR SCREEN -- {PRELIM_LABEL}",
             "Gates = build_roster_freeze.py L0 leader tier: uw_add<=0.20 mae_p90<=0.15 "
             "liq<=0.005 long-share 0.25-0.75 latency<=0.02 (4s copy latency). "
             "NA fails CLOSED (flagged NA_fail).",
             "=" * len(hdr), hdr, "-" * len(hdr)]
    mark = {"pass": "P", "fail": "F", "NA_fail": "N"}

    def _f(v, w, d=3):
        return f"{v:>{w}.{d}f}" if v == v else " " * (w - 2) + "na"

    for r in df.itertuples():
        marks = "/".join(mark[getattr(r, f"{g}_gate")] for g in BEHAVIOR_GATES)
        lines.append(
            f"{r.rank:>2} {r.primary_wallet:<42} {_f(r.uw_add, 7)} {_f(r.leader_mae_p90, 8)} "
            f"{_f(r.leader_liq, 7, 4)} {_f(r.long_share, 7)} {_f(r.latency_ratio, 7, 4)} "
            f"{_f(r.replica_hold_h, 7, 2)} {r.n_pos_july:>6} {marks:<25} {r.verdict}")
    lines.append("-" * len(hdr))
    n_pass = int((df["verdict"] == "PASS").sum())
    lines.append(f"PASS {n_pass}/{len(df)} | VETO_MEASURED "
                 f"{int((df['verdict'] == 'VETO_MEASURED').sum())} | VETO_NA_CLOSED "
                 f"{int((df['verdict'] == 'VETO_NA_CLOSED').sum())}")
    lines.append(f"NOTE: {PRELIM_LABEL}")
    return "\n".join(lines)


def run_behavior_screen(run_dir: Path, out_dir: Path, july_panel: Path) -> int:
    print(f"\n*** {PRELIM_LABEL} ***\n", flush=True)
    cohort_path = out_dir / "preliminary_cohort.csv"
    if not cohort_path.exists():
        raise FileNotFoundError(f"{cohort_path} not found -- run --rank-latest first")
    cohort = pd.read_csv(cohort_path)
    wallets = cohort["primary_wallet"].str.lower().tolist()
    july_panel = Path(july_panel)
    fresh_path = _resolve_run_file(run_dir, FRESH_PANEL_REL)
    july = pd.read_parquet(july_panel, columns=JULY_COLS)
    fresh = pd.read_parquet(fresh_path, columns=FRESH_COLS)
    # Fence: both panels are the cycle-1 artifacts pinned corpus-only (<= 2026-07-13) by
    # roster_freeze_provenance.json; defensively cap the fold calendars they may contain.
    if int(july["fold_id"].max()) > 8 or int(fresh["fold_id"].max()) > 11:
        raise ValueError("behavior panels contain folds beyond the corpus-only calendars "
                         "(july <= 8, fresh <= 11) -- refusing (fence)")
    screen = screen_wallet_behavior(wallets, july, fresh)
    screen = cohort[["rank", "primary_wallet"]].merge(screen, on="primary_wallet")
    screen["label"] = "PRELIMINARY"
    text = format_behavior_screen_text(screen)
    print(text)
    out_dir.mkdir(parents=True, exist_ok=True)
    screen.to_csv(out_dir / "cohort_behavior_screen.csv", index=False)
    (out_dir / "cohort_behavior_screen.txt").write_text(text + "\n")
    import build_roster_freeze as brf
    provenance = {
        "status": "complete", "label": PRELIM_LABEL,
        "created_utc": pd.Timestamp.now("UTC").isoformat(),
        "module": "research/v15/criterion_scorecard.py",
        "module_sha256": _sha256_file(Path(__file__).resolve()),
        "gate_code": {"file": "research/v15/build_roster_freeze.py",
                      "sha256": _sha256_file(Path(brf.__file__).resolve()),
                      "thresholds": {**brf.LADDER[0][1], "mae": brf.MAE_LEADER_MAX,
                                     "latency": brf.LATENCY_MAX,
                                     "copy_latency_s": brf.COPY_LATENCY_S}},
        "inputs": {"cohort": {"path": str(cohort_path), "sha256": _sha256_file(cohort_path)},
                   "july_panel": {"path": str(july_panel),
                                  "sha256": _sha256_file(july_panel),
                                  "folds": [int(july["fold_id"].min()),
                                            int(july["fold_id"].max())]},
                   "fresh_panel": {"path": str(fresh_path),
                                   "sha256": _sha256_file(fresh_path),
                                   "folds": [int(fresh["fold_id"].min()),
                                             int(fresh["fold_id"].max())]}},
        "git": _git_state(), "versions": _versions(),
        "verdicts": screen.set_index("primary_wallet")["verdict"].to_dict(),
        "exchange_tier_note": "lifetime-PnL/recency/API gates NOT applied here (live-state, "
                              "outside the fence); behavior tiers only",
    }
    (out_dir / "cohort_behavior_screen_provenance.json").write_text(
        json.dumps(provenance, indent=2))
    print(f"\nwrote {out_dir}/cohort_behavior_screen.csv, cohort_behavior_screen.txt, "
          f"cohort_behavior_screen_provenance.json")
    return 0


# --------------------------------------------------------------------------- #
# Screened-cohort extension (--screen-extend)
# --------------------------------------------------------------------------- #
def walk_until_target(universe: pd.DataFrame, screened: pd.DataFrame,
                      target_pass: int) -> tuple[pd.DataFrame, dict]:
    """Walk the ranked filtered universe in combined-rank order, attaching the behavior-screen
    verdicts, and cut at the first rank where the cumulative PASS count reaches `target_pass`
    (or exhaust the universe). Returns (walked rows incl. vetoed ones, summary)."""
    m = universe.merge(screened, on="primary_wallet", how="left", validate="one_to_one")
    if m["verdict"].isna().any():
        raise ValueError("screen results missing for some universe wallets -- refusing "
                         "(every walked wallet must be screened)")
    m["is_pass"] = m["verdict"] == "PASS"
    m["cum_pass"] = m["is_pass"].cumsum().astype(int)
    hit = m.index[m["cum_pass"] >= target_pass]
    depth = int(hit[0]) + 1 if len(hit) else len(m)
    walked = m.head(depth).copy()
    summary = {
        "target_pass": int(target_pass),
        "achieved_pass": int(walked["is_pass"].sum()),
        "depth": int(depth), "universe": int(len(m)),
        "exhausted": bool(len(hit) == 0),
        "verdict_counts": {k: int(v) for k, v in walked["verdict"].value_counts().items()},
        "kill_rate_by_gate": {
            g: {"measured_fail": int((walked[f"{g}_gate"] == "fail").sum()),
                "na_fail": int((walked[f"{g}_gate"] == "NA_fail").sum())}
            for g in BEHAVIOR_GATES},
    }
    return walked, summary


_SCREENED_COLS = ["rank", "primary_wallet", "combined_rank", *CRITERIA,
                  "prior_conservative_pnl", "n_prior_folds", "n_prior_active_folds",
                  "uw_add", "leader_mae_p90", "leader_liq", "long_share", "latency_ratio",
                  "replica_hold_h", "n_pos_july",
                  *[f"{g}_gate" for g in BEHAVIOR_GATES], "verdict", "cum_pass", "label"]


def format_screened_text(walked: pd.DataFrame, summary: dict) -> str:
    hdr = (f"{'#':>3} {'wallet':<42} {'C6':>6} {'C2':>4} {'C1_p':>7} {'C4@1m':>7} "
           f"{'uw_add':>7} {'liq':>7} {'longSh':>7} {'latRat':>7} "
           f"gates(uw/mae/liq/2s/lat)  verdict        cumP")
    kills = ", ".join(f"{g}: {v['measured_fail']}F/{v['na_fail']}N"
                      for g, v in summary["kill_rate_by_gate"].items())
    lines = ["=" * len(hdr),
             f"PRELIMINARY SCREENED COHORT -- {PRELIM_LABEL}",
             f"Walked the ranked filtered universe to depth {summary['depth']}/"
             f"{summary['universe']} to reach {summary['achieved_pass']} PASS "
             f"(target {summary['target_pass']}"
             + ("; UNIVERSE EXHAUSTED" if summary["exhausted"] else "") + ").",
             f"Kills along the walk (F=measured, N=NA-closed): {kills}",
             "=" * len(hdr), hdr, "-" * len(hdr)]
    mark = {"pass": "P", "fail": "F", "NA_fail": "N"}

    def _f(v, w, d=3):
        return f"{v:>{w}.{d}f}" if v == v else " " * (w - 2) + "na"

    for r in walked.itertuples():
        marks = "/".join(mark[getattr(r, f"{g}_gate")] for g in BEHAVIOR_GATES)
        lines.append(
            f"{r.rank:>3} {r.primary_wallet:<42} {r.C6_decay_winshare:>6.3f} "
            f"{int(r.C2_regime_agree):>4} {-r.C1_boot_p:>7.4f} {_f(r.C4_share_gt_1m, 7)} "
            f"{_f(r.uw_add, 7)} {_f(r.leader_liq, 7, 4)} {_f(r.long_share, 7)} "
            f"{_f(r.latency_ratio, 7, 4)} {marks:<25} {r.verdict:<14} {int(r.cum_pass):>4}")
    lines.append("-" * len(hdr))
    lines.append(f"FINAL PRELIMINARY COHORT = the {summary['achieved_pass']} PASS wallets "
                 f"above (of {summary['depth']} walked).")
    lines.append(f"NOTE: {PRELIM_LABEL}")
    return "\n".join(lines)


def run_screen_extend(run_dir: Path, out_dir: Path, mongo_uri: str, july_panel: Path,
                      target_pass: int = 18) -> int:
    """--screen-extend: walk the FULL ranked filtered universe (same funnel as --rank-latest),
    batch-apply the behavior screen (same panels, same L0 gates, NA-fails-closed), and cut when
    the cumulative PASS count reaches `target_pass` (or the universe is exhausted)."""
    print(f"\n*** {PRELIM_LABEL} ***\n", flush=True)
    panel, input_meta, regimes, mongo_obs = _load_latest_inputs(run_dir, mongo_uri)
    vetoes, veto_meta = load_vetoes(run_dir)
    universe, funnel = build_preliminary_cohort(panel, vetoes, top_n=None)
    universe["primary_wallet"] = universe["primary_wallet"].str.lower()
    july_panel = Path(july_panel)
    fresh_path = _resolve_run_file(run_dir, FRESH_PANEL_REL)
    july = pd.read_parquet(july_panel, columns=JULY_COLS)
    fresh = pd.read_parquet(fresh_path, columns=FRESH_COLS)
    if int(july["fold_id"].max()) > 8 or int(fresh["fold_id"].max()) > 11:
        raise ValueError("behavior panels contain folds beyond the corpus-only calendars "
                         "(july <= 8, fresh <= 11) -- refusing (fence)")
    screened = screen_wallet_behavior(universe["primary_wallet"].tolist(), july, fresh)
    walked, summary = walk_until_target(universe, screened, target_pass)
    walked["label"] = "PRELIMINARY"
    text = format_screened_text(walked, summary)
    print(text)
    out_dir.mkdir(parents=True, exist_ok=True)
    walked[[c for c in _SCREENED_COLS if c in walked.columns]].to_csv(
        out_dir / "preliminary_cohort_screened.csv", index=False)
    (out_dir / "preliminary_cohort_screened.txt").write_text(text + "\n")
    import build_roster_freeze as brf
    provenance = {
        "status": "complete", "label": PRELIM_LABEL,
        "created_utc": pd.Timestamp.now("UTC").isoformat(),
        "module": "research/v15/criterion_scorecard.py",
        "module_sha256": _sha256_file(Path(__file__).resolve()),
        "gate_code": {"file": "research/v15/build_roster_freeze.py",
                      "sha256": _sha256_file(Path(brf.__file__).resolve()),
                      "thresholds": {**brf.LADDER[0][1], "mae": brf.MAE_LEADER_MAX,
                                     "latency": brf.LATENCY_MAX,
                                     "copy_latency_s": brf.COPY_LATENCY_S}},
        "inputs": {**input_meta, "vetoes": veto_meta,
                   "july_panel": {"path": str(july_panel),
                                  "sha256": _sha256_file(july_panel)},
                   "fresh_panel": {"path": str(fresh_path),
                                   "sha256": _sha256_file(fresh_path)}},
        "git": _git_state(), "versions": _versions(),
        "funnel": funnel, "walk_summary": summary,
        "final_pass_wallets": walked.loc[walked["verdict"] == "PASS",
                                         "primary_wallet"].tolist(),
        "mongo_observations_sha256": hashlib.sha256(
            json.dumps(mongo_obs, sort_keys=True).encode("utf-8")).hexdigest(),
        "exchange_tier_note": "lifetime-PnL/recency/API gates NOT applied here (live-state, "
                              "outside the fence); behavior tiers only",
    }
    (out_dir / "preliminary_cohort_screened_provenance.json").write_text(
        json.dumps(provenance, indent=2))
    print(f"\nwrote {out_dir}/preliminary_cohort_screened.csv, "
          f"preliminary_cohort_screened.txt, preliminary_cohort_screened_provenance.json")
    return 0


# --------------------------------------------------------------------------- #
# Reporting
# --------------------------------------------------------------------------- #
_TBL_COLS = ["coverage_n", "n_fold_ics", "mean_ic", "t_plain", "nw_t", "p_1s", "q_bh",
             "sign_count", "top_decile_y", "bottom_decile_y", "nw_t_sens", "tier"]


def format_table(scored: pd.DataFrame, title: str) -> str:
    hdr = (f"{'criterion':<18} {'n':>7} {'#IC':>3} {'meanIC':>8} {'t':>7} {'NW_t':>7} "
           f"{'p_1s':>8} {'q_BH':>8} {'sign':>5} {'topD_y':>9} {'botD_y':>9} "
           f"{'NWt_sens':>8}  tier")
    lines = [title, "-" * len(hdr), hdr, "-" * len(hdr)]
    for c, r in scored.iterrows():
        lines.append(
            f"{c:<18} {int(r['coverage_n']):>7} {int(r['n_fold_ics']):>3} "
            f"{r['mean_ic']:>8.4f} {r['t_plain']:>7.2f} {r['nw_t']:>7.2f} "
            f"{r['p_1s']:>8.4f} {r['q_bh']:>8.4f} "
            f"{int(r['sign_count'])}/{int(r['n_eval_folds']):<3} "
            f"{r['top_decile_y']:>9.4f} {r['bottom_decile_y']:>9.4f} "
            f"{r['nw_t_sens']:>8.2f}  {r['tier']}")
    lines.append("-" * len(hdr))
    return "\n".join(lines)


def _git_state() -> dict:
    """SHA + dirty flag + diff hash, so provenance binds the exact code state, not just HEAD."""
    out = {"sha": "unknown", "dirty": None, "diff_sha256": None,
           "status_porcelain_sha256": None}
    try:
        run = lambda *a: subprocess.run(["git", *a], cwd=REPO_ROOT, text=True,  # noqa: E731
                                        capture_output=True, check=True).stdout
        out["sha"] = run("rev-parse", "HEAD").strip()
        porcelain = run("status", "--porcelain")
        out["dirty"] = bool(porcelain.strip())
        # NOTE: `git diff HEAD` excludes untracked files -- the porcelain hash binds their
        # names, and provenance separately binds the EXECUTED module bytes via module_sha256.
        out["diff_sha256"] = hashlib.sha256(run("diff", "HEAD").encode("utf-8")).hexdigest()
        out["status_porcelain_sha256"] = hashlib.sha256(porcelain.encode("utf-8")).hexdigest()
    except Exception:
        pass
    return out


def _versions() -> dict:
    import scipy
    v = {"python": sys.version.split()[0], "numpy": np.__version__, "pandas": pd.__version__,
         "scipy": scipy.__version__}
    for mod in ("pyarrow", "pymongo"):
        try:
            v[mod] = __import__(mod).__version__
        except Exception:
            v[mod] = None
    return v


def _redact_uri(uri: str) -> str:
    import re
    return re.sub(r"//[^/@]+@", "//***@", uri)


def registry_ids(run_dir: Path) -> list[str]:
    """Registry IDs from R/registry_*.json FILENAMES only (the files are not read; this narrow
    pattern is the only permitted listing of the run dir)."""
    return sorted(p.stem.replace("registry_", "") for p in run_dir.glob("registry_*.json"))


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-dir", default=str(DEFAULT_RUN_DIR))
    ap.add_argument("--out-dir", default=None,
                    help="default: <run-dir>/criterion_scorecard/")
    ap.add_argument("--mongo-uri", default=DEFAULT_MONGO_URI)
    ap.add_argument("--rank-latest", action="store_true",
                    help="PRELIMINARY cohort from the latest as-of criterion values "
                         "(no scorecard recompute; no capital decision)")
    ap.add_argument("--behavior-screen", action="store_true",
                    help="LEADER-tier behavior gates (build_roster_freeze L0) over the "
                         "PRELIMINARY cohort")
    ap.add_argument("--screen-extend", action="store_true",
                    help="walk the ranked filtered universe until --target-pass wallets "
                         "survive BOTH layers (PRELIMINARY)")
    ap.add_argument("--target-pass", type=int, default=18)
    ap.add_argument("--july-panel", default=str(JULY_PANEL_DEFAULT))
    args = ap.parse_args(argv)

    install_memory_guard(soft_gb=12.0, label="criterion_scorecard")

    if args.rank_latest or args.behavior_screen or args.screen_extend:
        run_dir = Path(args.run_dir)
        out_dir = Path(args.out_dir) if args.out_dir else run_dir / "criterion_scorecard"
        rc = 0
        if args.rank_latest:
            rc = run_rank_latest(run_dir, out_dir, args.mongo_uri)
        if args.behavior_screen and rc == 0:
            rc = run_behavior_screen(run_dir, out_dir, Path(args.july_panel))
        if args.screen_extend and rc == 0:
            rc = run_screen_extend(run_dir, out_dir, args.mongo_uri, Path(args.july_panel),
                                   args.target_pass)
        return rc

    # Pre-registration text printed verbatim BEFORE computing (binding).
    print(PREREGISTRATION, flush=True)

    t0 = time.time()
    run_dir = Path(args.run_dir)
    out_dir = Path(args.out_dir) if args.out_dir else run_dir / "criterion_scorecard"

    folds = load_folds(run_dir)
    mapping = load_mapping(run_dir)
    summary = load_summary(run_dir)
    positions = load_positions(run_dir)
    regimes, mongo_obs = load_btc_regimes(args.mongo_uri, folds)
    input_meta = {"m03_folds": folds.attrs["input_meta"],
                  "m06a_shortlist": mapping.attrs["input_meta"],
                  "m07_summary": summary.attrs["input_meta"],
                  "m07_positions": positions.attrs["input_meta"],
                  "mongo": {"uri": _redact_uri(args.mongo_uri),
                            "collection": "quants_lab.hyperliquid_candles",
                            "filter": "coin=BTC interval=1m",
                            "observations": mongo_obs,
                            "observations_sha256": hashlib.sha256(json.dumps(
                                mongo_obs, sort_keys=True).encode("utf-8")).hexdigest()}}

    seats = build_seats(summary, mapping)
    pos = attach_wallets_to_positions(positions, mapping)
    n_neg_dur = int(pos["neg_dur"].sum())
    pos_folds = sorted(int(f) for f in pos["fold_id"].unique())
    pos_max_exit = pd.Timestamp(int(pos["exit_ts"].max()), unit="ms", tz="UTC")

    panel = compute_panel(seats, pos, regimes, EVAL_FOLDS)
    zero_share = float(panel["zero_fill"].mean())
    degenerate_folds = [int(k) for k in EVAL_FOLDS
                        if panel.loc[panel["fold_id"] == k, "y_primary"].nunique() <= 1]
    per_fold_counts = {int(k): {"panel_rows": int((panel["fold_id"] == k).sum()),
                                "zero_fill_share": round(float(
                                    panel.loc[panel["fold_id"] == k, "zero_fill"].mean()), 4),
                                **{c: int(panel.loc[panel["fold_id"] == k, c].notna().sum())
                                   for c in CRITERIA}}
                      for k in EVAL_FOLDS}

    inter = panel[panel[list(CRITERIA)].notna().all(axis=1)]

    own_primary = score_family(panel, y_col="y_primary")
    own_sens = score_family(panel, y_col="y_sens")
    own = assign_tiers(own_primary, own_sens)
    int_primary = score_family(inter, y_col="y_primary")
    int_sens = score_family(inter, y_col="y_sens")
    intr = assign_tiers(int_primary, int_sens)

    fam_own = c4_family_verdict(own)
    fam_int = c4_family_verdict(intr)

    notes = [
        "DATA NOTES:",
        f"- panel rows: {len(panel)} across eval folds {list(EVAL_FOLDS)}; zero-fill seats kept, "
        f"share = {zero_share:.4f}",
        f"- fold regimes (BTC test-window return): "
        + ", ".join(f"{k}:{v}" for k, v in sorted(regimes.items())),
        f"- m07_positions covers folds {pos_folds} (max exit {pos_max_exit}); fold-12 positions "
        f"absent and fold 11 truncated in the artifact -- position-based criteria (C1/C4/C5) use "
        f"prior folds <= k-1 <= 11 only, so no eval fold lacks its input, but the k=12 pool is "
        f"missing the 2026-06-24..29 tail of fold 11",
        f"- negative durations clamped to 0: {n_neg_dur} positions",
        f"- intersection panel (rows where ALL {len(CRITERIA)} criteria defined): {len(inter)} "
        f"rows; empty at early k by construction (C2 needs >=2 prior folds in EACH regime; "
        f"folds 1-3 are all '{regimes[1]}')",
        f"- C4 family sign-agreement (own): {fam_own} | (intersection): {fam_int}",
        "- the dial proposal reads from the INTERSECTION table",
    ]
    # Frozen reporting rule: EVERY undefined fold-IC, per scoring, with its reason.
    undefined_fold_ics = {
        "own_y_primary": own_primary.attrs.get("fold_ic_undefined", {}),
        "own_y_sens": own_sens.attrs.get("fold_ic_undefined", {}),
        "intersection_y_primary": int_primary.attrs.get("fold_ic_undefined", {}),
        "intersection_y_sens": int_sens.attrs.get("fold_ic_undefined", {}),
    }

    def _fmt_undef(m: dict) -> str:
        if not m:
            return "none"
        return "; ".join(f"{c}{{{', '.join(f'{k}:{r}' for k, r in sorted(u.items()))}}}"
                         for c, u in m.items())

    for key, m in undefined_fold_ics.items():
        notes.append(f"- UNDEFINED FOLD-ICs [{key}]: {_fmt_undef(m)}")
    if degenerate_folds:
        notes.insert(1, (
            f"- DEGENERATE EVAL FOLD(S) {degenerate_folds}: every seat in the fold has constant "
            f"y_primary in m07_summary (fold 12: all n_fills=0, conservative_roe=0.0 -- the "
            f"artifact's fill simulation did not complete there). Cross-sectional IC is undefined "
            f"on a constant y, so these folds contribute NO fold-IC: the effective IC series is "
            f"{len(EVAL_FOLDS) - len(degenerate_folds)} folds (dof "
            f"{len(EVAL_FOLDS) - len(degenerate_folds) - 1}), while gate (3) still demands >= 6 "
            f"of the original {len(EVAL_FOLDS)} (conservative). Panel rows of these folds also "
            f"inflate the zero-fill share."))
    report = "\n".join([
        PREREGISTRATION,
        "\n".join(notes),
        "",
        format_table(own, f"SCORECARD -- OWN COVERAGE (y_primary=conservative_roe; "
                          f"{len(panel)} rows)"),
        "",
        format_table(intr, f"SCORECARD -- INTERSECTION PANEL (all criteria defined; "
                           f"{len(inter)} rows)"),
    ])
    print("\n".join(notes))
    print()
    print(format_table(own, f"SCORECARD -- OWN COVERAGE (y_primary=conservative_roe; "
                            f"{len(panel)} rows)"))
    print()
    print(format_table(intr, f"SCORECARD -- INTERSECTION PANEL (all criteria defined; "
                             f"{len(inter)} rows)"))

    out_dir.mkdir(parents=True, exist_ok=True)
    out_files = {"criterion_scorecard.txt": out_dir / "criterion_scorecard.txt",
                 "scorecard_own.csv": out_dir / "scorecard_own.csv",
                 "scorecard_intersection.csv": out_dir / "scorecard_intersection.csv"}
    out_files["criterion_scorecard.txt"].write_text(report + "\n")
    own.reset_index().to_csv(out_files["scorecard_own.csv"], index=False)
    intr.reset_index().to_csv(out_files["scorecard_intersection.csv"], index=False)

    wall_s = time.time() - t0
    provenance = {
        "status": "complete",
        "created_utc": pd.Timestamp.now("UTC").isoformat(),
        "completed_utc": pd.Timestamp.now("UTC").isoformat(),
        "module": "research/v15/criterion_scorecard.py",
        "module_sha256": _sha256_file(Path(__file__).resolve()),  # binds the EXECUTED source
        "argv": {"run_dir": str(run_dir), "out_dir": str(out_dir),
                 "mongo_uri": _redact_uri(args.mongo_uri)},
        "git": _git_state(),
        "versions": _versions(),
        "registry_ids": registry_ids(run_dir),
        "criterion_spec_sha256": CRITERION_SPEC_SHA256,
        "run_dir": str(run_dir),
        "inputs": input_meta,
        "output_sha256": {name: _sha256_file(p) for name, p in out_files.items()},
        "fence_ms": FENCE_MS,
        "eval_folds": list(EVAL_FOLDS),
        "folds": [{"fold_id": int(r.fold_id), "test_start": str(r.test_start),
                   "test_end_excl": str(r.test_end_excl),
                   "regime": regimes[int(r.fold_id)]} for r in folds.itertuples()],
        "per_fold_panel_counts": per_fold_counts,
        "panel_rows": len(panel), "intersection_rows": len(inter),
        "zero_fill_share": zero_share,
        "degenerate_eval_folds": degenerate_folds,
        "undefined_fold_ics": undefined_fold_ics,
        "y_definitions": Y_DEFINITIONS,
        "criteria": list(CRITERIA),
        "c4_family": {"own": fam_own, "intersection": fam_int},
        "positions_note": {"folds_present": pos_folds, "max_exit_utc": str(pos_max_exit),
                           "n_negative_durations_clamped": n_neg_dur},
        "tiers_own": {c: str(t) for c, t in own["tier"].items()},
        "tiers_intersection": {c: str(t) for c, t in intr["tier"].items()},
        "wall_time_s": round(wall_s, 1),
        "preregistration_text": PREREGISTRATION,
    }
    (out_dir / "provenance.json").write_text(json.dumps(provenance, indent=2))
    print(f"\nwrote {out_dir}/criterion_scorecard.txt, scorecard_own.csv, "
          f"scorecard_intersection.csv, provenance.json  (wall {wall_s:.1f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
