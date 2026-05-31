#!/usr/bin/env python3
"""V15 M3 — Walk-forward fold geometry + per-wallet activity.

CANONICAL pipeline LAYER 2 (cheap selection), built FIRST. Design (codex-SHIP):
projects/quant/v15/modules/m3-design.

Two outputs, calendar + activity ONLY (NO eligibility / profit / ranking / selection):
  (1) FOLD CALENDAR — 8 folds @ 42d train / 14d val / 14d test / 14d step, contiguous
      non-overlapping TEST windows. Chained OOS = [2026-01-26, 2026-05-18) = 112d (~3.7mo).
  (2) ACTIVITY — per (wallet, fold, window in {train,val,test,pretest}): action + journey
      counts and an `active` boolean (= >=1 journey OPENED in the window). PRETEST
      [train_start, test_start) is the look-ahead-safe SELECTION field for downstream.

NON-LOOK-AHEAD CONTRACT (codex): train/val MAY overlap across folds; TEST windows are
contiguous + non-overlapping (chained equity cannot pass through overlapping time). Folds
are NON-independent -> downstream CHAINS equity, never averages. Activity is purely
behavioral/time-based; a fold never "counts" because it was profitable or eligible.

CLI:
    python v15_m03_fold_geometry.py \
        --actions app/data/v15/m02_actions.parquet \
        --journeys app/data/v15/m02_journeys.parquet \
        --outdir app/data/v15 [--start 2025-12-01]
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v15_m03] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v15_m03")

# === Calendar constants (LOCKED 2026-05-31, codex-SHIP) ===
TRAIN_DAYS = 42
VAL_DAYS = 14
TEST_DAYS = 14
STEP_DAYS = 14
N_FOLDS = 8
DEFAULT_START = date(2025, 12, 1)
# Archive end (inclusive day 2026-05-23 -> exclusive 2026-05-24). Used only for tail logging.
DEFAULT_END_EXCL = date(2026, 5, 24)

WINDOWS = ("train", "val", "test", "pretest")


def _ms(d: date) -> int:
    """UTC midnight of `d` in epoch ms (half-open interval boundary)."""
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)


@dataclass
class Fold:
    fold_id: int
    train_start: date
    train_end_excl: date
    val_start: date
    val_end_excl: date
    test_start: date
    test_end_excl: date
    # pretest = [train_start, test_start) = train+val combined (look-ahead-safe selection window)
    pretest_start: date
    pretest_end_excl: date

    def window_bounds(self, window: str) -> tuple[date, date]:
        if window == "train":
            return self.train_start, self.train_end_excl
        if window == "val":
            return self.val_start, self.val_end_excl
        if window == "test":
            return self.test_start, self.test_end_excl
        if window == "pretest":
            return self.pretest_start, self.pretest_end_excl
        raise ValueError(f"unknown window {window!r}")


def build_folds(start: date = DEFAULT_START, n_folds: int = N_FOLDS) -> list[Fold]:
    """Build the n_folds 3-way walk-forward calendar.

    Fold i (1-indexed), s_i = start + (i-1)*STEP_DAYS:
        train = [s_i,            s_i+42d)
        val   = [s_i+42d,        s_i+56d)
        test  = [s_i+56d,        s_i+70d)
    Half-open intervals (end exclusive).
    """
    folds: list[Fold] = []
    for i in range(n_folds):
        s = start + timedelta(days=i * STEP_DAYS)
        train_start = s
        train_end_excl = s + timedelta(days=TRAIN_DAYS)
        val_start = train_end_excl
        val_end_excl = val_start + timedelta(days=VAL_DAYS)
        test_start = val_end_excl
        test_end_excl = test_start + timedelta(days=TEST_DAYS)
        folds.append(
            Fold(
                fold_id=i + 1,
                train_start=train_start,
                train_end_excl=train_end_excl,
                val_start=val_start,
                val_end_excl=val_end_excl,
                test_start=test_start,
                test_end_excl=test_end_excl,
                pretest_start=train_start,
                pretest_end_excl=test_start,
            )
        )
    return folds


# === Regime tagging — REPORTING ONLY (codex Q6). Never stratifies / gates / weights. ===

def classify_trend(now: Optional[float], past: Optional[float]) -> str:
    """30d trend bucket. BULL >+10%, BEAR <-10%, else NEUTRAL. Bad data -> UNKNOWN."""
    for v in (now, past):
        if v is None or isinstance(v, bool) or not isinstance(v, (int, float)) or not np.isfinite(v) or v <= 0:
            return "UNKNOWN"
    pct = (now - past) / past
    if pct > 0.10:
        return "BULL"
    if pct < -0.10:
        return "BEAR"
    return "NEUTRAL"


def classify_vol(now: Optional[float], train_series: list[float]) -> str:
    """DVOL bucket vs train-window p33/p66. Bad/insufficient data -> UNKNOWN."""
    if now is None or isinstance(now, bool) or not isinstance(now, (int, float)) or not np.isfinite(now) or now <= 0:
        return "UNKNOWN"
    arr = np.array([v for v in (train_series or []) if isinstance(v, (int, float)) and not isinstance(v, bool) and np.isfinite(v) and v > 0])
    if len(arr) < 5:
        return "UNKNOWN"
    p33, p66 = np.quantile(arr, [0.33, 0.66])
    if now > p66:
        return "HIGH"
    if now < p33:
        return "LOW"
    return "MID"


def tag_fold_regime(fold: Fold, market_data_fn: Optional[Callable[[date], dict]]) -> dict:
    """Regime tags at test_start. market_data_fn(date)->{btc_price,hl_price,btc_dvol} or None.

    30d trend lookback uses [test_start-30d, test_start) (NOT train_start; V13 m09 r1 bug).
    DVOL percentiles computed over the train window [train_start, train_end_excl).
    No market_data_fn -> all UNKNOWN (regime is reporting-only; absence never blocks M3).
    """
    if market_data_fn is None:
        return {"btc_trend_bucket": "UNKNOWN", "hl_trend_bucket": "UNKNOWN", "dvol_bucket": "UNKNOWN"}

    def _get(d: date, key: str):
        md = market_data_fn(d)
        if not md:
            return None
        v = md.get(key)
        if v is None or isinstance(v, bool) or not isinstance(v, (int, float)) or not np.isfinite(v) or v <= 0:
            return None
        return float(v)

    test_start = fold.test_start
    lb = test_start - timedelta(days=30)
    btc_trend = classify_trend(_get(test_start, "btc_price"), _get(lb, "btc_price"))
    hl_trend = classify_trend(_get(test_start, "hl_price"), _get(lb, "hl_price"))

    dvol_series = []
    d = fold.train_start
    while d < fold.train_end_excl:
        v = _get(d, "btc_dvol")
        if v is not None:
            dvol_series.append(v)
        d += timedelta(days=1)
    dvol_bucket = classify_vol(_get(test_start, "btc_dvol"), dvol_series)
    return {"btc_trend_bucket": btc_trend, "hl_trend_bucket": hl_trend, "dvol_bucket": dvol_bucket}


def folds_to_frame(folds: list[Fold], market_data_fn: Optional[Callable] = None) -> pd.DataFrame:
    rows = []
    for f in folds:
        tags = tag_fold_regime(f, market_data_fn)
        rows.append(
            {
                "fold_id": f.fold_id,
                "train_start": pd.Timestamp(f.train_start),
                "train_end_excl": pd.Timestamp(f.train_end_excl),
                "val_start": pd.Timestamp(f.val_start),
                "val_end_excl": pd.Timestamp(f.val_end_excl),
                "test_start": pd.Timestamp(f.test_start),
                "test_end_excl": pd.Timestamp(f.test_end_excl),
                "pretest_start": pd.Timestamp(f.pretest_start),
                "pretest_end_excl": pd.Timestamp(f.pretest_end_excl),
                "train_days": TRAIN_DAYS,
                "val_days": VAL_DAYS,
                "test_days": TEST_DAYS,
                "step_days": STEP_DAYS,
                "is_full_test_fold": True,
                "oos_chain_order": f.fold_id,
                **tags,
            }
        )
    return pd.DataFrame(rows)


# === Activity pass ===

def build_activity(
    folds: list[Fold],
    actions: pd.DataFrame,
    journeys: pd.DataFrame,
    key_col: str = "wallet",
    journeys_complete: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Per (key, fold, window) action/journey counts + active flags, and a per-key summary.

    activity_basis is GLOBAL: 'journey' when journeys input present+complete, else
    'action_fallback' (codex r1 fix #4 — NEVER per-key on zero rows; zero rows = inactive).
    `active` = >=1 journey OPENED (entry_ts) in the window (or action fallback). Open/unclosed
    journeys count (they carry an entry_ts).
    """
    # codex code-r1 #4: complete journeys input present => GLOBAL 'journey' even with zero rows
    # (zero rows = genuinely inactive, not missing data). Fallback only when input absent/incomplete.
    basis = "journey" if (journeys_complete and journeys is not None) else "action_fallback"

    def _to_dt(s):
        # codex code-r1 #5: pd.api.types, not np.issubdtype (handles extension/tz/nullable dtypes).
        return pd.to_datetime(s, unit="ms", utc=True) if pd.api.types.is_numeric_dtype(s) else pd.to_datetime(s, utc=True)

    a = actions.copy()
    a["ts"] = _to_dt(a["ts"])
    use_journeys = basis == "journey"
    if use_journeys:
        j = journeys.copy()
        # journey OPEN timestamp = entry_ts (ms). Open/unclosed journeys still have entry_ts.
        j["open_ts"] = _to_dt(j["entry_ts"])
    else:
        j = None

    keys = pd.Index(a[key_col].unique())
    if use_journeys:
        keys = keys.union(pd.Index(j[key_col].unique()))

    fold_rows = []
    for f in folds:
        for w in WINDOWS:
            s, e = f.window_bounds(w)
            s_ts, e_ts = pd.Timestamp(s, tz="UTC"), pd.Timestamp(e, tz="UTC")
            am = a[(a["ts"] >= s_ts) & (a["ts"] < e_ts)]
            n_actions = am.groupby(key_col).size()
            if use_journeys:
                jm = j[(j["open_ts"] >= s_ts) & (j["open_ts"] < e_ts)]
                n_journeys = jm.groupby(key_col).size()
                first_ts = am.groupby(key_col)["ts"].min()
                last_ts = am.groupby(key_col)["ts"].max()
            else:
                n_journeys = pd.Series(dtype=int)
                first_ts = am.groupby(key_col)["ts"].min()
                last_ts = am.groupby(key_col)["ts"].max()
            df = pd.DataFrame({"key": keys})
            df["fold_id"] = f.fold_id
            df["window"] = w
            df["n_actions"] = df["key"].map(n_actions).fillna(0).astype(int)
            df["n_journeys"] = df["key"].map(n_journeys).fillna(0).astype(int)
            if use_journeys:
                df["active"] = df["n_journeys"] >= 1
            else:
                df["active"] = df["n_actions"] >= 1
            # dict mapper (NOT a datetime-valued Series mapper — that hits a pandas
            # _map_values cast bug). pd.to_datetime forces datetime so the pivot does not
            # collide float64 (empty windows) with datetime64 (populated windows).
            df["first_action_ts"] = pd.to_datetime(df["key"].map(first_ts.to_dict()), utc=True)
            df["last_action_ts"] = pd.to_datetime(df["key"].map(last_ts.to_dict()), utc=True)
            fold_rows.append(df)
    long = pd.concat(fold_rows, ignore_index=True)

    # Raw per-key totals over the WHOLE archive (codex code-r1 #9: NOT summed across folds —
    # overlapping train/val windows would double-count the same action/journey).
    total_actions_raw = a.groupby(key_col).size()
    total_journeys_raw = j.groupby(key_col).size() if use_journeys else pd.Series(dtype=int)
    first_seen_raw = a.groupby(key_col)["ts"].min()
    last_seen_raw = a.groupby(key_col)["ts"].max()

    # Pivot to wide per (key, fold).
    wide = _pivot_wide(long, keys, folds, basis)
    summary = _summarize(wide, keys, basis, total_actions_raw, total_journeys_raw, first_seen_raw, last_seen_raw)
    return wide, summary


def _pivot_wide(long: pd.DataFrame, keys: pd.Index, folds: list[Fold], basis: str) -> pd.DataFrame:
    out = []
    for fid in [f.fold_id for f in folds]:
        block = long[long["fold_id"] == fid]
        piv = block.pivot(index="key", columns="window")
        row = pd.DataFrame(index=keys)
        row.index.name = "key"
        for w in WINDOWS:
            row[f"n_actions_{w}"] = piv[("n_actions", w)] if ("n_actions", w) in piv else 0
            row[f"n_journeys_{w}"] = piv[("n_journeys", w)] if ("n_journeys", w) in piv else 0
            row[f"active_{w}"] = piv[("active", w)] if ("active", w) in piv else False
        for w in ("train", "val", "test"):
            row[f"first_action_ts_{w}"] = piv[("first_action_ts", w)] if ("first_action_ts", w) in piv else pd.NaT
            row[f"last_action_ts_{w}"] = piv[("last_action_ts", w)] if ("last_action_ts", w) in piv else pd.NaT
        row["active_any"] = row[[f"active_{w}" for w in ("train", "val", "test")]].any(axis=1)
        row = row.reset_index()
        row.insert(1, "fold_id", fid)
        out.append(row)
    wide = pd.concat(out, ignore_index=True)
    wide["key_kind"] = "wallet"
    wide["activity_basis"] = basis
    for w in WINDOWS + ("any",):
        col = f"active_{w}"
        if col in wide:
            wide[col] = wide[col].fillna(False).astype(bool)
    for w in WINDOWS:
        wide[f"n_actions_{w}"] = wide[f"n_actions_{w}"].fillna(0).astype(int)
        wide[f"n_journeys_{w}"] = wide[f"n_journeys_{w}"].fillna(0).astype(int)
    # codex code-r1 #8: contract column order key, key_kind, fold_id, ...
    lead = ["key", "key_kind", "fold_id"]
    wide = wide[lead + [c for c in wide.columns if c not in lead]]
    return wide


def _summarize(
    wide: pd.DataFrame,
    keys: pd.Index,
    basis: str,
    total_actions_raw: pd.Series,
    total_journeys_raw: pd.Series,
    first_seen_raw: pd.Series,
    last_seen_raw: pd.Series,
) -> pd.DataFrame:
    g = wide.groupby("key")
    summary = pd.DataFrame({"key": list(keys)})
    summary["key_kind"] = "wallet"
    for w in WINDOWS:
        summary[f"active_{w}_folds"] = summary["key"].map(g[f"active_{w}"].sum()).fillna(0).astype(int)
    summary["active_any_folds"] = summary["key"].map(g["active_any"].sum()).fillna(0).astype(int)
    # G5: active in >=3 TEST windows.
    summary["active_folds_for_g5"] = summary["active_test_folds"]
    # codex code-r1 #9: raw archive-wide totals (NOT fold-summed — overlapping train/val
    # windows would double-count the same source row).
    summary["total_actions"] = summary["key"].map(total_actions_raw.to_dict()).fillna(0).astype(int)
    summary["total_journeys"] = summary["key"].map(total_journeys_raw.to_dict()).fillna(0).astype(int)
    summary["first_seen_ts"] = pd.to_datetime(summary["key"].map(first_seen_raw.to_dict()), utc=True)
    summary["last_seen_ts"] = pd.to_datetime(summary["key"].map(last_seen_raw.to_dict()), utc=True)
    summary["activity_basis"] = basis
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="V15 M3 fold geometry + activity")
    ap.add_argument("--actions", required=True)
    ap.add_argument("--journeys", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--start", default=str(DEFAULT_START))
    args = ap.parse_args()

    start = pd.Timestamp(args.start).date()
    folds = build_folds(start)
    folds_df = folds_to_frame(folds, market_data_fn=None)  # regime backfilled later (reporting-only)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    folds_df.to_parquet(outdir / "m03_folds.parquet", index=False)
    logger.info(f"folds calendar -> {outdir/'m03_folds.parquet'} (8 folds, chained OOS "
                f"{folds[0].test_start} .. {folds[-1].test_end_excl})")

    actions = pd.read_parquet(args.actions)
    journeys = pd.read_parquet(args.journeys)
    logger.info(f"loaded {len(actions):,} actions, {len(journeys):,} journeys")
    wide, summary = build_activity(folds, actions, journeys)
    wide.to_parquet(outdir / "m03_wallet_fold_activity.parquet", index=False)
    summary.to_parquet(outdir / "m03_wallet_activity_summary.parquet", index=False)
    logger.info(f"activity -> {len(wide):,} (key,fold) rows; {len(summary):,} keys")
    hist = summary["active_folds_for_g5"].value_counts().sort_index()
    logger.info(f"active_test_folds histogram:\n{hist}")
    logger.info(f"keys with >=3 active test folds (G5): {(summary['active_folds_for_g5']>=3).sum():,}")


if __name__ == "__main__":
    main()
