#!/usr/bin/env python
"""Agent D -- H17 (crossed-share) TRAIN-window re-validation.

Agent A's H17 PASS (maker-leaning leaders carry the copy edge) used a
crossed_share feature computed on the TEST window itself (2026-03-15..05-23),
i.e. in-sample for conditioning. This script recomputes each cohort wallet's
crossed_share from leader fills in each fold's TRAIN window ONLY, re-buckets,
and re-evaluates the TEST-window edge -- the deployable version of the filter.

Windows (calendar, end-exclusive; day files named YYYYMMDD):
  fold1: TRAIN 2025-12-01..2026-03-15 (days 20251201..20260314, 104 files)
         TEST  2026-03-15..2026-05-17 (days 20260315..20260516, 63 days)
  fold2: TRAIN 2025-12-15..2026-04-15 (days 20251215..20260414, 121 files)
         TEST  2026-04-15..2026-05-23 (days 20260415..20260522, 38 days)

Data (read-only):
  app/data/v16/sprint_trades_enriched.parquet   -- 7,911 validated-fold copy trades
  app/data/hl_s3_fills_v2/YYYYMMDD.parquet      -- daily leader fills

Outputs:
  /tmp/agentD_h17_train.md            -- report
  /tmp/agentD_crossed_counts.parquet  -- per-(window, wallet) fill/crossed counters (cache)

PRE-REGISTERED GATE (agent A's, applied to TRAIN-conditioned terciles;
favored bucket fixed a priori = T1, the lowest crossed-share tercile;
"monotone direction" per agent A's H11/H17 convention = the T1-T3 spread
direction is consistent across folds, not strict three-bucket ordering):
  PASS requires ALL of, in BOTH folds independently:
    (a) T1 - T3 pooled mean spread >= 5 bps, positive (T1 favored) in both;
    (b) wallet-median T1 - T3 spread same sign as pooled (positive);
    (c) T1 holds >= 50 trades.
  Reported diagnostics (not gating): strict three-tercile monotonicity
  (T1 >= T2 >= T3); agent A's coded favored-vs-complement spread (his
  judge_bucketed criterion); Spearman rho(train share, test share) per fold.

Memory: fills read ONE DAY FILE AT A TIME (columns wallet+crossed, filtered to
the 166 cohort wallets), reduced to per-window per-wallet counters immediately.
No giant concats. install_memory_guard() at start.
"""
from __future__ import annotations

import os
import sys
from collections import Counter

import numpy as np
import pandas as pd

sys.path.insert(0, "/Users/hermes/quants-lab/research/v15")
try:
    from _streaming_io import install_memory_guard  # noqa: E402
except Exception:  # pragma: no cover
    def install_memory_guard(**kw):
        pass

REPO = "/Users/hermes/quants-lab"
TRADES_PQ = f"{REPO}/app/data/v16/sprint_trades_enriched.parquet"
FILLS_DIR = f"{REPO}/app/data/hl_s3_fills_v2"
COUNTS_CACHE = "/tmp/agentD_crossed_counts.parquet"
REPORT = "/tmp/agentD_h17_train.md"

FOLD_DAYS = {"fold1": 63.0, "fold2": 38.0}
SPREAD_MIN = 5.0   # bps
MIN_BUCKET_TRADES = 50

# day-file ranges per window (inclusive day strings; calendar end-exclusive)
WINDOWS: dict[str, tuple[str, str]] = {
    "fold1_train": ("20251201", "20260314"),
    "fold2_train": ("20251215", "20260414"),
    "fold1_test": ("20260315", "20260516"),
    "fold2_test": ("20260415", "20260522"),
}
SCAN_D0 = min(a for a, _ in WINDOWS.values())
SCAN_D1 = max(b for _, b in WINDOWS.values())

md_lines: list[str] = []


def emit(s: str = "") -> None:
    md_lines.append(s)
    print(s, flush=True)


def fmt(x, nd=1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "--"
    return f"{x:.{nd}f}"


# ---------------------------------------------------------------------------
# agent A machinery (identical math)
# ---------------------------------------------------------------------------

def wallet_median(sub: pd.DataFrame) -> float:
    if len(sub) == 0:
        return float("nan")
    return float(sub.groupby("wallet")["ov_bps"].mean().median())


def tercile_assign(feat: pd.Series) -> pd.Series:
    """Wallet-level terciles, ties kept together (qcut on average rank)."""
    r = feat.rank(method="average")
    codes = pd.qcut(r, 3, labels=False, duplicates="drop")
    return pd.Series(codes, index=feat.index).map(lambda c: f"T{int(c) + 1}")


def bucket_stats(tf: pd.DataFrame, bucket: pd.Series, fold: str) -> pd.DataFrame:
    days = FOLD_DAYS[fold]
    rows = []
    total_n = len(tf)
    for b in bucket.dropna().unique():
        sub = tf[bucket == b]
        rows.append({
            "bucket": b,
            "n_wallets": sub["wallet"].nunique(),
            "n": len(sub),
            "trades_day": len(sub) / days,
            "mean_ov": sub["ov_bps"].mean(),
            "wmed_ov": wallet_median(sub),
            "flow_pct": 100.0 * len(sub) / total_n,
        })
    return pd.DataFrame(rows).sort_values("bucket").reset_index(drop=True)


def table_md(df: pd.DataFrame, extra_cols: dict[str, pd.Series] | None = None) -> None:
    hdr = "| bucket | n_wallets | n | trades/day | mean ov_bps | wallet-med | flow% |"
    sep = "|---|---|---|---|---|---|---|"
    if extra_cols:
        for name in extra_cols:
            hdr += f" {name} |"
            sep += "---|"
    emit(hdr)
    emit(sep)
    for _, r in df.iterrows():
        line = (f"| {r['bucket']} | {int(r['n_wallets'])} | {int(r['n'])} | "
                f"{fmt(r['trades_day'], 1)} | {fmt(r['mean_ov'], 1)} | "
                f"{fmt(r['wmed_ov'], 1)} | {fmt(r['flow_pct'], 1)} |")
        if extra_cols:
            for name, series in extra_cols.items():
                line += f" {series.get(r['bucket'], '--')} |"
        emit(line)


# ---------------------------------------------------------------------------
# fills scan: per-window per-wallet (n_fills, n_crossed)
# ---------------------------------------------------------------------------

def scan_fills(cohort: set[str], limit_files: int = 0) -> pd.DataFrame:
    if os.path.exists(COUNTS_CACHE) and "--refresh" not in sys.argv and not limit_files:
        print(f"[fills] using cache {COUNTS_CACHE}", flush=True)
        return pd.read_parquet(COUNTS_CACHE)

    days = sorted(f for f in os.listdir(FILLS_DIR)
                  if f.endswith(".parquet") and SCAN_D0 <= f[:8] <= SCAN_D1)
    if limit_files:
        days = days[:limit_files]
    print(f"[fills] scanning {len(days)} day files {days[0][:8]}..{days[-1][:8]}", flush=True)
    cohort_list = list(cohort)

    n_fills = {k: Counter() for k in WINDOWS}
    n_crossed = {k: Counter() for k in WINDOWS}
    for i, fn in enumerate(days):
        d = fn[:8]
        wins = [k for k, (a, b) in WINDOWS.items() if a <= d <= b]
        if not wins:
            continue
        df = pd.read_parquet(os.path.join(FILLS_DIR, fn), columns=["wallet", "crossed"],
                             filters=[("wallet", "in", cohort_list)])
        if len(df):
            vc_all = df["wallet"].value_counts().to_dict()
            vc_cr = df.loc[df["crossed"].astype(bool), "wallet"].value_counts().to_dict()
            for k in wins:
                n_fills[k].update(vc_all)
                n_crossed[k].update(vc_cr)
        del df
        if (i + 1) % 20 == 0:
            print(f"[fills] {i+1}/{len(days)} files", flush=True)

    rows = [{"window": k, "wallet": w, "n_fills": int(tot),
             "n_crossed": int(n_crossed[k].get(w, 0))}
            for k in WINDOWS for w, tot in sorted(n_fills[k].items())]
    out = pd.DataFrame(rows)
    if not limit_files:
        out.to_parquet(COUNTS_CACHE)
        print(f"[fills] cached -> {COUNTS_CACHE}", flush=True)
    return out


def window_share(counts: pd.DataFrame, window: str) -> pd.DataFrame:
    sub = counts[counts["window"] == window].set_index("wallet")
    return pd.DataFrame({"n_fills": sub["n_fills"],
                         "crossed_share": sub["n_crossed"] / sub["n_fills"]})


# ---------------------------------------------------------------------------

def main() -> None:
    install_memory_guard(soft_gb=12.0, label="agentD")
    limit = int(os.environ.get("AGENTD_LIMIT_FILES", "0"))

    trades = pd.read_parquet(TRADES_PQ)
    assert trades["ov_bps"].notna().all()
    cohort = set(trades["wallet"].unique())
    trades_by_fold = {f: trades[trades["fold"] == f].copy() for f in ("fold1", "fold2")}

    counts = scan_fills(cohort, limit_files=limit)
    if limit:
        print(counts.groupby("window").agg(wallets=("wallet", "nunique"),
                                           fills=("n_fills", "sum")))
        print("[smoke] ok -- exiting before analysis")
        return

    try:
        from scipy.stats import spearmanr
    except Exception:
        spearmanr = None

    emit("# Agent D -- H17 crossed-share re-validated on TRAIN windows")
    emit(f"Trade table: {TRADES_PQ} ({len(trades)} trades, {len(cohort)} wallets). "
         "Generated 2026-06-11.")
    emit("Agent A's H17 PASS used crossed_share computed on the TEST window (in-sample "
         "for conditioning). Here each fold's wallets are terciled by crossed_share "
         "computed from fills in that fold's TRAIN window ONLY; edge is then measured "
         "on the fold's TEST trades. Same tercile assignment (rank + qcut), same edge "
         "metrics, same gate.")
    emit("")
    emit("Windows: fold1 TRAIN 2025-12-01..2026-03-15 (104d), TEST 2026-03-15..2026-05-17 "
         "(63d); fold2 TRAIN 2025-12-15..2026-04-15 (121d), TEST 2026-04-15..2026-05-23 "
         "(38d). End-exclusive; TRAIN fills never overlap TEST.")
    emit("")
    emit("Pre-registered gate (favored bucket fixed a priori = T1, lowest crossed-share "
         "tercile; monotone direction = T1-T3 spread positive in both folds, per agent A's "
         "H11/H17 convention): PASS requires in BOTH folds: (a) T1-T3 pooled spread >= 5 bps "
         "and positive; (b) wallet-median T1-T3 spread positive; (c) T1 >= 50 trades. "
         "Strict three-tercile monotonicity (T1>=T2>=T3) and agent A's coded "
         "favored-vs-complement spread are reported as diagnostics.")

    # baseline (verify the constants quoted in the task)
    emit("\n## Baseline (all validated-fold trades)")
    emit("| fold | n_wallets | n | trades/day | mean ov_bps | wallet-med |")
    emit("|---|---|---|---|---|---|")
    for fold, tf in trades_by_fold.items():
        emit(f"| {fold} | {tf['wallet'].nunique()} | {len(tf)} | "
             f"{fmt(len(tf)/FOLD_DAYS[fold])} | {fmt(tf['ov_bps'].mean())} | "
             f"{fmt(wallet_median(tf))} |")

    gate_ok = True
    strict_mono_ok = True
    complement_ok = True
    gate_detail: list[str] = []
    mono_notes: list[str] = []
    comp_notes: list[str] = []
    terc_by_fold: dict[str, pd.Series] = {}

    for fold, tf in trades_by_fold.items():
        wallets = pd.Index(sorted(tf["wallet"].unique()))
        tr = window_share(counts, f"{fold}_train").reindex(wallets)
        te = window_share(counts, f"{fold}_test").reindex(wallets)
        missing = tr["crossed_share"].isna().sum()
        feat = tr["crossed_share"].dropna()
        terc = tercile_assign(feat)
        terc_by_fold[fold] = terc
        bk = tf["wallet"].map(terc)

        emit(f"\n## {fold} -- terciles by TRAIN crossed_share")
        emit(f"Cohort wallets: {len(wallets)}; with TRAIN-window fills: {len(feat)} "
             f"(missing: {missing}). TRAIN fills per wallet: "
             f"min={int(tr['n_fills'].min())}, p25={int(tr['n_fills'].quantile(.25))}, "
             f"median={int(tr['n_fills'].median())}, max={int(tr['n_fills'].max())}.")
        tbl = bucket_stats(tf, bk, fold)
        rng = {t: f"[{fmt(feat[terc == t].min(), 4)},{fmt(feat[terc == t].max(), 4)}]"
               for t in terc.unique()}
        nfl = {t: int(tr.loc[terc.index[terc == t], "n_fills"].median())
               for t in terc.unique()}
        emit("")
        table_md(tbl, extra_cols={"train crossed_share range": pd.Series(rng),
                                  "med train fills": pd.Series(nfl)})

        t = tbl.set_index("bucket")
        mono = bool(t.loc["T1", "mean_ov"] >= t.loc["T2", "mean_ov"]
                    >= t.loc["T3", "mean_ov"])
        sp13 = t.loc["T1", "mean_ov"] - t.loc["T3", "mean_ov"]
        wsp13 = t.loc["T1", "wmed_ov"] - t.loc["T3", "wmed_ov"]
        n1 = int(t.loc["T1", "n"])
        # agent A comparability: T1 vs complement
        m1 = bk == "T1"
        spc = tf.loc[m1, "ov_bps"].mean() - tf.loc[~m1 & bk.notna(), "ov_bps"].mean()
        wspc = wallet_median(tf[m1]) - wallet_median(tf[~m1 & bk.notna()])
        emit(f"\nT1-T3 spread = {fmt(sp13)} bps (wallet-med {fmt(wsp13)}); "
             f"T1-vs-complement = {fmt(spc)} bps (wallet-med {fmt(wspc)}); T1 n={n1}; "
             f"strict monotone(T1>=T2>=T3): {mono}")
        gate_detail.append(f"{fold}: T1-T3={fmt(sp13)}bps, wmed={fmt(wsp13)}bps, "
                           f"T1 n={n1}")
        mono_notes.append(f"{fold}={mono}")
        comp_notes.append(f"{fold}: {fmt(spc)}bps (wmed {fmt(wspc)})")
        if not (np.isfinite(sp13) and sp13 >= SPREAD_MIN
                and np.isfinite(wsp13) and wsp13 > 0 and n1 >= MIN_BUCKET_TRADES):
            gate_ok = False
        if not mono:
            strict_mono_ok = False
        if not (np.isfinite(spc) and spc >= SPREAD_MIN and np.isfinite(wspc)
                and np.sign(wspc) == np.sign(spc) and n1 >= MIN_BUCKET_TRADES):
            complement_ok = False

        # persistence: train share vs test share
        both = pd.DataFrame({"train": tr["crossed_share"], "test": te["crossed_share"]}).dropna()
        if spearmanr is not None and len(both) >= 3:
            rho, pval = spearmanr(both["train"], both["test"])
        else:
            rho, pval = both["train"].corr(both["test"], method="spearman"), float("nan")
        terc_te = tercile_assign(te["crossed_share"].dropna())
        stay = {t_: float((terc_te.reindex(terc.index[terc == t_]) == t_).mean())
                for t_ in ("T1", "T3")}
        emit(f"Persistence: Spearman rho(train, test crossed_share) = {fmt(rho, 3)} "
             f"(p={pval:.2e}, n={len(both)}); tercile stay-rate T1={fmt(100*stay['T1'])}% "
             f"T3={fmt(100*stay['T3'])}%")

    verdict = "PASS" if gate_ok else "KILL"
    emit(f"\n## Verdict\n**H17-train verdict: {verdict}** -- {'; '.join(gate_detail)}")
    emit(f"- Diagnostic, strict three-tercile monotonicity (T1>=T2>=T3): "
         f"{', '.join(mono_notes)} -- a strict-monotone reading of the gate would "
         f"{'PASS' if strict_mono_ok else 'KILL'} on this clause alone.")
    emit(f"- Diagnostic, agent A's coded gate (T1 vs complement >= 5 bps, wmed sign "
         f"agreement, n >= 50): {', '.join(comp_notes)} -- "
         f"{'PASS' if complement_ok else 'KILL'}.")

    # deployable framings
    emit("\n## Deployable framings (TRAIN-conditioned)")
    emit("Parent-specified framing: drop the TRAIN pure-taker tercile (T3).")
    for fold, tf in trades_by_fold.items():
        terc = terc_by_fold[fold]
        bk = tf["wallet"].map(terc)
        keep = tf[bk.isin(["T1", "T2"])]
        emit(f"- drop-T3 {fold}: keep n={len(keep)} ({100*len(keep)/len(tf):.1f}% flow, "
             f"{len(keep)/FOLD_DAYS[fold]:.1f}/day from "
             f"{keep['wallet'].nunique()} wallets), mean={keep['ov_bps'].mean():.1f} "
             f"vs baseline {tf['ov_bps'].mean():.1f} "
             f"({keep['ov_bps'].mean() - tf['ov_bps'].mean():+.1f} bps); "
             f"wmed {wallet_median(keep):.1f} vs {wallet_median(tf):.1f}")
    emit("Descriptive alternative (post-hoc, matches where the edge actually "
         "concentrates): keep ONLY the maker-leaning tercile (T1).")
    for fold, tf in trades_by_fold.items():
        terc = terc_by_fold[fold]
        bk = tf["wallet"].map(terc)
        keep = tf[bk == "T1"]
        emit(f"- keep-T1 {fold}: keep n={len(keep)} ({100*len(keep)/len(tf):.1f}% flow, "
             f"{len(keep)/FOLD_DAYS[fold]:.1f}/day from "
             f"{keep['wallet'].nunique()} wallets), mean={keep['ov_bps'].mean():.1f} "
             f"vs baseline {tf['ov_bps'].mean():.1f} "
             f"({keep['ov_bps'].mean() - tf['ov_bps'].mean():+.1f} bps); "
             f"wmed {wallet_median(keep):.1f} vs {wallet_median(tf):.1f}")

    with open(REPORT, "w") as f:
        f.write("\n".join(md_lines) + "\n")
    print(f"\n[done] report -> {REPORT}", flush=True)


if __name__ == "__main__":
    main()
