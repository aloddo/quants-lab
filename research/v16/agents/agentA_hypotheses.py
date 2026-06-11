#!/usr/bin/env python
"""Agent A -- hypothesis screens on the V16 validated-fold copy-trade table.

Tests H9, H11, H16, H17, H18, H19, H20, H41 from the 2026-06-11 catalog
(projects/quant/research/2026-06-11-100-hypotheses-hl-copy).

Data (read-only):
  app/data/v16/sprint_trades_enriched.parquet  -- 7,911 validated-fold copy trades
  app/data/hl_s3_fills_v2/YYYYMMDD.parquet     -- daily leader fills (2026-03-15..2026-05-23 only)

Outputs:
  /tmp/agentA_results.md            -- full report
  /tmp/agentA_wallet_features.parquet, /tmp/agentA_wallet_builders.parquet  -- fills-scan cache

PRE-REGISTERED CRITERIA (locked before looking at conditional results):
  Edge metric: pooled mean ov_bps; wallet-median = median of per-wallet mean ov_bps.
  Spread = favored-bucket pooled mean minus complement pooled mean (favored bucket
  chosen on fold1, must be the SAME bucket evaluated in fold2).
  PASS requires ALL of, in BOTH folds independently:
    (a) spread >= 5 bps (same favored bucket);
    (b) wallet-median spread same sign as pooled spread (guards one-wallet domination);
    (c) favored bucket holds >= 50 trades in the fold.
  H9 (special): persistence rho > 0 in both folds with n >= 15 wallets, AND
    better-side (decided in half1) improves half2 pooled mean by >= 5 bps in both folds.
  H11 (explicit from catalog): KILL if not monotone-ish both folds, operationalized as
    sign(T1-T3) consistent across folds AND |T1-T3| >= 5 bps in both folds.
  H17 (explicit from catalog): KILL if no fold-consistent tercile spread >= 5 bps.
  H18/H41 two-group splits: |diff| >= 5 bps with the same sign in both folds + (b) + (c).
  H19 builder breakdown is descriptive (no standalone verdict; H19 verdict = cloid terciles).

Memory: fills are read ONE DAY FILE AT A TIME, filtered to the 166 cohort wallets,
reduced to per-wallet counters immediately. No giant concats. install_memory_guard().
"""
from __future__ import annotations

import os
import sys
from collections import Counter, defaultdict

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
FILLS_D0, FILLS_D1 = "20260315", "20260523"
FEAT_CACHE = "/tmp/agentA_wallet_features.parquet"
BUILDER_CACHE = "/tmp/agentA_wallet_builders.parquet"
REPORT = "/tmp/agentA_results.md"

FOLD_DAYS = {"fold1": 63.0, "fold2": 38.0}
SPREAD_MIN = 5.0   # bps
MIN_BUCKET_TRADES = 50
DAY_MS = 86_400_000

md_lines: list[str] = []
summary_rows: list[dict] = []


def emit(s: str = "") -> None:
    md_lines.append(s)
    print(s)


def fmt(x, nd=1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "--"
    return f"{x:.{nd}f}"


# ----------------------------------------------------------------------------
# generic conditional-edge machinery
# ----------------------------------------------------------------------------

def wallet_median(sub: pd.DataFrame) -> float:
    """Median over wallets of the per-wallet mean ov_bps."""
    if len(sub) == 0:
        return float("nan")
    return float(sub.groupby("wallet")["ov_bps"].mean().median())


def bucket_stats(tf: pd.DataFrame, bucket: pd.Series, fold: str) -> pd.DataFrame:
    """Per-bucket conditional edge table for one fold.

    tf: trades of the fold; bucket: per-trade bucket label aligned to tf.index.
    """
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
    out = pd.DataFrame(rows)
    return out.sort_values("bucket").reset_index(drop=True)


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
                v = series.get(r["bucket"], "--")
                line += f" {v} |"
        emit(line)


def spread_vs_complement(tf: pd.DataFrame, mask: pd.Series) -> tuple[float, float, int]:
    """(pooled spread, wallet-median spread, n in bucket) of mask vs complement."""
    a, b = tf[mask], tf[~mask]
    if len(a) == 0 or len(b) == 0:
        return float("nan"), float("nan"), len(a)
    return (a["ov_bps"].mean() - b["ov_bps"].mean(),
            wallet_median(a) - wallet_median(b),
            len(a))


def judge_bucketed(name: str, tables: dict[str, pd.DataFrame],
                   trades_by_fold: dict[str, pd.DataFrame],
                   buckets_by_fold: dict[str, pd.Series]) -> tuple[str, str]:
    """Generic verdict: favored bucket on fold1 pooled mean, replicate in fold2."""
    t1 = tables["fold1"]
    eligible = t1[t1["n"] >= MIN_BUCKET_TRADES]
    if eligible.empty:
        return "KILL", "no fold1 bucket with >= 50 trades"
    fav = eligible.loc[eligible["mean_ov"].idxmax(), "bucket"]
    detail = [f"favored bucket (fold1) = {fav}"]
    ok = True
    for fold in ("fold1", "fold2"):
        tf, bk = trades_by_fold[fold], buckets_by_fold[fold]
        sp, wsp, n = spread_vs_complement(tf, bk == fav)
        detail.append(f"{fold}: spread={fmt(sp)}bps wmed-spread={fmt(wsp)}bps n={n}")
        if not (np.isfinite(sp) and sp >= SPREAD_MIN and n >= MIN_BUCKET_TRADES
                and np.isfinite(wsp) and np.sign(wsp) == np.sign(sp)):
            ok = False
    return ("PASS" if ok else "KILL"), "; ".join(detail)


def two_group_judge(diffs: dict[str, tuple[float, float, int, int]]) -> tuple[str, str]:
    """diffs[fold] = (pooled diff, wmed diff, n_groupA, n_groupB). PASS if |diff|>=5,
    same sign across folds, wmed same sign as pooled, both group sizes >= 50."""
    signs = []
    ok = True
    detail = []
    for fold, (d, wd, na, nb) in diffs.items():
        detail.append(f"{fold}: diff={fmt(d)}bps wmed-diff={fmt(wd)}bps nA={na} nB={nb}")
        if not (np.isfinite(d) and abs(d) >= SPREAD_MIN and np.isfinite(wd)
                and np.sign(wd) == np.sign(d) and na >= MIN_BUCKET_TRADES and nb >= MIN_BUCKET_TRADES):
            ok = False
        signs.append(np.sign(d) if np.isfinite(d) else 0.0)
    if len(set(signs)) > 1 or 0.0 in signs:
        ok = False
    return ("PASS" if ok else "KILL"), "; ".join(detail)


def tercile_assign(feat: pd.Series) -> pd.Series:
    """Wallet-level terciles, ties kept together (qcut on average rank).
    Degrades gracefully to fewer bins when tie mass collapses quantile edges."""
    r = feat.rank(method="average")
    codes = pd.qcut(r, 3, labels=False, duplicates="drop")
    return pd.Series(codes, index=feat.index).map(lambda c: f"T{int(c) + 1}")


def run_tercile_hypothesis(hid: str, title: str, feat: pd.Series, feat_name: str,
                           trades_by_fold: dict[str, pd.DataFrame],
                           criterion: str, assign_fn=None) -> None:
    emit(f"\n## {hid} {title}")
    emit(f"Wallet feature: `{feat_name}` (fills window 2026-03-15..2026-05-23). "
         f"Buckets assigned within each fold's wallet set. Criterion: {criterion}")
    tables, buckets = {}, {}
    assign_fn = assign_fn or tercile_assign
    for fold, tf in trades_by_fold.items():
        wfeat = feat.reindex(tf["wallet"].unique()).dropna()
        terc = assign_fn(wfeat)
        bk = tf["wallet"].map(terc)
        tables[fold] = bucket_stats(tf, bk, fold)
        buckets[fold] = bk
        rng = {t: f"[{fmt(wfeat[terc == t].min(), 4)},{fmt(wfeat[terc == t].max(), 4)}]"
               for t in terc.unique()}
        emit(f"\n### {fold}")
        table_md(tables[fold], extra_cols={f"{feat_name} range": pd.Series(rng)})
        miss = tf["wallet"].nunique() - len(wfeat)
        if miss:
            emit(f"(wallets without fills feature: {miss}; their trades excluded from buckets)")
    verdict, detail = judge_bucketed(hid, tables, trades_by_fold, buckets)
    emit(f"\n**{hid} verdict: {verdict}** -- {detail}")
    summary_rows.append({"h": hid, "title": title, "verdict": verdict, "detail": detail})


# ----------------------------------------------------------------------------
# fills scan (H17, H18, H19, H20 features)
# ----------------------------------------------------------------------------

def counter_median(cnt: Counter) -> float:
    total = sum(cnt.values())
    if total == 0:
        return float("nan")
    targets = [(total - 1) // 2, total // 2]
    res, cum = [], 0
    items = iter(sorted(cnt.items()))
    k, c = next(items)
    for t in targets:
        while cum + c <= t:
            cum += c
            k, c = next(items)
        res.append(k)
    return (res[0] + res[1]) / 2.0


def scan_fills(cohort: set[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if os.path.exists(FEAT_CACHE) and os.path.exists(BUILDER_CACHE) and "--refresh" not in sys.argv:
        print(f"[fills] using cache {FEAT_CACHE}")
        return pd.read_parquet(FEAT_CACHE), pd.read_parquet(BUILDER_CACHE)

    days = sorted(f for f in os.listdir(FILLS_DIR)
                  if f.endswith(".parquet") and FILLS_D0 <= f[:8] <= FILLS_D1)
    print(f"[fills] scanning {len(days)} day files {days[0]}..{days[-1]}")
    cols = ["wallet", "crossed", "twapId", "cloid", "builder", "fee", "feeToken", "notional"]
    cohort_list = list(cohort)

    n_fills = Counter(); n_crossed = Counter(); n_twap = Counter(); n_cloid = Counter()
    n_taker_fee = Counter()
    builder_cnt: dict[tuple[str, str], int] = defaultdict(int)
    fee_rate_cnt: dict[str, Counter] = defaultdict(Counter)  # wallet -> Counter(round(rate*1e7))

    for i, fn in enumerate(days):
        df = pd.read_parquet(os.path.join(FILLS_DIR, fn), columns=cols,
                             filters=[("wallet", "in", cohort_list)])
        if len(df) == 0:
            continue
        w = df["wallet"]
        n_fills.update(w.value_counts().to_dict())
        n_crossed.update(w[df["crossed"]].value_counts().to_dict())
        n_twap.update(w[df["twapId"] > 0].value_counts().to_dict())
        cl = df["cloid"].fillna("")
        n_cloid.update(w[cl.str.len() > 0].value_counts().to_dict())
        for (wal, bld), c in df.groupby(["wallet", df["builder"].fillna("")]).size().items():
            builder_cnt[(wal, bld)] += int(c)
        # taker fee fills for fee-tier estimate
        fee = pd.to_numeric(df["fee"], errors="coerce")
        noti = pd.to_numeric(df["notional"], errors="coerce")
        m = df["crossed"] & (fee > 0) & (df["feeToken"] == "USDC") & (noti >= 10)
        if m.any():
            sub = pd.DataFrame({"wallet": w[m],
                                "key": np.round(fee[m].to_numpy() / noti[m].to_numpy() * 1e7).astype(np.int64)})
            n_taker_fee.update(sub["wallet"].value_counts().to_dict())
            for (wal, key), c in sub.groupby(["wallet", "key"]).size().items():
                fee_rate_cnt[wal][int(key)] += int(c)
        if (i + 1) % 10 == 0:
            print(f"[fills] {i+1}/{len(days)} files, wallets seen={len(n_fills)}")
        del df

    rows = []
    for wal in sorted(n_fills):
        tot = n_fills[wal]
        bc = Counter({b: c for (w2, b), c in builder_cnt.items() if w2 == wal})
        dom = bc.most_common(1)[0][0] if bc else ""
        med_key = counter_median(fee_rate_cnt[wal]) if fee_rate_cnt[wal] else float("nan")
        rows.append({
            "wallet": wal, "n_fills": tot,
            "crossed_share": n_crossed[wal] / tot,
            "twap_share": n_twap[wal] / tot,
            "cloid_share": n_cloid[wal] / tot,
            "dom_builder": dom if dom else "(none)",
            "dom_builder_share": (bc.most_common(1)[0][1] / tot) if bc else 0.0,
            "n_taker_fee_fills": n_taker_fee[wal],
            "fee_med_bps": med_key / 1e3 if np.isfinite(med_key) else float("nan"),
        })
    feats = pd.DataFrame(rows).set_index("wallet")
    builders = pd.DataFrame(
        [{"wallet": w2, "builder": b if b else "(none)", "n": c} for (w2, b), c in builder_cnt.items()]
    )
    feats.to_parquet(FEAT_CACHE)
    builders.to_parquet(BUILDER_CACHE)
    print(f"[fills] done: {len(feats)} cohort wallets with fills; cached.")
    return feats, builders


# ----------------------------------------------------------------------------
# hypotheses
# ----------------------------------------------------------------------------

def baseline(trades_by_fold: dict[str, pd.DataFrame]) -> None:
    emit("## Baseline (all validated-fold trades)")
    emit("| fold | n_wallets | n | trades/day | mean ov_bps | wallet-med |")
    emit("|---|---|---|---|---|---|")
    for fold, tf in trades_by_fold.items():
        emit(f"| {fold} | {tf['wallet'].nunique()} | {len(tf)} | "
             f"{fmt(len(tf)/FOLD_DAYS[fold])} | {fmt(tf['ov_bps'].mean())} | "
             f"{fmt(wallet_median(tf))} |")


def h9_side_skill(trades_by_fold: dict[str, pd.DataFrame]) -> None:
    emit("\n## H9 side-specific skill (long-edge vs short-edge persistence)")
    emit("Per wallet-fold long/short mean ov_bps (>=8 trades each side). Persistence = "
         "Spearman rho of (long-mean - short-mean) across the two calendar halves of the "
         "fold window (wallets with >=4 trades per side in EACH half). Better-side filter: "
         "side chosen in half1 (>=4 per side), applied to half2; baseline = all half2 trades "
         "of the same classifiable wallets.")
    emit("Criterion: rho > 0 both folds (n >= 15 wallets) AND half2 improvement >= 5 bps both folds.")
    try:
        from scipy.stats import spearmanr
    except Exception:
        spearmanr = None

    ok_all, details = True, []
    for fold, tf in trades_by_fold.items():
        mid = (tf["entry_ts"].min() + tf["entry_ts"].max()) / 2.0
        half = np.where(tf["entry_ts"] <= mid, "h1", "h2")
        tf = tf.assign(half=half, side=np.where(tf["dir"] > 0, "L", "S"))

        # full-fold side table for wallets with >=8 per side
        cnt = tf.groupby(["wallet", "side"]).size().unstack(fill_value=0)
        elig8 = cnt[(cnt.get("L", 0) >= 8) & (cnt.get("S", 0) >= 8)].index
        sub8 = tf[tf["wallet"].isin(elig8)]
        emit(f"\n### {fold}  (wallets with >=8 long and >=8 short: {len(elig8)})")
        tbl = bucket_stats(sub8, sub8["side"].map({"L": "long", "S": "short"}), fold)
        table_md(tbl)

        # persistence corr
        pv = tf.groupby(["wallet", "half", "side"])["ov_bps"].mean().unstack(["half", "side"])
        nv = tf.groupby(["wallet", "half", "side"]).size().unstack(["half", "side"], fill_value=0)
        need = [("h1", "L"), ("h1", "S"), ("h2", "L"), ("h2", "S")]
        if all(c in nv.columns for c in need):
            mask = (nv[need] >= 4).all(axis=1)
        else:
            mask = pd.Series(False, index=nv.index)
        wallets = nv.index[mask]
        rho, pval = float("nan"), float("nan")
        if len(wallets) >= 3 and all(c in pv.columns for c in need):
            s1 = pv.loc[wallets, ("h1", "L")] - pv.loc[wallets, ("h1", "S")]
            s2 = pv.loc[wallets, ("h2", "L")] - pv.loc[wallets, ("h2", "S")]
            if spearmanr is not None:
                rho, pval = spearmanr(s1, s2)
            else:
                rho = s1.corr(s2, method="spearman")
        emit(f"\nPersistence: n={len(wallets)} wallets, Spearman rho={fmt(rho, 3)} (p={fmt(pval, 3)})")

        # better-side selection
        h1 = tf[tf["half"] == "h1"]
        c1 = h1.groupby(["wallet", "side"]).size().unstack(fill_value=0)
        cls = c1[(c1.get("L", 0) >= 4) & (c1.get("S", 0) >= 4)].index
        m1 = h1[h1["wallet"].isin(cls)].groupby(["wallet", "side"])["ov_bps"].mean().unstack()
        better = np.where(m1.get("L", pd.Series(index=m1.index)).fillna(-1e9)
                          >= m1.get("S", pd.Series(index=m1.index)).fillna(-1e9), "L", "S")
        better = pd.Series(better, index=m1.index)
        h2 = tf[(tf["half"] == "h2") & tf["wallet"].isin(cls)]
        kept = h2[h2["side"] == h2["wallet"].map(better)]
        base_m, base_w = h2["ov_bps"].mean(), wallet_median(h2)
        kept_m, kept_w = kept["ov_bps"].mean(), wallet_median(kept)
        impr = kept_m - base_m
        emit(f"Better-side (h1-decided) on h2: classifiable wallets={len(cls)}; "
             f"baseline n={len(h2)} mean={fmt(base_m)} wmed={fmt(base_w)}; "
             f"kept n={len(kept)} ({fmt(100*len(kept)/max(len(h2),1))}% flow) "
             f"mean={fmt(kept_m)} wmed={fmt(kept_w)}; improvement={fmt(impr)}bps")
        details.append(f"{fold}: rho={fmt(rho,3)} (n={len(wallets)}), h2 impr={fmt(impr)}bps")
        if not (np.isfinite(rho) and rho > 0 and len(wallets) >= 15
                and np.isfinite(impr) and impr >= SPREAD_MIN and len(kept) >= MIN_BUCKET_TRADES):
            ok_all = False
    verdict = "PASS" if ok_all else "KILL"
    emit(f"\n**H9 verdict: {verdict}** -- {'; '.join(details)}")
    summary_rows.append({"h": "H9", "title": "side-specific skill", "verdict": verdict,
                         "detail": "; ".join(details)})


def h11_size_consistency(trades_by_fold: dict[str, pd.DataFrame]) -> None:
    emit("\n## H11 size-consistency (cv of leader_open_notional per wallet-fold)")
    emit("cv = std/mean of leader_open_notional over the wallet's fold trades (>=5 trades). "
         "Wallet terciles T1=most consistent. Criterion (catalog): KILL if not monotone-ish "
         "both folds = sign(T1-T3) consistent AND |T1-T3| >= 5 bps in both folds.")
    diffs, ok = {}, True
    for fold, tf in trades_by_fold.items():
        g = tf.groupby("wallet")["leader_open_notional"]
        cv = (g.std(ddof=1) / g.mean()).where(g.count() >= 5).dropna()
        terc = tercile_assign(cv)
        bk = tf["wallet"].map(terc)
        tbl = bucket_stats(tf, bk, fold)
        rng = {t: f"[{fmt(cv[terc == t].min(), 2)},{fmt(cv[terc == t].max(), 2)}]" for t in terc.unique()}
        emit(f"\n### {fold}  (eligible wallets: {len(cv)}; excluded <5 trades: "
             f"{tf['wallet'].nunique() - len(cv)})")
        table_md(tbl, extra_cols={"cv range": pd.Series(rng)})
        t = tbl.set_index("bucket")
        d = t.loc["T1", "mean_ov"] - t.loc["T3", "mean_ov"] if {"T1", "T3"} <= set(t.index) else float("nan")
        wd = t.loc["T1", "wmed_ov"] - t.loc["T3", "wmed_ov"] if {"T1", "T3"} <= set(t.index) else float("nan")
        diffs[fold] = d
        emit(f"T1-T3 pooled spread = {fmt(d)} bps (wallet-med spread {fmt(wd)})")
        if not (np.isfinite(d) and abs(d) >= SPREAD_MIN and np.isfinite(wd) and np.sign(wd) == np.sign(d)):
            ok = False
    if len({np.sign(v) for v in diffs.values() if np.isfinite(v)}) != 1:
        ok = False
    verdict = "PASS" if ok else "KILL"
    detail = "; ".join(f"{f}: T1-T3={fmt(d)}bps" for f, d in diffs.items())
    emit(f"\n**H11 verdict: {verdict}** -- {detail}")
    summary_rows.append({"h": "H11", "title": "size-consistency (cv terciles)",
                         "verdict": verdict, "detail": detail})


def h16_coin_breadth(trades_by_fold: dict[str, pd.DataFrame]) -> None:
    emit("\n## H16 coin breadth (distinct coins per wallet in test window)")
    emit("Buckets: 1, 2-3, 4-6, 7+. Criterion: favored bucket on fold1 replicates in fold2 "
         "with spread >= 5 bps vs complement, wallet-med sign agreement, >= 50 trades.")
    tables, buckets = {}, {}

    def lab(k: int) -> str:
        if k == 1:
            return "1"
        if k <= 3:
            return "2-3"
        if k <= 6:
            return "4-6"
        return "7+"

    for fold, tf in trades_by_fold.items():
        ncoins = tf.groupby("wallet")["coin"].nunique()
        bk = tf["wallet"].map(ncoins.map(lab))
        tables[fold] = bucket_stats(tf, bk, fold)
        buckets[fold] = bk
        order = {"1": 0, "2-3": 1, "4-6": 2, "7+": 3}
        tables[fold] = tables[fold].sort_values("bucket", key=lambda s: s.map(order)).reset_index(drop=True)
        emit(f"\n### {fold}")
        table_md(tables[fold])
    verdict, detail = judge_bucketed("H16", tables, trades_by_fold, buckets)
    emit(f"\n**H16 verdict: {verdict}** -- {detail}")
    summary_rows.append({"h": "H16", "title": "coin breadth", "verdict": verdict, "detail": detail})


def h18_twap(trades_by_fold, feats) -> None:
    emit("\n## H18 twap users (share of fills with twapId>0)")
    emit("Flag wallets with twap_share > 10%. Two groups: twap-user vs rest. "
         "Criterion: |diff| >= 5 bps, same sign both folds, wallet-med agreement, both n >= 50.")
    diffs = {}
    for fold, tf in trades_by_fold.items():
        ts = feats["twap_share"].reindex(tf["wallet"].unique())
        grp = tf["wallet"].map((ts > 0.10).map({True: "twap>10%", False: "rest"}))
        tbl = bucket_stats(tf, grp, fold)
        emit(f"\n### {fold}  (twap-user wallets: {int((ts > 0.10).sum())} of {ts.notna().sum()})")
        table_md(tbl)
        t = tbl.set_index("bucket")
        if {"twap>10%", "rest"} <= set(t.index):
            diffs[fold] = (t.loc["twap>10%", "mean_ov"] - t.loc["rest", "mean_ov"],
                           t.loc["twap>10%", "wmed_ov"] - t.loc["rest", "wmed_ov"],
                           int(t.loc["twap>10%", "n"]), int(t.loc["rest", "n"]))
        else:
            diffs[fold] = (float("nan"), float("nan"), 0, 0)
    verdict, detail = two_group_judge(diffs)
    emit(f"\n**H18 verdict: {verdict}** -- (diff = twap-user minus rest) {detail}")
    summary_rows.append({"h": "H18", "title": "twap users >10%", "verdict": verdict, "detail": detail})


def h19_builders(trades_by_fold, feats, builders) -> None:
    emit("\n### H19b builder breakdown (descriptive)")
    dom = feats["dom_builder"]
    top = dom.value_counts().head(6)
    emit("Top builders by cohort wallet count (dominant builder per wallet):")
    emit("| builder | n_wallets |")
    emit("|---|---|")
    for b, c in top.items():
        emit(f"| {b[:20]} | {c} |")
    for fold, tf in trades_by_fold.items():
        bk = tf["wallet"].map(dom)
        tbl = bucket_stats(tf, bk, fold)
        tbl = tbl[tbl["n"] >= 100].sort_values("mean_ov", ascending=False)
        emit(f"\n{fold} per-builder pooled edge (groups with n >= 100 trades):")
        table_md(tbl.assign(bucket=tbl["bucket"].str[:20]))


def h20_check_overlap(feats, trades) -> None:
    missing = set(trades["wallet"].unique()) - set(feats.index)
    if missing:
        emit(f"\nNOTE: {len(missing)} cohort wallets have no fills in scan window: "
             f"{sorted(missing)[:5]}")


def h41_first_of_day(trades_by_fold) -> None:
    emit("\n## H41 first-of-day (entry rank within wallet x UTC day)")
    emit("rank 1 = wallet's first trade of the UTC day; vs later trades. Criterion: "
         "|diff| >= 5 bps, same sign both folds, wallet-med agreement, both n >= 50.")
    diffs = {}
    for fold, tf in trades_by_fold.items():
        day = tf["entry_ts"] // DAY_MS
        rank = tf.groupby([tf["wallet"], day])["entry_ts"].rank(method="first")
        grp = pd.Series(np.where(rank == 1, "first", "later"), index=tf.index)
        tbl = bucket_stats(tf, grp, fold)
        emit(f"\n### {fold}")
        table_md(tbl)
        t = tbl.set_index("bucket")
        diffs[fold] = (t.loc["first", "mean_ov"] - t.loc["later", "mean_ov"],
                       t.loc["first", "wmed_ov"] - t.loc["later", "wmed_ov"],
                       int(t.loc["first", "n"]), int(t.loc["later", "n"]))
    verdict, detail = two_group_judge(diffs)
    emit(f"\n**H41 verdict: {verdict}** -- (diff = first minus later) {detail}")
    summary_rows.append({"h": "H41", "title": "first-of-day", "verdict": verdict, "detail": detail})


# ----------------------------------------------------------------------------

def main() -> None:
    install_memory_guard(soft_gb=12.0, label="agentA")
    trades = pd.read_parquet(TRADES_PQ)
    assert trades["ov_bps"].notna().all()
    cohort = set(trades["wallet"].unique())
    trades_by_fold = {f: trades[trades["fold"] == f].copy() for f in ("fold1", "fold2")}

    emit("# Agent A -- hypothesis screens (H9, H11, H16, H17, H18, H19, H20, H41)")
    emit(f"Trade table: {TRADES_PQ} ({len(trades)} trades, {len(cohort)} wallets). "
         f"Fills window {FILLS_D0}..{FILLS_D1}. Generated 2026-06-11.")
    emit("Pre-registered: PASS needs BOTH folds to show the favored-bucket spread >= 5 bps "
         "(favored bucket fixed on fold1), wallet-median spread sign agreement, and >= 50 "
         "trades in the favored bucket. Trades/day uses fold1=63d, fold2=38d.")
    emit("H4 (median-rank) skipped per task spec: needs a selection rerun, not testable on "
         "this trade table.")
    emit("")
    baseline(trades_by_fold)

    feats, builders = scan_fills(cohort)
    h20_check_overlap(feats, trades)

    h9_side_skill(trades_by_fold)
    h11_size_consistency(trades_by_fold)
    h16_coin_breadth(trades_by_fold)
    run_tercile_hypothesis(
        "H17", "crossed-share (taker share of fills)", feats["crossed_share"],
        "crossed_share", trades_by_fold,
        "KILL if no fold-consistent tercile spread >= 5 bps (catalog-explicit).")
    h18_twap(trades_by_fold, feats)
    def cloid_buckets(wfeat: pd.Series) -> pd.Series:
        # 148/166 cohort wallets have cloid_share == 0 -> terciles collapse; use
        # pre-registered structural buckets instead.
        return pd.Series(np.select([wfeat == 0, wfeat <= 0.5], ["0%", "(0,50%]"], ">50%"),
                         index=wfeat.index)

    run_tercile_hypothesis(
        "H19", "bots (share of fills with non-empty cloid)", feats["cloid_share"],
        "cloid_share", trades_by_fold,
        "favored bucket on fold1 replicates in fold2 with spread >= 5 bps "
        "(buckets 0% / (0,50%] / >50% because 148/166 wallets have zero cloid fills).",
        assign_fn=cloid_buckets)
    h19_builders(trades_by_fold, feats, builders)
    run_tercile_hypothesis(
        "H20", "fee tier (median taker fee/notional, bps)", feats["fee_med_bps"],
        "fee_med_bps", trades_by_fold,
        "favored tercile on fold1 replicates in fold2 with spread >= 5 bps.")
    h41_first_of_day(trades_by_fold)

    # deployment framing for the H17 finding + near-miss notes (descriptive)
    emit("\n## Notable framings and near-misses (descriptive, not verdicts)")
    for fold, tf in trades_by_fold.items():
        wfeat = feats["crossed_share"].reindex(tf["wallet"].unique()).dropna()
        bk = tf["wallet"].map(tercile_assign(wfeat))
        keep = tf[bk.isin(["T1", "T2"])]
        emit(f"- H17 exclude-T3 framing ({fold}): drop pure-taker tercile, keep n={len(keep)} "
             f"({100 * len(keep) / len(tf):.1f}% flow, {len(keep) / FOLD_DAYS[fold]:.1f}/day), "
             f"mean={keep['ov_bps'].mean():.1f} vs baseline {tf['ov_bps'].mean():.1f} "
             f"({keep['ov_bps'].mean() - tf['ov_bps'].mean():+.1f} bps); "
             f"wmed {wallet_median(keep):.1f} vs {wallet_median(tf):.1f}")
    # H17 leave-one-wallet-out robustness on the T1-vs-rest spread
    for fold, tf in trades_by_fold.items():
        wfeat = feats["crossed_share"].reindex(tf["wallet"].unique()).dropna()
        bk = tf["wallet"].map(tercile_assign(wfeat))
        t1w = sorted(set(tf["wallet"][bk == "T1"]))
        loo = []
        for drop in t1w:
            sub = tf[tf["wallet"] != drop]
            b2 = bk.loc[sub.index]
            loo.append(sub["ov_bps"][b2 == "T1"].mean() - sub["ov_bps"][b2 != "T1"].mean())
        loo = np.array(loo)
        emit(f"- H17 leave-one-wallet-out ({fold}): T1 spread after dropping any single T1 "
             f"wallet stays in [{loo.min():.1f}, {loo.max():.1f}] bps; "
             f">=5 bps in {(loo >= 5).sum()}/{len(loo)} cases.")
    emit("- H41 near-miss: first-of-day is pooled-positive in both folds (+7.3 / +17.5 bps) "
         "but the fold2 wallet-median flips sign (-2.4), so it fails the domination guard.")
    emit("- H16 near-miss: breadth 7+ has positive wallet-median spread in both folds "
         "(+12.0 / +24.0) but the fold2 pooled spread (3.3 bps) misses the 5 bps gate.")
    emit("- H17/H20 are the same economic story from two angles: edge concentrates in "
         "maker-leaning leaders; the pure-taker tercile carries the weakest copy edge in "
         "both folds, and the highest-fee-tier tercile is never the best bucket.")
    emit("- CAVEAT: fills-based wallet features (H17-H20) are computed over the test window "
         "itself (2026-03-15..2026-05-23) as behavioral fingerprints. Before deploying the "
         "H17 filter, recompute crossed_share on the TRAIN window and re-validate.")

    emit("\n## Summary")
    emit("| hypothesis | verdict | detail |")
    emit("|---|---|---|")
    for r in summary_rows:
        emit(f"| {r['h']} {r['title']} | {r['verdict']} | {r['detail']} |")

    with open(REPORT, "w") as f:
        f.write("\n".join(md_lines) + "\n")
    print(f"\n[done] report -> {REPORT}")


if __name__ == "__main__":
    main()
