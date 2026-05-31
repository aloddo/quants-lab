#!/usr/bin/env python3
"""V15 M5 — Eligibility + Copyability floors (fold-pure, aligned to G5).

Design codex-SHIP: projects/quant/v15/modules/m05. LAYER 2 cheap selection, after M4.

A pass/fail FLOOR (NOT ranking, NOT danger, NOT survival). For each M4-non-KILL copyable entity
(its primary wallet), per M3 fold, computed ON THE PRETEST WINDOW [train_start_k, test_start_k)
(fold-pure: only data before fold k's OOS test). LOOSE by design ("keep when in doubt").

Floors (per entity, per fold, pretest window):
  1. net realized PnL (incl fees+funding) > 0
  2. flow-adjusted TWR ROE > 0 (+ structural ruin guard)         [50% is full-window @ pool, not here]
  3. max drawdown <= 0.80
  4. n_journeys_pretest >= 3
  5. copyability: median hold > HOLD_FLOOR (60s, measured from V11 copy-path latency) AND
     share of journeys closing faster than our p95 copy latency <= 0.25
  6. accessible coins >= 0.80 by notional, judged as-of test_start_k (unknown -> don't fire, flag)
Cross-fold G5 (active_test_folds>=3, total n_journeys>=5, source_6m_ROE>=50%) is REPORTED here as
g5_pool_candidate_pass, ENFORCED at pool assembly (M6/M9) — never suppresses per-fold eligibility.

CLI:
    python v15_m05_eligibility.py --folds m03_folds.parquet --journeys m02_journeys.parquet \
        --equity m01_universe_20k_series.parquet --m04 m04_authenticity.parquet \
        --outdir app/data/v15 [--accessible-coins accessible.json]
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v15_m05] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v15_m05")

# Floors (design modules/m05) ------------------------------------------------- #
MAXDD_CAP = 0.80
MIN_JOURNEYS_PRETEST = 3
HOLD_FLOOR_S = 60.0                 # measured from V11 copy-path latency (p95~15s -> max(2*15,60)=60)
P95_COPY_LATENCY_S = 15.0           # websocket copy path p95 (detection 2.3s + exec 11s); NOT the 2min reconcile tail
SHARE_BELOW_LATENCY_CAP = 0.25
ACCESSIBLE_FRAC_MIN = 0.80
ROE_FULL_FLOOR_G5 = 0.50            # full-window pool gate only
RUIN_EQUITY_FLOOR = 1.0            # equity path collapse / invalid-base guard ($)


def _ms(ts) -> int:
    return int(pd.Timestamp(ts, tz="UTC").timestamp() * 1000)


def flow_adjusted_twr(eq: pd.DataFrame) -> dict:
    """Flow-adjusted time-weighted return + maxDD over an equity-day frame (M1 series cols).

    eq: rows with `date`, `equity_usd`, `ext_flow_cum` (cumulative external deposits/withdrawals).
    r_t = (eq_t - eq_{t-1} - flow_t) / eq_{t-1}; ROE = prod(1+r)-1; maxDD on cumprod.
    structural_ruin = equity base unusable (no day with eq_prev > RUIN_EQUITY_FLOOR) or terminal
    collapse to ~0 from a real base.
    """
    d = eq.sort_values("date").reset_index(drop=True)
    if len(d) < 2:
        return {"roe": 0.0, "max_dd": 0.0, "n_days": len(d), "structural_ruin": True}
    e = d["equity_usd"].astype(float).fillna(0.0)
    flow = d["ext_flow_cum"].astype(float).ffill().fillna(0.0).diff().fillna(0.0)
    e_prev = e.shift(1)
    denom = e_prev.where(e_prev > RUIN_EQUITY_FLOOR, np.nan)   # need >$1 base to define a return
    r = ((e - e_prev - flow) / denom).replace([np.inf, -np.inf], np.nan).dropna()
    if len(r) == 0:
        return {"roe": 0.0, "max_dd": 0.0, "n_days": len(d), "structural_ruin": True}
    # codex code-r1 #3: prepend baseline 1.0 so a first-day drop is counted in maxDD.
    cum = pd.concat([pd.Series([1.0]), (1 + r).cumprod()], ignore_index=True)
    peak = cum.cummax()
    max_dd = float(abs(((cum - peak) / peak).min()))
    roe = float(cum.iloc[-1] - 1)
    # terminal collapse: started with a real base but ended ~0
    start_eq = float(e[e > RUIN_EQUITY_FLOOR].iloc[0]) if (e > RUIN_EQUITY_FLOOR).any() else 0.0
    ruin = (start_eq <= RUIN_EQUITY_FLOOR) or (float(e.iloc[-1]) <= RUIN_EQUITY_FLOOR and start_eq > 100)
    return {"roe": roe, "max_dd": max_dd, "n_days": len(d), "structural_ruin": bool(ruin)}


def journey_metrics(jr: pd.DataFrame, lo_ms: int, hi_ms: int, accessible: set | None) -> dict:
    """Pretest journey metrics. CLOSED-in-pretest journeys (entry & exit in [lo,hi)) for
    PnL/hold/count; accessibility by notional over those journeys."""
    if jr is None or len(jr) == 0:
        return _empty_jm()
    # codex code-r1 #4: closed-in-pretest = entry AND exit both inside [lo, hi).
    j = jr[(jr["entry_ts"] >= lo_ms) & (jr["entry_ts"] < hi_ms)
           & jr["exit_ts"].notna() & (jr["exit_ts"] >= lo_ms) & (jr["exit_ts"] < hi_ms)].copy()
    if len(j) == 0:
        return _empty_jm()
    net_pnl = float(j["net_realized_pnl"].sum())
    n_j = int(len(j))
    dur_s = (j["duration_h"].astype(float) * 3600.0)   # m02 emits duration_h (not duration_hours)
    median_hold_s = float(dur_s.median())
    share_below = float((dur_s < P95_COPY_LATENCY_S).mean())
    # accessibility by notional (max_position_notional proxy for journey size)
    # codex code-r1 #5: index-aligned fallback (j.index) so the in_acc boolean mask aligns.
    if "max_position_notional" in j.columns:
        notional = j["max_position_notional"].astype(float).abs()
    else:
        notional = pd.Series(1.0, index=j.index)
    if accessible is None:
        acc_notional = float("nan"); acc_count = float("nan")  # unknown
    else:
        in_acc = j["coin"].isin(accessible)
        tot = notional.sum()
        acc_notional = float(notional[in_acc].sum() / tot) if tot > 0 else 1.0
        acc_count = float(in_acc.mean())
    return {"net_pnl": net_pnl, "n_journeys": n_j, "median_hold_s": median_hold_s,
            "share_below_latency": share_below, "accessible_frac_notional": acc_notional,
            "accessible_frac_count": acc_count}


def _empty_jm() -> dict:
    return {"net_pnl": 0.0, "n_journeys": 0, "median_hold_s": 0.0, "share_below_latency": 1.0,
            "accessible_frac_notional": float("nan"), "accessible_frac_count": float("nan")}


def apply_floors(eqm: dict, jm: dict) -> tuple[bool, list[str]]:
    """Per-fold pretest floors. Returns (eligible, fail_reasons)."""
    f = []
    if jm["net_pnl"] <= 0:
        f.append(f"net_pnl<=0 ({jm['net_pnl']:.1f})")
    if eqm["structural_ruin"]:
        f.append("structural_ruin")
    elif eqm["roe"] <= 0:
        f.append(f"roe<=0 ({eqm['roe']:.2%})")
    if eqm["max_dd"] > MAXDD_CAP:
        f.append(f"max_dd>{MAXDD_CAP:.0%} ({eqm['max_dd']:.0%})")
    if jm["n_journeys"] < MIN_JOURNEYS_PRETEST:
        f.append(f"n_journeys<{MIN_JOURNEYS_PRETEST} ({jm['n_journeys']})")
    if jm["median_hold_s"] <= HOLD_FLOOR_S:
        f.append(f"median_hold<={HOLD_FLOOR_S:.0f}s ({jm['median_hold_s']:.0f}s)")
    if jm["share_below_latency"] > SHARE_BELOW_LATENCY_CAP:
        f.append(f"share_below_latency>{SHARE_BELOW_LATENCY_CAP:.0%} ({jm['share_below_latency']:.0%})")
    # accessibility: unknown (NaN) does NOT fire (loose); only fail when known and < min
    af = jm["accessible_frac_notional"]
    if af == af and af < ACCESSIBLE_FRAC_MIN:
        f.append(f"accessible_frac<{ACCESSIBLE_FRAC_MIN:.0%} ({af:.0%})")
    return (len(f) == 0, f)


def run(folds: pd.DataFrame, journeys: pd.DataFrame, equity: pd.DataFrame, m04: pd.DataFrame,
        accessible_by_fold: dict | None = None,
        active_test_folds_by_wallet: dict | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    # entities to evaluate: M4 copyable primaries — defensive filter on all three (codex code-r1 #2).
    prim = m04[(m04["copyable"] == True)  # noqa: E712
               & (m04.get("is_entity_primary", True) == True)  # noqa: E712
               & (m04["tier"] != "KILL")].copy()
    logger.info(f"M4 copyable primaries: {len(prim)} entities")
    jr_by = {w: g for w, g in journeys.groupby("wallet")} if len(journeys) else {}
    # codex code-r1 #6: precompute date_ms ONCE (no per-entity per-fold .apply(_ms)).
    eq_by = {}
    if len(equity):
        eq = equity.copy()
        # Force ms resolution then int64 = epoch ms (consistent with _ms). pandas 2.x infers
        # datetime64[s] from date objects, so a bare astype(int64) yields SECONDS — wrong by 10^6.
        eq["date_ms"] = pd.to_datetime(eq["date"]).astype("datetime64[ms]").astype("int64")
        eq_by = {w: g for w, g in eq.groupby("wallet")}

    rows = []
    waterfall = {"folds": {}}
    for _, fr in folds.sort_values("fold_id").iterrows():
        fid = int(fr["fold_id"])
        lo_ms, hi_ms = _ms(fr["train_start"]), _ms(fr["test_start"])  # pretest = [train_start, test_start)
        acc = (accessible_by_fold or {}).get(fid)            # set or None(unknown)
        acc_as_of = hi_ms if acc is not None else None
        n_eval = n_elig = 0
        for _, p in prim.iterrows():
            w = p["wallet"]
            if w in eq_by:
                g = eq_by[w]
                eqm = flow_adjusted_twr(g[(g["date_ms"] >= lo_ms) & (g["date_ms"] < hi_ms)])
            else:
                eqm = {"roe": 0.0, "max_dd": 0.0, "n_days": 0, "structural_ruin": True}
            jm = journey_metrics(jr_by.get(w), lo_ms, hi_ms, acc)
            elig, fails = apply_floors(eqm, jm)
            n_eval += 1; n_elig += int(elig)
            rows.append({
                "entity_id": int(p["entity_id"]), "primary_wallet": w, "fold_id": fid,
                "eligible": elig, "fail_reasons": "|".join(fails),
                "net_pnl_pretest": jm["net_pnl"], "roe_pretest_flow_adj": eqm["roe"],
                "max_dd_pretest": eqm["max_dd"], "structural_ruin_flag": eqm["structural_ruin"],
                "n_journeys_pretest": jm["n_journeys"], "median_hold_s_pretest": jm["median_hold_s"],
                "share_dur_below_latency": jm["share_below_latency"],
                "accessible_frac_notional": jm["accessible_frac_notional"],
                "accessible_frac_count": jm["accessible_frac_count"],
                "accessibility_unknown": acc is None,
                "accessible_set_as_of_ms": acc_as_of,
                "m4_tier": p["tier"], "q_codes": ",".join(c for c in str(p["reason_codes"]).split(",") if c.startswith("q:")),
                "as_of_ms": hi_ms,
            })
        waterfall["folds"][fid] = {"evaluated": n_eval, "eligible": n_elig}
        logger.info(f"  fold {fid}: {n_eval} evaluated -> {n_elig} eligible")
    elig_df = pd.DataFrame(rows)

    # pool summary (cross-fold DIAGNOSTIC; G5 enforced downstream at M6/M9, not here)
    # codex code-r1 #6: eligible_folds via one groupby, not a per-entity scan.
    elig_folds_by_eid = (elig_df.groupby("entity_id")["eligible"].sum().astype(int).to_dict()
                         if len(elig_df) else {})
    atf = active_test_folds_by_wallet or {}
    pool_rows = []
    for _, p in prim.iterrows():
        w = p["wallet"]; eid = int(p["entity_id"])
        full_jr = jr_by.get(w)
        total_nj = int((full_jr["exit_ts"].notna()).sum()) if full_jr is not None else 0
        full_roe = flow_adjusted_twr(eq_by[w])["roe"] if w in eq_by else 0.0
        active_tf = atf.get(w, np.nan)
        active_tf_known = bool(pd.notna(active_tf))   # codex code-r2: PER-WALLET, not global
        # codex code-r1 #1: G5 candidate REQUIRES active_test_folds>=3 too (from M3).
        g5 = bool(active_tf_known and active_tf >= 3 and total_nj >= 5 and full_roe >= ROE_FULL_FLOOR_G5)
        pool_rows.append({
            "entity_id": eid, "primary_wallet": w,
            "eligible_folds": elig_folds_by_eid.get(eid, 0),
            "active_test_folds": active_tf, "total_n_journeys": total_nj,
            "source_6m_roe_full": full_roe,
            "g5_pool_candidate_pass": g5,
            "g5_incomplete": not active_tf_known,   # this wallet's M3 active_test_folds missing -> undecidable
        })
    pool_df = pd.DataFrame(pool_rows)
    return elig_df, pool_df, waterfall


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--folds", required=True)
    ap.add_argument("--journeys", required=True)
    ap.add_argument("--equity", required=True)
    ap.add_argument("--m04", required=True)
    ap.add_argument("--m03-activity", default=None,
                    help="m03_wallet_activity_summary.parquet (for active_test_folds in the G5 pool flag)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--accessible-coins", default=None, help="JSON {fold_id: [coins]} or omit (unknown)")
    args = ap.parse_args()

    folds = pd.read_parquet(args.folds)
    journeys = pd.read_parquet(args.journeys)
    equity = pd.read_parquet(args.equity)
    m04 = pd.read_parquet(args.m04)
    acc = None
    if args.accessible_coins:
        raw = json.loads(Path(args.accessible_coins).read_text())
        acc = {int(k): set(v) for k, v in raw.items()}
    atf = None
    if args.m03_activity:
        a = pd.read_parquet(args.m03_activity)
        atf = dict(zip(a["key"], a["active_test_folds"]))
    logger.info(f"folds={len(folds)} journeys={len(journeys):,} equity_rows={len(equity):,} m04={len(m04)}")

    elig_df, pool_df, waterfall = run(folds, journeys, equity, m04, acc, atf)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    elig_df.to_parquet(outdir / "m05_eligibility.parquet", index=False)
    pool_df.to_parquet(outdir / "m05_pool_summary.parquet", index=False)
    (outdir / "m05_waterfall.json").write_text(json.dumps(waterfall, indent=2))
    elig_any = elig_df.groupby("entity_id")["eligible"].any().sum() if len(elig_df) else 0
    logger.info(f"eligible-entity-folds: {int(elig_df['eligible'].sum()) if len(elig_df) else 0}; "
                f"entities eligible in >=1 fold: {int(elig_any)}; "
                f"g5_pool_candidates: {int(pool_df['g5_pool_candidate_pass'].sum()) if len(pool_df) else 0}")


if __name__ == "__main__":
    main()
