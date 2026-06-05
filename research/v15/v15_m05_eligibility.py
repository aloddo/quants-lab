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
SWING_MAX_HOLD_S = 48 * 3600.0      # upper hold gate: V15 copies FAST directional (minutes-to-hours);
                                    # reject multi-day/week holders (median hold > 48h)
P95_COPY_LATENCY_S = 15.0           # WS copy path: median ~1s, but P95 tail can hit 15s (Alberto 2026-06-05)
SHARE_BELOW_LATENCY_CAP = 0.25
ACCESSIBLE_FRAC_MIN = 0.80
ROE_FULL_FLOOR_G5 = 0.50            # full-window pool gate only
RUIN_EQUITY_FLOOR = 1.0            # equity path collapse / invalid-base guard ($)
MIN_EQUITY_USD = 2000.0            # absolute pretest equity floor: drop tiny degen accounts that merely survived
# --- Alberto 2026-06-04 copy spec (aligned on Telegram; see handoffs/2026-06-04) ----------------- #
LEVERAGE_CAP = 10.0               # copy HIGH but NOT ultra-high leverage. Gate median daily notional/equity <= 10x.
DAYS_GREEN_MIN = 0.80            # consistency: >= 80% of active days flow-adjusted-positive
MIN_ACTIVE_DAYS_GREEN = 20       # only enforce the days-green gate when >= this many active days exist (else
                                 # the fold window is too short to judge consistency; other gates handle thin history)


def _ms(ts) -> int:
    return int(pd.Timestamp(ts, tz="UTC").timestamp() * 1000)


def _prim_from_m04(m04: pd.DataFrame) -> pd.DataFrame:
    """M4 copyable primary predicate, shared by single-file and per-fold M4 paths."""
    return m04[(m04["copyable"] == True)  # noqa: E712
               & (m04.get("is_entity_primary", True) == True)  # noqa: E712
               & (m04["tier"] != "KILL")].copy()


def _load_m04_by_fold(m04_dir: Path, folds: pd.DataFrame) -> dict[int, pd.DataFrame]:
    out = {}
    for fid in sorted(folds["fold_id"].astype(int).unique()):
        p = m04_dir / f"m04_authenticity_f{fid}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"missing fold-pure M4 file for fold {fid}: {p}")
        out[int(fid)] = pd.read_parquet(p)
    return out


def flow_adjusted_twr(eq: pd.DataFrame) -> dict:
    """Flow-adjusted time-weighted return + maxDD over an equity-day frame (M1 series cols).

    eq: rows with `date`, `equity_usd`, `ext_flow_cum` (cumulative external deposits/withdrawals).
    r_t = (eq_t - eq_{t-1} - flow_t) / eq_{t-1}; ROE = prod(1+r)-1; maxDD on cumprod.
    structural_ruin = equity base unusable (no day with eq_prev > RUIN_EQUITY_FLOOR) or terminal
    collapse to ~0 from a real base.
    """
    d = eq.sort_values("date").reset_index(drop=True)
    # median_equity (codex finding b): computed from RAW NON-NULL equity values -- NOT the 0.0-filled
    # series used for TWR. A data gap (missing equity_usd day) filled with 0.0 would drag the median
    # down and falsely fail the $2k floor. Median of an all-null/empty series is 0.0 (fails, correct).
    raw_eq = d["equity_usd"].astype(float).dropna() if len(d) else pd.Series(dtype=float)
    med_eq = float(raw_eq.median()) if len(raw_eq) else 0.0
    if len(d) < 2:
        return {"roe": 0.0, "max_dd": 0.0, "n_days": len(d), "structural_ruin": True,
                "median_equity": med_eq, "frac_days_green": 0.0, "n_active_days": 0,
                "median_leverage": 0.0}
    e = d["equity_usd"].astype(float).fillna(0.0)
    flow = d["ext_flow_cum"].astype(float).ffill().fillna(0.0).diff().fillna(0.0)
    e_prev = e.shift(1)
    denom = e_prev.where(e_prev > RUIN_EQUITY_FLOOR, np.nan)   # need >$1 base to define a return
    r = ((e - e_prev - flow) / denom).replace([np.inf, -np.inf], np.nan).dropna()
    # LEVERAGE (Alberto spec): median daily gross notional / equity over days with a real (>$1) base.
    if "position_value_usd" in d.columns:
        pv = d["position_value_usd"].astype(float).abs()
        lev = (pv / e.where(e > RUIN_EQUITY_FLOOR, np.nan)).replace([np.inf, -np.inf], np.nan).dropna()
        median_leverage = float(lev.median()) if len(lev) else 0.0
    else:
        median_leverage = 0.0
    if len(r) == 0:
        return {"roe": 0.0, "max_dd": 0.0, "n_days": len(d), "structural_ruin": True,
                "median_equity": med_eq, "frac_days_green": 0.0, "n_active_days": 0,
                "median_leverage": median_leverage}
    # CONSISTENCY (Alberto spec): share of ACTIVE days (a real flow-adjusted return defined) that are green.
    # "active" excludes flat/no-base days (r is already dropna'd to days with a >$1 prior base).
    active = r[r != 0.0]
    n_active = int(len(active))
    frac_green = float((active > 0).mean()) if n_active else 0.0
    # codex code-r1 #3: prepend baseline 1.0 so a first-day drop is counted in maxDD.
    cum = pd.concat([pd.Series([1.0]), (1 + r).cumprod()], ignore_index=True)
    peak = cum.cummax()
    max_dd = float(abs(((cum - peak) / peak).min()))
    roe = float(cum.iloc[-1] - 1)
    # terminal collapse: started with a real base but ended ~0
    start_eq = float(e[e > RUIN_EQUITY_FLOOR].iloc[0]) if (e > RUIN_EQUITY_FLOOR).any() else 0.0
    ruin = (start_eq <= RUIN_EQUITY_FLOOR) or (float(e.iloc[-1]) <= RUIN_EQUITY_FLOOR and start_eq > 100)
    return {"roe": roe, "max_dd": max_dd, "n_days": len(d), "structural_ruin": bool(ruin),
            "median_equity": med_eq, "frac_days_green": frac_green, "n_active_days": n_active,
            "median_leverage": median_leverage}


def journey_metrics(jr: pd.DataFrame, lo_ms: int, hi_ms: int, accessible: set | None) -> dict:
    """Pretest journey metrics. CLOSED-in-pretest journeys (entry & exit in [lo,hi)) for
    PnL/hold/count; accessibility by notional over those journeys.

    Boundary-straddling week-holder guard (codex finding a): a position OPENED in pretest but still
    OPEN at test_start (exit missing OR exit >= hi_ms) is invisible to median_hold_s. Its CENSORED
    hold = (hi_ms - entry_ts) -- using only info up to hi_ms (no future exit, no look-ahead) -- is
    surfaced as censored_max_hold_s so a multi-day/week hold straddling the boundary is rejected."""
    if jr is None or len(jr) == 0:
        return _empty_jm()
    # censored open-at-test_start holds: opened in pretest, not closed inside pretest (still open at
    # hi_ms). Censored hold = hi_ms - entry_ts (info only up to hi_ms). Used ONLY for the upper hold
    # (week-holder) gate; never enters PnL/count/median.
    opened = jr[(jr["entry_ts"] >= lo_ms) & (jr["entry_ts"] < hi_ms)]
    straddling = opened[opened["exit_ts"].isna() | (opened["exit_ts"] >= hi_ms)]
    censored_max_hold_s = (float((hi_ms - straddling["entry_ts"].astype("int64")).max()) / 1000.0
                           if len(straddling) else 0.0)
    # codex code-r1 #4: closed-in-pretest = entry AND exit both inside [lo, hi).
    j = jr[(jr["entry_ts"] >= lo_ms) & (jr["entry_ts"] < hi_ms)
           & jr["exit_ts"].notna() & (jr["exit_ts"] >= lo_ms) & (jr["exit_ts"] < hi_ms)].copy()
    if len(j) == 0:
        jm = _empty_jm()
        jm["censored_max_hold_s"] = censored_max_hold_s
        return jm
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
            "accessible_frac_count": acc_count, "censored_max_hold_s": censored_max_hold_s}


def _empty_jm() -> dict:
    return {"net_pnl": 0.0, "n_journeys": 0, "median_hold_s": 0.0, "share_below_latency": 1.0,
            "accessible_frac_notional": float("nan"), "accessible_frac_count": float("nan"),
            "censored_max_hold_s": 0.0}


def apply_floors(eqm: dict, jm: dict) -> tuple[bool, list[str]]:
    """Per-fold pretest floors. Returns (eligible, fail_reasons)."""
    f = []
    if jm["net_pnl"] <= 0:
        f.append(f"net_pnl<=0 ({jm['net_pnl']:.1f})")
    if eqm["structural_ruin"]:
        f.append("structural_ruin")
    elif eqm["roe"] <= 0:
        f.append(f"roe<=0 ({eqm['roe']:.2%})")
    # absolute pretest equity floor: drop tiny degen accounts that merely survived (fold-pure)
    me = eqm.get("median_equity", 0.0)
    if me < MIN_EQUITY_USD:
        f.append(f"equity_too_small (${me:.0f}<{MIN_EQUITY_USD:.0f})")
    if eqm["max_dd"] > MAXDD_CAP:
        f.append(f"max_dd>{MAXDD_CAP:.0%} ({eqm['max_dd']:.0%})")
    if jm["n_journeys"] < MIN_JOURNEYS_PRETEST:
        f.append(f"n_journeys<{MIN_JOURNEYS_PRETEST} ({jm['n_journeys']})")
    if jm["median_hold_s"] <= HOLD_FLOOR_S:
        f.append(f"median_hold<={HOLD_FLOOR_S:.0f}s ({jm['median_hold_s']:.0f}s)")
    # upper hold gate: V15 copies fast directional; reject multi-day/week holders
    if jm["median_hold_s"] > SWING_MAX_HOLD_S:
        f.append(f"hold_too_long (>{SWING_MAX_HOLD_S:.0f}s, {jm['median_hold_s']:.0f}s)")
    # boundary-straddling week-holder (codex finding a): a position opened in pretest still OPEN at
    # test_start whose CENSORED hold already exceeds the swing cap is a week-holder invisible to the
    # median (the close lands after the boundary). Reject on the censored lower-bound alone.
    cmh = jm.get("censored_max_hold_s", 0.0)
    if cmh > SWING_MAX_HOLD_S:
        f.append(f"hold_too_long_censored (>{SWING_MAX_HOLD_S:.0f}s, open-at-test_start {cmh:.0f}s)")
    if jm["share_below_latency"] > SHARE_BELOW_LATENCY_CAP:
        f.append(f"share_below_latency>{SHARE_BELOW_LATENCY_CAP:.0%} ({jm['share_below_latency']:.0%})")
    # LEVERAGE gate (Alberto spec): copy high but not ultra-high; reject median daily leverage > 10x.
    mlev = eqm.get("median_leverage", 0.0)
    if mlev > LEVERAGE_CAP:
        f.append(f"leverage>{LEVERAGE_CAP:.0f}x ({mlev:.1f}x)")
    # CONSISTENCY gate (Alberto spec): >=80% of active days green, enforced only when enough active days
    # exist to judge it (short fold windows can't prove consistency; thin history caught by other gates).
    nad = int(eqm.get("n_active_days", 0))
    fdg = eqm.get("frac_days_green", 0.0)
    if nad >= MIN_ACTIVE_DAYS_GREEN and fdg < DAYS_GREEN_MIN:
        f.append(f"days_green<{DAYS_GREEN_MIN:.0%} ({fdg:.0%} over {nad}d)")
    # accessibility: unknown (NaN) does NOT fire (loose); only fail when known and < min
    af = jm["accessible_frac_notional"]
    if af == af and af < ACCESSIBLE_FRAC_MIN:
        f.append(f"accessible_frac<{ACCESSIBLE_FRAC_MIN:.0%} ({af:.0%})")
    return (len(f) == 0, f)


def run(folds: pd.DataFrame, journeys: pd.DataFrame, equity: pd.DataFrame, m04: pd.DataFrame | None,
        accessible_by_fold: dict | None = None,
        active_test_folds_by_wallet: dict | None = None,
        m04_by_fold: dict[int, pd.DataFrame] | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    # entities to evaluate: M4 copyable primaries — defensive filter on all three (codex code-r1 #2).
    if m04_by_fold is None:
        if m04 is None:
            raise ValueError("run() requires either m04 or m04_by_fold")
        prim = _prim_from_m04(m04)
        prim_by_fold = None
        logger.warning("LOUD WARNING: using a single M4 authenticity file for all folds; "
                       "M5 selection is not fold-pure. Pass --m04-dir for fold-pure M4.")
        logger.info(f"M4 copyable primaries: {len(prim)} entities")
    else:
        prim_by_fold = {int(fid): _prim_from_m04(df) for fid, df in m04_by_fold.items()}
        prim = (pd.concat(prim_by_fold.values(), ignore_index=True).drop_duplicates("entity_id")
                if prim_by_fold else pd.DataFrame())
        logger.info("fold-pure M4 enabled: copyable primaries per fold = %s",
                    {fid: len(df) for fid, df in prim_by_fold.items()})
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
        fold_prim = prim_by_fold.get(fid, pd.DataFrame()) if prim_by_fold is not None else prim
        for _, p in fold_prim.iterrows():
            w = p["wallet"]
            if w in eq_by:
                g = eq_by[w]
                eqm = flow_adjusted_twr(g[(g["date_ms"] >= lo_ms) & (g["date_ms"] < hi_ms)])
            else:
                eqm = {"roe": 0.0, "max_dd": 0.0, "n_days": 0, "structural_ruin": True,
                       "median_equity": 0.0, "frac_days_green": 0.0, "n_active_days": 0,
                       "median_leverage": 0.0}
            jm = journey_metrics(jr_by.get(w), lo_ms, hi_ms, acc)
            elig, fails = apply_floors(eqm, jm)
            n_eval += 1; n_elig += int(elig)
            rows.append({
                "entity_id": int(p["entity_id"]), "primary_wallet": w, "fold_id": fid,
                "eligible": elig, "fail_reasons": "|".join(fails),
                "net_pnl_pretest": jm["net_pnl"], "roe_pretest_flow_adj": eqm["roe"],
                "median_equity_pretest": eqm.get("median_equity", 0.0),  # codex finding c: auditability
                "max_dd_pretest": eqm["max_dd"], "structural_ruin_flag": eqm["structural_ruin"],
                "median_leverage_pretest": eqm.get("median_leverage", 0.0),
                "frac_days_green_pretest": eqm.get("frac_days_green", 0.0),
                "n_active_days_pretest": eqm.get("n_active_days", 0),
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
    ap.add_argument("--m04", default=None, help="single m04_authenticity.parquet (compatibility mode)")
    ap.add_argument("--m04-dir", default=None,
                    help="directory with m04_authenticity_f{fold_id}.parquet for fold-pure M4")
    ap.add_argument("--m03-activity", default=None,
                    help="m03_wallet_activity_summary.parquet (for active_test_folds in the G5 pool flag)")
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--accessible-coins", default=None, help="JSON {fold_id: [coins]} or omit (unknown)")
    args = ap.parse_args()

    folds = pd.read_parquet(args.folds)
    journeys = pd.read_parquet(args.journeys)
    equity = pd.read_parquet(args.equity)
    if args.m04_dir:
        m04_by_fold = _load_m04_by_fold(Path(args.m04_dir), folds)
        m04 = None
    else:
        if not args.m04:
            raise ValueError("provide --m04-dir for fold-pure M4 or --m04 for compatibility mode")
        m04 = pd.read_parquet(args.m04)
        m04_by_fold = None
    acc = None
    if args.accessible_coins:
        raw = json.loads(Path(args.accessible_coins).read_text())
        acc = {int(k): set(v) for k, v in raw.items()}
    atf = None
    if args.m03_activity:
        a = pd.read_parquet(args.m03_activity)
        atf = dict(zip(a["key"], a["active_test_folds"]))
    m04_rows = sum(len(v) for v in m04_by_fold.values()) if m04_by_fold is not None else len(m04)
    logger.info(f"folds={len(folds)} journeys={len(journeys):,} equity_rows={len(equity):,} m04_rows={m04_rows}")

    elig_df, pool_df, waterfall = run(folds, journeys, equity, m04, acc, atf, m04_by_fold=m04_by_fold)

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
