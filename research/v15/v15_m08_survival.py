"""V15 M8 -- Survival / Danger Tiering (post-engine). M8-v1 = the COUNTERFACTUAL-SURVIVAL primitive
(the design centerpiece) + tier mapping + the M4/M8 tier-contribution attribution Alberto requested
(decisions/2026-06-01-m9-m10-manifest-freeze). The INFERENTIAL scorers (off-platform-hedge smell,
on-chain funder provenance, carry-timing, behavioral linkage) are PHASE-2 here -- they have open
data-availability questions (design modules/m08 Q3/Q4) and are emitted as deterministic no_flag stubs
with the contract in place, so the tier is currently survival-driven (mechanical, provable).

Design: brain projects/quant/v15/modules/m08 (codex DESIGN-SHIP r2, Alberto greenlit 2026-06-01).
ROLE: "is this entity DANGEROUS to copy?" -> coarse pessimistic CONFIDENCE TIER
{kill 0 / suspicious <=0.10 / uncertain <=0.25 / full_weight 1.0} = the SURVIVAL multiplier in the
sizing chain (M9 composes). Runs on the M6b INVESTABLE POOL only (small set -> M7 stress sims cheap).

Run:
  python research/v15/v15_m08_survival.py --m07-dir app/data/v15/m07_pretest --out app/data/v15
"""
from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass, asdict, replace
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("m08")

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m07_engine as E  # noqa: E402

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"


@dataclass(frozen=True)
class M8Manifest:
    manifest_version: str = "m08-v1"
    # tier survival multipliers (frozen; decisions/2026-06-01-m9-m10-manifest-freeze)
    mult_kill: float = 0.0
    mult_suspicious: float = 0.10
    mult_uncertain: float = 0.25
    mult_full_weight: float = 1.0
    suspicious_cohort_cap: float = 0.10        # <=10% of portfolio (enforced in M9)
    # stress run params (design §3)
    stress_slippage_band: str = "high"
    stress_adl: bool = True
    # entity_pre_m8_max = M6b_quality x M4_confidence x 1.0(survival placeholder) x M9_static_cap.
    m9_static_per_entity_cap: float = 0.10     # the pre-M9 static cap (NOT the suspicious-cohort cap)
    # Survival/ruin verdicts are bankroll-scale-dependent; production should pass the real bankroll.
    nominal_capital: float = 10_000.0          # reference bankroll for the absolute stress slice
    # multi-slice survival probe (design §3): test at the pre-M8 max, a smaller slice, and the min size
    smaller_slice_frac: float = 0.25           # "survives at a smaller slice" probe
    min_slice_capital: float = 50.0            # ~ min deployable subaccount (HL min-notional regime)
    inferential_layer_active: bool = False
    # outcome -> tier
    indeterminate_heavy_frac: float = 0.25     # >this share of indeterminate minutes -> cap at UNCERTAIN


TIER_ORDER = {"kill": 0, "suspicious": 1, "uncertain": 2, "full_weight": 3}
TIER_MULT = None  # set per-manifest


def _run_stress(actions: pd.DataFrame, md: E.MarketData, start_equity: float, t0: int, t1: int,
                m: M8Manifest) -> dict:
    """One counterfactual-survival run: M7 at the STRESS slice (high slippage + adl, causal carry-in,
    NO source top-ups -- the replica lives on its slice + PnL). Returns the typed outcome."""
    params = E.EngineParams(slippage_band=m.stress_slippage_band, adl_stress=m.stress_adl,
                            start_policy="causal_carry_in")
    res = E.step_subaccount(actions, md, start_equity, params, end_ts_ms=t1, start_ts_ms=t0)
    s = res["summary"]
    window_min = max((t1 - t0) / 60_000.0, 1.0)
    wiped = bool(s["ruin"]) or s["n_backstop_transfer"] > 0 or s["final_equity"] <= 0
    return {
        "wiped": wiped, "ruin": bool(s["ruin"]), "backstop": s["n_backstop_transfer"] > 0,
        "account_ruin": "account_ruin" in s["outcome_states"], "final_equity": s["final_equity"],
        "roe": s["roe_engine"], "max_dd": s["max_dd"], "time_to_ruin_ms": s["time_to_ruin_ms"],
        "n_fills": s["n_fills"],
        # codex r1#3: indeterminate MINUTES / simulated window MINUTES (not / action count).
        "indeterminate_frac": (s.get("n_indeterminate_minutes", 0) / window_min),
    }


def survival_tier(actions, md, t0, t1, pre_m8_max_capital, m: M8Manifest) -> dict:
    """Counterfactual-survival -> mechanical tier + sizing signals (design §1/§3; codex r1#1,#2).
    - survives at pre_m8_max -> full_weight (UNCERTAIN only if indeterminate-heavy).
    - wiped at pre_m8_max BUT survives at the MINIMUM tradable size -> copyability_fail_at_stress: a
      SIZING signal (M9 clamps the max slice); the tier stays SURVIVAL-OK = full_weight (the wallet
      isn't dangerous, our chosen slice was too big -- NOT downgraded). max_survivable_slice refined by
      the smaller-slice probe.
    - wiped even at the MINIMUM size (no positive slice survives) -> KILL (mechanical).
    Inferential SUSPICIOUS/UNCERTAIN downgrades are applied SEPARATELY (§4 scorers via _worst_tier)."""
    big = _run_stress(actions, md, pre_m8_max_capital, t0, t1, m)
    base = {"indeterminate_frac": big["indeterminate_frac"], "stress_roe": big["roe"],
            "stress_max_dd": big["max_dd"]}
    heavy = big["indeterminate_frac"] > m.indeterminate_heavy_frac
    if not big["wiped"]:
        return {"tier": "uncertain" if heavy else "full_weight", "survival_outcome": "survived",
                "copyability_fail_at_stress": False, "max_survivable_slice": pre_m8_max_capital, **base}
    # wiped at pre_m8_max -> the MIN-size probe decides KILL vs copyability (codex r1#2: KILL only when
    # even the smallest positive slice fails -- min is the smallest, so min-wipe == no slice survives).
    mn = _run_stress(actions, md, m.min_slice_capital, t0, t1, m)
    if mn["wiped"]:
        return {"tier": "kill", "survival_outcome": "ruin_at_min_size",
                "copyability_fail_at_stress": True, "max_survivable_slice": 0.0, **base}
    # survives at min -> copyability SIZING signal, tier stays survival-OK (codex r1#1: NOT downgraded).
    # refine max_survivable with a smaller-slice probe ONLY if it sits ABOVE min (codex r2#1: never
    # probe below the min slice -- min already survived, so the floor is min_slice_capital).
    smaller = pre_m8_max_capital * m.smaller_slice_frac
    surv_slice = m.min_slice_capital
    if smaller > m.min_slice_capital:
        small = _run_stress(actions, md, smaller, t0, t1, m)
        if not small["wiped"]:
            surv_slice = smaller
    return {"tier": "uncertain" if heavy else "full_weight",
            "survival_outcome": "copyability_fail_at_stress",
            "copyability_fail_at_stress": True, "max_survivable_slice": surv_slice, **base}


# --------------------------------------------------------------------------- #
# INFERENTIAL SCORERS -- PHASE 2 (deterministic contract; open data questions Q3/Q4). Stubbed no_flag.
# --------------------------------------------------------------------------- #
def inferential_scorers(entity_id, fold_id) -> dict:
    """Each scorer returns one of {no_flag, uncertain, suspicious} + reason + source_module=M8_new.
    PHASE 2: off-platform-hedge-smell, funder-provenance, carry-timing, behavioral-linkage. Stubbed to
    no_flag (the inferential layer is NOT yet active -> tier is survival-driven only). The WORST tier
    any scorer assigns wins; never auto-KILL (inferential -> at most SUSPICIOUS)."""
    return {"hedge_smell": "no_flag", "funder_provenance": "no_flag",
            "carry_timing": "no_flag", "behavioral_linkage": "no_flag",
            "funder_provenance_unavailable": True, "scorers_phase": "phase2_stub"}


def _worst_tier(survival_tier_name: str, scorer_out: dict) -> str:
    """WORST (lowest) tier wins. Inferential suspicious/uncertain can only DOWNGRADE, never KILL."""
    tier = survival_tier_name
    # phase2_stub means the inferential layer is inactive: a stubbed no_flag is only "not scored",
    # not verified clean. Cap its contribution at UNCERTAIN instead of silently granting full weight.
    no_flag_tier = "uncertain" if scorer_out.get("scorers_phase") == "phase2_stub" else "full_weight"
    sev = {"no_flag": no_flag_tier, "uncertain": "uncertain", "suspicious": "suspicious"}
    for k in ("hedge_smell", "funder_provenance", "carry_timing", "behavioral_linkage"):
        cand = sev.get(scorer_out.get(k, "no_flag"), "full_weight")
        if TIER_ORDER[cand] < TIER_ORDER[tier]:
            tier = cand
    return tier


# --------------------------------------------------------------------------- #
# TIER-CONTRIBUTION ATTRIBUTION (Alberto #4): ROE + key metrics by M4 tier AND M8 tier.
# --------------------------------------------------------------------------- #
def tier_attribution(tiers: pd.DataFrame, m07_summary: pd.DataFrame) -> pd.DataFrame:
    """Per (tier_axis, tier_value): n_entities, mean/median ROE, win-rate, ruin-rate, PnL share.
    Lets us SEE whether down-weighted/killed cohorts actually underperformed (label right) or were
    good (over-penalizing -> discarding alpha)."""
    # Degenerate-pool guard (2026-06-04): an empty tier set (e.g. zero pooled seats in a smoke or a
    # fold with no eligible entities) is an empty DataFrame with NO columns, so the merge-key lookup
    # raises KeyError. Return empty attribution instead of crashing the chain.
    if tiers.empty or not {"entity_id", "fold_id"}.issubset(tiers.columns):
        return pd.DataFrame()
    df = tiers.merge(m07_summary[["entity_id", "fold_id", "roe_engine", "ruin"]],
                     on=["entity_id", "fold_id"], how="left")
    # codex r1#5: a tiered entity with NO M7 row is a DATA GAP -> do NOT silently treat as roe=0; flag,
    # exclude from the metrics, and surface n_missing per group (a metric over phantom 0s hides
    # discarded alpha / missing runner coverage).
    df["_missing_m07"] = df["roe_engine"].isna()
    n_missing_total = int(df["_missing_m07"].sum())
    if n_missing_total:
        logger.warning("M8 attribution: %d tiered (entity,fold) rows have NO M07 summary -> excluded "
                       "from metrics + flagged (data gap, not roe=0)", n_missing_total)
    rows = []
    present = df[~df["_missing_m07"]]
    total_pnl = present["roe_engine"].clip(lower=-1.0).sum()
    for axis, col in (("m4_tier", "m4_tier"), ("m8_tier", "tier")):
        if col not in df.columns:
            continue
        for val, g in df.groupby(col):
            gp = g[~g["_missing_m07"]]
            rows.append({
                "tier_axis": axis, "tier": val, "n_entities": len(g),
                "n_missing_m07": int(g["_missing_m07"].sum()),
                "mean_roe": round(float(gp["roe_engine"].mean()), 4) if len(gp) else None,
                "median_roe": round(float(gp["roe_engine"].median()), 4) if len(gp) else None,
                "win_rate": round(float((gp["roe_engine"] > 0).mean()), 4) if len(gp) else None,
                "ruin_rate": round(float(gp["ruin"].fillna(False).mean()), 4) if len(gp) else None,
                "pnl_share": round(float(gp["roe_engine"].sum() / total_pnl), 4) if (len(gp) and total_pnl) else 0.0,
            })
    return pd.DataFrame(rows)


def entity_pre_m8_max(quality_weight, m4_confidence, m: M8Manifest) -> float:
    """The largest slice the system could deploy BEFORE M8 down-weights (design §3; EXCLUDES the
    M8 survival multiplier, the suspicious cohort cap, and anti-corr). = M6b quality x M4 confidence x
    1.0 (survival placeholder) x M9 static per-entity cap, as an absolute capital on nominal_capital."""
    return float(quality_weight) * float(m4_confidence) * 1.0 * m.m9_static_per_entity_cap * m.nominal_capital


def _load_m04_entities(data_dir: Path, folds: pd.DataFrame, m04_dir: Optional[Path]) -> tuple[pd.DataFrame, bool]:
    """Load M4 entity tiers. Fold-pure mode requires exact as_of_ms == fold test_start."""
    cols = ["entity_id", "primary_wallet", "entity_tier"]
    if m04_dir is None:
        logger.warning("LOUD WARNING: using single global m04_entities.parquet for all M8 folds; "
                       "M8 M4 confidence is NOT fold-pure. Pass --m04-dir to load m04_entities_f{fold}.parquet.")
        return pd.read_parquet(data_dir / "m04_entities.parquet")[cols], False

    fold_test_start_ms = {int(r.fold_id): pd.Timestamp(r.test_start).value // 1_000_000
                          for r in folds.itertuples()}
    parts = []
    for fid in sorted(fold_test_start_ms):
        p = Path(m04_dir) / f"m04_entities_f{fid}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"missing fold-pure M4 entities for fold {fid}: {p}")
        df = pd.read_parquet(p).copy()
        missing = [c for c in (*cols, "as_of_ms") if c not in df.columns]
        if missing:
            raise ValueError(f"M8 PROVENANCE FAIL-CLOSED: {p} missing columns {missing}")
        n_null = int(df["as_of_ms"].isna().sum())
        if n_null:
            raise ValueError(f"M8 PROVENANCE FAIL-CLOSED: {p} has {n_null} null as_of_ms rows")
        expected = float(fold_test_start_ms[fid])
        bad = df["as_of_ms"].astype("float64") != expected
        if bad.any():
            raise ValueError(f"M8 PROVENANCE FAIL-CLOSED: {p} has {int(bad.sum())} rows "
                             f"with as_of_ms != fold {fid} test_start ({int(expected)})")
        df["fold_id"] = int(fid)
        parts.append(df[[*cols, "as_of_ms", "fold_id"]])
    return (pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=[*cols, "as_of_ms", "fold_id"])), True


def run_m08(m07_dir: Path, data_dir: Path, m: M8Manifest, slip_calib_path: Optional[str] = None,
            limit: Optional[int] = None, pool_path: Optional[Path] = None,
            m04_dir: Optional[Path] = None) -> dict:
    """Tier the M6b INVESTABLE POOL: per pooled (entity, fold) run the counterfactual-survival
    primitive (M7 stress) + inferential scorers (phase-2 stub) -> tier; emit m08_tiers + attribution."""
    import pyarrow.dataset as ds
    pp = Path(pool_path) if pool_path else (data_dir / "m06b_pool.parquet")
    pool = pd.read_parquet(pp)
    pool = pool[pool["in_pool"]].copy()
    folds = pd.read_parquet(data_dir / "m03_folds.parquet")
    ent, m04_fold_pure = _load_m04_entities(data_dir, folds, m04_dir)
    fold_win = {int(r.fold_id): (pd.Timestamp(r.train_start).value // 1_000_000,
                                 pd.Timestamp(r.test_start).value // 1_000_000) for r in folds.itertuples()}
    conf_map = {"CLEAN": 1.0, "UNCERTAIN": 0.25, "SUSPICIOUS": 0.10, "KILL": 0.0}
    pool = pool.merge(ent, on=["entity_id", "fold_id"] if m04_fold_pure else ["entity_id"], how="left")
    if m04_fold_pure:
        # FAIL-CLOSED provenance (codex M8 re-review): in fold-pure mode every pooled (entity_id, fold_id)
        # MUST have a matching fold-pure M4 entity row. A left-merge miss would silently become str(NaN)->
        # default 0.25 confidence (m4_tier="nan"). Refuse to proceed instead of fabricating confidence.
        _missing = pool[pool["entity_tier"].isna()]
        if len(_missing):
            _ex = _missing[["entity_id", "fold_id"]].head(3).to_dict("records")
            raise ValueError(
                f"M8 fold-pure M4 provenance: {len(_missing)} pooled (entity_id,fold_id) rows lack a "
                f"fold-pure M4 entity row (e.g. {_ex}). Rebuild per-fold M4 via build_m4_perfold.sh so "
                f"every pooled entity-fold is covered. Fail-closed (no fabricated confidence).")
    if not m.inferential_layer_active:
        logger.warning("LOUD WARNING: M8 inferential layer is inactive (phase2_stub); stubbed no_flag "
                       "is capped at uncertain and must not be read as verified-safe.")

    md = E.MarketData(allow_mongo=True)
    if slip_calib_path:
        cal = json.loads(Path(slip_calib_path).read_text())
        per_fold = {int(k): v for k, v in cal.get("per_fold_asof", {}).items()}
    else:
        per_fold = {}
    acts_ds = ds.dataset(str(data_dir / "m02_actions.parquet"))

    rows = []
    seats = pool.itertuples()
    if limit:
        seats = list(seats)[:limit]
    for r in seats:
        fid = int(r.fold_id)
        if fid not in fold_win:
            continue
        t0, t1 = fold_win[fid]
        md.set_slip_calib(per_fold.get(fid), cal.get("version") if slip_calib_path else None)
        wdf = acts_ds.to_table(filter=ds.field("wallet") == r.primary_wallet).to_pandas()
        adf = wdf[(wdf.ts >= t0) & (wdf.ts < t1)]
        # TIER-DOMAIN FAIL-CLOSED (codex M8 confirm): a non-canonical entity_tier (e.g. a literal "nan"
        # string or unknown label from corrupted/nonstandard M4 input) must NOT silently get the 0.25
        # default. Canonical M4 writes only CLEAN/UNCERTAIN/SUSPICIOUS/KILL; anything else -> 0.0 (KILL-
        # equivalent, fail-closed conservative) + loud warning, so a bad tier excludes the entity rather
        # than fabricating mid confidence.
        _tier = str(r.entity_tier)
        if _tier not in conf_map:
            logger.warning(f"M8: non-canonical entity_tier {_tier!r} for entity {int(r.entity_id)} fold "
                           f"{fid} -> fail-closed to 0.0 confidence (expected one of {sorted(conf_map)})")
            m4_conf = 0.0
        else:
            m4_conf = conf_map[_tier]
        pre_max_raw = entity_pre_m8_max(r.quality_weight, m4_conf, m)   # codex r1#4: store the RAW formula
        # the big-slice probe runs at the deployable slice = max(raw, min) so a tiny-quality row still
        # gets a meaningful stress test; the RAW value is persisted unchanged.
        sv = survival_tier(adf, md, t0, t1, max(pre_max_raw, m.min_slice_capital), m)
        sc = inferential_scorers(int(r.entity_id), fid)
        final_tier = _worst_tier(sv["tier"], sc)
        mult = {"kill": m.mult_kill, "suspicious": m.mult_suspicious,
                "uncertain": m.mult_uncertain, "full_weight": m.mult_full_weight}[final_tier]
        rows.append({
            "entity_id": int(r.entity_id), "fold_id": fid, "m4_tier": str(r.entity_tier),
            "tier": final_tier, "survival_multiplier": mult, "lp_label":
            ("NO_OBSERVED_DANGER" if final_tier == "full_weight" else final_tier),
            "survival_outcome": sv["survival_outcome"],
            "copyability_fail_at_stress": sv["copyability_fail_at_stress"],
            "max_survivable_slice": sv["max_survivable_slice"], "entity_pre_m8_max": pre_max_raw,
            "stress_roe": sv["stress_roe"], "stress_max_dd": sv["stress_max_dd"],
            "hidden_hedge_not_observable": (final_tier != "kill"),
            "funder_provenance_unavailable": sc["funder_provenance_unavailable"],
            "inferential_layer_active": m.inferential_layer_active,
            "scorers_phase": sc["scorers_phase"],
            **{f"scorer_{k}": sc[k] for k in ("hedge_smell", "funder_provenance", "carry_timing", "behavioral_linkage")},
        })
    tiers = pd.DataFrame(rows)
    summ = _read_m07_summary(m07_dir)
    att = tier_attribution(tiers, summ)
    out_dir = data_dir
    tiers.to_parquet(out_dir / "m08_tiers.parquet", index=False)
    att.to_parquet(out_dir / "m08_tier_attribution.parquet", index=False)
    man = asdict(m)
    man["m04_fold_pure"] = bool(m04_fold_pure)
    Path(out_dir / "m08_manifest.json").write_text(json.dumps(man, indent=2, default=str))
    logger.info("M8 done: %d tiered | tier dist %s", len(tiers),
                tiers["tier"].value_counts().to_dict() if len(tiers) else {})
    return {"n_tiered": len(tiers), "tier_dist": tiers["tier"].value_counts().to_dict() if len(tiers) else {}}


def _read_m07_summary(m07_dir: Path) -> pd.DataFrame:
    p = m07_dir / "m07_summary.parquet"
    parts = m07_dir / "m07_summary.parquet.parts"
    if p.exists():
        return pd.read_parquet(p)
    return pd.concat((pd.read_parquet(s) for s in sorted(parts.glob("*.parquet"))), ignore_index=True)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--m07-dir", default=str(DATA_DIR / "m07_pretest_final"))
    ap.add_argument("--out", default=str(DATA_DIR))
    ap.add_argument("--slip-calib", default=str(DATA_DIR / "slippage_calib_v11.json"))
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--m04-dir", default=None,
                    help="directory with fold-pure m04_entities_f{fold_id}.parquet")
    ap.add_argument("--nominal-capital", type=float, default=10_000.0,
                    help="bankroll scale for absolute survival stress slices (default: 10000.0)")
    args = ap.parse_args()
    man = replace(M8Manifest(), nominal_capital=args.nominal_capital)
    run_m08(Path(args.m07_dir), Path(args.out), man, slip_calib_path=args.slip_calib,
            limit=args.limit, m04_dir=Path(args.m04_dir) if args.m04_dir else None)


if __name__ == "__main__":
    main()
