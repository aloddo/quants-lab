"""V15 M10 -- Gates G1-G7 + quality-matched null + percentile ladder. M10-CORE = the gate-evaluation +
significance statistics (pure functions on the M9 chained path + the matched-null distributions). The
NULL GENERATION (driving M9 with pool_provider=matched_null over N_NULL seeds) is the integration step
wired once M9-v2's chained sim + the re-run data are available; this module computes the verdict GIVEN
the strategy result + null distributions.

Design: brain projects/quant/v15/modules/m10 (codex DESIGN-SHIP r2) + decisions/2026-06-01-m9-m10-
manifest-freeze. dev_verdict gates LIVE-SMALL ONLY (one-regime caveat); LP claims need the §4 forward
holdout (p99). Reports the FULL percentile ladder p75/p90/p95/p99 (Alberto #5), not just the p95 gate.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("m10")


@dataclass(frozen=True)
class M10Manifest:
    manifest_version: str = "m10-v1"
    g1_chained_roe: float = 0.5302         # 2^(112/182.5)-1 (2x/6mo pace over the 112d OOS)
    g2_min_positive_folds: int = 6         # of 8
    expected_n_folds: int = 8              # G2 denominator MUST be verified (codex P0#3)
    g3_max_chained_dd: float = 0.50
    g4_fold_floor: float = 0.50            # fold-end >= 50% of fold-initial + no intra-fold kill
    g6_gate_pct: float = 95.0              # development gate percentile
    g6_holdout_pct: float = 99.0           # confirmatory holdout (stricter)
    g7_max_top_entity_pnl: float = 0.40
    n_null_dev: int = 1000                 # development gate
    n_null_holdout: int = 5000             # confirmatory holdout
    ladder: tuple = (75.0, 90.0, 95.0, 99.0)   # Alberto #5: report the whole curve


def _clean_null(null_dist) -> np.ndarray:
    """codex r1#4: coerce to numeric + keep only FINITE samples (None/pd.NA/nan/inf dropped), so they
    can't poison percentiles or be counted toward N_NULL."""
    nd = pd.to_numeric(pd.Series(list(null_dist)), errors="coerce").to_numpy(dtype="float64")
    return nd[np.isfinite(nd)]


def percentile_ladder(strategy_val: float, null_dist, m: M10Manifest) -> dict:
    """The null percentile ladder + where the strategy lands. Percentiles are kept RAW for gate logic
    (codex r1#1: rounding could let strategy > round(p95) while <= true p95); only display copies round.
    p-value (one-sided, fixes-r1#8) = (1 + #{null >= strategy}) / (N_valid + 1)."""
    nd = _clean_null(null_dist)
    n = int(nd.size)
    raw = {f"p{int(p)}": (float(np.percentile(nd, p)) if n else None) for p in m.ladder}
    # AUDIT 2026-07-10 (codex P1#5): a non-finite strategy value must NOT produce a strong-looking p-value.
    # (nd >= nan) is all-False -> n_ge=0 -> p_value=1/(n+1) ~ "significant". Report invalid instead.
    finite_strat = bool(np.isfinite(strategy_val))
    n_ge = int((nd >= strategy_val).sum()) if finite_strat else 0
    p_value = ((1 + n_ge) / (n + 1)) if (n and finite_strat) else None
    return {"strategy": float(strategy_val), "n_valid_null": n, "strategy_finite": finite_strat,
            "_raw_pct": raw,                                    # full precision -> gate logic uses this
            **{k: (round(v, 6) if v is not None else None) for k, v in raw.items()},  # display copies
            "p_value": (round(p_value, 8) if p_value is not None else None),
            "exceeds_p95": (bool(strategy_val > raw["p95"]) if (finite_strat and raw.get("p95") is not None) else None)}


def gate_g6_bivariate(strat_roe, strat_calmar, null_roe, null_calmar, m: M10Manifest,
                      pct: Optional[float] = None, required_n: Optional[int] = None) -> dict:
    """G6 (risk-adjusted, bivariate): strategy ROE > pPCT(null ROE) AND strategy Calmar > pPCT(null
    Calmar). A ROE win bought with more risk than the null does NOT pass. FAILS CLOSED (codex r1#2) if
    either null lacks `required_n` valid samples -> a p99 gate can't pass on a tiny null. Gate logic
    uses RAW percentiles (codex r1#1)."""
    pct = pct if pct is not None else m.g6_gate_pct
    required_n = required_n if required_n is not None else m.n_null_dev
    roe = percentile_ladder(strat_roe, null_roe, m)
    cal = percentile_ladder(strat_calmar, null_calmar, m)
    # AUDIT 2026-07-10 (codex P1#4): compute the gate percentile at the EXACT pct, not the truncated ladder
    # key f"p{int(pct)}" (pct=95.5 would silently use p95). Also require a FINITE strategy value (P0#2 partial).
    nd_roe = _clean_null(null_roe)
    nd_cal = _clean_null(null_calmar)
    enough = (nd_roe.size >= required_n) and (nd_cal.size >= required_n)
    roe_p = float(np.percentile(nd_roe, pct)) if nd_roe.size else None
    cal_p = float(np.percentile(nd_cal, pct)) if nd_cal.size else None
    roe_pass = enough and roe_p is not None and bool(np.isfinite(strat_roe)) and strat_roe > roe_p
    cal_pass = enough and cal_p is not None and bool(np.isfinite(strat_calmar)) and strat_calmar > cal_p
    reason = "" if enough else f"insufficient_null(roe={roe['n_valid_null']},calmar={cal['n_valid_null']},need={required_n})"
    return {"gate": "G6", "pass": bool(roe_pass and cal_pass), "at_pct": pct,
            "required_n": required_n, "n_valid_null_roe": roe["n_valid_null"],
            "n_valid_null_calmar": cal["n_valid_null"], "insufficient_null_reason": reason,
            "roe_ladder": roe, "calmar_ladder": cal,
            "roe_pass": bool(roe_pass), "calmar_pass": bool(cal_pass)}


def gate_g7(top_entity_pnl_share: float, ablation: Optional[dict], null_roe, null_calmar,
            m: M10Manifest, pct: float, required_n: int) -> dict:
    """G7 concentration: PASS iff top entity <= 40% of PnL OR the CONSERVATIVE ablation (remove top
    entity, hold its capital as cash, NO re-optimize) still clears G1+G3+G6. codex r1#3: M10 EVALUATES
    G1/G3/G6 on the ablation metrics here (not a stale caller boolean). `ablation` keys: chained_roe,
    max_chained_dd, chained_calmar (+ uses the same nulls/manifest/pct)."""
    under_cap = top_entity_pnl_share <= m.g7_max_top_entity_pnl
    abl_pass = False
    abl_detail = None
    if ablation is not None:
        a_g1 = ablation["chained_roe"] >= m.g1_chained_roe
        a_g3 = ablation["max_chained_dd"] <= m.g3_max_chained_dd
        a_g6 = gate_g6_bivariate(ablation["chained_roe"], ablation["chained_calmar"],
                                 null_roe, null_calmar, m, pct=pct, required_n=required_n)["pass"]
        abl_pass = bool(a_g1 and a_g3 and a_g6)
        abl_detail = {"g1": bool(a_g1), "g3": bool(a_g3), "g6": bool(a_g6)}
    return {"gate": "G7", "pass": bool(under_cap or abl_pass),
            "top_entity_pnl_share": float(top_entity_pnl_share), "under_cap": bool(under_cap),
            "conservative_ablation_passes_g1_g3_g6": bool(abl_pass), "ablation_detail": abl_detail}


def _validate_strat(strat: dict, ablation: Optional[dict], m: M10Manifest) -> Optional[str]:
    """Return an error string if the strategy/ablation inputs are unsafe to gate (fail closed), else None.
    Guards codex P0#1 (literal bools), P0#2 (finite + valid domains), P0#3 (verified fold denominator)."""
    def _fin(x):
        try:
            return isinstance(x, (int, float)) and not isinstance(x, bool) and np.isfinite(x)
        except TypeError:
            return False
    for k in ("chained_roe", "chained_calmar"):
        if k not in strat or not _fin(strat[k]):
            return f"{k} not finite ({strat.get(k)!r})"
    if not _fin(strat.get("max_chained_dd")) or not (0.0 <= strat["max_chained_dd"] <= 1.0):
        return f"max_chained_dd out of [0,1] ({strat.get('max_chained_dd')!r})"
    if not _fin(strat.get("top_entity_pnl_share")) or not (0.0 <= strat["top_entity_pnl_share"] <= 1.0):
        return f"top_entity_pnl_share out of [0,1] ({strat.get('top_entity_pnl_share')!r})"
    if "n_folds" not in strat or strat["n_folds"] != m.expected_n_folds:
        return f"n_folds != expected {m.expected_n_folds} ({strat.get('n_folds')!r}) — G2 denominator unverified"
    npf = strat.get("n_positive_folds")
    if not isinstance(npf, (int, np.integer)) or isinstance(npf, bool) or not (0 <= npf <= m.expected_n_folds):
        return f"n_positive_folds out of [0,{m.expected_n_folds}] ({npf!r})"
    for k in ("g4_no_kill", "g4_all_folds_above_floor", "g5_pool_ok"):
        # require an actual bool TYPE (int 1 == True in Python, so `in (True, False)` would wrongly admit 1).
        if not isinstance(strat.get(k), (bool, np.bool_)):
            return f"{k} must be a literal bool, got {strat.get(k)!r}"
    if ablation is not None:
        for k in ("chained_roe", "chained_calmar"):
            if not _fin(ablation.get(k)):
                return f"ablation.{k} not finite ({ablation.get(k)!r})"
        if not _fin(ablation.get("max_chained_dd")) or not (0.0 <= ablation["max_chained_dd"] <= 1.0):
            return f"ablation.max_chained_dd out of [0,1] ({ablation.get('max_chained_dd')!r})"
    return None


def evaluate_gates(strat: dict, null_roe, null_calmar, m: M10Manifest, holdout: bool = False,
                   ablation: Optional[dict] = None) -> dict:
    """Evaluate G1-G7 on the M9 chained result + null distributions. `strat` keys: chained_roe,
    n_positive_folds, max_chained_dd, g4_no_kill, g4_all_folds_above_floor, g5_pool_ok, chained_calmar,
    top_entity_pnl_share. `ablation` (conservative, top-entity-removed) keys: chained_roe,
    max_chained_dd, chained_calmar. holdout=True -> stricter p99 + n_null=5000 + CONFIRMATORY verdict."""
    pct = m.g6_holdout_pct if holdout else m.g6_gate_pct
    required_n = m.n_null_holdout if holdout else m.n_null_dev
    # AUDIT 2026-07-10 (codex P0#1,#2,#3): FAIL CLOSED on invalid inputs BEFORE gating. bool(np.nan) is True, so a
    # missing/NaN kill/floor/pool status would silently PASS G4/G5; inf ROE would pass G1/G6; a partial <8-fold run
    # would pass G2 "6 of 8". Require literal bools, finite metrics, valid domains, and the verified fold denominator.
    err = _validate_strat(strat, ablation, m)
    if err:
        return {"gates": {}, "all_pass": False, "verdict_kind": "invalid_input",
                "greenlight": "no", "required_n": required_n, "validation_error": err}
    g1 = {"gate": "G1", "pass": bool(strat["chained_roe"] >= m.g1_chained_roe),
          "value": strat["chained_roe"], "threshold": m.g1_chained_roe}
    # G2: n_positive_folds >= min AND the denominator is verified == expected_n_folds (codex P0#3).
    g2 = {"gate": "G2", "pass": bool(strat["n_positive_folds"] >= m.g2_min_positive_folds
                                     and strat["n_folds"] == m.expected_n_folds),
          "value": strat["n_positive_folds"], "n_folds": strat["n_folds"],
          "threshold": m.g2_min_positive_folds, "expected_n_folds": m.expected_n_folds}
    g3 = {"gate": "G3", "pass": bool(strat["max_chained_dd"] <= m.g3_max_chained_dd),
          "value": strat["max_chained_dd"], "threshold": m.g3_max_chained_dd}
    # G4/G5: _validate_strat has ALREADY rejected any non-bool (np.nan/1/"yes"/None) -> the field is now a real
    # bool type (python bool OR np.bool_). bool() normalizes np.bool_(True) correctly (np.True_ is NOT the python
    # True singleton, so `is True` would false-negative it). Safe here precisely because validation ran first.
    g4 = {"gate": "G4", "pass": bool(strat["g4_no_kill"]) and bool(strat["g4_all_folds_above_floor"]),
          "no_kill": bool(strat["g4_no_kill"]), "all_folds_above_floor": bool(strat["g4_all_folds_above_floor"])}
    g5 = {"gate": "G5", "pass": bool(strat["g5_pool_ok"])}
    g6 = gate_g6_bivariate(strat["chained_roe"], strat["chained_calmar"], null_roe, null_calmar, m,
                           pct=pct, required_n=required_n)
    g7 = gate_g7(strat["top_entity_pnl_share"], ablation, null_roe, null_calmar, m, pct=pct,
                 required_n=required_n)
    gates = [g1, g2, g3, g4, g5, g6, g7]
    all_pass = all(g["pass"] for g in gates)
    return {
        "gates": {g["gate"]: g for g in gates},
        "all_pass": bool(all_pass),
        "verdict_kind": ("confirmatory" if holdout else "development_verdict"),
        # development_verdict greenlights LIVE-SMALL ONLY (one-regime caveat); never an LP claim.
        "greenlight": ("live_small" if (all_pass and not holdout) else
                       ("lp_confirmatory" if (all_pass and holdout) else "no")),
        "required_n": required_n,
    }
