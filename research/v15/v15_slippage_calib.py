"""V15 slippage calibration from V11 real HL fills -> slippage_calibration_version = "v11-fills-v1".

Design: projects/quant/v15/modules/m06b-final-prep (CHANGE 3, codex DESIGN-SHIP r4) +
projects/quant/v15/slippage-calibration-v11-fills. Alberto 2026-06-01: use V11's real fills instead
of building the WS-L2 collector (deferred). This is the COST source that flips M6b provisional->FINAL.

WHAT: per-coin BASE half-spread (bps) from V11's real fills, used to replace the engine's hard-coded
DEFAULT half-spread for COVERED coins (clears slippage_uncalibrated for those). The size-impact SLOPE
(k, alpha) stays the conservative PRIOR -- V11 only samples small size ($<50 notional), so it
calibrates the spread INTERCEPT, not the slope (documented boundary).

REFERENCE PRICE (codex DESIGN r2/r3): fully CAUSAL -- the CLOSE of the last COMPLETED 1m bar BEFORE
the fill ts (MarketData.mark(..., causal=True)). NOT the containing-bar close (causal=False), which
mixes intra-minute drift/alpha into "cost". Robust MEDIAN over fills (mean is drift-dominated).

AS-OF PURITY: per fold k, the calibration uses ONLY V11 fills with ts <= test_start[k] (no leak).
A coin with < N_MIN as-of fills falls back to the prior. Empirical-Bayes shrinkage toward the prior
by sample size: base = (n/(n+K))*emp + (K/(n+K))*prior. Nonnegative floor (taker cost >= 0).

Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v15/v15_slippage_calib.py \
      --out app/data/v15/slippage_calib_v11.json
"""
from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("slip_calib")

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
CALIB_VERSION = "v11-fills-v2"   # v2: liquidity-CLASS comps for uncovered coins (Alberto 2026-06-01)

# Liquidity classes by dollar ADV (the real slippage proxy; Alberto: majors/mid/micro/xyz + global).
# xyz:/flx: (HIP-3 equities/markets) are their own class regardless of ADV.
ADV_MAJOR_USD = 200e6     # BTC/ETH/SOL/HYPE/ZEC ...
ADV_MIDCAP_USD = 20e6     # NEAR/TON/DOGE/VVV ...  (< major, >= this)
# below ADV_MIDCAP_USD -> microcap (ADA/INJ/PUMP/BIO ... thin HL books, high slippage)


def coin_class(coin: str, adv_usd: Optional[float]) -> str:
    if coin.startswith("xyz:") or coin.startswith("flx:"):
        return "hip3"
    if adv_usd is None or adv_usd != adv_usd:
        return "unknown"
    if adv_usd >= ADV_MAJOR_USD:
        return "major"
    if adv_usd >= ADV_MIDCAP_USD:
        return "midcap"
    return "microcap"


def compute_adv_usd(coins: list, mongo_uri: str = "mongodb://localhost:27017") -> dict:
    """Dollar ADV per coin = avg daily sum(volume*close) over the last ~14d of 1m candles. One
    aggregation. Returns {coin: adv_usd}."""
    from pymongo import MongoClient
    db = MongoClient(mongo_uri)["quants_lab"]
    out = {}
    for c in coins:
        cur = db.hyperliquid_candles.aggregate([
            {"$match": {"coin": c, "interval": "1m"}},
            {"$sort": {"timestamp_utc": -1}}, {"$limit": 20160},
            {"$group": {"_id": None,
                        "dvol": {"$sum": {"$multiply": [{"$toDouble": "$volume"}, {"$toDouble": "$close"}]}},
                        "n": {"$sum": 1}}},
        ], allowDiskUse=True)
        r = list(cur)
        out[c] = (r[0]["dvol"] / (r[0]["n"] / 1440.0)) if r and r[0]["n"] else None
    return out

# Defaults (frozen in the emitted manifest)
N_MIN = 20                 # min as-of fills for a coin to be calibrated (else prior)
SHRINK_K = 20.0            # empirical-Bayes shrinkage strength toward the prior
PRIOR_BASE_BPS = 8.0       # conservative blanket prior for the half-spread intercept (uncovered coins)
PRIOR_IMPACT_K_BPS = 8.0   # engine DEFAULT_IMPACT_K_BPS (slope prior; V11 can't calibrate it)
PRIOR_IMPACT_ALPHA = 0.5   # engine DEFAULT_IMPACT_ALPHA
WINSOR_PCT = 1.0           # winsorize realized-slip tails before the median (robustness)


def load_v11_fills(mongo_uri: str = "mongodb://localhost:27017") -> pd.DataFrame:
    from pymongo import MongoClient
    cli = MongoClient(mongo_uri)
    docs = list(cli.quants_lab.v11_exchange_fills.find(
        {}, {"_id": 0, "coin": 1, "px": 1, "sz": 1, "side": 1, "time": 1}))
    if not docs:
        return pd.DataFrame(columns=["coin", "px", "sz", "side", "time"])
    df = pd.DataFrame(docs)
    df["px"] = df["px"].astype(float)
    df["sz"] = df["sz"].astype(float)
    df["time"] = df["time"].astype("int64")
    df["sgn"] = np.where(df["side"].isin(["B", "b"]), 1.0, -1.0)  # buy +1, sell -1
    df["notional"] = (df["px"] * df["sz"].abs())
    return df


def realized_slip_bps(df: pd.DataFrame, mark_fn) -> pd.DataFrame:
    """Per fill: signed realized slip vs the CAUSAL reference. + = adverse (paid up).
    mark_fn(coin, ts_ms) -> reference price (caller passes a causal=True mark)."""
    refs = np.array([mark_fn(r.coin, int(r.time)) for r in df.itertuples()], dtype="float64")
    out = df.copy()
    out["ref"] = refs
    ok = np.isfinite(refs) & (refs > 0)
    out = out[ok].copy()
    out["slip_bps"] = out["sgn"] * (out["px"] - out["ref"]) / out["ref"] * 1e4
    return out


def _robust_base_bps(slips: np.ndarray) -> float:
    """Winsorized MEDIAN realized slip, nonnegative-floored (taker cost >= 0). FILTERS non-finite
    first (codex r1#1: max(0.0, nan)==0.0 would otherwise mint a bogus covered base). NaN if empty."""
    s = np.asarray(slips, dtype="float64")
    s = s[np.isfinite(s)]
    if s.size == 0:
        return float("nan")
    lo, hi = np.percentile(s, [WINSOR_PCT, 100 - WINSOR_PCT])
    w = np.clip(s, lo, hi)
    med = np.median(w)
    return float(max(0.0, med)) if np.isfinite(med) else float("nan")


def _shrink(emp_bps: float, n: int, prior_bps: float, k: float) -> float:
    if not np.isfinite(emp_bps):
        return prior_bps
    return (n / (n + k)) * emp_bps + (k / (n + k)) * prior_bps


def _class_comps(own: dict, coin_cls: dict) -> tuple:
    """Per-liquidity-class comp = median of the OWN-covered empirical bases in that class; global =
    median over all own-covered bases. (Alberto 2026-06-01: uncovered coins inherit their class comp.)"""
    by_cls: dict = {}
    allv = []
    for c, rec in own.items():
        if rec["covered"]:
            by_cls.setdefault(coin_cls.get(c, "unknown"), []).append(rec["base_half_spread_bps"])
            allv.append(rec["base_half_spread_bps"])
    comps = {k: float(np.median(v)) for k, v in by_cls.items() if v}
    glob = float(np.median(allv)) if allv else None
    return comps, glob


def calibrate(slipped: pd.DataFrame, fold_test_start_ms: dict[int, int],
              n_min: int = N_MIN, shrink_k: float = SHRINK_K, prior_bps: float = PRIOR_BASE_BPS,
              coin_universe: Optional[list] = None, adv_map: Optional[dict] = None) -> dict:
    """v2: EVERY coin gets a calibrated base (-> clears slippage_uncalibrated everywhere). Per coin per
    fold, base = own as-of empirical (if >=n_min as-of fills) ELSE its LIQUIDITY-CLASS comp (median of
    covered coins in the class) ELSE the global median ELSE the conservative prior. Class = dollar-ADV
    bucket {major/midcap/microcap/hip3} (Alberto 2026-06-01). Slippage is a structural liquidity
    property, so class comps are computed FULL-WINDOW (a cost constant, NOT selection -> no pool leak);
    own-coin empirical still uses as-of fills where available."""
    coins = sorted(set(coin_universe) | set(slipped["coin"].unique())) if coin_universe is not None \
        else sorted(slipped["coin"].unique())
    adv_map = adv_map or {}
    coin_cls = {c: coin_class(c, adv_map.get(c)) for c in coins}

    def _own(sub: pd.DataFrame) -> dict:
        table = {}
        for c in coins:
            cs = sub.loc[sub.coin == c, "slip_bps"].to_numpy(dtype="float64")
            cs = cs[np.isfinite(cs)]
            n = int(cs.size)
            emp = _robust_base_bps(cs)
            covered = n >= n_min and np.isfinite(emp)
            base = _shrink(emp, n, prior_bps, shrink_k) if covered else float("nan")
            table[c] = {"base_half_spread_bps": round(float(base), 4) if covered else float("nan"),
                        "n_fills": n, "emp_median_bps": round(float(emp), 4) if np.isfinite(emp) else None,
                        "covered": bool(covered)}
        return table

    # full-window own bases -> the class comps (structural liquidity cost).
    full_own = _own(slipped)
    comps, glob = _class_comps(full_own, coin_cls)

    def _assign(own_tbl: dict) -> dict:
        """assign EVERY coin a base + source; all marked covered (calibrated)."""
        table = {}
        for c in coins:
            o = own_tbl[c]
            cls = coin_cls.get(c, "unknown")
            if o["covered"]:
                base, src = o["base_half_spread_bps"], "own_empirical"
            elif cls in comps:
                base, src = comps[cls], f"class_comp:{cls}"
            elif glob is not None:
                base, src = glob, "global_median"
            else:
                base, src = prior_bps, "prior"
            table[c] = {
                "base_half_spread_bps": round(float(base), 4),
                "impact_k_bps": PRIOR_IMPACT_K_BPS, "impact_alpha": PRIOR_IMPACT_ALPHA,
                "liquidity_class": cls, "n_fills": o["n_fills"],
                "emp_median_bps": o["emp_median_bps"], "base_source": src,
                "covered": True,            # v2: ALWAYS calibrated (own / class / global) -> clears flag
            }
        return table

    per_fold = {}
    for k, ts in sorted(fold_test_start_ms.items()):
        per_fold[str(k)] = _assign(_own(slipped[slipped["time"] <= ts]))
    full = _assign(full_own)

    return {
        "version": CALIB_VERSION,
        "reference": "hl_mark_causal_true_1m_completed_bar_close",
        "estimator": "winsorized_median_nonneg_floor",
        "shrinkage": {"kind": "empirical_bayes_n_over_n_plus_k", "k": shrink_k},
        "n_min_covered": n_min, "prior_base_bps": prior_bps,
        "prior_impact_k_bps": PRIOR_IMPACT_K_BPS, "prior_impact_alpha": PRIOR_IMPACT_ALPHA,
        "winsor_pct": WINSOR_PCT,
        "liquidity_classes": {"proxy": "dollar_adv_14d", "major_usd": ADV_MAJOR_USD,
                              "midcap_usd": ADV_MIDCAP_USD, "class_comps_full_window": comps,
                              "global_median": glob},
        "boundary_note": "v2: every coin calibrated via own-empirical / liquidity-class comp / global "
                         "median -> clears slippage_uncalibrated everywhere. Impact SLOPE stays prior "
                         "(V11 small-size only). Class comps are full-window (structural cost, not selection).",
        "per_fold_asof": per_fold,
        "full_window": full,
    }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import v15_m07_engine as E  # for MarketData.mark (causal=True)

    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=str(DATA_DIR / "slippage_calib_v11.json"))
    ap.add_argument("--folds", default=str(DATA_DIR / "m03_folds.parquet"))
    ap.add_argument("--traded-coins", default=str(DATA_DIR / "m02_actions.parquet"),
                    help="parquet whose 'coin' column = the ATTEMPTED universe (actions, not just "
                         "fills -- the engine calls liquidity for rejected orders too) to assign comps to")
    args = ap.parse_args()

    fills = load_v11_fills()
    logger.info("loaded %d V11 fills, %d coins", len(fills), fills["coin"].nunique())
    md = E.MarketData(allow_mongo=True)
    slipped = realized_slip_bps(fills, lambda c, t: md.mark(c, t, causal=True))
    logger.info("ref coverage %.1f%% (%d usable)", 100.0 * len(slipped) / max(len(fills), 1), len(slipped))

    universe = set(fills["coin"].unique())
    try:
        tc = pd.read_parquet(args.traded_coins, columns=["coin"])
        universe |= set(tc["coin"].unique())
    except Exception as e:
        logger.warning("traded-coins union skipped: %s", e)
    universe = sorted(universe)
    logger.info("computing dollar ADV for %d coins (V11 + traded universe)...", len(universe))
    adv_map = compute_adv_usd(universe)

    folds = pd.read_parquet(args.folds)
    fold_test_start_ms = {int(r.fold_id): pd.Timestamp(r.test_start).value // 1_000_000
                          for r in folds.itertuples()}
    calib = calibrate(slipped, fold_test_start_ms, coin_universe=universe, adv_map=adv_map)
    cls_counts = {}
    for v in calib["full_window"].values():
        cls_counts[v["liquidity_class"]] = cls_counts.get(v["liquidity_class"], 0) + 1
    logger.info("liquidity classes: %s | class comps: %s", cls_counts,
                {k: round(v, 2) for k, v in calib["liquidity_classes"]["class_comps_full_window"].items()})
    n_cov = sum(1 for v in calib["full_window"].values() if v["covered"])
    logger.info("calibrated: %d/%d coins covered (full window)", n_cov, len(calib["full_window"]))
    Path(args.out).write_text(json.dumps(calib, indent=2))
    logger.info("wrote %s", args.out)


if __name__ == "__main__":
    main()
