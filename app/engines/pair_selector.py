"""
Pair selector — combines historical prior (MongoDB) + latest features (FeatureStorage).

Returns eligible pairs per engine, ranked by current suitability.
Migrated from crypto-quant/factory/live/pair_selector.py — SQLite replaced with MongoDB.
"""
import logging
from typing import Optional

from core.services.mongodb_client import MongoClient

logger = logging.getLogger(__name__)


async def get_eligible_pairs(
    mongo: MongoClient,
    engine: str,
    top_n: int = 5,
    include_watch: bool = False,
) -> tuple[list[dict], list[dict]]:
    """
    Get top-N eligible pairs for an engine.

    Returns:
        (ranked_pairs, rejection_log)
    """
    # Step 1: Get historical allowlist from MongoDB
    verdicts = ["ALLOW"]
    if include_watch:
        verdicts.append("WATCH")

    hist_docs = await mongo.get_documents(
        "pair_historical",
        {"engine": engine, "verdict": {"$in": verdicts}},
    )

    if not hist_docs:
        return [], [{"pair": "ALL", "reason": f"no {engine} pair_historical with verdict in {verdicts}"}]

    allowed_pairs = {d["pair"]: d for d in hist_docs}

    # Step 2: Get latest features from features collection
    # For each allowed pair, get its most recent feature set
    candidates = []
    rejection_log = []

    for pair, hist in allowed_pairs.items():
        # Get latest ATR feature
        atr_docs = await mongo.get_documents(
            "features",
            {"feature_name": "atr", "trading_pair": pair},
            limit=1,
        )
        if not atr_docs:
            rejection_log.append({"pair": pair, "reason": "no_features"})
            continue

        atr_val = atr_docs[0].get("value", {})
        atr_pct = atr_val.get("atr_percentile_90d")

        # Get volume feature
        vol_docs = await mongo.get_documents(
            "features",
            {"feature_name": "volume", "trading_pair": pair},
            limit=1,
        )
        vol_val = vol_docs[0].get("value", {}) if vol_docs else {}

        # Get range feature
        range_docs = await mongo.get_documents(
            "features",
            {"feature_name": "range", "trading_pair": pair},
            limit=1,
        )
        range_val = range_docs[0].get("value", {}) if range_docs else {}

        # Get momentum feature
        mom_docs = await mongo.get_documents(
            "features",
            {"feature_name": "momentum", "trading_pair": pair},
            limit=1,
        )
        mom_val = mom_docs[0].get("value", {}) if mom_docs else {}

        # Get candle structure feature
        cs_docs = await mongo.get_documents(
            "features",
            {"feature_name": "candle_structure", "trading_pair": pair},
            limit=1,
        )
        cs_val = cs_docs[0].get("value", {}) if cs_docs else {}

        candidates.append({
            "pair": pair,
            "hist": hist,
            "features": {
                "atr_pct": atr_pct,
                "range_expanding": range_val.get("range_expanding"),
                "range_width": range_val.get("range_width"),
                "vol_zscore": vol_val.get("vol_zscore_20"),
                "price_position": cs_val.get("price_position_in_range"),
                "ema_alignment": mom_val.get("ema_alignment"),
            },
        })

    # Step 3: Engine-specific ranking
    if engine == "E1":
        ranked = _rank_e1(candidates)
    elif engine == "E2":
        ranked = _rank_e2(candidates)
    else:
        ranked = candidates

    # Step 4: Build output (top_n <= 0 means return all)
    selected = ranked if top_n <= 0 else ranked[:top_n]
    result = []
    for i, c in enumerate(selected):
        result.append({
            "pair": c["pair"],
            "rank": i + 1,
            "hist_pf": c["hist"].get("profit_factor"),
            "hist_wr": c["hist"].get("win_rate"),
            "hist_verdict": c["hist"].get("verdict"),
            "live_score": c.get("live_score", 0),
            **c["features"],
        })

    return result, rejection_log


def _rank_e1(candidates: list[dict]) -> list[dict]:
    """E1 ranking: low ATR percentile, tight range, volume wake-up."""
    for c in candidates:
        f = c["features"]
        score = 0.0

        atr_pct = f.get("atr_pct")
        if atr_pct is not None:
            score += (1.0 - atr_pct) * 45

        rw = f.get("range_width")
        if rw is not None and rw > 0:
            # Normalize: narrower range = higher score
            score += max(0, 20 - rw * 0.01) * 1.0

        vz = f.get("vol_zscore")
        if vz is not None and 0 < vz < 1.5:
            score += (vz / 1.5) * 15

        hist_pf = c["hist"].get("profit_factor", 1.0)
        score += min(hist_pf / 3.0, 1.0) * 20

        c["live_score"] = round(score, 2)

    candidates.sort(key=lambda x: x["live_score"], reverse=True)
    return candidates


def _rank_e2(candidates: list[dict]) -> list[dict]:
    """E2 ranking: low ATR percentile, stable range, price near boundary."""
    for c in candidates:
        f = c["features"]
        score = 0.0

        atr_pct = f.get("atr_pct")
        if atr_pct is not None:
            score += (1.0 - atr_pct) * 40

        rng_exp = f.get("range_expanding")
        if rng_exp is not None:
            stability = max(0, 1.0 - abs(rng_exp) * 2)
            score += stability * 25

        pp = f.get("price_position")
        if pp is not None:
            boundary_proximity = max(0, 1.0 - pp)  # E2 long-only: near low = better
            score += boundary_proximity * 20

        hist_pf = c["hist"].get("profit_factor", 1.0)
        score += min(hist_pf / 3.0, 1.0) * 15

        c["live_score"] = round(score, 2)

    candidates.sort(key=lambda x: x["live_score"], reverse=True)
    return candidates
