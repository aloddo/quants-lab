"""
Engine 1 — Compression Breakout
V7.1 spec Section 6.

Primary Trigger (both must be true):
  1. ATR percentile < 20th (90-day rolling) — compression confirmed
  2. Price breaks above/below 20-period range high/low on 1h — expansion begins

Hard Filters:
  - BTC state NOT Risk-Off Contagion
  - Volume expansion floor: current volume > 1.3x 20-period average

Soft Filters (conviction scoring 0-5):
  - OI increasing (+1)
  - Market state = Trend Expansion (+1, Phase 2+ only — IGNORED in Phase 1)
  - Volume spike > 2x 20-period average (+1)
  - Funding rate neutral -0.01% to +0.05% (+1)
  - Relative strength vs BTC aligned (+1, alts only)

Entry Quality Constraint on 5m trigger (3 checks, ALL must pass):
  - Distance from breakout level < 0.3x ATR(14) on 5m
  - 5m candle body < 0.5x ATR(14) on 5m
  - Gap < min(5 bps, 0.15 × ATR_5m in bps) — BTC-calibrated constants

Migrated from crypto-quant/engines/e1_compression_breakout.py.
SQLite removed — candidate storage handled by task layer.
"""
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from app.engines.fmt import fp

from app.engines.models import DecisionSnapshot, validate_staleness


# Gap threshold constants — BTC-calibrated (Phase 1).
GAP_THRESHOLD_BPS_FLOOR = 5.0
GAP_THRESHOLD_ATR_MULT = 0.15


@dataclass
class E1Candidate:
    candidate_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    snapshot_id: str = ""
    pair: str = ""
    direction: str = ""
    timestamp_utc: int = field(default_factory=lambda: int(time.time() * 1000))
    # Trigger
    trigger_fired: bool = False
    trigger_reason: str = ""
    # Hard filters
    hard_filters_passed: bool = False
    hard_filter_fail_reason: str = ""
    # Soft filters
    soft_filter_score: int = 0
    soft_filter_breakdown: dict = field(default_factory=dict)
    # State
    market_state: str = ""
    feature_staleness_flags: list = field(default_factory=list)
    composite_score: float = 0.0
    # Disposition (set by execute layer)
    disposition: str = "PENDING"
    filter_reason: str = ""
    # P3 execution quality fields
    decision_price: Optional[float] = None
    signal_level: Optional[float] = None
    # Entry quality (set after 5m trigger)
    entry_quality_check_passed: Optional[bool] = None
    entry_quality_fail_reason: str = ""
    max_adverse_excursion_before_fill: Optional[float] = None
    # Fillability
    touched_signal_price: Optional[bool] = None
    fillable_estimate: Optional[bool] = None
    actual_fill: Optional[bool] = None
    actual_fill_price: Optional[float] = None


def evaluate_e1(snap: DecisionSnapshot) -> E1Candidate:
    """
    Full E1 evaluation pipeline. Reads ONLY from snapshot.

    V7.1 pipeline order (enforced):
      1. SNAPSHOT already built (input)
      2. VALIDATE STALENESS
      3. ENGINE TRIGGER CHECK
      4. EVALUATE HARD + SOFT FILTERS
      5. REGISTER CANDIDATE (done by caller)
      6. EXECUTION LOGIC (done by portfolio allocator)
    """
    cand = E1Candidate(
        snapshot_id=snap.snapshot_id,
        pair=snap.pair,
        market_state=snap.market_state,
    )

    fr = snap.features
    if fr is None:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = "No feature data in snapshot"
        return cand

    # STEP 2: Validate staleness
    stale_ok, stale_flags = validate_staleness(snap)
    cand.feature_staleness_flags = stale_flags
    if not stale_ok:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = f"Stale: {stale_flags}"
        return cand

    # STEP 3: Primary trigger check
    if fr.atr_percentile_90d is None:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = "ATR percentile not available (need 90d data)"
        return cand

    compression_ok = fr.range_compression_confirmed
    if not compression_ok:
        cand.trigger_fired = False
        cand.disposition = "SKIPPED_NO_TRIGGER"
        cand.trigger_reason = f"No compression (ATR pct={fr.atr_percentile_90d:.2f}, need < 0.20)"
        return cand

    if fr.range_high_20 is None or fr.range_low_20 is None:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = "Range high/low not computed"
        return cand

    price = fr.close
    if price > fr.range_high_20:
        direction = "LONG"
        breakout_level = fr.range_high_20
        trigger_reason = f"Breakout above range high {fp(fr.range_high_20)} (price={fp(price)})"
    elif price < fr.range_low_20:
        direction = "SHORT"
        breakout_level = fr.range_low_20
        trigger_reason = f"Breakdown below range low {fp(fr.range_low_20)} (price={fp(price)})"
    else:
        cand.trigger_fired = False
        cand.disposition = "SKIPPED_NO_TRIGGER"
        cand.trigger_reason = f"Price {fp(price)} inside range [{fp(fr.range_low_20)}, {fp(fr.range_high_20)}]"
        return cand

    cand.trigger_fired = True
    cand.direction = direction
    cand.trigger_reason = trigger_reason
    cand.decision_price = price
    cand.signal_level = breakout_level

    # STEP 4a: Hard filters
    if snap.market_state == "Risk-Off Contagion":
        cand.hard_filters_passed = False
        cand.hard_filter_fail_reason = "BTC Risk-Off Contagion — hard block"
        cand.disposition = "FILTERED_RISK_OFF"
        return cand

    if not fr.volume_floor_passed:
        cand.hard_filters_passed = False
        cand.hard_filter_fail_reason = f"Volume below floor (z-score={fr.volume_zscore_20:.2f})"
        cand.disposition = "FILTERED_VOLUME_FLOOR"
        return cand

    cand.hard_filters_passed = True

    # STEP 4b: Soft filters (conviction scoring 0-5)
    score = 0
    breakdown = {}

    if fr.oi_increasing:
        score += 1
        breakdown["oi_increasing"] = True
    else:
        breakdown["oi_increasing"] = False

    vol_spike = fr.volume_zscore_20 is not None and fr.volume_zscore_20 > 2.0
    if vol_spike:
        score += 1
        breakdown["volume_spike_2x"] = True
    else:
        breakdown["volume_spike_2x"] = False

    if fr.funding_neutral:
        score += 1
        breakdown["funding_neutral"] = True
    else:
        breakdown["funding_neutral"] = False

    if snap.pair != "BTC-USDT" and fr.rs_aligned:
        score += 1
        breakdown["rs_aligned"] = True
    elif snap.pair == "BTC-USDT":
        breakdown["rs_aligned"] = "N/A (BTC)"
    else:
        breakdown["rs_aligned"] = False

    cand.soft_filter_score = score
    cand.soft_filter_breakdown = breakdown
    cand.disposition = "CANDIDATE_READY"
    cand.composite_score = float(score) / 4.0

    return cand


def check_entry_quality_5m(
    snap: DecisionSnapshot, direction: str, breakout_level: float
) -> tuple[bool, str]:
    """
    Entry quality gate on 5m trigger (V7.1 Section 6).
    Returns (passed, fail_reason).

    WARNING: Gap threshold constants (5 bps, 0.15x) are BTC-calibrated.
    """
    fr = snap.features
    if fr is None or fr.atr_14_5m is None:
        return False, "atr_5m_missing"

    price = fr.close
    atr_5m = fr.atr_14_5m

    # Check 1: Distance
    distance = abs(price - breakout_level)
    distance_threshold = 0.3 * atr_5m
    if distance > distance_threshold:
        return False, f"distance ({distance:.4f} > {distance_threshold:.4f} = 0.3x ATR_5m)"

    # Check 2: Candle body < 0.5x ATR(5m)
    if fr.open_5m is not None:
        candle_body = abs(price - fr.open_5m)
        body_threshold = 0.5 * atr_5m
        if candle_body > body_threshold:
            return (
                False,
                f"candle body ({candle_body:.4f} > {body_threshold:.4f} = 0.5x ATR_5m)",
            )

    # Check 3: Gap threshold (BTC-calibrated)
    gap_bps = (distance / breakout_level) * 10_000
    gap_threshold_bps = min(
        GAP_THRESHOLD_BPS_FLOOR,
        GAP_THRESHOLD_ATR_MULT * (atr_5m / breakout_level * 10_000),
    )
    if gap_bps > gap_threshold_bps:
        return False, f"gap ({gap_bps:.2f} bps > {gap_threshold_bps:.2f} bps threshold)"

    return True, ""
