"""
Engine 2 — Range Fade (Chop Regime Mean Reversion)
V1 P3 — April 2026

Thesis: In non-trending regimes, fade touches/pierces of the 20-period
range boundaries when price rejects and closes back inside.

TRIGGER (all must be true on candle N):
  1. ATR compression: ATR percentile < 0.30 (30th percentile)
  2. Range NOT expanding: 5-bar range width growth <= 20%
  3. Boundary touch/pierce: low of candle N <= range_low_20 (LONG only P3)
  4. Rejection: close > range_low_20
  5. No breakout confirmation (ALL must hold):
     a. Volume z-score <= 1.5
     b. OI change 1h <= 2.0%
     c. Candle body <= 0.8 * ATR_14_1h

EXIT:
  - TP: range midpoint
  - SL: boundary - 0.75 * ATR_14_1h
  - Time limit: 12 bars (12h on 1h TF)

Migrated from crypto-quant/engines/e2_range_fade.py.
SQLite removed — candidate storage handled by task layer.
"""
from dataclasses import dataclass, field
from typing import Optional

from app.engines.fmt import fp

from app.engines.models import CandidateBase, DecisionSnapshot, validate_staleness

# Constants
VOLUME_ZSCORE_CEILING = 1.5
OI_EXPANSION_CEILING_PCT = 2.0
BODY_ATR_RATIO_MAX = 0.8
STOP_ATR_MULTIPLE = 0.75
MIN_REWARD_ATR_MULTIPLE = 0.5
MAX_HOLD_BARS = 12
ATR_COMPRESSION_THRESHOLD = 0.30
RANGE_EXPANDING_MAX = 0.20


@dataclass
class E2Candidate(CandidateBase):
    """E2-specific candidate fields (extends CandidateBase)."""
    # Price levels
    entry_price: Optional[float] = None
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None
    range_midpoint: Optional[float] = None
    # Rejection candle data
    candle_high: Optional[float] = None
    candle_low: Optional[float] = None
    candle_body: Optional[float] = None
    volume_zscore: Optional[float] = None
    oi_change_1h: Optional[float] = None


def evaluate_e2(snap: DecisionSnapshot) -> E2Candidate:
    """
    Full E2 evaluation pipeline. Reads ONLY from snapshot.
    Returns E2Candidate with disposition.

    Candle OHLC and range_expanding are read from FeatureRow
    (populated by signal scan before calling this function).
    """
    cand = E2Candidate(
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

    # 3a: ATR compression
    if fr.atr_percentile_90d is None or fr.atr_percentile_90d >= ATR_COMPRESSION_THRESHOLD:
        cand.trigger_fired = False
        cand.disposition = "SKIPPED_NO_COMPRESSION"
        cand.trigger_reason = (
            f"No compression (ATR pct={fr.atr_percentile_90d} >= {ATR_COMPRESSION_THRESHOLD})"
        )
        return cand

    # 3b: Range expanding filter
    range_expanding = fr.range_expanding
    if range_expanding is not None and range_expanding > RANGE_EXPANDING_MAX:
        cand.trigger_fired = False
        cand.disposition = "SKIPPED_RANGE_EXPANDING"
        cand.trigger_reason = f"Range expanding ({range_expanding:.2%} > {RANGE_EXPANDING_MAX:.0%})"
        return cand

    # 3c: Range boundaries
    if fr.range_high_20 is None or fr.range_low_20 is None:
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = "Range high/low not computed"
        return cand

    c_high = fr.candle_high if fr.candle_high is not None else fr.close
    c_low = fr.candle_low if fr.candle_low is not None else fr.close
    c_close = fr.close
    c_open = fr.candle_open

    # P3: LONG ONLY
    touched_low = c_low <= fr.range_low_20

    if not touched_low:
        cand.trigger_fired = False
        cand.disposition = "SKIPPED_NO_TRIGGER"
        cand.trigger_reason = (
            f"Price range [{fp(c_low)}, {fp(c_high)}] inside "
            f"[{fp(fr.range_low_20)}, {fp(fr.range_high_20)}] — no low boundary touch"
        )
        return cand

    direction = "LONG"
    boundary = fr.range_low_20

    # Rejection: close must be back inside range
    if c_close <= fr.range_low_20:
        cand.trigger_fired = False
        cand.disposition = "SKIPPED_NO_REJECTION"
        cand.trigger_reason = (
            f"Touched low {fp(boundary)} but closed at/below ({fp(c_close)}) — no rejection"
        )
        return cand

    # 3d: No breakout confirmation filters

    # No volume spike
    if fr.volume_zscore_20 is not None and fr.volume_zscore_20 > VOLUME_ZSCORE_CEILING:
        cand.trigger_fired = False
        cand.disposition = "FILTERED_EXPANSION"
        cand.filter_reason = f"Volume spike (z={fr.volume_zscore_20:.2f} > {VOLUME_ZSCORE_CEILING})"
        cand.direction = direction
        return cand

    # No OI expansion — require OI data (skip pair if missing)
    if fr.oi_change_1h_pct is None:
        cand.trigger_fired = False
        cand.disposition = "FILTERED_STALENESS"
        cand.filter_reason = "OI data missing — cannot confirm no-breakout"
        cand.direction = direction
        return cand
    if fr.oi_change_1h_pct > OI_EXPANSION_CEILING_PCT:
        cand.trigger_fired = False
        cand.disposition = "FILTERED_EXPANSION"
        cand.filter_reason = (
            f"OI expanding ({fr.oi_change_1h_pct:.2f}% > {OI_EXPANSION_CEILING_PCT}%)"
        )
        cand.direction = direction
        return cand

    # No momentum candle
    if fr.atr_14_1h is not None:
        candle_body_val = abs(c_close - c_open) if c_open is not None else None
        if candle_body_val is not None:
            body_threshold = BODY_ATR_RATIO_MAX * fr.atr_14_1h
            if candle_body_val > body_threshold:
                cand.trigger_fired = False
                cand.disposition = "FILTERED_EXPANSION"
                cand.filter_reason = (
                    f"Momentum candle: body={fp(candle_body_val)} > "
                    f"{fp(body_threshold)} (0.8 * ATR={fp(fr.atr_14_1h)})"
                )
                cand.direction = direction
                return cand
        cand.candle_body = candle_body_val

    # All trigger conditions met
    cand.trigger_fired = True
    cand.direction = direction
    cand.signal_level = boundary
    cand.decision_price = c_close
    cand.candle_high = c_high
    cand.candle_low = c_low
    cand.volume_zscore = fr.volume_zscore_20
    cand.oi_change_1h = fr.oi_change_1h_pct

    # Compute TP and SL
    midpoint = (fr.range_high_20 + fr.range_low_20) / 2.0
    cand.range_midpoint = midpoint
    cand.tp_price = midpoint

    atr = fr.atr_14_1h
    cand.sl_price = fr.range_low_20 - STOP_ATR_MULTIPLE * atr

    # STEP 4: Minimum reward check
    distance_to_midpoint = abs(c_close - midpoint)
    min_reward = MIN_REWARD_ATR_MULTIPLE * atr
    if distance_to_midpoint < min_reward:
        cand.disposition = "FILTERED_MIN_REWARD"
        cand.filter_reason = (
            f"Distance to midpoint ({fp(distance_to_midpoint)}) < "
            f"0.5 ATR ({fp(min_reward)}) — no room"
        )
        return cand

    # STEP 4b: Risk-reward check — TP distance must exceed SL distance
    tp_dist = abs(cand.tp_price - c_close)
    sl_dist = abs(cand.sl_price - c_close)
    if sl_dist > 0 and tp_dist / sl_dist < 1.0:
        cand.disposition = "FILTERED_MIN_REWARD"
        cand.filter_reason = (
            f"R:R {tp_dist/sl_dist:.2f} < 1.0 — reward does not exceed risk"
        )
        return cand

    cand.trigger_reason = (
        f"Range fade {direction}: boundary={fp(boundary)}, "
        f"close={fp(c_close)} (rejected), midpoint={fp(midpoint)}, "
        f"SL={fp(cand.sl_price)}"
    )

    cand.disposition = "CANDIDATE_READY"
    cand.hard_filters_passed = True

    return cand
