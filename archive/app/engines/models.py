"""
Data models for the engine evaluation pipeline.

CandidateBase: Common base for all engine candidate outputs.
DecisionSnapshot: Immutable frozen inputs for engine evaluation.
FeatureRow: Computed features for a pair at a point in time.

Migrated from crypto-quant/data/snapshot.py + features.py.
SQLite removed — all storage via MongoDB.
"""
import time
import uuid
from dataclasses import dataclass, field, asdict
from typing import Optional

from pydantic import BaseModel, Field


@dataclass
class CandidateBase:
    """Common fields for all engine candidate outputs.

    Every engine's candidate (E1Candidate, E2Candidate, etc.) inherits from
    this.  The signal scan and testnet resolver can handle any engine's
    candidates via this interface without engine-specific branching.
    """
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

    # Soft filters (optional, engine-specific scoring)
    soft_filter_score: int = 0

    # State
    market_state: str = ""
    feature_staleness_flags: list = field(default_factory=list)
    composite_score: float = 0.0

    # Disposition
    disposition: str = "PENDING"
    filter_reason: str = ""

    # Price levels
    decision_price: Optional[float] = None
    signal_level: Optional[float] = None

    def to_dict(self) -> dict:
        """Convert to dict for MongoDB storage."""
        return asdict(self)


class FeatureRow(BaseModel):
    """Computed features for a pair at a point in time."""

    pair: str
    timestamp_utc: int
    close: float

    # Returns
    return_1h: Optional[float] = None
    return_4h: Optional[float] = None
    return_24h: Optional[float] = None

    # ATR / Volatility
    atr_14_1h: Optional[float] = None
    atr_14_5m: Optional[float] = None
    open_5m: Optional[float] = None
    realized_vol_4h: Optional[float] = None
    atr_percentile_90d: Optional[float] = None  # 0.0-1.0
    atr_median_90d: Optional[float] = None  # median ATR over 90d (normal volatility reference)

    # Range compression
    range_high_20: Optional[float] = None
    range_low_20: Optional[float] = None
    range_width: Optional[float] = None  # range_high_20 - range_low_20
    range_compression_confirmed: bool = False  # ATR percentile < 20th

    # Volume
    volume_1h: Optional[float] = None
    volume_zscore_20: Optional[float] = None
    volume_floor_passed: bool = False  # volume > 1.3x 20-period avg

    # Funding
    funding_rate_current: Optional[float] = None
    funding_rate_delta_3p: Optional[float] = None
    funding_neutral: bool = False

    # OI
    oi_current: Optional[float] = None
    oi_change_1h_pct: Optional[float] = None
    oi_change_4h_pct: Optional[float] = None
    oi_increasing: bool = False

    # L/S ratio
    ls_buy_ratio: Optional[float] = None
    ls_change: Optional[float] = None

    # Relative strength vs BTC
    rs_vs_btc_4h: Optional[float] = None
    rs_vs_btc_24h: Optional[float] = None
    rs_aligned: bool = False

    # Latest candle OHLC (for engines that need intra-candle data like E2)
    candle_high: Optional[float] = None
    candle_low: Optional[float] = None
    candle_open: Optional[float] = None
    # Range expansion rate (for E2 range stability check)
    range_expanding: Optional[float] = None

    # Shadow columns for E2/E3
    oi_direction: Optional[int] = None
    funding_extreme: Optional[int] = None
    funding_direction: Optional[int] = None
    atr_regime: Optional[str] = None
    regime_label: Optional[str] = None

    # Metadata
    feature_timestamp_utc: int = Field(default_factory=lambda: int(time.time() * 1000))
    feature_staleness_ok: bool = True
    staleness_flags: list = Field(default_factory=list)


class DecisionSnapshot(BaseModel):
    """
    Immutable struct. Built once at evaluation time.
    All engine logic reads from this — never from live data.
    """

    snapshot_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp_utc: int = Field(default_factory=lambda: int(time.time() * 1000))
    pair: str = ""
    market_state: str = "Unknown"
    features: Optional[FeatureRow] = None
    staleness_ok: bool = False
    staleness_flags: list = Field(default_factory=list)
    built_at: int = Field(default_factory=lambda: int(time.time() * 1000))


def validate_staleness(snap: DecisionSnapshot) -> tuple[bool, list]:
    """Check staleness from the snapshot (not live data)."""
    return snap.staleness_ok, snap.staleness_flags
