"""
Data models for the engine evaluation pipeline.

DecisionSnapshot: Immutable frozen inputs for engine evaluation.
FeatureRow: Computed features for a pair at a point in time.

Migrated from crypto-quant/data/snapshot.py + features.py.
SQLite removed — all storage via MongoDB.
"""
import time
import uuid
from typing import Optional

from pydantic import BaseModel, Field


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

    # Range compression
    range_high_20: Optional[float] = None
    range_low_20: Optional[float] = None
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
