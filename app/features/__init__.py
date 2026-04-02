from app.features.atr_feature import ATRFeature, ATRConfig
from app.features.range_feature import RangeFeature, RangeConfig
from app.features.volume_feature import VolumeFeature, VolumeConfig
from app.features.momentum_feature import MomentumFeature, MomentumConfig
from app.features.candle_structure_feature import CandleStructureFeature, CandleStructureConfig
from app.features.derivatives_feature import DerivativesFeature, DerivativesConfig
from app.features.market_regime_feature import MarketRegimeFeature, MarketRegimeConfig

ALL_FEATURES = [
    ATRFeature,
    RangeFeature,
    VolumeFeature,
    MomentumFeature,
    CandleStructureFeature,
    DerivativesFeature,
    MarketRegimeFeature,
]
