from app.features.atr_feature import ATRFeature, ATRConfig
from app.features.range_feature import RangeFeature, RangeConfig
from app.features.volume_feature import VolumeFeature, VolumeConfig
from app.features.momentum_feature import MomentumFeature, MomentumConfig
from app.features.candle_structure_feature import CandleStructureFeature, CandleStructureConfig
from app.features.derivatives_feature import DerivativesFeature, DerivativesConfig
from app.features.market_regime_feature import MarketRegimeFeature, MarketRegimeConfig
from app.features.microstructure_features import MicrostructureFeature, MicrostructureConfig
from app.features.options_features import OptionsFeature, OptionsConfig
from app.features.liquidation_features import LiquidationFeature, LiquidationConfig
from app.features.external_feature_base import ExternalFeatureBase

# All registered features — FeatureComputationTask iterates this list.
# source_type: "computed" = derived from candle data (default)
#              "derivatives" = reads from MongoDB derivatives collections
#              "external" = fetched from external API via ExternalFeatureBase.fetch()
#              "microstructure" = computed from 1m candle data (requires 1m parquet)
ALL_FEATURES = [
    ATRFeature,          # computed
    RangeFeature,        # computed
    VolumeFeature,       # computed
    MomentumFeature,     # computed
    CandleStructureFeature,  # computed
    DerivativesFeature,  # derivatives (from MongoDB, not candles)
    MarketRegimeFeature, # computed (depends on ATR)
    MicrostructureFeature,  # microstructure (from 1m candle data)
]

# External features are registered here when implemented.
# They run in a separate pass in FeatureComputationTask.
# Example: ALL_EXTERNAL_FEATURES = [SentimentFeature, OnChainFeature]
ALL_EXTERNAL_FEATURES = []
