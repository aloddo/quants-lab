"""
Feature registry — all screening features for FeatureComputationTask.

Features are registered via the @screening_feature decorator. Each import is
wrapped in try/except for fault isolation — one broken feature doesn't kill the
entire feature pipeline.

To add a new feature:
1. Create a FeatureBase subclass in app/features/
2. Add @screening_feature decorator to the class
3. Add a fault-isolated import below
"""

import logging

from app.features.decorators import screening_feature, get_all_features

logger = logging.getLogger(__name__)

# ── Fault-isolated imports ──────────────────────────────────────────
# Each import triggers @screening_feature registration.
# If one fails, the rest still load.

try:
    from app.features.atr_feature import ATRFeature, ATRConfig
except ImportError as e:
    logger.warning(f"Failed to import ATRFeature: {e}")

try:
    from app.features.range_feature import RangeFeature, RangeConfig
except ImportError as e:
    logger.warning(f"Failed to import RangeFeature: {e}")

try:
    from app.features.volume_feature import VolumeFeature, VolumeConfig
except ImportError as e:
    logger.warning(f"Failed to import VolumeFeature: {e}")

try:
    from app.features.momentum_feature import MomentumFeature, MomentumConfig
except ImportError as e:
    logger.warning(f"Failed to import MomentumFeature: {e}")

try:
    from app.features.candle_structure_feature import CandleStructureFeature, CandleStructureConfig
except ImportError as e:
    logger.warning(f"Failed to import CandleStructureFeature: {e}")

try:
    from app.features.derivatives_feature import DerivativesFeature, DerivativesConfig
except ImportError as e:
    logger.warning(f"Failed to import DerivativesFeature: {e}")

try:
    from app.features.market_regime_feature import MarketRegimeFeature, MarketRegimeConfig
except ImportError as e:
    logger.warning(f"Failed to import MarketRegimeFeature: {e}")

try:
    from app.features.microstructure_features import MicrostructureFeature, MicrostructureConfig
except ImportError as e:
    logger.warning(f"Failed to import MicrostructureFeature: {e}")

try:
    from app.features.options_features import OptionsFeature, OptionsConfig
except ImportError as e:
    logger.warning(f"Failed to import OptionsFeature: {e}")

try:
    from app.features.liquidation_features import LiquidationFeature, LiquidationConfig
except ImportError as e:
    logger.warning(f"Failed to import LiquidationFeature: {e}")

try:
    from app.features.external_feature_base import ExternalFeatureBase
except ImportError as e:
    logger.warning(f"Failed to import ExternalFeatureBase: {e}")

# ── Public API ──────────────────────────────────────────────────────

ALL_FEATURES = get_all_features()

# External features are registered here when implemented.
# They run in a separate pass in FeatureComputationTask.
ALL_EXTERNAL_FEATURES = []
