"""
Feature discovery decorators for safe auto-registration.

Use @screening_feature on FeatureBase subclasses that should be included
in the FeatureComputationTask pipeline. This prevents accidental inclusion
of core/features/ base classes or helper classes.

Usage:
    @screening_feature
    class ATRFeature(FeatureBase):
        ...
"""

import logging
from typing import List, Type

logger = logging.getLogger(__name__)

_SCREENING_FEATURES: List[Type] = []
_REGISTERED_NAMES: set = set()


def screening_feature(cls: Type) -> Type:
    """Mark a FeatureBase subclass as a screening feature.

    Classes decorated with this are automatically included in ALL_FEATURES
    and computed by FeatureComputationTask. Idempotent — re-importing
    the same class does not duplicate registration.
    """
    name = f"{cls.__module__}.{cls.__qualname__}"
    if name not in _REGISTERED_NAMES:
        _SCREENING_FEATURES.append(cls)
        _REGISTERED_NAMES.add(name)
    return cls


def get_all_features() -> List[Type]:
    """Return all registered screening features."""
    return list(_SCREENING_FEATURES)
