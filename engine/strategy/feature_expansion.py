"""
FILE: feature_expansion.py

Builds the structured numeric features appended to text embeddings at train and
predict time. Feature order is intentionally strict because model compatibility
depends on it.
"""

import os
from typing import Dict, List, Optional

from engine.strategy.feature_registry import (
    BASE_FEATURE_IDS,
    build_feature_snapshot,
    feature_set_tag_from_ids,
    resolve_feature_ids,
)

# ------------            -- ------------------------------------------------------
# BASE FEATURE LAYOUT (KEEP ORDER STABLE)
# ------------            -- ------------------------------------------------------
#
# [0] source_credibility
# [1] log_recency_hours
# [2] normalized_text_len
# [3] scheduled_flag
# [4] session_asia
# [5] session_eu
# [6] session_us
# [7] asset_class_match
#
BASE_FEATURE_DIM = len(BASE_FEATURE_IDS)

# ------------            -- ------------------------------------------------------
# Optional feature flags (MUST MATCH train/predict)
# ------------            -- ------------------------------------------------------
USE_TECH_FEATURES = os.environ.get("USE_TECH_FEATURES", "0") == "1"
USE_STRESS_FEATURES = os.environ.get("USE_STRESS_FEATURES", "0") == "1"
USE_SOCIAL_FEATURES = os.environ.get("USE_SOCIAL_FEATURES", "0") == "1"
USE_SOCIAL_REGIME = os.environ.get("USE_SOCIAL_REGIME", "0") == "1"
USE_WEATHER_FEATURES = os.environ.get("USE_WEATHER_FEATURES", "0") == "1"
USE_FACTOR_UNIVERSE = os.environ.get("USE_FACTOR_UNIVERSE", "0") == "1"
USE_TSFRESH_FEATURES = os.environ.get("USE_TSFRESH_FEATURES", "0") == "1"

def feature_set_tag(feature_ids: Optional[List[str]] = None) -> str:
    """
    Stable feature-set tag for model-key namespacing.
    """
    return feature_set_tag_from_ids(resolve_feature_ids(feature_ids))


def build_feature_vector(*, event: Dict, symbol: str, feature_ids: Optional[List[str]] = None) -> list:
    """
    Returns a STRICTLY ORDERED feature list.

    Length and order come from `feature_ids`, including optional groups such as
    persisted `tsfresh.*` features resolved through the shared registry.
    """
    ids = resolve_feature_ids(feature_ids)
    snap = build_feature_snapshot(event=event, symbol=str(symbol), feature_ids=ids)
    return [float(snap.get(fid, 0.0) or 0.0) for fid in ids]
