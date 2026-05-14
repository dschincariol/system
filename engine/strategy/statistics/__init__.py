"""Statistical acceptance gates for feature and model promotion.

The package centralizes multiple-testing and bootstrap checks used by the
promotion pipeline so model acceptance decisions can be reproduced from their
recorded evidence.
"""

from engine.strategy.statistics.factor_threshold import (
    FactorThresholdResult,
    harvey_liu_zhu_threshold_result,
    newey_west_t_statistic,
)
from engine.strategy.statistics.multiple_testing import (
    MultipleTestResult,
    benjamini_hochberg,
    bonferroni,
    holm,
)
from engine.strategy.statistics.reality_check import (
    RealityCheckResult,
    white_reality_check,
)

__all__ = [
    "FactorThresholdResult",
    "MultipleTestResult",
    "RealityCheckResult",
    "benjamini_hochberg",
    "bonferroni",
    "harvey_liu_zhu_threshold_result",
    "holm",
    "newey_west_t_statistic",
    "white_reality_check",
]
