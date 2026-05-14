"""
FILE: volatility.py

Price-series utility module for `volatility`.
"""

# dev_core/prices/volatility.py
import math
from typing import List

def compute_volatility(series: List[dict]) -> float:
    """
    Simple realized volatility estimate (std of log returns).
    """
    rets = []
    for i in range(1, len(series)):
        p0 = series[i - 1]["price"]
        p1 = series[i]["price"]
        if p0 > 0 and p1 > 0:
            rets.append(math.log(p1 / p0))
    if not rets:
        return 0.0
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / max(1, len(rets) - 1)
    return math.sqrt(var)
