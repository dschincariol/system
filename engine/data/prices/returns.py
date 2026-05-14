"""
FILE: returns.py

Price-series utility module for `returns`.
"""

# dev_core/prices/returns.py
from bisect import bisect_right
from typing import List, Optional

def price_at_or_after(series: List[dict], ts_ms: int) -> Optional[float]:
    times = [p["ts_ms"] for p in series]
    idx = bisect_right(times, ts_ms)
    if idx >= len(series):
        return None
    return series[idx]["price"]

def compute_return(series: List[dict], event_ts: int, horizon_ms: int) -> Optional[float]:
    p0 = price_at_or_after(series, event_ts)
    p1 = price_at_or_after(series, event_ts + horizon_ms)
    if p0 is None or p1 is None:
        return None
    return (p1 - p0) / p0
