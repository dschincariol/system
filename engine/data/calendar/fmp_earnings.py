"""
FILE: fmp_earnings.py

Calendar and scheduled-event data helper for `fmp_earnings`.
"""

import requests

from engine.data._credentials import get_data_credential

BASE = "https://financialmodelingprep.com/api/v3"


def _fmp_key() -> str:
    return get_data_credential("FMP_API_KEY")


def fetch_earnings_calendar(from_date: str, to_date: str) -> list:
    """
    Returns list of dicts:
      { symbol, date, time, epsEstimated, eps, revenueEstimated, revenue }
    """
    # Missing credentials should degrade quietly because earnings context is
    # advisory to downstream models, not a hard startup dependency.
    fmp_key = _fmp_key()
    if not fmp_key:
        return []

    r = requests.get(
        f"{BASE}/earning_calendar",
        params={"from": from_date, "to": to_date, "apikey": fmp_key},
        timeout=20,
    )
    r.raise_for_status()
    j = r.json()
    return j if isinstance(j, list) else []
