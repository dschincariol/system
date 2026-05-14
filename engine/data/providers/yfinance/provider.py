"""
FILE: provider.py

Market-data provider integration module for `provider`.
"""

from engine.data.providers.base import BasePriceProvider


PROVIDER = {
    "name": "yfinance",
    # YFinance is the lightweight polling fallback. It is intentionally always
    # available so the system can degrade gracefully when premium feeds fail.
    "mode": "polling",
    "daemon": "poll_prices",
    "priority": 50,
    "enabled": True,
}


class YFinanceProvider(BasePriceProvider):
    provider_name = "yfinance"

    def __init__(self):
        # Registry/build compatibility wrapper around the live-price adapter.
        from engine.data.live_prices.yfinance_live import YFinancePriceProvider
        self.impl = YFinancePriceProvider()

    def fetch_last_prices(self, symbol_map):
        return self.impl.fetch_last_prices(symbol_map)


def build_provider():
    return YFinanceProvider()
