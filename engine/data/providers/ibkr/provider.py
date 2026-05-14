"""
FILE: provider.py

Market-data provider integration module for `provider`.
"""

import os

from engine.data.providers.base import BasePriceProvider

PROVIDER = {
    "name": "ibkr",
    "provider_name": "ibkr",
    "mode": "streaming",
    # IBKR is registered as a daemon-backed streaming provider because the
    # supervised runtime owns reconnects and health, not ad hoc callers.
    "implementation_kind": "daemon",
    "daemon": "stream_prices_ibkr",
    "daemon_job_name": "stream_prices_ibkr",
    "daemon_script": "engine/data/providers/ibkr/daemon_stream.py",
    "priority": 20,
    "enabled": str(os.environ.get("IBKR_ENABLED", "")).strip().lower() in ("1", "true", "yes", "on"),
    "supports": {
        "asset_classes": ["equities"],
        "transport": "gateway",
    },
}


class IBKRProvider(BasePriceProvider):
    provider_name = "ibkr"

    def __init__(self):
        # This thin wrapper exists for registry compatibility. The canonical
        # streaming behavior lives in the daemon/session path, not here.
        from engine.data.live_prices.ibkr_live import IBKRPriceProvider
        self.impl = IBKRPriceProvider()

    def fetch_last_prices(self, symbol_map):
        return self.impl.fetch_last_prices(symbol_map)


def build_provider():
    return IBKRProvider()
