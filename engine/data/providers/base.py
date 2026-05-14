"""
FILE: base.py

Market-data provider integration module for `base`.
"""

from typing import Dict, Any


class BasePriceProvider:
    provider_name: str = "unknown"

    # ----------------------------
    # Connection lifecycle
    # ----------------------------

    def connect(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    # ----------------------------
    # Data interfaces
    # ----------------------------

    def subscribe(self, symbols) -> None:
        pass

    def fetch_last_prices(self, symbol_map: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        # Polling providers normalize into this shape so the router can score
        # providers without caring about each source's native response format.
        raise NotImplementedError("fetch_last_prices not implemented")

    # ----------------------------
    # Health + telemetry
    # ----------------------------

    def health(self) -> Dict[str, Any]:
        # Health is advisory here; session-based providers publish richer health
        # through their session telemetry instead of this minimal interface.
        return {"ok": True}

    def latency(self) -> float | None:
        return None
