"""
FILE: __init__.py

Package marker for `engine/data/provider_sessions`.
"""

from .base_session import BaseProviderSession
from .session_manager import ProviderSessionManager

__all__ = [
    "BaseProviderSession",
    "ProviderSessionManager",
    "PolygonWSSession",
    "IBKRSession",
]


def __getattr__(name):
    if name == "PolygonWSSession":
        from .polygon_ws_session import PolygonWSSession
        return PolygonWSSession
    if name == "IBKRSession":
        from .ibkr_session import IBKRSession
        return IBKRSession

    raise AttributeError(name)