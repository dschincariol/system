"""
FILE: __init__.py

Package marker for `engine/data/providers/yfinance`.
"""

__all__ = ["build_provider"]


def __getattr__(name):
    if name == "build_provider":
        from .provider import build_provider
        return build_provider
    raise AttributeError(name)