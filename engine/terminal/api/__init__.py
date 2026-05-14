"""
FILE: __init__.py

Package marker for `engine/terminal/api`.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .api_terminal import ROUTE_SPECS_TERMINAL
    from .api_terminal_orders import ROUTE_SPECS_TERMINAL_ORDERS
    ROUTE_SPECS_TERMINAL_ALL = list(ROUTE_SPECS_TERMINAL) + list(ROUTE_SPECS_TERMINAL_ORDERS)

__all__ = [
    "ROUTE_SPECS_TERMINAL",
    "ROUTE_SPECS_TERMINAL_ORDERS",
    "ROUTE_SPECS_TERMINAL_ALL",
]


def __getattr__(name):
    # Lazy import keeps the package importable without eagerly pulling in the
    # full terminal API graph, which is useful for route registration and tests.
    if name == "ROUTE_SPECS_TERMINAL":
        from .api_terminal import ROUTE_SPECS_TERMINAL
        return ROUTE_SPECS_TERMINAL

    if name == "ROUTE_SPECS_TERMINAL_ORDERS":
        from .api_terminal_orders import ROUTE_SPECS_TERMINAL_ORDERS
        return ROUTE_SPECS_TERMINAL_ORDERS

    if name == "ROUTE_SPECS_TERMINAL_ALL":
        from .api_terminal import ROUTE_SPECS_TERMINAL
        from .api_terminal_orders import ROUTE_SPECS_TERMINAL_ORDERS
        return list(ROUTE_SPECS_TERMINAL) + list(ROUTE_SPECS_TERMINAL_ORDERS)

    raise AttributeError(name)
