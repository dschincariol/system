"""Authoritative control-plane server entrypoint.

`dashboard_server.py` remains the compatibility surface for route contracts and
legacy imports, but server startup ownership lives here.
"""

from __future__ import annotations

import importlib


def _dashboard_module(dashboard_module=None):
    if dashboard_module is not None:
        return dashboard_module
    return importlib.import_module("dashboard_server")


def run_server(*, dashboard_module=None):
    module = _dashboard_module(dashboard_module=dashboard_module)
    runner = getattr(module, "_run_dashboard_control_plane", None)
    if not callable(runner):
        raise RuntimeError("dashboard_control_plane_runner_unavailable")
    return runner()


def main():
    return run_server()


if __name__ == "__main__":
    main()
