"""Optional live smoke checks for public/keyless feed endpoints.

This tool is intentionally inert unless PUBLIC_FEED_LIVE_SMOKE=1 is set.
Normal CI and local validation should use mocked unit tests instead.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any


LIVE_FLAG = "PUBLIC_FEED_LIVE_SMOKE"
DEFAULT_SOURCES = (
    "stocktwits",
    "gdelt",
    "sec",
    "form4",
    "congressional_trades",
    "finra_short_volume",
    "finra_short_interest",
    "weather_alerts",
    "macro",
)


def _scrub(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _scrub(v) for k, v in value.items() if "credential" not in str(k).lower()}
    if isinstance(value, list):
        return [_scrub(item) for item in value]
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run explicitly gated live public-feed smoke checks.")
    parser.add_argument("--source", action="append", choices=DEFAULT_SOURCES, help="Source key to test; defaults to all public-feed checks.")
    args = parser.parse_args(argv)

    if str(os.environ.get(LIVE_FLAG, "0")).strip().lower() not in {"1", "true", "yes", "on"}:
        print(json.dumps({"ok": True, "skipped": True, "reason": f"set {LIVE_FLAG}=1 to contact public services"}, sort_keys=True))
        return 0

    from services.data_source_manager import get_manager

    manager = get_manager()
    sources = tuple(args.source or DEFAULT_SOURCES)
    results: dict[str, Any] = {}
    ok = True
    for source_key in sources:
        try:
            result = manager.test_connection(str(source_key), actor="public-feed-live-smoke")
        except Exception as exc:
            result = {"ok": False, "status": "fail", "classification": "exception", "message": type(exc).__name__}
        result = _scrub(result)
        results[str(source_key)] = result
        ok = bool(ok and bool(result.get("ok")))

    print(json.dumps({"ok": ok, "skipped": False, "sources": results}, sort_keys=True))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
