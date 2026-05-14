"""
FILE: http_parsing.py

HTTP request parsing and coercion helpers.
"""

# engine/api/http_parsing.py
# Shared HTTP helpers used by multiple API modules.
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, overload
from urllib.parse import parse_qs

from engine.runtime.failure_diagnostics import log_failure


LOG = logging.getLogger(__name__)


@overload
def qs(parsed: Any, key: None = None, default: Optional[str] = None) -> Dict[str, str]: ...


@overload
def qs(parsed: Any, key: str, default: Optional[str] = None) -> str: ...


def qs(parsed: Any, key: Optional[str] = None, default: Optional[str] = None) -> Dict[str, str] | str:
    """
    Parse querystring into a simple dict[str,str].
    Accepts:
      - parsed=None -> {}
      - parsed.query (string) -> parse
      - parsed as dict -> returns shallow stringified values
      - key/default -> convenience accessor for legacy call sites
    """
    out: Dict[str, str]
    if parsed is None:
        out = {}
        return out if key is None else str(out.get(str(key), default if default is not None else ""))

    if isinstance(parsed, dict):
        out = {}
        for k, v in parsed.items():
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                out[str(k)] = "" if not v else str(v[0])
            else:
                out[str(k)] = str(v)
        return out if key is None else str(out.get(str(key), default if default is not None else ""))

    q = getattr(parsed, "query", None)
    if not q:
        out = {}
        return out if key is None else str(out.get(str(key), default if default is not None else ""))

    raw = parse_qs(str(q), keep_blank_values=True)
    out2: Dict[str, str] = {}
    for k, v in raw.items():
        out2[str(k)] = "" if not v else str(v[0])
    return out2 if key is None else str(out2.get(str(key), default if default is not None else ""))


def deny_if_shutdown() -> Optional[Dict[str, Any]]:
    """
    Returns an error dict if lifecycle is in shutdown, else None.
    This is a transport-level guard so handlers can short-circuit mutating work
    once the runtime has entered shutdown.
    """
    try:
        from engine.runtime.lifecycle import lifecycle_snapshot
        snap = lifecycle_snapshot()
        if snap.get("state") == "SHUTDOWN":
            return {"ok": False, "error": "shutdown_in_progress"}
    except Exception as e:
        log_failure(
            LOG,
            event="http_parsing_deny_if_shutdown_failed",
            code="HTTP_PARSING_DENY_IF_SHUTDOWN_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.api.http_parsing",
            include_health=False,
            persist=True,
        )
    return None
