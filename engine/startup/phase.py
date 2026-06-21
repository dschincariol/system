"""Startup phase trace helpers."""

import time
import traceback
from typing import Any, MutableMapping, Optional


def record_phase(
    trace: MutableMapping[str, Any],
    phase: str,
    *,
    status: str = "started",
    detail: str = "",
    extra: Optional[dict] = None,
    now_ms: Optional[int] = None,
) -> None:
    """Append a phase event to the startup trace mapping."""
    ts_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)
    entry = {
        "phase": str(phase),
        "status": str(status),
        "detail": str(detail or ""),
        "ts_ms": ts_ms,
    }
    if isinstance(extra, dict) and extra:
        entry["extra"] = dict(extra)
    trace["phase"] = str(phase)
    trace.setdefault("phases", []).append(entry)


def record_first_failure(
    trace: MutableMapping[str, Any],
    phase: str,
    exc: BaseException,
    *,
    file_path: str = "",
    line_no: Optional[int] = None,
    module: str = "",
    now_ms: Optional[int] = None,
) -> None:
    """Record the first startup failure details if none has been recorded."""
    if trace.get("first_failure"):
        return
    tb = traceback.extract_tb(exc.__traceback__) if getattr(exc, "__traceback__", None) else []
    leaf = tb[-1] if tb else None
    trace["first_failure"] = {
        "phase": str(phase),
        "type": type(exc).__name__,
        "error": str(exc),
        "module": str(module or (leaf.name if leaf else "")),
        "file": str(file_path or (leaf.filename if leaf else "")),
        "line": int(line_no or (leaf.lineno if leaf else 0) or 0),
        "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-12000:],
        "ts_ms": int(time.time() * 1000) if now_ms is None else int(now_ms),
    }
