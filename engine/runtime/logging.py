from __future__ import annotations
"""
Central structured logging for trading engine.
Safe for import anywhere.
"""

"""
FILE: logging.py

Runtime subsystem module for `logging`.
"""

import json
import logging
import os
import socket
import sys
import threading
import time
import traceback
from logging import FileHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

LOG_LEVEL = os.environ.get("ENGINE_LOG_LEVEL", os.environ.get("LOG_LEVEL", "INFO")).upper().strip()
LOG_JSON = os.environ.get("ENGINE_LOG_JSON", "1") == "1"
LOG_FILE = os.environ.get(
    "ENGINE_LOG_FILE",
    str(
        (
            Path(
                os.environ.get("TRADING_LOGS")
                or os.environ.get("LOG_DIR")
                or str((Path(__file__).resolve().parents[2] / "logs").resolve())
            )
            / "engine.log"
        ).resolve()
    ),
)
LOG_MAX_BYTES = int(os.environ.get("ENGINE_LOG_MAX_BYTES", str(25 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.environ.get("ENGINE_LOG_BACKUP_COUNT", "5"))
LOG_ENABLE_ROTATION = os.environ.get(
    "ENGINE_LOG_ENABLE_ROTATION",
    "0" if os.name == "nt" else "1",
) == "1"
LOG_INCLUDE_PROCESS = os.environ.get("ENGINE_LOG_INCLUDE_PROCESS", "1") == "1"
LOG_INCLUDE_THREAD = os.environ.get("ENGINE_LOG_INCLUDE_THREAD", "1") == "1"
LOG_HOSTNAME = socket.gethostname()
_SERVICE_NAME = os.environ.get("ENGINE_SERVICE_NAME", "trading-engine").strip() or "trading-engine"
_TRACE_ID_LOCAL = threading.local()
_WARNED_NONFATAL_KEYS: set[str] = set()

_root_logger = logging.getLogger("engine")
_root_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
_root_logger.propagate = False


def _stderr_nonfatal(event: str, error: BaseException, *, warn_key: str | None = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    payload = {
        "event": str(event),
        "component": "engine.runtime.logging",
        "error_type": type(error).__name__,
        "error_message": str(error),
        "extra": dict(extra or {}),
        "ts_ms": int(time.time() * 1000),
    }
    try:
        sys.stderr.write(json.dumps(payload, separators=(",", ":"), sort_keys=True, default=_json_default) + "\n")
        sys.stderr.flush()
    except Exception as stderr_error:
        stderr = getattr(sys, "__stderr__", None)
        if stderr is not None:
            try:
                stderr.write(
                    f"[engine.logging] stderr_nonfatal_write_failed event={event!r}: "
                    f"{type(stderr_error).__name__}: {stderr_error}\n"
                )
                stderr.flush()
            except Exception:
                _stderr_fallback_failed = True
        return
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


class SafeRotatingFileHandler(RotatingFileHandler):
    """
    Windows-safe rollover wrapper.

    Multiple engine processes may share the same log path. On Windows, rename
    during rollover can fail with WinError 32 if another process still has the
    file open. In that case, skip rollover for this emit instead of surfacing
    noisy logging tracebacks into child stderr.
    """

    def doRollover(self) -> None:
        try:
            super().doRollover()
        except PermissionError as e:
            try:
                if self.stream:
                    self.stream.close()
            except Exception as close_error:
                _stderr_nonfatal(
                    "engine_logging_rollover_stream_close_failed",
                    close_error,
                    warn_key="engine_logging_rollover_stream_close_failed",
                    path=self.baseFilename,
                )
            try:
                self.stream = self._open()
            except Exception as reopen_error:
                _stderr_nonfatal(
                    "engine_logging_rollover_stream_reopen_failed",
                    reopen_error,
                    warn_key="engine_logging_rollover_stream_reopen_failed",
                    path=self.baseFilename,
                    original_error=f"{type(e).__name__}: {e}",
                )
            try:
                sys.stderr.write(
                    f"[engine.logging] rollover skipped path={self.baseFilename!r}: {type(e).__name__}: {e}\n"
                )
                sys.stderr.flush()
            except Exception as stderr_error:
                _stderr_nonfatal(
                    "engine_logging_rollover_stderr_write_failed",
                    stderr_error,
                    warn_key="engine_logging_rollover_stderr_write_failed",
                    path=self.baseFilename,
                    original_error=f"{type(e).__name__}: {e}",
                )

    def emit(self, record: logging.LogRecord) -> None:
        try:
            super().emit(record)
        except PermissionError as e:
            try:
                sys.stderr.write(
                    f"[engine.logging] emit skipped path={self.baseFilename!r}: {type(e).__name__}: {e}\n"
                )
                sys.stderr.flush()
            except Exception as stderr_error:
                _stderr_nonfatal(
                    "engine_logging_emit_stderr_write_failed",
                    stderr_error,
                    warn_key="engine_logging_emit_stderr_write_failed",
                    path=self.baseFilename,
                    original_error=f"{type(e).__name__}: {e}",
                )

    def handleError(self, record: logging.LogRecord) -> None:
        exc_type, exc, _tb = sys.exc_info()
        if isinstance(exc, PermissionError):
            try:
                sys.stderr.write(
                    f"[engine.logging] handler error suppressed path={self.baseFilename!r}: "
                    f"{type(exc).__name__}: {exc}\n"
                )
                sys.stderr.flush()
            except Exception as stderr_error:
                _stderr_nonfatal(
                    "engine_logging_handle_error_stderr_write_failed",
                    stderr_error,
                    warn_key="engine_logging_handle_error_stderr_write_failed",
                    path=self.baseFilename,
                    original_error=f"{type(exc).__name__}: {exc}",
                )
            return
        super().handleError(record)


def _json_default(value: Any) -> str:
    try:
        return str(value)
    except Exception as e:
        _stderr_nonfatal(
            "engine_logging_json_default_failed",
            e,
            warn_key="engine_logging_json_default_failed",
            value_type=type(value).__name__,
        )
        return repr(value)


def bind_log_context(*, trace_id: Optional[str] = None, span_id: Optional[str] = None) -> None:
    if trace_id is None:
        try:
            delattr(_TRACE_ID_LOCAL, "trace_id")
        except Exception as e:
            _stderr_nonfatal(
                "engine_logging_clear_trace_id_failed",
                e,
                warn_key="engine_logging_clear_trace_id_failed",
            )
    else:
        _TRACE_ID_LOCAL.trace_id = str(trace_id)

    if span_id is None:
        try:
            delattr(_TRACE_ID_LOCAL, "span_id")
        except Exception as e:
            _stderr_nonfatal(
                "engine_logging_clear_span_id_failed",
                e,
                warn_key="engine_logging_clear_span_id_failed",
            )
    else:
        _TRACE_ID_LOCAL.span_id = str(span_id)


def get_bound_log_context() -> Dict[str, str]:
    out: Dict[str, str] = {}
    trace_id = getattr(_TRACE_ID_LOCAL, "trace_id", None)
    span_id = getattr(_TRACE_ID_LOCAL, "span_id", None)
    if trace_id:
        out["trace_id"] = str(trace_id)
    if span_id:
        out["span_id"] = str(span_id)
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Emit one normalized JSON payload shape for all engine logs so search
        # and event correlation stay consistent across modules.
        payload: Dict[str, Any] = {
            "timestamp": int(time.time() * 1000),
            "service": _SERVICE_NAME,
            "logger": record.name,
            "level": record.levelname,
            "event": getattr(record, "event", None) or record.getMessage(),
            "message": record.getMessage(),
            "component": getattr(record, "component", None) or record.module,
            "module": record.module,
            "pathname": record.pathname,
            "lineno": int(record.lineno),
            "hostname": LOG_HOSTNAME,
        }

        if LOG_INCLUDE_PROCESS:
            if record.process is not None:
                payload["pid"] = int(record.process)
            payload["process_name"] = str(record.processName)

        if LOG_INCLUDE_THREAD:
            if record.thread is not None:
                payload["thread"] = int(record.thread)
            payload["thread_name"] = str(record.threadName)

        for key in (
            "job",
            "symbol",
            "strategy",
            "latency_ms",
            "trace_id",
            "span_id",
            "provider",
            "broker",
        ):
            value = getattr(record, key, None)
            if value is not None:
                payload[key] = value

        bound = get_bound_log_context()
        for key, value in bound.items():
            payload.setdefault(key, value)

        extra = getattr(record, "extra_json", None)
        if isinstance(extra, dict) and extra:
            payload.update(extra)

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=_json_default)


if LOG_JSON:
    _formatter: logging.Formatter = JsonFormatter()
else:
    base = "%(asctime)s | %(levelname)s | %(name)s | %(module)s:%(lineno)d"
    if LOG_INCLUDE_PROCESS:
        base += " | pid=%(process)d"
    if LOG_INCLUDE_THREAD:
        base += " | tid=%(thread)d"
    base += " | %(message)s"
    _formatter = logging.Formatter(base)

_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(_formatter)

if not _root_logger.handlers:
    _root_logger.addHandler(_stream_handler)
    try:
        Path(LOG_FILE).parent.mkdir(parents=True, exist_ok=True)
        if LOG_ENABLE_ROTATION:
            _file_handler = SafeRotatingFileHandler(
                LOG_FILE,
                maxBytes=LOG_MAX_BYTES,
                backupCount=LOG_BACKUP_COUNT,
                encoding="utf-8",
            )
        else:
            # On Windows multiple runtime processes share engine.log; append-only
            # writes are stable, while in-process rollover is not.
            _file_handler = FileHandler(LOG_FILE, encoding="utf-8")
        _file_handler.setFormatter(_formatter)
        _root_logger.addHandler(_file_handler)
    except Exception as e:
        try:
            sys.stderr.write(
                f"[engine.logging] failed to initialize file handler path={LOG_FILE!r}: {type(e).__name__}: {e}\n"
            )
            sys.stderr.flush()
        except Exception as stderr_error:
            _stderr_nonfatal(
                "engine_logging_file_handler_init_stderr_write_failed",
                stderr_error,
                warn_key="engine_logging_file_handler_init_stderr_write_failed",
                path=LOG_FILE,
                original_error=f"{type(e).__name__}: {e}",
            )


def get_logger(name: str | None = None) -> logging.Logger:
    if not name:
        return _root_logger
    if name == "engine":
        return _root_logger
    if name.startswith("engine."):
        return logging.getLogger(name)
    return _root_logger.getChild(name)


def log_event(
    logger: logging.Logger,
    level: int,
    event: str,
    *,
    component: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    # This helper standardizes structured logging so callers only provide the
    # event name and payload instead of hand-building logging `extra`.
    payload = dict(extra or {})
    logger.log(
        level,
        str(event),
        extra={
            "event": str(event),
            "component": (str(component) if component else None),
            "extra_json": payload,
            **get_bound_log_context(),
        },
    )


def flush_logging_handlers() -> None:
    # Used on fatal and shutdown paths to minimize the chance of losing the
    # final structured events during abrupt process exit.
    seen = set()
    for logger_name in ("engine",):
        logger = logging.getLogger(logger_name)
        for handler in list(getattr(logger, "handlers", []) or []):
            ident = id(handler)
            if ident in seen:
                continue
            seen.add(ident)
            try:
                handler.flush()
            except Exception as e:
                try:
                    sys.stderr.write(
                        f"[engine.logging] handler flush failed logger={logger_name!r} handler={type(handler).__name__}: {type(e).__name__}: {e}\n"
                    )
                    sys.stderr.flush()
                except Exception as stderr_error:
                    _stderr_nonfatal(
                        "engine_logging_handler_flush_stderr_write_failed",
                        stderr_error,
                        warn_key="engine_logging_handler_flush_stderr_write_failed",
                        logger=logger_name,
                        handler=type(handler).__name__,
                        original_error=f"{type(e).__name__}: {e}",
                    )
