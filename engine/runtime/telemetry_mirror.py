"""Best-effort SQLite -> Timescale mirror for append-only telemetry tables."""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.data_source_log_store import sanitize_data_source_log_detail_json
from engine.runtime.logging import get_logger
from engine.runtime.observability import record_component_health
from engine.runtime.storage import connect_ro, get_timescale_client

LOG = get_logger("runtime.telemetry_mirror")
_MIRROR_LOCK = threading.Lock()
_MIRROR: "TelemetryMirror | None" = None

_TABLE_SELECT_SQL: dict[str, str] = {
    "runtime_metrics": """
        SELECT rowid AS sqlite_rowid, ts_ms, metric, value_num, value_text, tags_json
        FROM runtime_metrics
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ?
    """,
    "event_log": """
        SELECT rowid AS sqlite_rowid, ts_ms, event_type, event_source, event_version, entity_type, entity_id, correlation_id, payload_json
        FROM event_log
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ?
    """,
    "ingestion_pipeline_health": """
        SELECT rowid AS sqlite_rowid, ts_ms, pipeline, ok, latency_ms, raw_rows, event_rows, last_ingested_ts_ms, error, meta_json
        FROM ingestion_pipeline_health
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ?
    """,
    "price_provider_health": """
        SELECT rowid AS sqlite_rowid, ts_ms, provider, ok, latency_ms, n_symbols, error, last_success_ts_ms, error_count
        FROM price_provider_health
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ?
    """,
    "weather_provider_health": """
        SELECT rowid AS sqlite_rowid, ts_ms, provider, ok, latency_ms, error
        FROM weather_provider_health
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ?
    """,
    "data_source_logs": """
        SELECT rowid AS sqlite_rowid, ts_ms, source_key, level, event_type, message, detail_json
        FROM data_source_logs
        WHERE rowid > ?
        ORDER BY rowid ASC
        LIMIT ?
    """,
}

_TABLE_ENQUEUE_METHODS: dict[str, str] = {
    "runtime_metrics": "enqueue_runtime_metrics",
    "event_log": "enqueue_event_log",
    "ingestion_pipeline_health": "enqueue_ingestion_pipeline_health",
    "price_provider_health": "enqueue_price_provider_health",
    "weather_provider_health": "enqueue_weather_provider_health",
    "data_source_logs": "enqueue_data_source_logs",
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _warn_nonfatal(code: str, error: BaseException, **extra: Any) -> None:
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.runtime.telemetry_mirror",
        extra=dict(extra or {}) or None,
        persist=False,
    )


@dataclass(frozen=True)
class TelemetryMirrorConfig:
    enabled: bool
    poll_interval_s: float
    batch_size: int
    start_mode: str

    @classmethod
    def from_env(cls) -> "TelemetryMirrorConfig":
        return cls(
            enabled=_env_bool("TIMESCALE_TELEMETRY_MIRROR_ENABLED", default=False),
            poll_interval_s=max(0.1, _env_float("TIMESCALE_TELEMETRY_MIRROR_POLL_INTERVAL_S", 1.0)),
            batch_size=max(1, _env_int("TIMESCALE_TELEMETRY_MIRROR_BATCH_SIZE", 500)),
            start_mode=str(os.environ.get("TIMESCALE_TELEMETRY_MIRROR_START_MODE", "current") or "current").strip().lower(),
        )


class TelemetryMirror:
    def __init__(self, config: TelemetryMirrorConfig | None = None):
        self._config = config or TelemetryMirrorConfig.from_env()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._state_lock = threading.RLock()
        self._cursors: dict[str, int] = {name: 0 for name in _TABLE_SELECT_SQL}
        self._metrics: dict[str, Any] = {
            "poll_count": 0,
            "mirrored_batches": 0,
            "mirrored_rows": 0,
            "last_poll_ts_ms": 0,
            "last_mirror_ts_ms": 0,
            "last_error": "",
            "last_error_ts_ms": 0,
            "table_stats": {
                table_name: {"mirrored_rows": 0, "last_rowid": 0}
                for table_name in _TABLE_SELECT_SQL
            },
        }

    @property
    def enabled(self) -> bool:
        return bool(self._config.enabled)

    def start(self) -> dict[str, Any]:
        if not self.enabled:
            return self.get_snapshot()
        client = get_timescale_client()
        if client is None or not bool(getattr(client, "enabled", False)):
            with self._state_lock:
                self._metrics["last_error"] = "timescale_client_not_enabled_for_telemetry_mirror"
                self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
            return self.get_snapshot()
        with self._state_lock:
            if self._thread is not None and self._thread.is_alive():
                return self.get_snapshot()
            self._initialize_cursors()
            self._stop.clear()
            self._metrics["last_error"] = ""
            self._thread = threading.Thread(target=self._run, name="telemetry-mirror", daemon=True)
            self._thread.start()
        record_component_health(
            "telemetry_mirror",
            ok=True,
            status="ok",
            detail="mirror_started",
            extra={"enabled": bool(self.enabled)},
        )
        return self.get_snapshot()

    def close(self, timeout_s: float = 2.0) -> dict[str, Any]:
        self._stop.set()
        with self._state_lock:
            thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.1, float(timeout_s)))
        with self._state_lock:
            self._thread = None
        return self.get_snapshot()

    def get_snapshot(self) -> dict[str, Any]:
        with self._state_lock:
            thread_alive = bool(self._thread is not None and self._thread.is_alive())
            cursors = dict(self._cursors)
            metrics = {
                **dict(self._metrics),
                "table_stats": {
                    str(name): dict(stats or {})
                    for name, stats in dict(self._metrics.get("table_stats") or {}).items()
                },
            }
        last_error = str(metrics.get("last_error") or "").strip()
        degraded_reasons: list[str] = []
        if self.enabled and not thread_alive:
            degraded_reasons.append("mirror_stopped")
        if self.enabled and last_error:
            if last_error == "timescale_client_not_enabled_for_telemetry_mirror":
                degraded_reasons.append("timescale_client_unavailable")
            else:
                degraded_reasons.append("last_error")
        degraded = bool(degraded_reasons)
        detail = "mirror_disabled"
        if self.enabled:
            detail = "ok" if not degraded else (last_error or ",".join(degraded_reasons[:2]))
        return {
            "ok": (not self.enabled) or (thread_alive and not last_error),
            "enabled": bool(self.enabled),
            "started": bool(thread_alive),
            "degraded": bool(degraded),
            "degraded_reasons": degraded_reasons,
            "detail": str(detail),
            "thread_alive": bool(thread_alive),
            "poll_interval_s": float(self._config.poll_interval_s),
            "batch_size": int(self._config.batch_size),
            "start_mode": str(self._config.start_mode),
            "last_rowids": {str(name): int(value or 0) for name, value in cursors.items()},
            "metrics": metrics,
            "ts_ms": int(time.time() * 1000),
        }

    def _initialize_cursors(self) -> None:
        start_from_current = str(self._config.start_mode or "current") != "beginning"
        if not start_from_current:
            with self._state_lock:
                self._cursors = {name: 0 for name in _TABLE_SELECT_SQL}
            return
        con = connect_ro()
        try:
            cursors: dict[str, int] = {}
            for table_name in _TABLE_SELECT_SQL:
                if not self._sqlite_table_exists(con, table_name):
                    cursors[table_name] = 0
                    continue
                row = con.execute(f"SELECT COALESCE(MAX(rowid), 0) FROM {table_name}").fetchone()
                cursors[table_name] = int((row[0] if row is not None else 0) or 0)
            with self._state_lock:
                self._cursors = dict(cursors)
                table_stats = dict(self._metrics.get("table_stats") or {})
                for table_name, last_rowid in cursors.items():
                    stats = dict(table_stats.get(table_name) or {})
                    stats["last_rowid"] = int(last_rowid)
                    table_stats[table_name] = stats
                self._metrics["table_stats"] = table_stats
        finally:
            con.close()

    def _sqlite_table_exists(self, con: Any, table_name: str) -> bool:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
        return bool(row)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                mirrored_any = self._poll_once()
                if not mirrored_any:
                    self._stop.wait(timeout=float(self._config.poll_interval_s))
            except Exception as exc:
                with self._state_lock:
                    self._metrics["last_error"] = f"{type(exc).__name__}:{exc}"
                    self._metrics["last_error_ts_ms"] = int(time.time() * 1000)
                _warn_nonfatal("TELEMETRY_MIRROR_POLL_FAILED", exc)
                record_component_health(
                    "telemetry_mirror",
                    ok=False,
                    status="error",
                    detail=f"{type(exc).__name__}:{exc}",
                    extra={"enabled": bool(self.enabled)},
                )
                self._stop.wait(timeout=float(self._config.poll_interval_s))

    def _poll_once(self) -> bool:
        client = get_timescale_client()
        if client is None or not bool(getattr(client, "enabled", False)):
            raise RuntimeError("timescale_client_not_enabled_for_telemetry_mirror")
        con = connect_ro()
        now_ts_ms = int(time.time() * 1000)
        try:
            mirrored_any = False
            with self._state_lock:
                self._metrics["poll_count"] = int(self._metrics.get("poll_count") or 0) + 1
                self._metrics["last_poll_ts_ms"] = int(now_ts_ms)
            for table_name, sql in _TABLE_SELECT_SQL.items():
                if not self._sqlite_table_exists(con, table_name):
                    continue
                last_rowid = int(self._cursors.get(table_name) or 0)
                rows = con.execute(sql, (int(last_rowid), int(self._config.batch_size))).fetchall() or []
                if not rows:
                    continue
                payload = [dict(row) for row in rows]
                if table_name == "data_source_logs":
                    for item in payload:
                        item["detail_json"] = sanitize_data_source_log_detail_json(item.get("detail_json"))
                enqueue_name = _TABLE_ENQUEUE_METHODS[str(table_name)]
                enqueue_fn = getattr(client, enqueue_name, None)
                if not callable(enqueue_fn):
                    raise RuntimeError(f"timescale_client_missing_enqueue:{enqueue_name}")
                mirrored_rows = int(enqueue_fn(payload))
                if mirrored_rows <= 0:
                    continue
                next_rowid = max(int((row["sqlite_rowid"] if isinstance(row, dict) else row[0]) or 0) for row in payload)
                with self._state_lock:
                    self._cursors[table_name] = int(next_rowid)
                    self._metrics["mirrored_batches"] = int(self._metrics.get("mirrored_batches") or 0) + 1
                    self._metrics["mirrored_rows"] = int(self._metrics.get("mirrored_rows") or 0) + int(mirrored_rows)
                    self._metrics["last_mirror_ts_ms"] = int(now_ts_ms)
                    self._metrics["last_error"] = ""
                    table_stats = dict(self._metrics.get("table_stats") or {})
                    stats = dict(table_stats.get(table_name) or {})
                    stats["mirrored_rows"] = int(stats.get("mirrored_rows") or 0) + int(mirrored_rows)
                    stats["last_rowid"] = int(next_rowid)
                    table_stats[table_name] = stats
                    self._metrics["table_stats"] = table_stats
                mirrored_any = True
            with self._state_lock:
                self._metrics["last_error"] = ""
            record_component_health(
                "telemetry_mirror",
                ok=True,
                status="ok",
                detail=("mirrored_rows" if mirrored_any else "idle"),
                extra={"enabled": bool(self.enabled)},
            )
            return bool(mirrored_any)
        finally:
            con.close()


def get_telemetry_mirror() -> TelemetryMirror:
    global _MIRROR
    mirror = _MIRROR
    if mirror is not None:
        return mirror
    with _MIRROR_LOCK:
        mirror = _MIRROR
        if mirror is None:
            mirror = TelemetryMirror()
            _MIRROR = mirror
        return mirror


def init_telemetry_mirror() -> dict[str, Any]:
    return get_telemetry_mirror().start()


def shutdown_telemetry_mirror(timeout_s: float = 2.0) -> dict[str, Any]:
    global _MIRROR
    with _MIRROR_LOCK:
        mirror = _MIRROR
        _MIRROR = None
    if mirror is None:
        return {
            "ok": True,
            "enabled": False,
            "thread_alive": False,
            "detail": "telemetry_mirror_not_started",
            "ts_ms": int(time.time() * 1000),
        }
    snapshot = dict(mirror.close(timeout_s=timeout_s) or {})
    snapshot["detail"] = "telemetry_mirror_stopped"
    return snapshot


def get_telemetry_mirror_snapshot() -> dict[str, Any]:
    return get_telemetry_mirror().get_snapshot()


__all__ = [
    "TelemetryMirror",
    "TelemetryMirrorConfig",
    "get_telemetry_mirror",
    "get_telemetry_mirror_snapshot",
    "init_telemetry_mirror",
    "shutdown_telemetry_mirror",
]
