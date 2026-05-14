"""Postgres slow-query log parser and tailer."""

from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable, Iterator

from engine.runtime.logging import get_logger

LOG = get_logger("runtime.observability.slow_log")

MetricWriter = Callable[..., None]

SLOW_QUERY_RE = re.compile(
    r"duration:\s*(?P<duration_ms>[0-9]+(?:\.[0-9]+)?)\s*ms\s+"
    r"(?:(?:statement)|(?:execute\s+\S+)):\s*(?P<statement>.*)$",
    re.IGNORECASE,
)
_STRING_RE = re.compile(r"'(?:''|[^'])*'")
_NUMBER_RE = re.compile(r"\b[-+]?(?:\d+\.\d+|\d+)\b")
_WHITESPACE_RE = re.compile(r"\s+")


def _default_metric_writer() -> MetricWriter:
    from engine.runtime.metrics_store import write_runtime_metric

    return write_runtime_metric


def normalize_statement(statement: str) -> str:
    text = str(statement or "").strip()
    text = _STRING_RE.sub("?", text)
    text = _NUMBER_RE.sub("?", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    return text[:4096]


def _query_hash(normalized_statement: str) -> str:
    return hashlib.sha1(str(normalized_statement or "").encode("utf-8")).hexdigest()[:16]


def parse_slow_log_line(line: str) -> dict[str, Any] | None:
    match = SLOW_QUERY_RE.search(str(line or "").strip())
    if not match:
        return None
    duration_ms = float(match.group("duration_ms"))
    statement = str(match.group("statement") or "").strip()
    normalized = normalize_statement(statement)
    return {
        "duration_ms": float(duration_ms),
        "statement": statement,
        "normalized_statement": normalized,
        "query_hash": _query_hash(normalized),
    }


def emit_slow_query_event(
    event: dict[str, Any],
    *,
    metric_writer: MetricWriter | None = None,
    ts_ms: int | None = None,
) -> int:
    if not event:
        return 0
    writer = metric_writer or _default_metric_writer()
    emitted_ts_ms = int(ts_ms or time.time() * 1000)
    normalized = str(event.get("normalized_statement") or "")
    writer(
        "postgres.slow_query.duration_ms",
        value_num=float(event.get("duration_ms") or 0.0),
        value_text=normalized,
        tags={
            "query_hash": str(event.get("query_hash") or _query_hash(normalized)),
        },
        ts_ms=emitted_ts_ms,
    )
    return 1


def iter_new_lines(path: str | Path, *, start_at_end: bool = True, poll_s: float = 0.25) -> Iterator[str]:
    log_path = Path(path)
    with log_path.open("r", encoding="utf-8", errors="replace") as handle:
        if start_at_end:
            handle.seek(0, os.SEEK_END)
        while True:
            line = handle.readline()
            if line:
                yield line
                continue
            time.sleep(max(0.05, float(poll_s)))


def tail_slow_log(
    path: str | Path,
    *,
    metric_writer: MetricWriter | None = None,
    stop_event: threading.Event | None = None,
    start_at_end: bool = True,
    poll_s: float = 0.25,
) -> dict[str, Any]:
    writer = metric_writer or _default_metric_writer()
    stop = stop_event or threading.Event()
    emitted = 0
    parsed = 0
    for line in iter_new_lines(path, start_at_end=start_at_end, poll_s=poll_s):
        if stop.is_set():
            break
        event = parse_slow_log_line(line)
        if event is None:
            continue
        parsed += 1
        emitted += emit_slow_query_event(event, metric_writer=writer)
    return {"ok": True, "parsed": int(parsed), "emitted": int(emitted)}


def start_slow_log_tail_thread(
    path: str | Path,
    *,
    metric_writer: MetricWriter | None = None,
    stop_event: threading.Event | None = None,
    start_at_end: bool = True,
) -> threading.Thread:
    thread = threading.Thread(
        target=tail_slow_log,
        kwargs={
            "path": path,
            "metric_writer": metric_writer,
            "stop_event": stop_event,
            "start_at_end": start_at_end,
        },
        name="postgres-slow-log-tail",
        daemon=True,
    )
    thread.start()
    return thread


__all__ = [
    "emit_slow_query_event",
    "normalize_statement",
    "parse_slow_log_line",
    "start_slow_log_tail_thread",
    "tail_slow_log",
]
