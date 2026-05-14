from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.observability.slow_log import emit_slow_query_event, parse_slow_log_line


def test_slow_log_line_parses_and_normalizes_statement():
    event = parse_slow_log_line(
        "2026-05-02 12:00:00 UTC LOG: duration: 251.123 ms statement: "
        "SELECT * FROM prices WHERE symbol = 'AAPL' AND ts_ms > 123456"
    )

    assert event is not None
    assert event["duration_ms"] == 251.123
    assert event["normalized_statement"] == "SELECT * FROM prices WHERE symbol = ? AND ts_ms > ?"
    assert len(event["query_hash"]) == 16


def test_slow_log_event_emits_runtime_metric_row():
    captured = []

    def writer(metric, value_num=None, value_text=None, tags=None, ts_ms=None):
        captured.append(
            {
                "metric": metric,
                "value_num": value_num,
                "value_text": value_text,
                "tags": dict(tags or {}),
                "ts_ms": ts_ms,
            }
        )

    event = parse_slow_log_line(
        "duration: 300.5 ms statement: UPDATE runtime_metrics SET value_num = 1 WHERE id = 99"
    )
    assert emit_slow_query_event(event, metric_writer=writer, ts_ms=42) == 1

    assert captured == [
        {
            "metric": "postgres.slow_query.duration_ms",
            "value_num": 300.5,
            "value_text": "UPDATE runtime_metrics SET value_num = ? WHERE id = ?",
            "tags": {"query_hash": event["query_hash"]},
            "ts_ms": 42,
        }
    ]
