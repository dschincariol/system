from __future__ import annotations

import importlib
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _FakePriceStorage:
    def __init__(self) -> None:
        self.enabled = True
        self.calls: list[dict[str, object]] = []
        self.flushed = threading.Event()

    def write_batch(self, *, prices=(), quotes=(), raw=()):
        self.calls.append(
            {
                "prices": [dict(row) for row in (prices or ())],
                "quotes": [dict(row) for row in (quotes or ())],
                "raw": [dict(row) for row in (raw or ())],
            }
        )
        self.flushed.set()
        return {
            "ok": True,
            "prices": len(prices or ()),
            "quotes": len(quotes or ()),
            "raw": len(raw or ()),
            "enabled": True,
        }


class AsyncPriceWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "async_price_writer.db")
        os.environ["ASYNC_PRICE_WRITER_ENABLED"] = "1"
        (self.async_writer,) = _reload_modules("engine.runtime.async_writer")

    def tearDown(self) -> None:
        try:
            self.async_writer.shutdown_async_writer(timeout_s=2.0)
        except Exception:
            pass
        self.tmp.cleanup()

    def _config(self, **overrides):
        values = {
            "enabled": True,
            "queue_maxsize": 32,
            "batch_size": 8,
            "flush_interval_s": 0.05,
            "retry_attempts": 1,
            "retry_base_s": 0.01,
            "retry_max_s": 0.02,
            "enqueue_timeout_s": 0.01,
            "dead_letter_path": str(Path(self.tmp.name) / "dead_letter.jsonl"),
        }
        values.update(overrides)
        return self.async_writer.AsyncPriceWriterConfig(**values)

    def test_close_flushes_queued_rows_before_writer_exits(self) -> None:
        fake_storage = _FakePriceStorage()
        writer = self.async_writer.AsyncPriceWriter(config=self._config())

        with patch.object(self.async_writer, "get_price_storage", return_value=fake_storage):
            started = writer.start()
            self.assertTrue(bool(started.get("enabled")))
            queued = writer.enqueue(
                prices=(
                    {
                        "symbol": "AAPL",
                        "ts_ms": 1_700_000_000_000,
                        "price": 201.25,
                        "volume": 1500.0,
                        "source": "unit_test",
                    },
                ),
                source="unit_test",
            )
            self.assertTrue(bool(queued))
            snapshot = writer.close(timeout_s=2.0)

        self.assertTrue(fake_storage.flushed.wait(timeout=1.0))
        self.assertEqual(len(fake_storage.calls), 1)
        written_prices = list((fake_storage.calls[0] or {}).get("prices") or [])
        self.assertEqual(len(written_prices), 1)
        self.assertEqual(str(written_prices[0]["symbol"]), "AAPL")
        self.assertEqual(float(written_prices[0]["price"]), 201.25)
        self.assertFalse(bool(snapshot.get("thread_alive")))
        self.assertEqual(int(snapshot.get("flushed_rows") or 0), 1)
        self.assertIn("last_flush_latency_ms", snapshot)
        self.assertIn("last_db_write_duration_ms", snapshot)
        self.assertIn("dropped_rows", snapshot)
        self.assertIn("residual_dropped_rows", snapshot)

    def test_close_synchronously_drains_residual_queue_when_worker_not_running(self) -> None:
        fake_storage = _FakePriceStorage()
        writer = self.async_writer.AsyncPriceWriter(config=self._config())

        with patch.object(writer, "start", return_value={}):
            queued = writer.enqueue(
                prices=(
                    {
                        "symbol": "MSFT",
                        "ts_ms": 1_700_000_000_001,
                        "price": 301.25,
                        "source": "unit_test",
                    },
                ),
                source="unit_test",
            )
        self.assertTrue(bool(queued))

        with patch.object(self.async_writer, "get_price_storage", return_value=fake_storage):
            snapshot = writer.close(timeout_s=1.0)

        self.assertEqual(len(fake_storage.calls), 1)
        self.assertEqual(int(snapshot.get("shutdown_drained_rows") or 0), 1)
        self.assertEqual(int(snapshot.get("residual_dropped_rows") or 0), 0)
        self.assertEqual(int(snapshot.get("queue_depth") or 0), 0)

    def test_close_reports_residual_spooled_rows_after_hard_deadline(self) -> None:
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                dead_letter_path=str(Path(self.tmp.name) / "dead_letter_deadline.jsonl"),
                shutdown_drain_max_s=0.0,
            )
        )
        emitted_counters: list[tuple[str, int, dict[str, object]]] = []

        with patch.object(writer, "start", return_value={}):
            queued = writer.enqueue(
                prices=(
                    {
                        "symbol": "NVDA",
                        "ts_ms": 1_700_000_000_002,
                        "price": 901.25,
                        "source": "unit_test",
                    },
                ),
                source="unit_test",
            )
        self.assertTrue(bool(queued))

        with patch.object(
            self.async_writer,
            "emit_counter",
            side_effect=lambda metric, value=1, **kwargs: emitted_counters.append(
                (metric, int(value), dict(kwargs))
            ),
        ):
            snapshot = writer.close(timeout_s=0.0)

        self.assertEqual(int(snapshot.get("residual_spooled_rows") or 0), 1)
        self.assertEqual(int(snapshot.get("residual_dropped_rows") or 0), 0)
        self.assertEqual(int(snapshot.get("dropped_rows") or 0), 0)
        self.assertTrue(
            any(
                metric == "async_price_writer_residual_spooled_rows" and value == 1
                for metric, value, _ in emitted_counters
            )
        )

    def test_enqueue_reports_high_watermark_before_overflow(self) -> None:
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                queue_maxsize=4,
                batch_size=8,
                dead_letter_path=str(Path(self.tmp.name) / "dead_letter_watermark.jsonl"),
                high_watermark_ratio=0.50,
                shutdown_drain_max_s=0.0,
            )
        )

        with patch.object(writer, "start", return_value={}):
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "AAPL", "ts_ms": 1, "price": 1.0},),
                    source="unit_test",
                )
            )
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "MSFT", "ts_ms": 2, "price": 2.0},),
                    source="unit_test",
                )
            )

        snapshot = writer.get_snapshot()
        self.assertEqual(int(snapshot.get("queue_depth") or 0), 2)
        self.assertEqual(int(snapshot.get("high_watermark_depth") or 0), 2)
        self.assertTrue(bool(snapshot.get("backpressure_active")))
        self.assertGreaterEqual(int(snapshot.get("high_watermark_events") or 0), 1)
        self.assertEqual(int(snapshot.get("dropped_rows") or 0), 0)
        writer.close(timeout_s=0.0)


if __name__ == "__main__":
    unittest.main()
