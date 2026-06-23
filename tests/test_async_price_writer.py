from __future__ import annotations

import importlib
import json
import os
import sqlite3
import sys
import tempfile
import threading
import time
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
        self._lock = threading.Lock()

    def write_batch(self, *, prices=(), quotes=(), raw=()):
        with self._lock:
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


class _FailingPriceStorage:
    enabled = True

    def __init__(self) -> None:
        self.calls = 0

    def write_batch(self, *, prices=(), quotes=(), raw=()):
        self.calls += 1
        raise RuntimeError("unit_test_write_failed")


class _CircuitOpenPriceStorage(_FailingPriceStorage):
    def write_batch(self, *, prices=(), quotes=(), raw=()):
        self.calls += 1
        raise RuntimeError("storage_pg_prices_write_batch_circuit_open:unit_test")


class _BlockingPriceStorage(_FakePriceStorage):
    def __init__(self, *, block_price: float, block_timeout_s: float | None = 2.0) -> None:
        super().__init__()
        self.block_price = float(block_price)
        self.block_timeout_s = None if block_timeout_s is None else float(block_timeout_s)
        self.entered = threading.Event()
        self.release = threading.Event()

    def write_batch(self, *, prices=(), quotes=(), raw=()):
        price_values = [float(row.get("price")) for row in (prices or ()) if row.get("price") is not None]
        if self.block_price in price_values:
            self.entered.set()
            if self.block_timeout_s is None:
                self.release.wait()
            else:
                self.release.wait(timeout=float(self.block_timeout_s))
        return super().write_batch(prices=prices, quotes=quotes, raw=raw)


class _ConcurrentPriceStorage(_FakePriceStorage):
    def __init__(self, *, target_entries: int = 2) -> None:
        super().__init__()
        self.target_entries = int(target_entries)
        self.entered = threading.Event()
        self.release = threading.Event()
        self.active = 0
        self.max_active = 0
        self.thread_names: list[str] = []

    def write_batch(self, *, prices=(), quotes=(), raw=()):
        with self._lock:
            self.active += 1
            self.max_active = max(int(self.max_active), int(self.active))
            self.thread_names.append(str(threading.current_thread().name))
            if int(self.max_active) >= int(self.target_entries):
                self.entered.set()
        self.release.wait(timeout=2.0)
        try:
            return super().write_batch(prices=prices, quotes=quotes, raw=raw)
        finally:
            with self._lock:
                self.active = max(0, int(self.active) - 1)


class _SnapshotPriceStorage(_FakePriceStorage):
    def __init__(self, *, pool_max_size: int, enabled: bool = True) -> None:
        super().__init__()
        self.enabled = bool(enabled)
        self.pool_max_size = int(pool_max_size)

    def get_snapshot(self):
        return {
            "enabled": bool(self.enabled),
            "pool_max_size": int(self.pool_max_size),
        }


def _spool_count(path: Path) -> int:
    with sqlite3.connect(str(path)) as con:
        row = con.execute("SELECT COUNT(*) FROM async_price_writer_spool").fetchone()
    return int(row[0] or 0)


def _insert_legacy_spool_row(path: Path, *, payload: dict[str, object], created_ts_ms: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload_json = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(str(path)) as con:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS async_price_writer_spool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                created_ts_ms INTEGER NOT NULL,
                price_rows INTEGER NOT NULL DEFAULT 0,
                quote_rows INTEGER NOT NULL DEFAULT 0,
                raw_rows INTEGER NOT NULL DEFAULT 0,
                total_rows INTEGER NOT NULL,
                payload_bytes INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        con.execute(
            """
            INSERT INTO async_price_writer_spool(
                source, created_ts_ms, price_rows, quote_rows, raw_rows,
                total_rows, payload_bytes, payload_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(payload.get("source") or "legacy"),
                int(created_ts_ms),
                len(list(payload.get("prices") or [])),
                len(list(payload.get("quotes") or [])),
                len(list(payload.get("raw") or [])),
                len(list(payload.get("prices") or []))
                + len(list(payload.get("quotes") or []))
                + len(list(payload.get("raw") or [])),
                len(payload_json.encode("utf-8")),
                payload_json,
            ),
        )
        con.commit()


class AsyncPriceWriterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self._env_backup = {
            name: os.environ.get(name)
            for name in (
                "ASYNC_PRICE_WRITER_ENABLED",
                "DB_PATH",
                "TS_STORAGE_BACKEND",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "async_price_writer.db")
        os.environ["ASYNC_PRICE_WRITER_ENABLED"] = "1"
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        (self.async_writer,) = _reload_modules("engine.runtime.async_writer")

    def tearDown(self) -> None:
        try:
            self.async_writer.shutdown_async_writer(timeout_s=2.0)
        except Exception:
            pass
        for name, value in self._env_backup.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
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
        self.assertEqual(int(snapshot.get("worker_count") or 0), 4)
        self.assertEqual(len(list(snapshot.get("shards") or [])), 4)

    def test_default_spool_uses_normal_wal_durability(self) -> None:
        writer = self.async_writer.AsyncPriceWriter(config=self._config(shutdown_drain_max_s=0.0))

        snapshot = writer.get_snapshot()
        writer.close(timeout_s=0.0)

        self.assertEqual(str(snapshot.get("spool_synchronous")), "NORMAL")
        self.assertEqual(int(snapshot.get("residual_loss_rows") or 0), 0)

    def test_stricter_full_spool_durability_is_explicit(self) -> None:
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_synchronous="FULL",
                shutdown_drain_max_s=0.0,
            )
        )

        snapshot = writer.get_snapshot()
        writer.close(timeout_s=0.0)

        self.assertEqual(str(snapshot.get("spool_synchronous")), "FULL")

    def test_enqueue_reports_spooled_and_queued_rows(self) -> None:
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(Path(self.tmp.name) / "queued_rows_spool.sqlite"),
                shutdown_drain_max_s=0.0,
            )
        )

        with patch.object(writer, "start", return_value={}):
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "AAPL", "ts_ms": 1, "price": 1.0},),
                    quotes=({"symbol": "AAPL", "ts_ms": 1, "bid": 0.9, "ask": 1.1},),
                    source="unit_test",
                )
            )

        snapshot = writer.get_snapshot()
        self.assertEqual(int(snapshot.get("enqueued_rows") or 0), 2)
        self.assertEqual(int(snapshot.get("spooled_rows") or 0), 2)
        self.assertEqual(int(snapshot.get("queue_rows") or 0), 2)
        self.assertEqual(int(snapshot.get("spool_pending_rows") or 0), 2)
        self.assertEqual(int(snapshot.get("dropped_rows") or 0), 0)
        writer.close(timeout_s=0.0)

    def test_enqueue_copies_once_at_spool_boundary_for_caller_mutation_safety(self) -> None:
        spool_path = Path(self.tmp.name) / "mutation_boundary_spool.sqlite"
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(spool_path),
                shutdown_drain_max_s=0.0,
            )
        )
        row = {"symbol": "AAPL", "ts_ms": 1, "price": 1.0}

        with patch.object(writer, "start", return_value={}):
            self.assertTrue(writer.enqueue(prices=(row,), source="unit_test"))
        row["symbol"] = "MSFT"
        row["price"] = 999.0

        with sqlite3.connect(str(spool_path)) as con:
            payload_json = con.execute(
                "SELECT payload_json FROM async_price_writer_spool LIMIT 1"
            ).fetchone()[0]
        payload = json.loads(str(payload_json))
        persisted = dict(payload["prices"][0])
        snapshot = writer.get_snapshot()

        self.assertEqual(persisted["symbol"], "AAPL")
        self.assertEqual(float(persisted["price"]), 1.0)
        self.assertEqual(int(snapshot.get("row_copy_avoided_rows") or 0), 1)
        self.assertEqual(int(snapshot.get("row_copy_fallback_rows") or 0), 0)
        writer.close(timeout_s=0.0)

    def test_enqueue_splits_mixed_symbol_batch_into_deterministic_shards(self) -> None:
        spool_path = Path(self.tmp.name) / "sharded_spool.sqlite"
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                worker_count=4,
                spool_path=str(spool_path),
                shutdown_drain_max_s=0.0,
            )
        )

        expected_aapl_shard = writer._shard_for_row({"symbol": "AAPL"})
        other_symbol = next(
            symbol
            for symbol in ("MSFT", "NVDA", "GOOG", "TSLA", "SPY", "QQQ")
            if writer._shard_for_row({"symbol": symbol}) != expected_aapl_shard
        )
        rows = (
            {"symbol": "AAPL", "ts_ms": 1, "price": 101.0},
            {"symbol": other_symbol, "ts_ms": 1, "price": 201.0},
            {"symbol": "aapl", "ts_ms": 2, "price": 102.0},
        )
        with patch.object(writer, "start", return_value={}):
            self.assertTrue(writer.enqueue(prices=rows, source="unit_test"))

        with sqlite3.connect(str(spool_path)) as con:
            persisted = con.execute(
                "SELECT shard_id, payload_json FROM async_price_writer_spool ORDER BY id"
            ).fetchall()

        self.assertGreaterEqual(len(persisted), 2)
        aapl_prices: list[float] = []
        for shard_id, payload_json in persisted:
            payload = json.loads(str(payload_json))
            for row in payload.get("prices") or []:
                self.assertEqual(int(shard_id), writer._shard_for_row(dict(row)))
                if str(row.get("symbol") or "").upper() == "AAPL":
                    self.assertEqual(int(shard_id), expected_aapl_shard)
                    aapl_prices.append(float(row.get("price")))
        self.assertEqual(aapl_prices, [101.0, 102.0])

        snapshot = writer.get_snapshot()
        shards_with_rows = [
            item
            for item in list(snapshot.get("shards") or [])
            if int(item.get("spool_pending_batches") or 0) > 0
        ]
        self.assertGreaterEqual(len(shards_with_rows), 2)
        for shard in list(snapshot.get("shards") or []):
            self.assertEqual(int(shard.get("batch_size") or 0), 8)
            self.assertIn("pending_lag_ms", shard)
            self.assertIn("last_batch_rows", shard)
            self.assertIn("write_failures", shard)
        writer.close(timeout_s=0.0)

    def test_worker_pool_preserves_same_symbol_flush_order(self) -> None:
        fake_storage = _FakePriceStorage()
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                worker_count=4,
                batch_size=1,
                flush_interval_s=0.01,
            )
        )

        with patch.object(self.async_writer, "get_price_storage", return_value=fake_storage):
            writer.start()
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "AAPL", "ts_ms": 1_700_000_000_020, "price": 101.0},),
                    source="unit_test",
                )
            )
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "AAPL", "ts_ms": 1_700_000_000_020, "price": 102.0},),
                    source="unit_test",
                )
            )
            snapshot = writer.close(timeout_s=2.0)

        aapl_prices = [
            float(row["price"])
            for call in fake_storage.calls
            for row in list(call.get("prices") or [])
            if str(row.get("symbol") or "") == "AAPL"
        ]
        self.assertEqual(aapl_prices, [101.0, 102.0])
        self.assertEqual(int(snapshot.get("spool_pending_batches") or 0), 0)
        self.assertEqual(int(snapshot.get("worker_count") or 0), 4)

    def test_worker_pool_flushes_different_shards_concurrently(self) -> None:
        storage = _ConcurrentPriceStorage(target_entries=2)
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                worker_count=4,
                batch_size=1,
                flush_interval_s=0.01,
            )
        )
        first_symbol = "AAPL"
        first_shard = writer._shard_for_row({"symbol": first_symbol})
        second_symbol = next(
            symbol
            for symbol in ("MSFT", "NVDA", "GOOG", "TSLA", "SPY", "QQQ")
            if writer._shard_for_row({"symbol": symbol}) != first_shard
        )

        try:
            with patch.object(self.async_writer, "get_price_storage", return_value=storage):
                writer.start()
                self.assertTrue(
                    writer.enqueue(
                        prices=({"symbol": first_symbol, "ts_ms": 1_700_000_000_040, "price": 101.0},),
                        source="unit_test",
                    )
                )
                self.assertTrue(
                    writer.enqueue(
                        prices=({"symbol": second_symbol, "ts_ms": 1_700_000_000_041, "price": 201.0},),
                        source="unit_test",
                    )
                )
                self.assertTrue(storage.entered.wait(timeout=1.0))
                in_flight_snapshot = writer.get_snapshot()
                storage.release.set()
                snapshot = writer.close(timeout_s=2.0)
        finally:
            storage.release.set()

        self.assertGreaterEqual(int(storage.max_active), 2)
        self.assertGreaterEqual(len(set(storage.thread_names)), 2)
        self.assertEqual(int(snapshot.get("spool_pending_batches") or 0), 0)
        active_shards = [
            item
            for item in list(in_flight_snapshot.get("shards") or [])
            if int(item.get("inflight_rows") or 0) > 0
        ]
        self.assertGreaterEqual(len(active_shards), 2)
        for shard in active_shards:
            self.assertEqual(int(shard.get("last_batch_rows") or 0), 1)
            self.assertEqual(int(shard.get("last_batch_envelopes") or 0), 1)
            self.assertEqual(int(shard.get("write_failures") or 0), 0)

    def test_start_rejects_worker_count_above_price_pool_capacity(self) -> None:
        storage = _SnapshotPriceStorage(pool_max_size=1, enabled=True)
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                worker_count=2,
                spool_path=str(Path(self.tmp.name) / "pool_capacity_spool.sqlite"),
            )
        )

        with patch.object(self.async_writer, "get_price_storage", return_value=storage):
            with self.assertRaisesRegex(RuntimeError, "async_price_writer_pool_too_small"):
                writer.start()

        snapshot = writer.get_snapshot()
        self.assertEqual(int(snapshot.get("worker_alive_count") or 0), 0)
        writer.close(timeout_s=0.0)

    def test_legacy_unsharded_spool_blocks_new_shards_until_drained(self) -> None:
        spool_path = Path(self.tmp.name) / "legacy_spool.sqlite"
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                worker_count=4,
                batch_size=1,
                flush_interval_s=0.01,
                spool_path=str(spool_path),
            )
        )
        symbol = next(
            f"SYM{idx}"
            for idx in range(100)
            if writer._shard_for_row({"symbol": f"SYM{idx}"}) != 0
        )
        _insert_legacy_spool_row(
            spool_path,
            payload={
                "source": "legacy",
                "created_ts_ms": 1_700_000_000_030,
                "prices": [{"symbol": symbol, "ts_ms": 1_700_000_000_030, "price": 101.0}],
                "quotes": [],
                "raw": [],
            },
            created_ts_ms=1_700_000_000_030,
        )
        fake_storage = _BlockingPriceStorage(block_price=101.0, block_timeout_s=None)

        try:
            with patch.object(self.async_writer, "get_price_storage", return_value=fake_storage):
                writer.start()
                self.assertTrue(fake_storage.entered.wait(timeout=5.0))
                self.assertTrue(
                    writer.enqueue(
                        prices=({"symbol": symbol, "ts_ms": 1_700_000_000_030, "price": 102.0},),
                        source="unit_test",
                    )
                )
                time.sleep(0.05)
                self.assertEqual(len(fake_storage.calls), 0)
                blocked_snapshot = writer.get_snapshot()
                self.assertEqual(int(blocked_snapshot.get("legacy_unsharded_spool_pending_batches") or 0), 1)
                fake_storage.release.set()
                snapshot = writer.close(timeout_s=2.0)
        finally:
            fake_storage.release.set()

        prices = [
            float(row["price"])
            for call in fake_storage.calls
            for row in list(call.get("prices") or [])
            if str(row.get("symbol") or "") == symbol
        ]
        self.assertEqual(prices, [101.0, 102.0])
        self.assertEqual(int(snapshot.get("legacy_unsharded_spool_pending_batches") or 0), 0)

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
        self.assertGreaterEqual(int(snapshot.get("spool_oldest_age_ms") or 0), 0)
        self.assertEqual(int(snapshot.get("dropped_rows") or 0), 0)
        writer._spool.delete([1, 2])
        writer._clear_backpressure_if_recovered(0, pending_bytes=0)
        recovered = writer.get_snapshot()
        self.assertFalse(bool(recovered.get("backpressure_active")))
        self.assertEqual(int(recovered.get("backpressure_recovered_events") or 0), 1)
        writer.close(timeout_s=0.0)

    def test_failed_write_leaves_spool_row_for_startup_replay(self) -> None:
        spool_path = Path(self.tmp.name) / "async_price_writer.db"
        failing = _FailingPriceStorage()
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(spool_path),
                dead_letter_path=str(Path(self.tmp.name) / "dead_letter_failed.jsonl"),
            )
        )

        with patch.object(writer, "start", return_value={}):
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "SPY", "ts_ms": 1_700_000_000_010, "price": 501.25},),
                    source="unit_test",
                )
            )

        with patch.object(self.async_writer, "get_price_storage", return_value=failing):
            snapshot = writer.close(timeout_s=1.0)

        self.assertEqual(failing.calls, 1)
        self.assertEqual(int(snapshot.get("spool_pending_batches") or 0), 1)
        self.assertEqual(_spool_count(spool_path), 1)
        self.assertFalse((Path(self.tmp.name) / "dead_letter_failed.jsonl").exists())

        replay_storage = _FakePriceStorage()
        replay = self.async_writer.AsyncPriceWriter(config=self._config(spool_path=str(spool_path)))
        with patch.object(self.async_writer, "get_price_storage", return_value=replay_storage):
            replay_snapshot = replay.close(timeout_s=1.0)

        self.assertEqual(len(replay_storage.calls), 1)
        self.assertEqual(int(replay_snapshot.get("spool_pending_batches") or 0), 0)
        self.assertEqual(_spool_count(spool_path), 0)

    def test_circuit_open_write_leaves_spool_row_for_startup_replay(self) -> None:
        spool_path = Path(self.tmp.name) / "async_price_writer_circuit_open.db"
        circuit_open = _CircuitOpenPriceStorage()
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(spool_path),
                dead_letter_path=str(Path(self.tmp.name) / "dead_letter_circuit_open.jsonl"),
                retry_attempts=1,
            )
        )

        with patch.object(writer, "start", return_value={}):
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "SPY", "ts_ms": 1_700_000_000_012, "price": 502.25},),
                    source="unit_test",
                )
            )

        with patch.object(self.async_writer, "get_price_storage", return_value=circuit_open):
            snapshot = writer.close(timeout_s=1.0)

        self.assertEqual(circuit_open.calls, 1)
        self.assertEqual(int(snapshot.get("spool_pending_batches") or 0), 1)
        self.assertEqual(_spool_count(spool_path), 1)
        self.assertFalse((Path(self.tmp.name) / "dead_letter_circuit_open.jsonl").exists())

        replay_storage = _FakePriceStorage()
        replay = self.async_writer.AsyncPriceWriter(config=self._config(spool_path=str(spool_path)))
        with patch.object(self.async_writer, "get_price_storage", return_value=replay_storage):
            replay_snapshot = replay.close(timeout_s=1.0)

        self.assertEqual(len(replay_storage.calls), 1)
        self.assertEqual(int(replay_snapshot.get("spool_pending_batches") or 0), 0)
        self.assertEqual(_spool_count(spool_path), 0)

    def test_startup_replay_reports_replayed_and_deleted_rows(self) -> None:
        spool_path = Path(self.tmp.name) / "startup_replay_spool.sqlite"
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(spool_path),
                shutdown_drain_max_s=0.0,
            )
        )
        with patch.object(writer, "start", return_value={}):
            self.assertTrue(
                writer.enqueue(
                    prices=({"symbol": "QQQ", "ts_ms": 1_700_000_000_011, "price": 401.25},),
                    source="unit_test",
                )
            )

        replay_storage = _FakePriceStorage()
        replay = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(spool_path),
                flush_interval_s=0.01,
            )
        )
        with patch.object(self.async_writer, "get_price_storage", return_value=replay_storage):
            start_snapshot = replay.start()
            self.assertTrue(replay_storage.flushed.wait(timeout=1.0))
            replay_snapshot = replay.close(timeout_s=1.0)

        self.assertEqual(int(start_snapshot.get("startup_pending_rows") or 0), 1)
        self.assertEqual(int(replay_snapshot.get("replayed_rows") or 0), 1)
        self.assertEqual(int(replay_snapshot.get("spool_deleted_rows") or 0), 1)
        self.assertEqual(int(replay_snapshot.get("spool_pending_batches") or 0), 0)

    def test_spool_enforces_envelope_backpressure_before_accepting_enqueue(self) -> None:
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                queue_maxsize=1,
                batch_size=8,
                dead_letter_path=str(Path(self.tmp.name) / "dead_letter_full.jsonl"),
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
            self.assertFalse(
                writer.enqueue(
                    prices=({"symbol": "MSFT", "ts_ms": 2, "price": 2.0},),
                    source="unit_test",
                )
            )

        snapshot = writer.get_snapshot()
        self.assertEqual(int(snapshot.get("spool_pending_batches") or 0), 1)
        self.assertEqual(int(snapshot.get("spool_enqueue_failures") or 0), 1)
        self.assertEqual(int(snapshot.get("rejected_rows") or 0), 1)
        self.assertEqual(int(snapshot.get("dropped_rows") or 0), 1)
        writer.close(timeout_s=0.0)

    def test_corrupt_spool_file_is_quarantined_and_recreated(self) -> None:
        spool_path = Path(self.tmp.name) / "corrupt_spool.sqlite"
        spool_path.write_bytes(b"not a sqlite database")
        writer = self.async_writer.AsyncPriceWriter(
            config=self._config(
                spool_path=str(spool_path),
                dead_letter_path=str(Path(self.tmp.name) / "dead_letter_corrupt.jsonl"),
            )
        )

        snapshot = writer.start()
        writer.close(timeout_s=0.0)

        self.assertEqual(int(snapshot.get("spool_corruption_events") or 0), 1)
        self.assertTrue(spool_path.exists())
        self.assertTrue(list(Path(self.tmp.name).glob("corrupt_spool.sqlite.corrupt.*")))


if __name__ == "__main__":
    unittest.main()
