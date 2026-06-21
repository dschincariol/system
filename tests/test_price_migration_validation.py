from __future__ import annotations

import importlib
import os
import sys
import tempfile
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


class _FakeWriter:
    def __init__(self, snapshot: dict[str, object]) -> None:
        self._snapshot = dict(snapshot)

    def get_snapshot(self) -> dict[str, object]:
        return dict(self._snapshot)


class _FakeStorage:
    def __init__(self, snapshot: dict[str, object]) -> None:
        self._snapshot = dict(snapshot)

    def get_snapshot(self) -> dict[str, object]:
        return dict(self._snapshot)


class PriceMigrationValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env: dict[str, str | None] = {}
        self._set_env("DB_PATH", str(Path(self.tmp.name) / "price_migration_validation.db"))
        self._set_env("ENGINE_SUPERVISED", "1")

    def tearDown(self) -> None:
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _set_env(self, key: str, value: str | None) -> None:
        if key not in self.prev_env:
            self.prev_env[key] = os.environ.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = str(value)

    def test_validation_snapshot_blocks_cutover_on_health_and_parity_failures(self) -> None:
        self._set_env("TIMESCALE_PRICES_ENABLED", "1")
        self._set_env("TIMESCALE_PRICES_DSN", "postgres://unit-test")
        (validation,) = _reload_modules("engine.runtime.price_migration_validation")

        fake_writer = _FakeWriter({"enabled": True, "ok": False, "queue_depth": 3})
        fake_storage = _FakeStorage({"enabled": True, "ok": False})
        sqlite_summary = {
            "prices": {"count": 10, "max_ts_ms": 1_700_000_000_010},
            "price_quotes": {"count": 8, "max_ts_ms": 1_700_000_000_020},
            "price_quotes_raw": {"count": 12, "max_ts_ms": 1_700_000_000_030},
        }
        timescale_summary = {
            "price_ticks": {"count": 8, "max_ts_ms": 1_700_000_000_000},
            "price_quotes": {"count": 8, "max_ts_ms": 1_700_000_000_020},
            "price_quotes_raw": {"count": 9, "max_ts_ms": 1_700_000_000_000},
        }

        with patch.object(validation, "get_async_writer", return_value=fake_writer):
            with patch.object(validation, "get_price_storage", return_value=fake_storage):
                with patch.object(validation, "psycopg", object()):
                    with patch.object(validation, "_sqlite_summary", return_value=sqlite_summary):
                        with patch.object(validation, "_timescale_summary", return_value=timescale_summary):
                            snapshot = validation.build_price_migration_validation_snapshot(
                                lookback_minutes=5,
                                max_count_delta=0,
                                max_last_ts_lag_ms=5,
                                require_async_writer=True,
                                require_pg_storage=True,
                                max_queue_depth=0,
                            )

        self.assertFalse(bool(snapshot.get("ok")))
        self.assertIn("async_price_writer_not_ok", list(snapshot.get("reasons") or []))
        self.assertIn("pg_price_storage_not_ok", list(snapshot.get("reasons") or []))
        self.assertIn("prices_parity_out_of_bounds", list(snapshot.get("reasons") or []))
        self.assertIn("raw_parity_out_of_bounds", list(snapshot.get("reasons") or []))

    def test_price_read_router_falls_back_to_sqlite_when_validation_fails(self) -> None:
        self._set_env("PRICE_READ_BACKEND", "timescale")
        self._set_env("PRICE_READ_REQUIRE_VALIDATION", "1")
        (router,) = _reload_modules("engine.runtime.price_read_router")

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(
                router,
                "get_price_migration_validation_snapshot",
                return_value={"enabled": True, "ok": False, "detail": "parity_failed"},
            ):
                backend = router.get_price_read_backend()

        self.assertEqual(backend, "sqlite")

    def test_price_read_router_uses_timescale_when_validation_passes(self) -> None:
        self._set_env("PRICE_READ_BACKEND", "timescale")
        self._set_env("PRICE_READ_REQUIRE_VALIDATION", "1")
        (router,) = _reload_modules("engine.runtime.price_read_router")

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(
                router,
                "get_price_migration_validation_snapshot",
                return_value={"enabled": True, "ok": True, "detail": "validation_ok"},
            ):
                backend = router.get_price_read_backend()

        self.assertEqual(backend, "timescale")

    def test_price_read_router_auto_mode_prefers_timescale_when_validation_passes(self) -> None:
        self._set_env("PRICE_READ_BACKEND", None)
        self._set_env("PRICE_READ_REQUIRE_VALIDATION", "1")
        (router,) = _reload_modules("engine.runtime.price_read_router")

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(
                router,
                "get_price_migration_validation_snapshot",
                return_value={"enabled": True, "ok": True, "detail": "validation_ok"},
            ):
                backend = router.get_price_read_backend()

        self.assertEqual(backend, "timescale")

    def test_price_read_router_sqlite_override_keeps_sqlite_primary(self) -> None:
        self._set_env("PRICE_READ_BACKEND", "sqlite")
        self._set_env("PRICE_READ_REQUIRE_VALIDATION", "1")
        (router,) = _reload_modules("engine.runtime.price_read_router")

        with patch.object(router, "_timescale_enabled", return_value=True):
            with patch.object(
                router,
                "get_price_migration_validation_snapshot",
                return_value={"enabled": True, "ok": True, "detail": "validation_ok"},
            ):
                backend = router.get_price_read_backend()

        self.assertEqual(backend, "sqlite")

    def test_health_snapshot_exposes_price_migration_validation_section(self) -> None:
        storage, health, validation = _reload_modules(
            "engine.runtime.storage",
            "engine.runtime.health",
            "engine.runtime.price_migration_validation",
        )
        storage.init_db()

        with patch.object(
            validation,
            "get_price_migration_validation_snapshot",
            return_value={"ok": False, "enabled": True, "detail": "unit_test", "reasons": ["mismatch"], "ts_ms": 123},
        ):
            snapshot = health.get_health_snapshot()

        self.assertIn("price_migration_validation", snapshot)
        self.assertFalse(bool((snapshot.get("price_migration_validation") or {}).get("ok")))
        self.assertEqual(str((snapshot.get("price_migration_validation") or {}).get("detail") or ""), "unit_test")


if __name__ == "__main__":
    unittest.main()
