from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
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


class UniversePitTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.db_path = Path(self.tmp.name) / "universe_pit.db"
        self._env_backup = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "MODEL_CONFIG_JSON",
                "USE_PIT_UNIVERSE",
                "PIT_UNIVERSE_BACKFILL_ENABLED",
                "EMBED_MODEL_MIN_NEW_LABELS",
            )
        }
        os.environ["DB_PATH"] = str(self.db_path)
        os.environ["PIT_UNIVERSE_BACKFILL_ENABLED"] = "0"
        self.storage, self.universe_pit = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.data.universe_pit",
        )[1:]
        self.storage.init_db()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        self.tmp.cleanup()

    def test_init_db_creates_universe_pit_schema(self) -> None:
        con = self.storage.connect(readonly=True)
        try:
            row = con.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table' AND name='universe_pit'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertIsNotNone(row)

    def test_backfill_and_snapshot_follow_simulated_symbol_lifecycle(self) -> None:
        con = self.storage.connect()
        try:
            con.executemany(
                """
                INSERT INTO symbols(
                  symbol, asset_class, status, score, last_seen_event_ts_ms, last_traded_ts_ms,
                  meta_json, created_ts_ms, updated_ts_ms
                )
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                [
                    ("AAA", "EQUITY", "ACTIVE", 1.0, None, None, "{}", 1000, 5000),
                    ("BBB", "EQUITY", "DISABLED", 1.0, None, None, "{}", 2000, 4000),
                ],
            )
            con.executemany(
                """
                INSERT INTO prices(ts_ms, symbol, price, px, source)
                VALUES (?,?,?,?,?)
                """,
                [
                    (1000, "AAA", 10.0, 10.0, "test"),
                    (5000, "AAA", 12.0, 12.0, "test"),
                    (2000, "BBB", 20.0, 20.0, "test"),
                    (4000, "BBB", 21.0, 21.0, "test"),
                ],
            )
            summary = self.universe_pit.backfill_universe_pit(con, now_ts_ms=5000)
            con.commit()
        finally:
            con.close()

        self.assertTrue(bool(summary.get("ok")))
        self.assertEqual(int(summary.get("row_count") or 0), 2)

        con = self.storage.connect(readonly=True)
        try:
            snap_1500 = self.universe_pit.get_pit_universe_symbols(con, ts_ms=1500)
            snap_3500 = self.universe_pit.get_pit_universe_symbols(con, ts_ms=3500)
            snap_4500 = self.universe_pit.get_pit_universe_symbols(con, ts_ms=4500)
            row = con.execute(
                """
                SELECT first_seen_ts, last_seen_ts, is_active, metadata_json
                FROM universe_pit
                WHERE symbol='BBB'
                LIMIT 1
                """
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(list(snap_1500), ["AAA"])
        self.assertEqual(list(snap_3500), ["AAA", "BBB"])
        self.assertEqual(list(snap_4500), ["AAA"])
        self.assertIsNotNone(row)
        self.assertEqual(int(row[0] or 0), 2000)
        self.assertEqual(int(row[1] or 0), 4000)
        self.assertEqual(int(row[2] or 0), 0)
        self.assertTrue(bool(json.loads(str(row[3] or "{}")).get("delisted_inferred")))

    def _seed_training_inputs(self) -> None:
        now_ms = int(time.time() * 1000)
        con = self.storage.connect()
        try:
            con.executemany(
                """
                INSERT INTO universe_pit(symbol, first_seen_ts, last_seen_ts, is_active, metadata_json)
                VALUES (?,?,?,?,?)
                """,
                [
                    ("AAA", int(now_ms - (10 * 24 * 60 * 60 * 1000)), None, 1, "{}"),
                    ("BBB", int(now_ms - (20 * 24 * 60 * 60 * 1000)), int(now_ms - (5 * 24 * 60 * 60 * 1000)), 0, "{}"),
                    ("CCC", int(now_ms - (200 * 24 * 60 * 60 * 1000)), int(now_ms - (120 * 24 * 60 * 60 * 1000)), 0, "{}"),
                ],
            )
            con.execute(
                """
                INSERT INTO labels(event_id, horizon_s, symbol, impact_z, created_at_ms)
                VALUES (?,?,?,?,?)
                """,
                (1, 300, "AAA", 0.5, now_ms),
            )
            con.commit()
        finally:
            con.close()

    def _run_train_embed_main(self, *, use_pit: bool) -> list[str]:
        os.environ["USE_PIT_UNIVERSE"] = "1" if use_pit else "0"
        os.environ["EMBED_MODEL_MIN_NEW_LABELS"] = "0"
        os.environ["MODEL_CONFIG_JSON"] = json.dumps(
            [
                {
                    "model_name": "embed_regressor.pit_test",
                    "family": "embed_regressor",
                    "enabled": True,
                    "prediction_enabled": True,
                    "experimental": False,
                    "symbol_universe": ["*"],
                    "horizons_s": [300],
                    "training_window_days": 30,
                }
            ],
            separators=(",", ":"),
            sort_keys=True,
        )
        train_mod = _reload_modules(
            "engine.strategy.model_config",
            "engine.strategy.train_embed_models",
        )[1]
        captured: dict[str, list[str]] = {}

        def _fake_train_embed_models(*args, **kwargs):
            captured["symbols"] = list(kwargs.get("symbols") or [])
            return {}

        with patch.object(train_mod, "train_embed_models", side_effect=_fake_train_embed_models), patch.object(
            train_mod, "build_dataset_snapshot", return_value={"fingerprint": "pit-test"}
        ), patch.object(train_mod, "load_lifecycle_plan", return_value={}), patch.object(
            train_mod, "start_lifecycle_run", return_value=0
        ), patch.object(
            train_mod, "acquire_job_lock", return_value=True
        ), patch.object(
            train_mod, "release_job_lock", return_value=None
        ):
            rc = train_mod.main()

        self.assertEqual(int(rc), 0)
        return list(captured.get("symbols") or [])

    def test_train_embed_models_uses_pit_window_symbols_when_enabled(self) -> None:
        self._seed_training_inputs()
        symbols = self._run_train_embed_main(use_pit=True)
        self.assertEqual(symbols, ["AAA", "BBB"])

    def test_train_embed_models_preserves_existing_symbols_when_pit_disabled(self) -> None:
        self._seed_training_inputs()
        symbols = self._run_train_embed_main(use_pit=False)
        self.assertEqual(symbols, ["*"])


if __name__ == "__main__":
    unittest.main()
