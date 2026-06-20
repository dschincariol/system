from __future__ import annotations

import importlib
import json
import os
import sqlite3
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
        self.universe_pit = _reload_modules("engine.data.universe_pit")[0]
        self._init_sqlite_schema()

    def _connect(self, readonly: bool = False):
        del readonly
        return sqlite3.connect(str(self.db_path))

    def _init_sqlite_schema(self) -> None:
        con = self._connect()
        try:
            self.universe_pit.ensure_universe_pit_schema(con)
            con.executescript(
                """
                CREATE TABLE IF NOT EXISTS symbols(
                  symbol TEXT,
                  asset_class TEXT,
                  status TEXT,
                  score REAL,
                  last_seen_event_ts_ms INTEGER,
                  last_traded_ts_ms INTEGER,
                  meta_json TEXT,
                  created_ts_ms INTEGER,
                  updated_ts_ms INTEGER
                );
                CREATE TABLE IF NOT EXISTS prices(
                  ts_ms INTEGER,
                  symbol TEXT,
                  price REAL,
                  px REAL,
                  source TEXT
                );
                CREATE TABLE IF NOT EXISTS labels(
                  event_id INTEGER,
                  horizon_s INTEGER,
                  symbol TEXT,
                  impact_z REAL,
                  created_at_ms INTEGER
                );
                CREATE TABLE IF NOT EXISTS embed_model_eval(
                  ts_ms INTEGER,
                  model_kind TEXT,
                  n_train INTEGER,
                  n_eval INTEGER,
                  rmse REAL,
                  spearman REAL,
                  directional_acc REAL
                );
                """
            )
            con.commit()
        finally:
            con.close()

    def tearDown(self) -> None:
        for key, value in self._env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        try:
            pass
        except Exception:
            pass
        self.tmp.cleanup()

    def test_init_db_creates_universe_pit_schema(self) -> None:
        con = self._connect(readonly=True)
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
        con = self._connect()
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

        con = self._connect(readonly=True)
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
        con = self._connect()
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

        with patch.object(train_mod, "connect", side_effect=self._connect), patch.object(
            train_mod, "init_db", return_value=None
        ), patch.object(
            train_mod, "training_allowed", return_value=True
        ), patch.object(
            train_mod, "train_embed_models", side_effect=_fake_train_embed_models
        ), patch.object(
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

    def test_tabular_training_job_uses_pit_window_symbols_when_enabled(self) -> None:
        self._seed_training_inputs()
        os.environ["USE_PIT_UNIVERSE"] = "1"
        os.environ["LGBM_REGRESSOR_MIN_SAMPLES"] = "1"
        lgbm_mod = _reload_modules("engine.strategy.models.lgbm_regressor")[0]
        captured: dict[str, list[str]] = {}

        class FakeModel:
            def __init__(self, *, model_name, feature_ids, hyperparams):
                self.model_name = str(model_name)
                self.feature_ids = list(feature_ids)
                self.hyperparams = dict(hyperparams or {})
                self.training_metrics = {"rmse": 0.0, "directional_acc": 1.0}
                self.feature_schema = {"feature_ids": list(feature_ids), "feature_set_tag": "unit"}

            def fit(self, X, y):
                return self

            def predict(self, X):
                return [0.0 for _ in list(X or [])]

        def _fake_load_rows(**kwargs):
            captured["symbols"] = list(kwargs.get("symbols") or [])
            return (
                [{"base.source_credibility": 0.1}, {"base.source_credibility": 0.2}],
                [0.1, 0.2],
                [
                    {"symbol": "AAA", "ts": 1, "horizon": 300},
                    {"symbol": "BBB", "ts": 2, "horizon": 300},
                ],
            )

        with patch.dict(
            os.environ,
            {"RUNTIME_WORKLOAD_PROFILE": "offline", "ALLOW_TRAINING": "1"},
        ), patch.object(lgbm_mod, "connect", side_effect=self._connect), patch.object(
            lgbm_mod, "init_db", return_value=None
        ), patch.object(
            lgbm_mod, "load_lifecycle_plan", return_value={}
        ), patch.object(
            lgbm_mod,
            "_resolve_training_config",
            return_value={
                "model_name": "lgbm_regressor.pit_unit",
                "feature_ids": ["base.source_credibility"],
                "symbol_universe": ["*"],
                "training_window_days": 30,
                "horizon_s": 300,
                "hyperparams": {},
            },
        ), patch.object(lgbm_mod, "_load_training_rows", side_effect=_fake_load_rows), patch.object(
            lgbm_mod, "upsert_oos_predictions", return_value=None
        ), patch.object(
            lgbm_mod,
            "register_shadow_model",
            return_value={"version": "unit", "stage": "shadow", "artifact_manifest": {}, "metrics": {"rmse": 0.0, "directional_acc": 1.0}},
        ), patch.object(
            lgbm_mod, "record_version_performance", return_value=None
        ), patch.object(
            lgbm_mod, "update_model_version_status", return_value=None
        ):
            rc = lgbm_mod.run_tabular_training_job(
                family="lgbm_regressor",
                model_cls=FakeModel,
                model_kind="unit",
                version_prefix="unit",
            )

        self.assertEqual(int(rc), 0)
        self.assertEqual(captured.get("symbols"), ["AAA", "BBB"])


if __name__ == "__main__":
    unittest.main()
