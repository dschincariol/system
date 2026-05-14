from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class RuntimeConfigSchemaTests(unittest.TestCase):
    def _load(self):
        from engine.runtime.config_schema import load_runtime_config

        return load_runtime_config()

    def test_load_runtime_config_rejects_invalid_hmm_num_states(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "HMM_NUM_STATES": "9",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError):
                self._load()

    def test_load_runtime_config_rejects_invalid_cpcv_split_shape(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "CPCV_N_SPLITS": "4",
                "CPCV_N_TEST_SPLITS": "4",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError):
                self._load()

    def test_load_runtime_config_rejects_invalid_tsfresh_profile(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "TSFRESH_FC_PROFILE": "unknown",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError):
                self._load()

    def test_load_runtime_config_rejects_invalid_black_litterman_confidence(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "BLACK_LITTERMAN_VIEW_CONFIDENCE": "1.5",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError):
                self._load()

    def test_load_runtime_config_rejects_invalid_bool_value(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "SUPERVISOR_ENABLED": "definitely",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError):
                self._load()

    def test_load_runtime_config_requires_explicit_db_path_in_ambiguous_live_mode(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        self.assertIn("DB_PATH must be explicitly set", str(ctx.exception))

    def test_load_runtime_config_requires_explicit_training_flag_in_ambiguous_live_mode(self) -> None:
        db_path = str((Path.cwd() / "runtime_config_live.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        self.assertIn("ALLOW_TRAINING must be explicitly set", str(ctx.exception))

    def test_load_runtime_config_allows_explicit_dev_override_for_live_mode(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "ENGINE_MODE": "live",
            },
            clear=True,
        ):
            cfg = self._load()

        self.assertEqual(cfg.env, "dev")
        self.assertTrue(str(cfg.db_path).endswith(str(Path("data") / "trading.db")))
        self.assertTrue(cfg.allow_training)

    def test_prod_preflight_runtime_config_gate_rejects_ambiguous_live_contract(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(notes, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("ALLOW_TRAINING must be explicitly set", errors[0])

    def test_prod_preflight_runtime_config_gate_accepts_explicit_live_contract(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live_ok.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertEqual(len(notes), 1)
        self.assertIn("allow_training=0", notes[0])


if __name__ == "__main__":
    unittest.main()
