from __future__ import annotations

import importlib
import base64
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

LIVE_RISK_THRESHOLDS = {
    "PORTFOLIO_RISK_MC_VAR_95_BLOCK": "0.04",
    "PORTFOLIO_RISK_MC_VAR_99_BLOCK": "0.06",
    "PORTFOLIO_RISK_MC_CVAR_95_BLOCK": "0.05",
    "PORTFOLIO_RISK_MC_CVAR_99_BLOCK": "0.08",
    "PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK": "0.10",
    "PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK": "0.16",
    "PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE": "1",
    "PORTFOLIO_RISK_VOL_HARD_BLOCK": "0.12",
    "KILL_SWITCH_MODEL_MAX_DRAWDOWN": "5000",
    "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES": "4",
}
VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")
LIVE_ENV_CONTRACT = {
    "EXECUTION_MODE": "live",
    "DASHBOARD_API_TOKEN": "live-token-1234567890",
    "OPERATOR_API_TOKEN": "operator-token-1234567890",
    "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
    "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_LIVE_TRADING",
    "DISABLE_LIVE_EXECUTION": "0",
    "KILL_SWITCH_GLOBAL": "1",
    "BROKER": "alpaca",
    "BROKER_NAME": "alpaca",
    "LIVE_BROKER": "alpaca",
    "BROKER_FAILOVER": "alpaca",
    "BROKER_SHUTDOWN_POLICY": "cancel_only",
    "ALPACA_BASE_URL": "https://api.alpaca.markets",
    "ALPACA_KEY_ID": "alpaca-key",
    "ALPACA_SECRET_KEY": "alpaca-secret",
}
GOOD_BACKUP_EVIDENCE = {
    "ok": True,
    "fresh": True,
    "required": True,
    "reason": "ok",
    "blockers": [],
    "policy": {"restore_rto_s": 300},
    "base_backup": {"age_s": 1},
    "wal_archive": {"age_s": 1},
    "restore_drill": {"age_s": 1},
}
GOOD_WAL_ARCHIVER_RUNTIME = {
    "ok": True,
    "required": True,
    "reason": "ok",
    "blockers": [],
    "warnings": [],
    "archive_mode": "on",
    "archive_command": '/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
    "last_archived_wal": "0000000100000000000000AA",
    "age_s": 1,
    "failed_count": 0,
}
GOOD_POSITION_RECONCILE_EVIDENCE = {
    "ok": True,
    "fresh": True,
    "required": True,
    "reason": "ok",
    "blockers": [],
    "broker": "alpaca",
    "status": "ok",
    "exercised": True,
}
GOOD_LIVE_AI_SAFETY = {
    "ok": True,
    "required": True,
    "reason": "ok",
    "blockers": [],
    "decision_gate": {
        "ok": True,
        "required": True,
        "blockers": [],
        "decision_engine": {"enabled": True},
    },
    "uncertainty_thresholds": {
        "ok": True,
        "required": True,
        "blockers": [],
        "production_policy": "strict",
    },
    "rl_policy": {"ok": True, "required": True, "blockers": []},
    "model_serving": {"ok": True, "required": True, "blockers": []},
}
GOOD_CLOCK_HEALTH = {
    "ok": True,
    "required": True,
    "mode": "live",
    "reason": "ok",
    "blockers": [],
    "healthy_sources": ["chronyc"],
    "skew_sources": ["chronyc"],
    "max_observed_skew_ms": 1.0,
    "timezone": {"ok": True, "required_timezone": "UTC", "actual_timezone": "UTC"},
}


def _var_db_path(name: str) -> str:
    path = (REPO_ROOT / "var" / "db" / name).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


class RuntimeConfigSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self._clock_patch = patch(
            "engine.runtime.live_trading_preflight.clock_health_snapshot",
            return_value=GOOD_CLOCK_HEALTH,
        )
        self._clock_patch.start()
        self.addCleanup(self._clock_patch.stop)

    def _load(self):
        from engine.runtime.config_schema import load_runtime_config

        return load_runtime_config()

    def test_load_runtime_config_uses_var_db_for_local_default(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "ENGINE_MODE": "safe",
            },
            clear=True,
        ):
            cfg = self._load()

        self.assertEqual(Path(cfg.db_path), (REPO_ROOT / "var" / "db" / "trading.db").resolve())

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

    def test_load_runtime_config_rejects_tsfresh_snapshot_limit_above_max(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "TSFRESH_SNAPSHOT_SYMBOL_LIMIT": "200",
                "TSFRESH_SNAPSHOT_MAX_SYMBOLS": "100",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        self.assertIn("TSFRESH_SNAPSHOT_SYMBOL_LIMIT must be <= TSFRESH_SNAPSHOT_MAX_SYMBOLS", str(ctx.exception))

    def test_load_runtime_config_rejects_tune_trials_above_max(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "dev",
                "TUNE_N_TRIALS": "11",
                "TUNE_MAX_N_TRIALS": "10",
            },
            clear=False,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        self.assertIn("TUNE_N_TRIALS must be <= TUNE_MAX_N_TRIALS", str(ctx.exception))

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
        db_path = _var_db_path("runtime_config_live.db")
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

    def test_load_runtime_config_rejects_relative_db_path_in_prod(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "DB_PATH": "data/runtime",
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        self.assertIn("DB_PATH must be absolute", str(ctx.exception))

    def test_load_runtime_config_rejects_relative_data_root_in_prod(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "DB_PATH": "/var/lib/trading",
                "TS_DATA_ROOT": "data/runtime",
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        self.assertIn("TS_DATA_ROOT must be absolute", str(ctx.exception))

    def test_storage_facade_rejects_sqlite_backend_in_real_prod_runtime(self) -> None:
        import engine.runtime.storage as storage

        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "DB_PATH": "/var/lib/trading",
                "ALLOW_TRAINING": "0",
                "TS_STORAGE_BACKEND": "sqlite",
            },
            clear=True,
        ):
            with patch.object(storage, "running_python_tests", return_value=False):
                with self.assertRaises(RuntimeError) as ctx:
                    storage._use_sqlite_test_backend()

        self.assertIn("SQLite runtime storage backend is forbidden", str(ctx.exception))

    def test_storage_facade_allows_startup_validation_sqlite_harness(self) -> None:
        import engine.runtime.storage as storage

        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "ENGINE_SUPERVISED": "1",
                "DB_PATH": "/tmp/runtime-graph-check.sqlite",
                "ALLOW_TRAINING": "0",
                "TS_STORAGE_BACKEND": "sqlite",
                "TRADING_VALIDATION_MODE": "startup",
                "DATA_SOURCE_MANAGER_READ_ONLY": "1",
                "ENGINE_PRIMARY_BOOTSTRAP_DONE": "1",
            },
            clear=True,
        ):
            with patch.object(storage, "running_python_tests", return_value=False):
                self.assertTrue(storage._use_sqlite_test_backend())

    def test_entrypoints_do_not_default_db_path_in_strict_runtime(self) -> None:
        with patch.dict(
            os.environ,
            {
                "ENV": "prod",
                "ENGINE_MODE": "safe",
                "ALLOW_TRAINING": "0",
            },
            clear=True,
        ):
            import start_system
            import start_ingestion

            self.assertTrue(start_system._strict_runtime_requires_explicit_db_path())
            self.assertTrue(start_ingestion._strict_runtime_requires_explicit_db_path())

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
        self.assertTrue(str(cfg.db_path).endswith(str(Path("var") / "db" / "trading.db")))
        self.assertEqual(cfg.runtime_workload_profile, "live")
        self.assertFalse(cfg.allow_training)

    def test_prod_preflight_runtime_config_gate_rejects_ambiguous_live_contract(self) -> None:
        db_path = _var_db_path("prod_preflight_live.db")
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

    def test_live_runtime_config_rejects_placeholder_data_source_master_key(self) -> None:
        db_path = _var_db_path("runtime_config_live_weak_master_key.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": "change-me",
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        message = str(ctx.exception)
        self.assertIn("data source master key invalid", message)
        self.assertIn("placeholder_or_known_default", message)

    def test_prod_preflight_runtime_config_gate_rejects_low_entropy_master_key(self) -> None:
        db_path = _var_db_path("prod_preflight_live_low_entropy_master_key.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": base64.b64encode(b"x" * 32).decode("ascii"),
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(notes, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("data source master key invalid", errors[0])
        self.assertIn("low_entropy", errors[0])

    def test_prod_preflight_runtime_config_gate_accepts_explicit_live_contract(self) -> None:
        db_path = _var_db_path("prod_preflight_live_ok.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                **LIVE_ENV_CONTRACT,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertEqual(len(notes), 5)
        self.assertIn("workload_profile=live", notes[0])
        self.assertIn("allow_training=0", notes[0])
        self.assertIn("runtime hardware ok", notes[1])
        self.assertIn("dependency_profile=cpu", notes[1])
        self.assertIn("torch_device=cpu", notes[1])
        self.assertIn("live risk thresholds ok", notes[2])
        self.assertIn("promotion observation governance ok", notes[3])
        self.assertIn("non_bypassable=1", notes[3])
        self.assertIn("live environment contract ok", notes[4])
        self.assertIn("execution_arming_audited=1", notes[4])

    def test_prod_preflight_runtime_config_gate_rejects_live_profile_training_without_ack(self) -> None:
        db_path = _var_db_path("prod_preflight_live_training_no_ack.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "RUNTIME_WORKLOAD_PROFILE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "1",
                **LIVE_ENV_CONTRACT,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(notes, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("offline training in live workload profile requires explicit acknowledgement", errors[0])
        self.assertIn("ALLOW_TRAINING", errors[0])

    def test_prod_preflight_runtime_config_gate_accepts_live_profile_training_with_ack(self) -> None:
        db_path = _var_db_path("prod_preflight_live_training_ack.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "RUNTIME_WORKLOAD_PROFILE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "1",
                "OFFLINE_TRAINING_LIVE_PROFILE_ACK": "I_UNDERSTAND_OFFLINE_TRAINING_IN_LIVE_PROFILE",
                "OFFLINE_TRAINING_LIVE_PROFILE_OWNER": "ops",
                "OFFLINE_TRAINING_LIVE_PROFILE_REASON": "isolated maintenance window",
                **LIVE_ENV_CONTRACT,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertIn("allow_training=1", notes[0])
        self.assertIn("offline_ack=1", notes[0])

    def test_prod_preflight_rejects_nvidia_hardware_profile_without_dependency_profile(self) -> None:
        db_path = _var_db_path("prod_preflight_nvidia_profile_mismatch.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "safe",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
                "RUNTIME_HARDWARE_PROFILE": "nvidia",
                "TRADING_DEPENDENCY_PROFILE": "cpu",
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(any("runtime hardware ok" in note for note in notes))
        self.assertTrue(any("dependency_profile=cpu" in note for note in notes))
        self.assertTrue(any("nvidia_runtime_requires_nvidia_dependency_profile" in err for err in errors))

    def test_prod_preflight_runtime_config_gate_rejects_sim_live_broker_contract(self) -> None:
        db_path = _var_db_path("prod_preflight_live_sim_broker.db")
        unsafe_env = dict(LIVE_ENV_CONTRACT)
        unsafe_env.update(
            {
                "BROKER": "sim",
                "BROKER_NAME": "sim",
                "LIVE_BROKER": "sim",
                "BROKER_FAILOVER": "sim",
            }
        )
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                **unsafe_env,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(errors)
        self.assertIn("live environment contract invalid", errors[0])
        self.assertIn("broker_must_be_live", errors[0])
        self.assertIn("sim_broker_forbidden_in_live", errors[0])

    def test_prod_preflight_runtime_config_gate_requires_initial_kill_switch_hold(self) -> None:
        db_path = _var_db_path("prod_preflight_live_no_initial_hold.db")
        unsafe_env = dict(LIVE_ENV_CONTRACT)
        unsafe_env["KILL_SWITCH_GLOBAL"] = "0"
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                **unsafe_env,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(errors)
        self.assertIn("live environment contract invalid", errors[0])
        self.assertIn("kill_switch_global_initial_hold_required", errors[0])

    def test_prod_preflight_runtime_config_gate_rejects_disabled_prelive_reconcile(self) -> None:
        db_path = _var_db_path("prod_preflight_live_prelive_disabled.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "EXECUTION_PRELIVE_RECONCILE": "0",
                **LIVE_ENV_CONTRACT,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(len(errors), 1)
        self.assertIn("pre-live reconcile invalid", errors[0])
        self.assertIn("prelive_reconcile_disabled_for_live", errors[0])

    def test_prod_preflight_runtime_config_gate_rejects_disabled_live_execution(self) -> None:
        db_path = _var_db_path("prod_preflight_live_disabled_execution.db")
        unsafe_env = dict(LIVE_ENV_CONTRACT)
        unsafe_env["DISABLE_LIVE_EXECUTION"] = "1"
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                **unsafe_env,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(errors)
        self.assertIn("live trading preflight invalid", errors[-1])
        self.assertIn("disable_live_execution_env", errors[-1])

    def test_prod_preflight_runtime_config_gate_accepts_audited_prelive_break_glass(self) -> None:
        db_path = _var_db_path("prod_preflight_live_prelive_breakglass.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "EXECUTION_PRELIVE_RECONCILE": "0",
                "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS": "1",
                "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_ACTOR": "ops@example.com",
                "EXECUTION_PRELIVE_RECONCILE_BREAK_GLASS_REASON": "temporary audited incident response override",
                **LIVE_ENV_CONTRACT,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertTrue(any("pre-live reconcile break-glass accepted" in note for note in notes))
        self.assertTrue(any("actor=ops@example.com" in note for note in notes))

    def test_live_runtime_config_requires_explicit_live_risk_thresholds(self) -> None:
        db_path = _var_db_path("runtime_config_live_risk_unset.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        message = str(ctx.exception)
        self.assertIn("live risk thresholds invalid", message)
        self.assertIn("PORTFOLIO_RISK_MC_VAR_95_BLOCK unset", message)
        self.assertIn("PORTFOLIO_RISK_VOL_HARD_BLOCK unset", message)
        self.assertIn("KILL_SWITCH_MODEL_MAX_DRAWDOWN unset", message)

    def test_live_runtime_config_requires_confirmation_phrase(self) -> None:
        db_path = _var_db_path("runtime_config_live_confirmation_unset.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        message = str(ctx.exception)
        self.assertIn("live trading confirmation invalid", message)
        self.assertIn("live_trading_confirmation_required", message)
        self.assertIn("LIVE_TRADING_CONFIRM=I_UNDERSTAND_LIVE_TRADING", message)

    def test_live_runtime_config_rejects_disabled_confirmation_requirement(self) -> None:
        db_path = _var_db_path("runtime_config_live_confirmation_disabled.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
                "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_LIVE_TRADING",
                "LIVE_TRADING_REQUIRE_CONFIRMATION": "0",
                **LIVE_RISK_THRESHOLDS,
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        message = str(ctx.exception)
        self.assertIn("live_trading_confirmation_cannot_be_disabled", message)

    def test_prod_preflight_runtime_config_gate_rejects_unset_live_risk_thresholds(self) -> None:
        db_path = _var_db_path("prod_preflight_live_risk_unset.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(notes, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("live risk thresholds invalid", errors[0])
        self.assertIn("PORTFOLIO_RISK_MC_CVAR_95_BLOCK unset", errors[0])
        self.assertIn("KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES unset", errors[0])

    def test_live_runtime_config_rejects_zero_placeholder_and_disabled_risk_gates(self) -> None:
        db_path = _var_db_path("runtime_config_live_risk_zero.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
                **LIVE_RISK_THRESHOLDS,
                "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
                "PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE": "0",
                "PORTFOLIO_RISK_MC_VAR_95_BLOCK": "0",
                "PORTFOLIO_RISK_MC_CVAR_95_BLOCK": "todo",
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        message = str(ctx.exception)
        self.assertIn("PORTFOLIO_RISK_USE_MONTE_CARLO disabled", message)
        self.assertIn("PORTFOLIO_RISK_MC_REQUIRED_IN_LIVE disabled", message)
        self.assertIn("PORTFOLIO_RISK_MC_VAR_95_BLOCK must be > 0", message)
        self.assertIn("PORTFOLIO_RISK_MC_CVAR_95_BLOCK placeholder", message)

    def test_live_risk_acceptance_override_requires_audit_fields(self) -> None:
        db_path = _var_db_path("runtime_config_live_risk_override_missing.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "DATA_SOURCE_MASTER_KEY": VALID_DATA_SOURCE_MASTER_KEY,
                "LIVE_RISK_THRESHOLD_ACCEPTANCE_OVERRIDE": "1",
                "LIVE_RISK_THRESHOLD_ACCEPTANCE_ID": "RISK-123",
            },
            clear=True,
        ):
            from engine.runtime.config_schema import ConfigError

            with self.assertRaises(ConfigError) as ctx:
                self._load()

        message = str(ctx.exception)
        self.assertIn("LIVE_RISK_THRESHOLD_ACCEPTANCE_OWNER required", message)
        self.assertIn("LIVE_RISK_THRESHOLD_ACCEPTANCE_REASON required", message)

    def test_live_risk_acceptance_override_allows_audited_exception(self) -> None:
        db_path = _var_db_path("prod_preflight_live_risk_override.db")
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                "LIVE_RISK_THRESHOLD_ACCEPTANCE_OVERRIDE": "1",
                "LIVE_RISK_THRESHOLD_ACCEPTANCE_ID": "RISK-123",
                "LIVE_RISK_THRESHOLD_ACCEPTANCE_OWNER": "risk@example.com",
                "LIVE_RISK_THRESHOLD_ACCEPTANCE_REASON": "temporary live readiness exception for staged rollout",
                **LIVE_ENV_CONTRACT,
            },
            clear=True,
        ):
            import engine.runtime.prod_preflight as prod_preflight

            prod_preflight = importlib.reload(prod_preflight)
            with patch(
                "engine.runtime.live_trading_preflight.backup_restore_evidence_snapshot",
                return_value=GOOD_BACKUP_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.wal_archiver_runtime_snapshot",
                return_value=GOOD_WAL_ARCHIVER_RUNTIME,
            ), patch(
                "engine.runtime.live_trading_preflight.position_reconcile_evidence_snapshot",
                return_value=GOOD_POSITION_RECONCILE_EVIDENCE,
            ), patch(
                "engine.runtime.live_trading_preflight.live_ai_safety_snapshot",
                return_value=GOOD_LIVE_AI_SAFETY,
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertEqual(len(notes), 5)
        self.assertIn("runtime hardware ok", notes[1])
        self.assertIn("torch_device=cpu", notes[1])
        self.assertIn("live risk thresholds override accepted", notes[2])
        self.assertIn("id=RISK-123", notes[2])
        self.assertIn("promotion observation governance ok", notes[3])
        self.assertIn("non_bypassable=1", notes[3])
        self.assertIn("live environment contract ok", notes[4])


if __name__ == "__main__":
    unittest.main()
