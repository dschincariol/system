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

LIVE_RISK_THRESHOLDS = {
    "PORTFOLIO_RISK_MC_VAR_95_BLOCK": "0.04",
    "PORTFOLIO_RISK_MC_VAR_99_BLOCK": "0.06",
    "PORTFOLIO_RISK_MC_CVAR_95_BLOCK": "0.05",
    "PORTFOLIO_RISK_MC_CVAR_99_BLOCK": "0.08",
    "PORTFOLIO_RISK_MC_DRAWDOWN_P95_BLOCK": "0.10",
    "PORTFOLIO_RISK_MC_WORST_DRAWDOWN_BLOCK": "0.16",
    "PORTFOLIO_RISK_VOL_HARD_BLOCK": "0.12",
    "KILL_SWITCH_MODEL_MAX_DRAWDOWN": "5000",
    "KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES": "4",
}
LIVE_ENV_CONTRACT = {
    "EXECUTION_MODE": "live",
    "DASHBOARD_API_TOKEN": "live-token-1234567890",
    "LIVE_TRADING_CONFIRM": "I_UNDERSTAND_LIVE_TRADING",
    "KILL_SWITCH_GLOBAL": "1",
    "BROKER": "alpaca",
    "BROKER_NAME": "alpaca",
    "LIVE_BROKER": "alpaca",
    "BROKER_FAILOVER": "alpaca",
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertEqual(len(notes), 3)
        self.assertIn("allow_training=0", notes[0])
        self.assertIn("live risk thresholds ok", notes[1])
        self.assertIn("live environment contract ok", notes[2])
        self.assertIn("execution_arming_audited=1", notes[2])

    def test_prod_preflight_runtime_config_gate_rejects_sim_live_broker_contract(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live_sim_broker.db").resolve())
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(errors)
        self.assertIn("live environment contract invalid", errors[0])
        self.assertIn("broker_must_be_live", errors[0])
        self.assertIn("sim_broker_forbidden_in_live", errors[0])

    def test_prod_preflight_runtime_config_gate_requires_initial_kill_switch_hold(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live_no_initial_hold.db").resolve())
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(errors)
        self.assertIn("live environment contract invalid", errors[0])
        self.assertIn("kill_switch_global_initial_hold_required", errors[0])

    def test_prod_preflight_runtime_config_gate_rejects_disabled_prelive_reconcile(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live_prelive_disabled.db").resolve())
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(len(errors), 1)
        self.assertIn("pre-live reconcile invalid", errors[0])
        self.assertIn("prelive_reconcile_disabled_for_live", errors[0])

    def test_prod_preflight_runtime_config_gate_rejects_disabled_live_execution(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live_disabled_execution.db").resolve())
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertTrue(errors)
        self.assertIn("live trading preflight invalid", errors[-1])
        self.assertIn("disable_live_execution_env", errors[-1])

    def test_prod_preflight_runtime_config_gate_accepts_audited_prelive_break_glass(self) -> None:
        db_path = str((Path.cwd() / "prod_preflight_live_prelive_breakglass.db").resolve())
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertTrue(any("pre-live reconcile break-glass accepted" in note for note in notes))
        self.assertTrue(any("actor=ops@example.com" in note for note in notes))

    def test_live_runtime_config_requires_explicit_live_risk_thresholds(self) -> None:
        db_path = str((Path.cwd() / "runtime_config_live_risk_unset.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
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
        db_path = str((Path.cwd() / "runtime_config_live_confirmation_unset.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
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
        db_path = str((Path.cwd() / "runtime_config_live_confirmation_disabled.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
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
        db_path = str((Path.cwd() / "prod_preflight_live_risk_unset.db").resolve())
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

        self.assertEqual(notes, [])
        self.assertEqual(len(errors), 1)
        self.assertIn("live risk thresholds invalid", errors[0])
        self.assertIn("PORTFOLIO_RISK_MC_CVAR_95_BLOCK unset", errors[0])
        self.assertIn("KILL_SWITCH_MODEL_MAX_CONSECUTIVE_LOSSES unset", errors[0])

    def test_live_runtime_config_rejects_zero_placeholder_and_disabled_risk_gates(self) -> None:
        db_path = str((Path.cwd() / "runtime_config_live_risk_zero.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
                **LIVE_RISK_THRESHOLDS,
                "PORTFOLIO_RISK_USE_MONTE_CARLO": "0",
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
        self.assertIn("PORTFOLIO_RISK_MC_VAR_95_BLOCK must be > 0", message)
        self.assertIn("PORTFOLIO_RISK_MC_CVAR_95_BLOCK placeholder", message)

    def test_live_risk_acceptance_override_requires_audit_fields(self) -> None:
        db_path = str((Path.cwd() / "runtime_config_live_risk_override_missing.db").resolve())
        with patch.dict(
            os.environ,
            {
                "ENGINE_MODE": "live",
                "DB_PATH": db_path,
                "ALLOW_TRAINING": "0",
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
        db_path = str((Path.cwd() / "prod_preflight_live_risk_override.db").resolve())
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
            ):
                notes, errors = prod_preflight._runtime_config_gate()

        self.assertEqual(errors, [])
        self.assertEqual(len(notes), 3)
        self.assertIn("live risk thresholds override accepted", notes[1])
        self.assertIn("id=RISK-123", notes[1])
        self.assertIn("live environment contract ok", notes[2])


if __name__ == "__main__":
    unittest.main()
