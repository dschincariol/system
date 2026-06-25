from __future__ import annotations

from types import SimpleNamespace
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.runtime import prod_preflight


def _live_preflight_ok() -> dict:
    return {
        "required": True,
        "ok": True,
        "blockers": [],
        "deployment_contract": {"ok": True},
        "prelive_reconcile": {"required": False, "ok": True, "blockers": []},
        "backup_restore_evidence": {"required": False, "ok": True, "blockers": []},
        "clock_health": {"required": False, "ok": True, "blockers": []},
        "execution_arming_audit": {"required": False, "ok": True, "blockers": []},
        "live_ai_safety": {"required": False, "ok": True, "blockers": []},
        "lob_deeplob_shadow": {"enabled": False, "ok": True, "blockers": []},
        "options_instruments": {"required": False, "ok": True, "blockers": []},
    }


class OptionsPreflightCredentialAssertionTests(unittest.TestCase):
    def _run_runtime_config_gate(
        self,
        *,
        credentials_configured: bool,
        options_ingestion: dict,
    ) -> tuple[list[str], list[str]]:
        config = SimpleNamespace(
            runtime_workload_profile="test",
            db_path=":memory:",
            allow_training=False,
        )
        patches = (
            patch.dict(os.environ, {"ENGINE_MODE": "paper", "EXECUTION_MODE": "paper"}, clear=False),
            patch("engine.runtime.config_schema.get_runtime_safety_context", return_value={"engine_mode": "paper", "env": "test", "strict_runtime": True}),
            patch("engine.runtime.config_schema.load_runtime_config", return_value=config),
            patch("engine.runtime.config_schema.validate_workload_profile_guardrails", return_value={"acknowledged": True}),
            patch("engine.runtime.config_schema.live_risk_threshold_validation_snapshot", return_value={"required": False}),
            patch("engine.runtime.hardware.runtime_hardware_snapshot", return_value={"ok": True, "devices": {}, "threads": {}}),
            patch("engine.runtime.live_trading_preflight.live_trading_preflight", return_value=_live_preflight_ok()),
            patch(
                "engine.runtime.health._options_credentials_configured",
                return_value=(bool(credentials_configured), ["POLYGON_API_KEY"] if credentials_configured else []),
            ),
            patch("engine.runtime.health._options_ingestion_snapshot", return_value=dict(options_ingestion)),
        )
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], patches[8]:
            return prod_preflight._runtime_config_gate()

    def test_configured_options_credential_with_stale_chain_blocks_preflight(self) -> None:
        notes, errors = self._run_runtime_config_gate(
            credentials_configured=True,
            options_ingestion={
                "ok": False,
                "degraded": True,
                "credentials_configured": True,
                "detail": "options_credentials_configured_but_chain_stale",
            },
        )

        del notes
        self.assertTrue(any("options_chain_stale_despite_credentials" in error for error in errors))

    def test_missing_options_credential_does_not_raise_stale_chain_blocker(self) -> None:
        notes, errors = self._run_runtime_config_gate(
            credentials_configured=False,
            options_ingestion={
                "ok": True,
                "degraded": False,
                "credentials_configured": False,
                "detail": "options_provider_unconfigured",
            },
        )

        self.assertFalse(any("options_chain_stale_despite_credentials" in error for error in errors))
        self.assertTrue(any("options ingestion shadow-only" in note for note in notes))


if __name__ == "__main__":
    unittest.main()
