from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class ComposeDeploymentAssetTests(unittest.TestCase):
    def test_compose_stack_defines_runtime_and_operator(self) -> None:
        compose_path = REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml"
        text = compose_path.read_text(encoding="utf-8")

        self.assertIn("runtime:", text)
        self.assertIn("operator:", text)
        self.assertIn("PROD_LOCK: ${PROD_LOCK:-1}", text)
        self.assertIn("ALLOW_TRAINING: ${ALLOW_TRAINING:-0}", text)
        self.assertIn("TRADING_IMPORT_SMOKE_IMPORT_JOBS: ${TRADING_IMPORT_SMOKE_IMPORT_JOBS:-0}", text)
        self.assertIn("OPERATOR_DISABLE_INTERNAL_ENGINE_START: \"1\"", text)
        self.assertIn("OPERATOR_API_TOKEN: ${OPERATOR_API_TOKEN:?set OPERATOR_API_TOKEN}", text)
        self.assertIn("docker-compose.external-services.yml", (REPO_ROOT / "deploy" / "compose" / "README.md").read_text(encoding="utf-8"))

    def test_compose_dockerfiles_and_ignore_exist(self) -> None:
        runtime_dockerfile = REPO_ROOT / "deploy" / "compose" / "Dockerfile.runtime"
        operator_dockerfile = REPO_ROOT / "deploy" / "compose" / "Dockerfile.operator"
        dockerignore = REPO_ROOT / ".dockerignore"

        self.assertTrue(runtime_dockerfile.exists())
        self.assertTrue(operator_dockerfile.exists())
        self.assertTrue(dockerignore.exists())

        runtime_text = runtime_dockerfile.read_text(encoding="utf-8")
        operator_text = operator_dockerfile.read_text(encoding="utf-8")
        ignore_text = dockerignore.read_text(encoding="utf-8")

        self.assertIn("start_system.py", runtime_text)
        self.assertIn("boot/operator_server.js", operator_text)
        self.assertIn("logs/", ignore_text)
        self.assertIn("data/", ignore_text)

    def test_minio_initializer_retries_until_object_store_is_ready(self) -> None:
        external_compose = REPO_ROOT / "deploy" / "compose" / "docker-compose.external-services.yml"
        text = external_compose.read_text(encoding="utf-8")

        self.assertIn("until mc alias set local http://minio:9000", text)
        self.assertIn('"$${MINIO_ROOT_USER}"', text)
        self.assertIn('"$${MINIO_ROOT_PASSWORD}"', text)
        self.assertIn('mc mb --ignore-existing "local/$${OBJECT_STORE_BUCKET}"', text)

    def test_timescaledb_compose_archives_wal_to_host_backup_dir(self) -> None:
        external_compose = REPO_ROOT / "deploy" / "compose" / "docker-compose.external-services.yml"
        env_example = REPO_ROOT / "deploy" / "compose" / ".env.example"
        text = external_compose.read_text(encoding="utf-8")
        env_text = env_example.read_text(encoding="utf-8")

        self.assertIn("${TRADING_BACKUP_ROOT:-/var/backups/trading}:/var/backups/trading", text)
        self.assertIn("archive_mode=on", text)
        self.assertIn("archive_command=test ! -f /var/backups/trading/wal/%f", text)
        self.assertIn("archive_timeout=60s", text)
        self.assertIn("TRADING_BACKUP_ROOT=/var/backups/trading", env_text)
        self.assertIn("TRADING_BACKUP_WAL_DIR=/var/backups/trading/wal", env_text)

    def test_runtime_compose_mounts_backup_evidence_read_only(self) -> None:
        stack_text = (REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml").read_text(encoding="utf-8")
        env_example_text = (REPO_ROOT / "deploy" / "compose" / ".env.example").read_text(encoding="utf-8")

        self.assertIn("${TRADING_BACKUP_ROOT:-/var/backups/trading}:/var/backups/trading:ro", stack_text)
        self.assertIn("PREFLIGHT_REQUIRE_BACKUP_EVIDENCE: ${PREFLIGHT_REQUIRE_BACKUP_EVIDENCE:-1}", stack_text)
        self.assertIn(
            "BACKUP_EVIDENCE_PATH: ${BACKUP_EVIDENCE_PATH:-/var/backups/trading/evidence/latest_backup_restore_evidence.json}",
            stack_text,
        )
        self.assertIn("BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S: ${BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S:-93600}", stack_text)
        self.assertIn("BACKUP_EVIDENCE_RPO_S: ${BACKUP_EVIDENCE_RPO_S:-120}", stack_text)
        self.assertIn("BACKUP_EVIDENCE_WAL_RPO_S: ${BACKUP_EVIDENCE_WAL_RPO_S:-120}", stack_text)
        self.assertIn("BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S: ${BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S:-7776000}", stack_text)
        self.assertIn("BACKUP_EVIDENCE_RTO_S: ${BACKUP_EVIDENCE_RTO_S:-1800}", stack_text)

        for key in (
            "PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1",
            "BACKUP_EVIDENCE_PATH=/var/backups/trading/evidence/latest_backup_restore_evidence.json",
            "BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S=93600",
            "BACKUP_EVIDENCE_RPO_S=120",
            "BACKUP_EVIDENCE_WAL_RPO_S=120",
            "BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S=7776000",
            "BACKUP_EVIDENCE_RTO_S=1800",
        ):
            with self.subTest(key=key):
                self.assertIn(key, env_example_text)

    def test_backup_scripts_support_compose_docker_pg_tools(self) -> None:
        base_backup = (REPO_ROOT / "ops" / "backup" / "base_backup.sh").read_text(encoding="utf-8")
        restore_drill = (REPO_ROOT / "ops" / "backup" / "restore_drill.sh").read_text(encoding="utf-8")
        installer = (REPO_ROOT / "ops" / "server" / "install_backup_evidence_gate.sh").read_text(encoding="utf-8")

        self.assertIn("TS_BACKUP_DOCKER_IMAGE", base_backup)
        self.assertIn("TS_BACKUP_DOCKER_EXEC_CONTAINER", base_backup)
        self.assertIn("TS_RESTORE_DOCKER_IMAGE", restore_drill)
        self.assertIn("--compose", installer)
        self.assertIn("install_compose_systemd_overrides", installer)

    def test_runtime_compose_exposes_provider_and_broker_contract(self) -> None:
        stack_text = (REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml").read_text(encoding="utf-8")
        env_example_text = (REPO_ROOT / "deploy" / "compose" / ".env.example").read_text(encoding="utf-8")

        required_provider_contract = [
            "POLYGON_REST_ENABLED",
            "POLYGON_WS_ENABLED",
            "POLYGON_API_KEY",
            "YFINANCE_ENABLED",
            "TRADIER_ENABLED",
            "TRADIER_API_TOKEN",
            "OPTIONS_PROVIDER_CHAIN",
            "OPTIONS_CRITICAL_SYMBOLS",
            "LIVE_BROKER",
            "BROKER_NAME",
            "BROKER",
            "BROKER_FAILOVER",
            "ALPACA_BASE_URL",
            "ALPACA_KEY_ID",
            "ALPACA_SECRET_KEY",
            "IBKR_HOST",
            "IBKR_PORT",
            "IBKR_CLIENT_ID",
            "OPERATOR_API_TOKEN",
            "LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN",
            "LIVE_TRADING_REQUIRE_CONFIRMATION",
            "AUTO_PIPELINE",
            "AUTO_PIPELINE_INCLUDE_EXECUTION",
            "AUTO_PIPELINE_START_DELAY_S",
            "MODEL_FEATURE_SNAPSHOT_SLEEP_S",
            "MODEL_FEATURE_SNAPSHOT_BUCKET_SEC",
            "INFERENCE_HEALTH_PROBE_ENABLED",
            "INFERENCE_HEALTH_PROBE_SYMBOLS",
            "INFERENCE_HEALTH_PROBE_INTERVAL_S",
            "KILL_SWITCH_GLOBAL",
            "DISABLE_LIVE_EXECUTION",
        ]
        for key in required_provider_contract:
            with self.subTest(key=key):
                self.assertIn(key, stack_text)
                self.assertIn(key, env_example_text)

        self.assertIn("POLYGON_REST_ENABLED: ${POLYGON_REST_ENABLED:-0}", stack_text)
        self.assertIn("YFINANCE_ENABLED: ${YFINANCE_ENABLED:-1}", stack_text)
        self.assertIn("TRADIER_ENABLED: ${TRADIER_ENABLED:-0}", stack_text)
        self.assertIn("AUTO_PIPELINE: ${AUTO_PIPELINE:-0}", stack_text)
        self.assertIn("AUTO_PIPELINE_START_DELAY_S: ${AUTO_PIPELINE_START_DELAY_S:-90}", stack_text)
        self.assertIn("MODEL_FEATURE_SNAPSHOT_SLEEP_S: ${MODEL_FEATURE_SNAPSHOT_SLEEP_S:-60}", stack_text)
        self.assertIn("INFERENCE_HEALTH_PROBE_SYMBOLS: ${INFERENCE_HEALTH_PROBE_SYMBOLS:-AMD}", stack_text)
        self.assertIn("INFERENCE_HEALTH_PROBE_INTERVAL_S: ${INFERENCE_HEALTH_PROBE_INTERVAL_S:-20}", stack_text)
        self.assertIn("KILL_SWITCH_GLOBAL: ${KILL_SWITCH_GLOBAL:-1}", stack_text)
        self.assertIn("LIVE_TRADING_CONFIRM: ${LIVE_TRADING_CONFIRM:-0}", stack_text)
        self.assertIn("LIVE_TRADING_CONFIRM=0", env_example_text)
        self.assertIn("BROKER_NAME: ${BROKER_NAME:-sim}", stack_text)
        self.assertIn("BROKER_FAILOVER: ${BROKER_FAILOVER:-sim}", stack_text)

    def test_operator_package_lock_matches_manifest_dependencies(self) -> None:
        manifest = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))
        lock = json.loads((REPO_ROOT / "package-lock.json").read_text(encoding="utf-8"))

        manifest_dependencies = manifest.get("dependencies", {})
        locked_root_dependencies = lock.get("packages", {}).get("", {}).get("dependencies", {})

        self.assertEqual(manifest_dependencies, locked_root_dependencies)


if __name__ == "__main__":
    unittest.main()
