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
        self.assertIn("offline-worker:", text)
        self.assertIn("operator:", text)
        self.assertIn("x-trading-logging:", text)
        self.assertIn('driver: "${DOCKER_LOG_DRIVER:-local}"', text)
        self.assertEqual(text.count("logging: *trading-logging"), 3)
        self.assertIn("RUNTIME_WORKLOAD_PROFILE: ${RUNTIME_WORKLOAD_PROFILE:-live}", text)
        self.assertIn("PROD_LOCK: ${PROD_LOCK:-1}", text)
        self.assertIn("ALLOW_TRAINING: ${ALLOW_TRAINING:-0}", text)
        self.assertIn("OFFLINE_TRAINING_LIVE_PROFILE_ACK: ${OFFLINE_TRAINING_LIVE_PROFILE_ACK:-}", text)
        self.assertIn("MODEL_TRAIN_N_JOBS: ${MODEL_TRAIN_N_JOBS:-1}", text)
        self.assertIn("TSFRESH_N_JOBS: ${TSFRESH_N_JOBS:-0}", text)
        self.assertIn("TSFRESH_SNAPSHOT_SYMBOL_LIMIT: ${TSFRESH_SNAPSHOT_SYMBOL_LIMIT:-100}", text)
        self.assertIn("TSFRESH_SNAPSHOT_BATCH_SIZE: ${TSFRESH_SNAPSHOT_BATCH_SIZE:-25}", text)
        self.assertIn("TUNE_N_TRIALS: ${TUNE_N_TRIALS:-10}", text)
        self.assertIn("TRADING_IMPORT_SMOKE_IMPORT_JOBS: ${TRADING_IMPORT_SMOKE_IMPORT_JOBS:-0}", text)
        self.assertIn("OPERATOR_DISABLE_INTERNAL_ENGINE_START: \"1\"", text)
        self.assertIn("OPERATOR_API_TOKEN: ${OPERATOR_API_TOKEN:?set OPERATOR_API_TOKEN}", text)
        self.assertIn("OPERATOR_SIDECAR_HOST: operator", text)
        self.assertIn("OPERATOR_SIDECAR_INTERNAL_ONLY: \"1\"", text)
        self.assertIn("cpus: ${RUNTIME_CPUS:-12}", text)
        self.assertIn("mem_limit: ${RUNTIME_MEM_LIMIT:-48g}", text)
        self.assertIn("shm_size: ${RUNTIME_SHM_SIZE:-8g}", text)
        self.assertIn("PREFLIGHT_CHECK_RESOURCE_LIMITS: ${PREFLIGHT_CHECK_RESOURCE_LIMITS:-1}", text)
        self.assertIn("TRADING_RESOURCE_MIN_HEADROOM_MEMORY: ${TRADING_RESOURCE_MIN_HEADROOM_MEMORY:-24g}", text)
        self.assertIn("POSTGRES_SHARED_BUFFERS: ${TIMESCALE_SHARED_BUFFERS:-8GB}", text)
        self.assertIn("POSTGRES_MAX_CONNECTIONS: ${TIMESCALE_MAX_CONNECTIONS:-100}", text)
        self.assertIn("OMP_NUM_THREADS: ${RUNTIME_OMP_NUM_THREADS:-8}", text)
        self.assertIn("TORCH_CPU_THREADS: ${TORCH_CPU_THREADS:-8}", text)
        self.assertIn("RUNTIME_HARDWARE_PROFILE: ${RUNTIME_HARDWARE_PROFILE:-cpu}", text)
        self.assertIn("TORCH_DEVICE: ${TORCH_DEVICE:-cpu}", text)
        self.assertIn("EMBED_DEVICE: ${EMBED_DEVICE:-cpu}", text)
        self.assertIn("NLP_DEVICE: ${NLP_DEVICE:-cpu}", text)
        self.assertIn("FINBERT_DEVICE: ${FINBERT_DEVICE:-cpu}", text)
        self.assertIn("TS_FOUNDATION_DEVICE: ${TS_FOUNDATION_DEVICE:-cpu}", text)
        self.assertIn("TORCH_INTEROP_THREADS: ${TORCH_INTEROP_THREADS:-4}", text)
        self.assertIn("NVIDIA_TELEMETRY_ENABLED: ${NVIDIA_TELEMETRY_ENABLED:-0}", text)
        offline_block = text.split("  offline-worker:", 1)[1].split("\n\n  operator:", 1)[0]
        self.assertIn("profiles:", offline_block)
        self.assertIn("- offline", offline_block)
        self.assertIn("cpus: ${OFFLINE_CPUS:-16}", offline_block)
        self.assertIn("mem_limit: ${OFFLINE_MEM_LIMIT:-64g}", offline_block)
        self.assertIn("RUNTIME_WORKLOAD_PROFILE: offline", offline_block)
        self.assertIn("ALLOW_TRAINING: ${OFFLINE_ALLOW_TRAINING:-1}", offline_block)
        self.assertIn("TSFRESH_SNAPSHOT_SYMBOL_LIMIT: ${OFFLINE_TSFRESH_SNAPSHOT_SYMBOL_LIMIT:-5000}", offline_block)
        self.assertIn("TSFRESH_SNAPSHOT_BATCH_SIZE: ${OFFLINE_TSFRESH_SNAPSHOT_BATCH_SIZE:-250}", offline_block)
        self.assertIn("TUNE_N_TRIALS: ${OFFLINE_TUNE_N_TRIALS:-200}", offline_block)
        self.assertIn("TS_PG_DSN: ${OFFLINE_TS_PG_DSN:?set OFFLINE_TS_PG_DSN to an offline clone}", offline_block)
        self.assertIn("cpus: ${OPERATOR_CPUS:-1}", text)
        self.assertIn("mem_limit: ${OPERATOR_MEM_LIMIT:-2g}", text)
        operator_block = text.split("  operator:", 1)[1].split("\n\nnetworks:", 1)[0]
        self.assertNotIn("\n    ports:", operator_block)
        self.assertIn("\n    expose:", operator_block)
        self.assertIn("- \"4001\"", operator_block)
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
        self.assertIn("x-trading-logging:", text)
        self.assertEqual(text.count("logging: *trading-logging"), 4)
        self.assertIn("archive_mode=${TIMESCALE_ARCHIVE_MODE:-on}", text)
        self.assertIn("archive_command=mkdir -p /var/backups/trading/wal", text)
        self.assertIn("archive_timeout=${TIMESCALE_ARCHIVE_TIMEOUT:-60s}", text)
        self.assertIn("cpus: ${TIMESCALE_CPUS:-8}", text)
        self.assertIn("mem_limit: ${TIMESCALE_MEM_LIMIT:-32g}", text)
        self.assertIn("shm_size: ${TIMESCALE_SHM_SIZE:-2g}", text)
        self.assertIn("shared_buffers=${TIMESCALE_SHARED_BUFFERS:-8GB}", text)
        self.assertIn("effective_cache_size=${TIMESCALE_EFFECTIVE_CACHE_SIZE:-22GB}", text)
        self.assertIn("work_mem=${TIMESCALE_WORK_MEM:-48MB}", text)
        self.assertIn("TRADING_BACKUP_ROOT=/var/backups/trading", env_text)
        self.assertIn("TRADING_BACKUP_WAL_DIR=/var/backups/trading/wal", env_text)
        self.assertIn("TIMESCALE_MEM_LIMIT=32g", env_text)
        self.assertIn("TIMESCALE_SHARED_BUFFERS=8GB", env_text)

    def test_external_services_are_resource_bounded(self) -> None:
        external_compose = REPO_ROOT / "deploy" / "compose" / "docker-compose.external-services.yml"
        env_example = REPO_ROOT / "deploy" / "compose" / ".env.example"
        text = external_compose.read_text(encoding="utf-8")
        env_text = env_example.read_text(encoding="utf-8")

        for expected in (
            "cpus: ${REDIS_CPUS:-2}",
            "mem_limit: ${REDIS_MEM_LIMIT:-8g}",
            "REDIS_PASSWORD: ${REDIS_PASSWORD:?set REDIS_PASSWORD}",
            "${REDIS_MAXMEMORY:-6gb}",
            "${REDIS_MAXMEMORY_POLICY:-allkeys-lru}",
            "cpus: ${MINIO_CPUS:-2}",
            "mem_limit: ${MINIO_MEM_LIMIT:-6g}",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, text)

        for expected in (
            "TRADING_RESOURCE_HOST_CPUS=32",
            "TRADING_RESOURCE_HOST_MEMORY=123g",
            "RUNTIME_MEM_LIMIT=48g",
            "RUNTIME_SHM_SIZE=8g",
            "REDIS_MAXMEMORY=6gb",
            "MINIO_MEM_LIMIT=6g",
            "OPERATOR_MEM_LIMIT=2g",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, env_text)

    def test_runtime_compose_mounts_backup_evidence_read_only(self) -> None:
        stack_text = (REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml").read_text(encoding="utf-8")
        env_example_text = (REPO_ROOT / "deploy" / "compose" / ".env.example").read_text(encoding="utf-8")

        self.assertIn("${TRADING_BACKUP_ROOT:-/var/backups/trading}:/var/backups/trading:ro", stack_text)
        self.assertIn("TRADING_BACKUP_ROOT: ${TRADING_BACKUP_ROOT:-/var/backups/trading}", stack_text)
        self.assertIn("DISK_PRESSURE_WARN_FREE_PCT: ${DISK_PRESSURE_WARN_FREE_PCT:-15}", stack_text)
        self.assertIn("BACKUP_ACCOUNTING_DU_TIMEOUT_S: ${BACKUP_ACCOUNTING_DU_TIMEOUT_S:-8}", stack_text)
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
        self.assertIn("BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S: ${BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S:-120}", stack_text)
        self.assertIn("BACKUP_EVIDENCE_REQUIRE_SIGNATURE: ${BACKUP_EVIDENCE_REQUIRE_SIGNATURE:-1}", stack_text)
        self.assertIn("BACKUP_EVIDENCE_HMAC_KEY_FILE: /run/secrets/backup_evidence_hmac_key", stack_text)
        self.assertIn("- backup_evidence_hmac_key", stack_text)
        self.assertIn("file: ${BACKUP_EVIDENCE_HMAC_KEY_FILE:?set BACKUP_EVIDENCE_HMAC_KEY_FILE}", stack_text)

        for key in (
            "PREFLIGHT_REQUIRE_BACKUP_EVIDENCE=1",
            "BACKUP_EVIDENCE_PATH=/var/backups/trading/evidence/latest_backup_restore_evidence.json",
            "BACKUP_EVIDENCE_BASE_BACKUP_MAX_AGE_S=93600",
            "BACKUP_EVIDENCE_RPO_S=120",
            "BACKUP_EVIDENCE_WAL_RPO_S=120",
            "BACKUP_EVIDENCE_RESTORE_DRILL_MAX_AGE_S=7776000",
            "BACKUP_EVIDENCE_RTO_S=1800",
            "BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S=120",
            "BACKUP_EVIDENCE_REQUIRE_SIGNATURE=1",
            "BACKUP_EVIDENCE_HMAC_KEY_FILE=/etc/trading/backup_evidence.hmac.key",
            "DOCKER_LOG_DRIVER=local",
            "DOCKER_LOG_MAX_SIZE=50m",
            "DOCKER_LOG_MAX_FILE=5",
            "DISK_PRESSURE_WARN_FREE_PCT=15",
            "DISK_PRESSURE_WARN_FREE_BYTES=21474836480",
            "DISK_PRESSURE_CRITICAL_FREE_PCT=5",
            "DISK_PRESSURE_CRITICAL_FREE_BYTES=5368709120",
            "BACKUP_ACCOUNTING_DU_TIMEOUT_S=8",
        ):
            with self.subTest(key=key):
                self.assertIn(key, env_example_text)

    def test_logrotate_bounds_runtime_and_container_mounted_logs(self) -> None:
        logrotate = (REPO_ROOT / "deploy" / "logrotate" / "trading-system").read_text(encoding="utf-8")

        self.assertIn("/opt/trading-system/logs/*.log", logrotate)
        self.assertIn("/opt/trading-system/logs/*.jsonl", logrotate)
        self.assertIn("/opt/trading-system/repo/var/log/*.log", logrotate)
        self.assertIn("/opt/trading/app/logs/*.log", logrotate)
        self.assertIn("/opt/trading/app/var/log/*.log", logrotate)
        self.assertIn("/opt/trading/app/boot/*.log", logrotate)
        self.assertIn("/var/lib/docker/volumes/*trading-logs*/_data/*.log", logrotate)
        self.assertIn("/var/lib/docker/volumes/*trading-data*/_data/ai_operator_log.jsonl", logrotate)
        self.assertIn("ingestion.stdout.log", logrotate)
        self.assertIn("data/ai_operator_log.jsonl", logrotate)
        self.assertIn("maxsize 50M", logrotate)
        self.assertIn("rotate 10", logrotate)
        self.assertIn("maxage 21", logrotate)
        self.assertIn("copytruncate", logrotate)

        local_rotate = (REPO_ROOT / "deploy" / "bin" / "rotate_local_logs.sh").read_text(encoding="utf-8")
        self.assertIn("TRADING_LOCAL_LOGROTATE_MAX_SIZE:-50M", local_rotate)
        self.assertIn("TRADING_LOCAL_LOGROTATE_ROTATE:-5", local_rotate)
        self.assertIn("TRADING_LOCAL_LOGROTATE_MAXAGE:-14", local_rotate)
        self.assertIn("copytruncate", local_rotate)

        start_local = (REPO_ROOT / "start_local.sh").read_text(encoding="utf-8")
        self.assertIn("TRADING_LOCAL_LOGROTATE_ENABLED:-1", start_local)
        self.assertIn("rotate_local_logs.sh --quiet", start_local)

        upgrade_unit = (REPO_ROOT / "deploy" / "systemd" / "trading-upgrade.service").read_text(encoding="utf-8")
        self.assertIn("/etc/logrotate.d/trading-system", upgrade_unit)
        self.assertIn("logrotate -d /etc/logrotate.d/trading-system", upgrade_unit)

    def test_backup_scripts_support_compose_docker_pg_tools(self) -> None:
        base_backup = (REPO_ROOT / "ops" / "backup" / "base_backup.sh").read_text(encoding="utf-8")
        restore_drill = (REPO_ROOT / "ops" / "backup" / "restore_drill.sh").read_text(encoding="utf-8")
        installer = (REPO_ROOT / "ops" / "server" / "install_backup_evidence_gate.sh").read_text(encoding="utf-8")

        self.assertIn("TS_BACKUP_DOCKER_IMAGE", base_backup)
        self.assertIn("TS_BACKUP_DOCKER_EXEC_CONTAINER", base_backup)
        self.assertIn("TS_BACKUP_READ_GROUP", base_backup)
        self.assertIn('chown -R "${pg_uid_gid%%:*}:${TS_BACKUP_READ_GROUP}" "$work_dir"', base_backup)
        self.assertIn("TS_RESTORE_DOCKER_IMAGE", restore_drill)
        self.assertTrue((REPO_ROOT / "ops" / "backup" / "accounting.sh").exists())
        accounting = (REPO_ROOT / "ops" / "backup" / "accounting.sh").read_text(encoding="utf-8")
        self.assertIn("backup_accounting", accounting)
        self.assertIn("TRADING_BACKUP_ROOT", accounting)
        self.assertIn("docker inspect", accounting)
        self.assertIn("retention_status=configured", accounting)
        self.assertIn("--compose", installer)
        self.assertIn("install_compose_systemd_overrides", installer)
        self.assertIn("ensure_backup_evidence_hmac_key", installer)
        self.assertIn('set_env_var BACKUP_EVIDENCE_REQUIRE_SIGNATURE "1"', installer)
        self.assertIn('set_env_var BACKUP_EVIDENCE_HMAC_KEY_FILE "$BACKUP_EVIDENCE_HMAC_KEY_FILE"', installer)
        self.assertIn('chown -R "${pg_uid}:${TRADING_GROUP}" "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR"', installer)
        self.assertIn('find "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR" -type f -exec chmod 0640 {} +', installer)
        self.assertIn("set_env_var TS_BACKUP_READ_GROUP", installer)
        self.assertIn("Environment=TS_BACKUP_READ_GROUP=${TRADING_GROUP}", installer)

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
            "OPERATOR_SIDECAR_INTERNAL_ONLY",
            "LIVE_TRADING_REQUIRE_DASHBOARD_API_TOKEN",
            "LIVE_TRADING_REQUIRE_CONFIRMATION",
            "RUNTIME_WORKLOAD_PROFILE",
            "OFFLINE_TRAINING_LIVE_PROFILE_ACK",
            "MODEL_TRAIN_N_JOBS",
            "MODEL_TRAIN_MAX_N_JOBS",
            "META_LABEL_N_JOBS",
            "TSFRESH_N_JOBS",
            "TSFRESH_MAX_N_JOBS",
            "TSFRESH_SNAPSHOT_SYMBOL_LIMIT",
            "TSFRESH_SNAPSHOT_BATCH_SIZE",
            "TUNE_N_TRIALS",
            "TUNE_MAX_N_TRIALS",
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
            "TRADING_DEPENDENCY_PROFILE",
            "RUNTIME_HARDWARE_PROFILE",
            "TORCH_DEVICE",
            "EMBED_DEVICE",
            "NLP_DEVICE",
            "FINBERT_DEVICE",
            "TS_FOUNDATION_DEVICE",
            "TORCH_CPU_THREADS",
            "TORCH_INTEROP_THREADS",
            "NVIDIA_TELEMETRY_ENABLED",
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
        self.assertIn("LIVE_BROKER: ${LIVE_BROKER:-ibkr}", stack_text)
        self.assertIn("BROKER_NAME: ${BROKER_NAME:-ibkr}", stack_text)
        self.assertIn("BROKER: ${BROKER:-ibkr}", stack_text)
        self.assertIn("BROKER_FAILOVER: ${BROKER_FAILOVER:-ibkr}", stack_text)
        self.assertIn("IBKR_HOST: ${IBKR_HOST:-host.docker.internal}", stack_text)
        self.assertIn("IBKR_PORT: ${IBKR_PORT:-7497}", stack_text)
        self.assertIn("IBKR_CLIENT_ID: ${IBKR_CLIENT_ID:-42}", stack_text)
        self.assertIn("host.docker.internal:host-gateway", stack_text)
        self.assertIn("IBKR_HOST=host.docker.internal", env_example_text)
        self.assertIn("IBKR_PORT=7497", env_example_text)
        self.assertIn("IBKR_CLIENT_ID=42", env_example_text)
        self.assertIn("TRADING_DEPENDENCY_PROFILE=cpu", env_example_text)
        self.assertIn("TORCH_DEVICE=cpu", env_example_text)
        self.assertIn("EMBED_DEVICE=cpu", env_example_text)
        self.assertIn("NLP_DEVICE=cpu", env_example_text)
        self.assertIn("FINBERT_DEVICE=cpu", env_example_text)
        self.assertIn("TS_FOUNDATION_DEVICE=cpu", env_example_text)

    def test_operator_package_lock_matches_manifest_dependencies(self) -> None:
        manifest = json.loads((REPO_ROOT / "package.json").read_text(encoding="utf-8"))
        lock = json.loads((REPO_ROOT / "package-lock.json").read_text(encoding="utf-8"))

        manifest_dependencies = manifest.get("dependencies", {})
        locked_root_dependencies = lock.get("packages", {}).get("", {}).get("dependencies", {})

        self.assertEqual(manifest_dependencies, locked_root_dependencies)


if __name__ == "__main__":
    unittest.main()
