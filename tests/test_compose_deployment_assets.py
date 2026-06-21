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
        self.assertIn("OPERATOR_API_TOKEN_FILE: /run/secrets/operator_api_token", text)
        self.assertIn("DASHBOARD_API_TOKEN_FILE: /run/secrets/dashboard_api_token", text)
        self.assertIn("DASHBOARD_BIND_CONTEXT: container_internal", text)
        self.assertIn("${DASHBOARD_DANGEROUS_PUBLIC_BIND_HOST:-127.0.0.1}:${DASHBOARD_PUBLIC_PORT:-8000}:8000", text)
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
        self.assertIn("OBJECT_STORE_ACCESS_KEY_FILE: /run/secrets/minio_root_user", offline_block)
        self.assertIn("OBJECT_STORE_SECRET_KEY_FILE: /run/secrets/minio_root_password", offline_block)
        self.assertNotIn("OBJECT_STORE_ACCESS_KEY: ${OFFLINE_OBJECT_STORE_ACCESS_KEY", offline_block)
        self.assertNotIn("OBJECT_STORE_SECRET_KEY: ${OFFLINE_OBJECT_STORE_SECRET_KEY", offline_block)
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

        self.assertIn("${TRADING_TIMESCALE_DATA:?set TRADING_TIMESCALE_DATA to a ZFS-backed host path}:/var/lib/postgresql/data", text)
        self.assertIn("${TRADING_BACKUP_ROOT:?set TRADING_BACKUP_ROOT to the ZFS-backed backup mount}:/var/backups/trading", text)
        self.assertIn("${TRADING_REDIS_DATA:?set TRADING_REDIS_DATA to a ZFS-backed host path}:/data", text)
        self.assertIn("${TRADING_MINIO_DATA:?set TRADING_MINIO_DATA to a ZFS-backed host path}:/data", text)
        self.assertNotIn("timescaledb-data:/var/lib/postgresql/data", text)
        self.assertNotIn("redis-data:/data", text)
        self.assertNotIn("minio-data:/data", text)
        self.assertIn("x-trading-logging:", text)
        self.assertEqual(text.count("logging: *trading-logging"), 4)
        self.assertIn("archive_mode=${TIMESCALE_ARCHIVE_MODE:-on}", text)
        self.assertIn("TS_BACKUP_ROOT: /var/backups/trading", text)
        self.assertIn('TS_WAL_ARCHIVE_REQUIRE_MOUNT: "1"', text)
        self.assertIn("${TRADING_WAL_ARCHIVE_SCRIPT:-../../ops/backup/wal_archive.sh}:/opt/trading/ops/backup/wal_archive.sh:ro", text)
        self.assertIn(
            "${TRADING_WAL_ARCHIVE_CATCHUP_SCRIPT:-../../ops/backup/wal_archive_catchup.sh}:/opt/trading/ops/backup/wal_archive_catchup.sh:ro",
            text,
        )
        self.assertIn('archive_command=${TIMESCALE_ARCHIVE_COMMAND:-/opt/trading/ops/backup/wal_archive.sh "%p" "%f"}', text)
        self.assertNotIn("archive_command=mkdir -p /var/backups/trading/wal", text)
        self.assertNotIn("cp %p /var/backups/trading/wal/%f", text)
        self.assertIn("archive_timeout=${TIMESCALE_ARCHIVE_TIMEOUT:-60s}", text)
        self.assertIn("cpus: ${TIMESCALE_CPUS:-8}", text)
        self.assertIn("mem_limit: ${TIMESCALE_MEM_LIMIT:-32g}", text)
        self.assertIn("shm_size: ${TIMESCALE_SHM_SIZE:-2g}", text)
        self.assertIn("shared_buffers=${TIMESCALE_SHARED_BUFFERS:-8GB}", text)
        self.assertIn("effective_cache_size=${TIMESCALE_EFFECTIVE_CACHE_SIZE:-22GB}", text)
        self.assertIn("work_mem=${TIMESCALE_WORK_MEM:-48MB}", text)
        self.assertIn("TRADING_BACKUP_ROOT=/var/backups/trading", env_text)
        self.assertIn("TRADING_BACKUP_WAL_DIR=/var/backups/trading/wal", env_text)
        self.assertIn("PREFLIGHT_REQUIRE_ZFS_STORAGE=1", env_text)
        self.assertIn("PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS=1", env_text)
        self.assertIn("TRADING_ZFS_ROOT=/zpool", env_text)
        self.assertIn("TRADING_TIMESCALE_DATA=/zpool/trading/timescaledb/data", env_text)
        self.assertIn("TRADING_REDIS_DATA=/zpool/trading/redis/data", env_text)
        self.assertIn("TRADING_MINIO_DATA=/zpool/trading/minio/data", env_text)
        self.assertIn("TRADING_WAL_ARCHIVE_SCRIPT=../../ops/backup/wal_archive.sh", env_text)
        self.assertIn("TRADING_WAL_ARCHIVE_CATCHUP_SCRIPT=../../ops/backup/wal_archive_catchup.sh", env_text)
        self.assertIn('TIMESCALE_ARCHIVE_COMMAND=/opt/trading/ops/backup/wal_archive.sh "%p" "%f"', env_text)
        self.assertIn("TIMESCALE_MEM_LIMIT=32g", env_text)
        self.assertIn("TIMESCALE_SHARED_BUFFERS=8GB", env_text)
        self.assertIn("TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1", env_text)
        self.assertIn("TIMESCALE_ALLOW_DANGEROUS_PUBLIC_BIND=0", env_text)
        self.assertIn("${TIMESCALE_DANGEROUS_PUBLIC_BIND_HOST:-127.0.0.1}:${TIMESCALE_PORT:-5432}:5432", text)
        self.assertNotIn("POSTGRES_PASSWORD:", text)
        self.assertIn("POSTGRES_PASSWORD_FILE: /run/secrets/timescale_password", text)
        self.assertIn("file: ${TIMESCALE_PASSWORD_FILE:?set TIMESCALE_PASSWORD_FILE}", text)
        stack_text = (REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml").read_text(encoding="utf-8")
        self.assertIn("TS_PG_PASSWORD_FILE: /run/secrets/timescale_password", stack_text)
        self.assertIn("TIMESCALE_DSN: postgresql://${TIMESCALE_USER:?set TIMESCALE_USER}@timescaledb:5432/${TIMESCALE_DB:?set TIMESCALE_DB}", stack_text)
        self.assertNotIn("password=${TIMESCALE_PASSWORD", stack_text)
        self.assertNotIn(":${TIMESCALE_PASSWORD", stack_text)
        self.assertIn("REDIS_URL: redis://redis:6379/0", stack_text)
        self.assertNotIn("redis://:${REDIS_PASSWORD", stack_text)

    def test_external_services_are_resource_bounded(self) -> None:
        external_compose = REPO_ROOT / "deploy" / "compose" / "docker-compose.external-services.yml"
        env_example = REPO_ROOT / "deploy" / "compose" / ".env.example"
        text = external_compose.read_text(encoding="utf-8")
        env_text = env_example.read_text(encoding="utf-8")

        for expected in (
            "cpus: ${REDIS_CPUS:-2}",
            "mem_limit: ${REDIS_MEM_LIMIT:-8g}",
            "redis_password:",
            "file: ${REDIS_PASSWORD_FILE:?set REDIS_PASSWORD_FILE}",
            'redis-cli -a "$$(cat /run/secrets/redis_password)" ping',
            "${REDIS_DANGEROUS_PUBLIC_BIND_HOST:-127.0.0.1}:${REDIS_PORT:-6379}:6379",
            "${REDIS_MAXMEMORY:-6gb}",
            "${REDIS_MAXMEMORY_POLICY:-allkeys-lru}",
            "cpus: ${MINIO_CPUS:-2}",
            "mem_limit: ${MINIO_MEM_LIMIT:-6g}",
            "MINIO_ROOT_USER_FILE: /run/secrets/minio_root_user",
            "MINIO_ROOT_PASSWORD_FILE: /run/secrets/minio_root_password",
            "${MINIO_DANGEROUS_PUBLIC_BIND_HOST:-127.0.0.1}:${MINIO_PORT:-9000}:9000",
            "${MINIO_CONSOLE_DANGEROUS_PUBLIC_BIND_HOST:-127.0.0.1}:${MINIO_CONSOLE_PORT:-9001}:9001",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, text)

        for expected in (
            "TRADING_RESOURCE_HOST_CPUS=32",
            "TRADING_RESOURCE_HOST_MEMORY=123g",
            "RUNTIME_MEM_LIMIT=48g",
            "RUNTIME_SHM_SIZE=8g",
            "REDIS_MAXMEMORY=6gb",
            "REDIS_PASSWORD_FILE=../../data/secrets/redis_password",
            "REDIS_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1",
            "REDIS_ALLOW_DANGEROUS_PUBLIC_BIND=0",
            "MINIO_MEM_LIMIT=6g",
            "MINIO_ROOT_USER_FILE=../../data/secrets/minio_root_user",
            "MINIO_ROOT_PASSWORD_FILE=../../data/secrets/minio_root_password",
            "MINIO_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1",
            "MINIO_ALLOW_DANGEROUS_PUBLIC_BIND=0",
            "MINIO_CONSOLE_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1",
            "MINIO_CONSOLE_ALLOW_DANGEROUS_PUBLIC_BIND=0",
            "OPERATOR_MEM_LIMIT=2g",
            "DASHBOARD_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1",
            "DASHBOARD_ALLOW_DANGEROUS_PUBLIC_BIND=0",
            "OPERATOR_DANGEROUS_PUBLIC_BIND_HOST=127.0.0.1",
            "OPERATOR_ALLOW_DANGEROUS_PUBLIC_BIND=0",
        ):
            with self.subTest(expected=expected):
                self.assertIn(expected, env_text)

    def test_runtime_compose_mounts_backup_evidence_read_only(self) -> None:
        stack_text = (REPO_ROOT / "deploy" / "compose" / "docker-compose.stack.yml").read_text(encoding="utf-8")
        env_example_text = (REPO_ROOT / "deploy" / "compose" / ".env.example").read_text(encoding="utf-8")

        self.assertIn("${TRADING_RUNTIME_DATA:?set TRADING_RUNTIME_DATA to a ZFS-backed host path}:/app/data", stack_text)
        self.assertIn("${TRADING_RUNTIME_LOGS:?set TRADING_RUNTIME_LOGS to a ZFS-backed host path}:/app/logs", stack_text)
        self.assertIn("${TRADING_BACKUP_ROOT:?set TRADING_BACKUP_ROOT to the ZFS-backed backup mount}:/var/backups/trading:ro", stack_text)
        self.assertIn("TRADING_BACKUP_ROOT: ${TRADING_BACKUP_ROOT:?set TRADING_BACKUP_ROOT to the ZFS-backed backup mount}", stack_text)
        self.assertIn("TRADING_BACKUP_WAL_DIR: ${TRADING_BACKUP_WAL_DIR:?set TRADING_BACKUP_WAL_DIR to the ZFS-backed backup WAL path}", stack_text)
        self.assertIn("PREFLIGHT_REQUIRE_ZFS_STORAGE: ${PREFLIGHT_REQUIRE_ZFS_STORAGE:-1}", stack_text)
        self.assertIn("PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS: ${PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS:-1}", stack_text)
        self.assertNotIn("trading-data:/app/data", stack_text)
        self.assertNotIn("trading-logs:/app/logs", stack_text)
        self.assertIn("DISK_PRESSURE_WARN_FREE_PCT: ${DISK_PRESSURE_WARN_FREE_PCT:-15}", stack_text)
        self.assertIn("PREFLIGHT_REQUIRE_PG_WAL_RISK: ${PREFLIGHT_REQUIRE_PG_WAL_RISK:-1}", stack_text)
        self.assertIn("PREFLIGHT_PG_WAL_CRITICAL_BYTES: ${PREFLIGHT_PG_WAL_CRITICAL_BYTES:-40g}", stack_text)
        self.assertIn(
            "PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT: ${PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT:-16}",
            stack_text,
        )
        self.assertIn("BACKUP_ACCOUNTING_DU_TIMEOUT_S: ${BACKUP_ACCOUNTING_DU_TIMEOUT_S:-8}", stack_text)
        self.assertIn('TIMESCALE_ARCHIVE_COMMAND: ${TIMESCALE_ARCHIVE_COMMAND:-/opt/trading/ops/backup/wal_archive.sh "%p" "%f"}', stack_text)
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
            'TIMESCALE_ARCHIVE_COMMAND=/opt/trading/ops/backup/wal_archive.sh "%p" "%f"',
            "DOCKER_LOG_DRIVER=local",
            "DOCKER_LOG_MAX_SIZE=50m",
            "DOCKER_LOG_MAX_FILE=5",
            "DISK_PRESSURE_WARN_FREE_PCT=15",
            "DISK_PRESSURE_WARN_FREE_BYTES=21474836480",
            "DISK_PRESSURE_CRITICAL_FREE_PCT=5",
            "DISK_PRESSURE_CRITICAL_FREE_BYTES=5368709120",
            "PREFLIGHT_REQUIRE_PG_WAL_RISK=1",
            "PREFLIGHT_PG_WAL_WARN_BYTES=30g",
            "PREFLIGHT_PG_WAL_CRITICAL_BYTES=40g",
            "PREFLIGHT_PG_WAL_READY_WARN_COUNT=4",
            "PREFLIGHT_PG_WAL_READY_CRITICAL_COUNT=16",
            "PREFLIGHT_PG_WAL_WARN_FREE_BYTES=21474836480",
            "PREFLIGHT_PG_WAL_CRITICAL_FREE_BYTES=5368709120",
            "BACKUP_ACCOUNTING_DU_TIMEOUT_S=8",
            "TRADING_RUNTIME_DATA=/zpool/trading/runtime/data",
            "TRADING_RUNTIME_LOGS=/zpool/trading/runtime/logs",
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
        self.assertIn("/zpool/trading/runtime/logs/*.log", logrotate)
        self.assertIn("/zpool/trading/runtime/data/ai_operator_log.jsonl", logrotate)
        self.assertNotIn("/var/lib/docker/volumes/*trading-logs*/_data/*.log", logrotate)
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
        self.assertIn("normalize_compose_storage_env", installer)
        self.assertIn("ensure_compose_env_mount_source TRADING_TIMESCALE_DATA", installer)
        self.assertIn("ensure_compose_env_mount_source TRADING_BACKUP_ROOT", installer)
        self.assertIn("refusing to move storage", installer)
        self.assertIn('set_env_var BACKUP_EVIDENCE_REQUIRE_SIGNATURE "1"', installer)
        self.assertIn('set_env_var BACKUP_EVIDENCE_HMAC_KEY_FILE "$BACKUP_EVIDENCE_HMAC_KEY_FILE"', installer)
        self.assertIn('set_env_var TS_BACKUP_EVIDENCE_RUN_BASE_BACKUP "0"', installer)
        self.assertIn('set_env_var TS_BACKUP_EVIDENCE_RUN_RESTORE_DRILL "0"', installer)
        self.assertIn('set_env_var TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP "0"', installer)
        self.assertIn('set_compose_env_var TRADING_WAL_ARCHIVE_SCRIPT "${BACKUP_SCRIPT_DST_DIR}/wal_archive.sh"', installer)
        self.assertIn(
            'set_compose_env_var TRADING_WAL_ARCHIVE_CATCHUP_SCRIPT "${BACKUP_SCRIPT_DST_DIR}/wal_archive_catchup.sh"',
            installer,
        )
        self.assertIn('set_compose_env_var TIMESCALE_ARCHIVE_COMMAND "/opt/trading/ops/backup/wal_archive.sh \\"%p\\" \\"%f\\""', installer)
        self.assertIn('grep -q \'wal_archive.sh "%p" "%f"\' "$COMPOSE_EXTERNAL_FILE"', installer)
        self.assertIn("run_compose_archive_selftest", installer)
        self.assertIn("run_compose_wal_catchup", installer)
        self.assertIn("up -d --no-deps --force-recreate timescaledb", installer)
        self.assertIn('chown -R "${pg_uid}:${TRADING_GROUP}" "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR"', installer)
        self.assertIn('find "$BACKUP_BASE_DIR" "$BACKUP_WAL_DIR" "$BACKUP_DRILL_DIR" -type f -exec chmod 0640 {} +', installer)
        self.assertIn("set_env_var TS_BACKUP_READ_GROUP", installer)
        self.assertIn("Environment=TS_BACKUP_READ_GROUP=${TRADING_GROUP}", installer)
        self.assertIn("Environment=TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP=0", installer)
        self.assertIn("Environment=TS_BACKUP_EVIDENCE_WAL_CATCHUP_TIMEOUT_S=300", installer)
        self.assertIn("Restart=no", installer)
        self.assertIn("TS_BACKUP_EVIDENCE_RUN_WAL_CATCHUP=1", installer)

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
            "TRADING_CLOCK_MAX_SKEW_MS",
            "TRADING_CLOCK_REQUIRED_SOURCES",
            "TRADING_CLOCK_HTTPS_TIME_URLS",
            "TRADING_CLOCK_REQUIRED_TIMEZONE",
            "TRADING_CLOCK_CHECK_TIMEOUT_S",
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
        self.assertIn("POLYGON_API_KEY_FILE: /run/secrets/polygon_api_key", stack_text)
        self.assertIn("YFINANCE_ENABLED: ${YFINANCE_ENABLED:-1}", stack_text)
        self.assertIn("TRADIER_ENABLED: ${TRADIER_ENABLED:-0}", stack_text)
        self.assertIn("TRADIER_API_TOKEN_FILE: /run/secrets/tradier_api_token", stack_text)
        self.assertIn("ALPACA_KEY_ID_FILE: /run/secrets/alpaca_key_id", stack_text)
        self.assertIn("ALPACA_SECRET_KEY_FILE: /run/secrets/alpaca_secret_key", stack_text)
        self.assertNotIn("POLYGON_API_KEY: ${POLYGON_API_KEY", stack_text)
        self.assertNotIn("TRADIER_API_TOKEN: ${TRADIER_API_TOKEN", stack_text)
        self.assertNotIn("ALPACA_KEY_ID: ${ALPACA_KEY_ID", stack_text)
        self.assertNotIn("ALPACA_SECRET_KEY: ${ALPACA_SECRET_KEY", stack_text)
        self.assertIn("AUTO_PIPELINE: ${AUTO_PIPELINE:-0}", stack_text)
        self.assertIn("AUTO_PIPELINE_START_DELAY_S: ${AUTO_PIPELINE_START_DELAY_S:-90}", stack_text)
        self.assertIn("MODEL_FEATURE_SNAPSHOT_SLEEP_S: ${MODEL_FEATURE_SNAPSHOT_SLEEP_S:-60}", stack_text)
        self.assertIn("INFERENCE_HEALTH_PROBE_SYMBOLS: ${INFERENCE_HEALTH_PROBE_SYMBOLS:-AMD}", stack_text)
        self.assertIn("INFERENCE_HEALTH_PROBE_INTERVAL_S: ${INFERENCE_HEALTH_PROBE_INTERVAL_S:-20}", stack_text)
        self.assertIn("KILL_SWITCH_GLOBAL: ${KILL_SWITCH_GLOBAL:-1}", stack_text)
        self.assertIn("LIVE_TRADING_CONFIRM: ${LIVE_TRADING_CONFIRM:-0}", stack_text)
        self.assertIn("LIVE_TRADING_CONFIRM=0", env_example_text)
        self.assertIn("TRADING_CLOCK_MAX_SKEW_MS: ${TRADING_CLOCK_MAX_SKEW_MS:-2000}", stack_text)
        self.assertIn("TRADING_CLOCK_REQUIRED_SOURCES=system_or_https", env_example_text)
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
