from __future__ import annotations

import json

from engine.runtime.effective_runtime_state import effective_runtime_state_snapshot, main, redact_evidence
from engine.runtime.postgres_tuning import parse_size_bytes


def _env() -> dict[str, str]:
    return {
        "PREFLIGHT_REQUIRE_DOCKER_RUNTIME_EVIDENCE": "1",
        "TRADING_RUNTIME_DATA": "/auxpool/trading/runtime/data",
        "TRADING_RUNTIME_LOGS": "/auxpool/trading/runtime/logs",
        "TRADING_BACKUP_ROOT": "/var/backups/trading",
        "TRADING_TIMESCALE_DATA": "/dbpool/trading/timescaledb/data",
        "TRADING_REDIS_DATA": "/auxpool/trading/redis",
        "TRADING_MINIO_DATA": "/auxpool/trading/minio",
        "RUNTIME_CPUS": "12",
        "RUNTIME_MEM_LIMIT": "48g",
        "RUNTIME_MEMSWAP_LIMIT": "48g",
        "RUNTIME_SHM_SIZE": "8g",
        "TIMESCALE_CPUS": "8",
        "TIMESCALE_MEM_LIMIT": "32g",
        "TIMESCALE_MEMSWAP_LIMIT": "32g",
        "TIMESCALE_SHM_SIZE": "2g",
        "REDIS_CPUS": "2",
        "REDIS_MEM_LIMIT": "8g",
        "REDIS_MEMSWAP_LIMIT": "8g",
        "REDIS_MAXMEMORY": "6gb",
        "REDIS_MAXMEMORY_POLICY": "allkeys-lru",
        "MINIO_CPUS": "2",
        "MINIO_MEM_LIMIT": "6g",
        "MINIO_MEMSWAP_LIMIT": "6g",
        "OPERATOR_CPUS": "1",
        "OPERATOR_MEM_LIMIT": "2g",
        "OPERATOR_MEMSWAP_LIMIT": "2g",
        "DOCKER_LOG_DRIVER": "local",
        "DOCKER_LOG_MAX_SIZE": "50m",
        "DOCKER_LOG_MAX_FILE": "5",
    }


def _inspect_item(
    *,
    service: str,
    container: str,
    cpus: str,
    memory: str,
    memswap: str,
    mounts: dict[str, str],
    ports: dict[str, list[dict[str, str]]] | None = None,
    shm: str | None = None,
) -> dict:
    return {
        "Name": f"/{container}",
        "Config": {
            "Labels": {"com.docker.compose.service": service},
            "Env": [
                "DASHBOARD_API_TOKEN=should-not-leak",
                "TIMESCALE_DSN=postgresql://trading:secret@timescaledb:5432/trading",
            ],
        },
        "HostConfig": {
            "NanoCpus": int(float(cpus) * 1_000_000_000),
            "Memory": parse_size_bytes(memory),
            "MemorySwap": parse_size_bytes(memswap),
            "ShmSize": parse_size_bytes(shm or "64m"),
            "LogConfig": {"Type": "local", "Config": {"max-size": "50m", "max-file": "5"}},
            "PortBindings": ports or {},
        },
        "Mounts": [
            {"Type": "bind", "Source": source, "Destination": dest, "RW": True}
            for dest, source in mounts.items()
        ],
    }


def _good_inspect() -> list[dict]:
    return [
        _inspect_item(
            service="runtime",
            container="trading-runtime",
            cpus="12",
            memory="48g",
            memswap="48g",
            shm="8g",
            ports={"8000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "8000"}]},
            mounts={
                "/app/data": "/auxpool/trading/runtime/data",
                "/app/logs": "/auxpool/trading/runtime/logs",
                "/var/backups/trading": "/var/backups/trading",
            },
        ),
        _inspect_item(
            service="timescaledb",
            container="trading-timescaledb",
            cpus="8",
            memory="32g",
            memswap="32g",
            shm="2g",
            ports={"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "5432"}]},
            mounts={
                "/var/lib/postgresql/data": "/dbpool/trading/timescaledb/data",
                "/var/backups/trading": "/var/backups/trading",
            },
        ),
        _inspect_item(
            service="redis",
            container="trading-redis",
            cpus="2",
            memory="8g",
            memswap="8g",
            ports={"6379/tcp": [{"HostIp": "127.0.0.1", "HostPort": "6379"}]},
            mounts={"/data": "/auxpool/trading/redis"},
        ),
        _inspect_item(
            service="minio",
            container="trading-minio",
            cpus="2",
            memory="6g",
            memswap="6g",
            ports={
                "9000/tcp": [{"HostIp": "127.0.0.1", "HostPort": "9000"}],
                "9001/tcp": [{"HostIp": "127.0.0.1", "HostPort": "9001"}],
            },
            mounts={"/data": "/auxpool/trading/minio"},
        ),
        _inspect_item(
            service="operator",
            container="trading-operator",
            cpus="1",
            memory="2g",
            memswap="2g",
            mounts={
                "/app/data": "/auxpool/trading/runtime/data",
                "/app/logs": "/auxpool/trading/runtime/logs",
            },
        ),
    ]


def test_effective_runtime_state_accepts_matching_docker_and_redis_evidence() -> None:
    snapshot = effective_runtime_state_snapshot(
        _env(),
        required=True,
        docker_inspect=_good_inspect(),
        redis_config={"maxmemory": str(parse_size_bytes("6gb")), "maxmemory-policy": "allkeys-lru"},
    )

    assert snapshot["ok"] is True
    assert snapshot["errors"] == []
    assert snapshot["docker"]["services"]["timescaledb"]["actual"]["memory_gib"] == 32.0


def test_effective_runtime_state_rejects_unbounded_container_memory() -> None:
    evidence = _good_inspect()
    evidence[0]["HostConfig"]["Memory"] = 0

    snapshot = effective_runtime_state_snapshot(
        _env(),
        required=True,
        docker_inspect=evidence,
        redis_config={"maxmemory": str(parse_size_bytes("6gb")), "maxmemory-policy": "allkeys-lru"},
    )

    assert snapshot["ok"] is False
    assert any("docker runtime memory limit drift service=runtime" in item for item in snapshot["errors"])


def test_effective_runtime_state_rejects_redis_maxmemory_drift() -> None:
    snapshot = effective_runtime_state_snapshot(
        _env(),
        required=True,
        docker_inspect=_good_inspect(),
        redis_config="maxmemory\n0\nmaxmemory-policy\nnoeviction\n",
    )

    assert snapshot["ok"] is False
    assert any("redis effective maxmemory drift" in item for item in snapshot["errors"])
    assert any("maxmemory-policy drift" in item for item in snapshot["errors"])


def test_effective_runtime_state_missing_required_evidence_returns_operator_commands() -> None:
    snapshot = effective_runtime_state_snapshot(_env(), required=True, docker_inspect=[], redis_config={})

    assert snapshot["ok"] is False
    assert "docker_runtime_evidence_missing" in snapshot["errors"]
    commands = "\n".join(str(row.get("command")) for row in snapshot["operator_commands"])
    assert "sudo docker inspect trading-runtime trading-timescaledb trading-redis trading-minio trading-operator" in commands
    assert "redis-cli" in commands
    assert "/run/secrets/redis_password" in commands


def test_effective_runtime_state_redaction_uses_canonical_api_redactor() -> None:
    payload = {
        "DASHBOARD_API_TOKEN": "live-token",
        "master_key": "abc123",
        "session_token": "xyz789",
        "dsn": "postgresql://trading:secret@timescaledb:5432/trading",
        "pg_dsn": "postgresql://u:p@h:5432/db",
        "broker_account_number": "U1234567",
        "message": "password=secret token=live-token",
    }

    rendered = json.dumps(redact_evidence(payload), sort_keys=True)

    assert "live-token" not in rendered
    assert "abc123" not in rendered
    assert "xyz789" not in rendered
    assert "password=secret" not in rendered
    assert ":secret@" not in rendered
    assert "p@h" not in rendered
    assert "U1234567" not in rendered
    assert "<redacted" in rendered


def test_effective_runtime_state_cli_honors_json_flag(capsys) -> None:
    rc = main(["--json", "--required"])

    captured = capsys.readouterr().out
    assert rc == 3
    assert captured.startswith("{")
    assert "docker_runtime_evidence_missing" in captured
