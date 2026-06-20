"""
Canonical repository validation entrypoint.

Runs the deterministic checks that should pass on a clean workstation and in CI.
Use ``--live`` to include runtime-coupled smoke checks against a running operator
and engine instance.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
_LOCAL_RUNTIME_EXTERNAL_SERVICE_POP_KEYS = (
    "TS_PG_DSN",
    "TS_PG_DSN_FILE",
    "PG_DSN",
    "TIMESCALE_DSN",
    "TIMESCALE_URL",
    "TIMESCALE_DATABASE_URL",
    "TIMESCALE_PRICES_DSN",
    "TIMESCALE_PRICES_URL",
    "TIMESCALE_PRICES_DATABASE_URL",
    "LIVE_CACHE_REDIS_URL",
    "REDIS_URL",
    "REDIS_CACHE_URL",
    "TS_REDIS_URL",
    "OBJECT_STORE_ENDPOINT",
    "OBJECT_STORE_BUCKET",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_SECRET_KEY",
    "OBJECT_STORE_SESSION_TOKEN",
    "MINIO_ENDPOINT",
    "MINIO_BUCKET",
    "MINIO_ACCESS_KEY",
    "MINIO_SECRET_KEY",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
)
_LOCAL_RUNTIME_EXTERNAL_SERVICE_DEFAULTS = {
    "TS_STORAGE_BACKEND": "sqlite",
    "TIMESCALE_ENABLED": "0",
    "TIMESCALE_PRICES_ENABLED": "0",
    "TELEMETRY_READ_BACKEND": "sqlite",
    "PRICE_READ_BACKEND": "sqlite",
    "LIVE_CACHE_BACKEND": "memory",
    "PREFLIGHT_REQUIRE_TIMESCALE": "0",
    "PREFLIGHT_REQUIRE_REDIS": "0",
    "PREFLIGHT_REQUIRE_OBJECT_STORAGE": "0",
}

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run(label: str, args: list[str], env: dict[str, str]) -> None:
    print(f"\n=== {label} ===")
    print(" ".join(args))
    run_env = env
    if label in {"unit-tests", "pytest-tests"}:
        run_env = _unit_test_env(env)
    elif label == "runtime-graph-startup":
        run_env = _runtime_graph_startup_env(env)
    subprocess.run(args, check=True, cwd=str(ROOT), env=run_env)


def _cleanup_validation_pg_schemas(env: dict[str, str]) -> None:
    if _env_truthy(env.get("TRADING_KEEP_VALIDATION_PG_SCHEMAS")):
        return
    if not _production_dependency_validation(env):
        return
    if not str(env.get("TS_PG_DSN") or "").strip():
        return
    previous = dict(os.environ)
    try:
        os.environ.update(env)
        from tools.validation_pg_cleanup import cleanup_validation_schemas, schema_for_db_path

        current_db_path = str(env.get("DB_PATH") or "").strip()
        exclude = [schema_for_db_path(current_db_path)] if current_db_path else []
        result = cleanup_validation_schemas(exclude=exclude)
        dropped = list(result.get("dropped") or [])
        if dropped:
            print("\n=== validation-pg-schema-cleanup ===")
            print(f"dropped {len(dropped)} hashed validation schema(s)")
    except Exception as exc:
        print(f"\nWARNING validation-pg-schema-cleanup failed: {type(exc).__name__}: {exc}")
    finally:
        os.environ.clear()
        os.environ.update(previous)


def _project_python() -> str:
    if os.name == "nt":
        candidate = ROOT / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = ROOT / ".venv" / "bin" / "python"
    if candidate.exists():
        return str(candidate)
    return sys.executable


def _project_pytest(python: str) -> list[str]:
    if os.name == "nt":
        candidate = ROOT / ".venv" / "Scripts" / "pytest.exe"
    else:
        candidate = ROOT / ".venv" / "bin" / "pytest"
    if candidate.exists():
        return [str(candidate)]
    return [python, "-m", "pytest"]


def _env_truthy(value: str | None) -> bool:
    raw = str(value or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _production_dependency_validation(env: dict[str, str]) -> bool:
    return bool(
        _env_truthy(env.get("TRADING_VALIDATE_REPO_LIVE"))
        or _env_truthy(env.get("TRADING_VALIDATION_REQUIRE_PROD_DEPS"))
    )


def _scrub_local_runtime_external_service_env(env: dict[str, str]) -> None:
    for key in _LOCAL_RUNTIME_EXTERNAL_SERVICE_POP_KEYS:
        env.pop(key, None)
    env.update(_LOCAL_RUNTIME_EXTERNAL_SERVICE_DEFAULTS)


def _runtime_graph_startup_env(env: dict[str, str]) -> dict[str, str]:
    run_env = dict(env)
    run_env["TRADING_VALIDATION_MODE"] = "startup"
    if not _production_dependency_validation(run_env):
        _scrub_local_runtime_external_service_env(run_env)
    return run_env


def _parse_simple_env_file(env_path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for raw_line in lines:
        line = str(raw_line or "").strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip("'").strip('"')
        values[key] = value
    return values


def _load_env_file_defaults(env: dict[str, str], env_path: Path) -> None:
    if not env_path.exists():
        return
    values: dict[str, str] = {}
    try:
        from dotenv import dotenv_values

        values = {
            str(key): str(value)
            for key, value in (dotenv_values(env_path) or {}).items()
            if key and value is not None
        }
    except Exception:
        values = _parse_simple_env_file(env_path)
    for key, value in values.items():
        if key and value is not None:
            env.setdefault(str(key), str(value))


def _load_dotenv_defaults(env: dict[str, str]) -> None:
    _load_env_file_defaults(env, ROOT / ".env")


def _load_compose_live_defaults(env: dict[str, str]) -> None:
    _load_env_file_defaults(env, ROOT / "deploy" / "compose" / ".env")

    dashboard_port = str(env.get("DASHBOARD_PUBLIC_PORT") or "8000").strip() or "8000"
    env.setdefault("PIPELINE_SMOKE_BASE", f"http://127.0.0.1:{dashboard_port}")
    env.setdefault("PIPELINE_SMOKE_OPERATOR_BASE", f"http://127.0.0.1:{dashboard_port}/operator")
    if str(env.get("OPERATOR_API_TOKEN") or "").strip():
        env.setdefault("PIPELINE_SMOKE_OPERATOR_TOKEN", str(env.get("OPERATOR_API_TOKEN") or "").strip())


def _unit_test_env(env: dict[str, str]) -> dict[str, str]:
    run_env = dict(env)
    run_env.setdefault("TRADING_UNIT_TEST_SCHEMA_FAST", "1")
    run_env["TRADING_FAILURE_DIAGNOSTICS_PERSIST"] = "0"
    run_env["TRADING_PG_AUTOINIT_ON_CONNECT"] = "1"
    run_env["TS_PG_POOL_SIZE"] = "6"
    run_env["TS_PG_POOL_MIN_SIZE"] = "1"
    run_env.setdefault("TS_PG_POOL_TIMEOUT", "15")
    run_env.setdefault("TS_REDIS_CONNECT_TIMEOUT_S", "0.05")
    run_env.setdefault("TS_REDIS_SOCKET_TIMEOUT_S", "0.05")
    run_env.setdefault("TS_REDIS_CIRCUIT_COOLDOWN_S", "600")
    run_env.setdefault("ENGINE_MODE", "safe")
    run_env.setdefault("EXECUTION_MODE", "safe")
    run_env.setdefault("OPERATOR_MODE", "safe")
    run_env["APP_ENV"] = "test"
    run_env["PROD_LOCK"] = "0"
    run_env.pop("ENV", None)
    run_env.pop("NODE_ENV", None)
    run_env.pop("TS_ENV", None)
    run_env.setdefault("AUTO_BOOT_DAEMONS", "0")
    run_env.setdefault("AUTO_PIPELINE", "0")
    run_env["KILL_SWITCH_GLOBAL"] = "0"

    for key in (
        "IBKR_ENABLED",
        "CCXT_ENABLED",
        "POLYGON_WS_ENABLED",
        "POLYGON_REST_ENABLED",
        "TRADIER_ENABLED",
        "ALPACA_ENABLED",
    ):
        run_env[key] = "0"

    for key in (
        "LIVE_CACHE_BACKEND",
        "LIVE_CACHE_REDIS_URL",
        "REDIS_URL",
        "REDIS_CACHE_URL",
        "PREFLIGHT_REQUIRE_TIMESCALE",
        "PREFLIGHT_REQUIRE_REDIS",
        "PREFLIGHT_REQUIRE_OBJECT_STORAGE",
        "TIMESCALE_ENABLED",
        "TIMESCALE_DSN",
        "TIMESCALE_PRICES_ENABLED",
        "TIMESCALE_PRICES_DSN",
        "TELEMETRY_READ_BACKEND",
        "PRICE_READ_BACKEND",
        "OBJECT_STORE_ENDPOINT",
        "OBJECT_STORE_BUCKET",
        "OBJECT_STORE_ACCESS_KEY",
        "OBJECT_STORE_SECRET_KEY",
        "OBJECT_STORE_SESSION_TOKEN",
        "MINIO_ENDPOINT",
        "MINIO_BUCKET",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "TRAINING_DATASET_URI_PREFIX",
    ):
        run_env.pop(key, None)
    return run_env


def _telemetry_dual_write_burnin_required(env: dict[str, str]) -> bool:
    telemetry_validation_enabled = _env_truthy(env.get("TIMESCALE_TELEMETRY_VALIDATION_ENABLED"))
    telemetry_mirror_enabled = _env_truthy(env.get("TIMESCALE_TELEMETRY_MIRROR_ENABLED"))
    if telemetry_validation_enabled or telemetry_mirror_enabled:
        return True

    storage_backend = str(env.get("TS_STORAGE_BACKEND") or "").strip().lower()
    sqlite_storage = storage_backend in {"sqlite", "sqlite-test", "test"} or _env_truthy(env.get("TS_TESTING"))
    if not sqlite_storage:
        return False

    telemetry_read_backend = str(env.get("TELEMETRY_READ_BACKEND", "sqlite") or "sqlite").strip().lower()
    timescale_configured = _env_truthy(env.get("TIMESCALE_ENABLED")) or bool(
        str(env.get("TIMESCALE_DSN") or "").strip()
    )
    return bool(timescale_configured and telemetry_read_backend in {"auto", "timescale"})


def _telemetry_dual_write_burnin_command(python: str, env: dict[str, str]) -> list[str] | None:
    if not _telemetry_dual_write_burnin_required(env):
        return None
    return [
        python,
        "tools/compare_timescale_telemetry_dual_write.py",
        "--strict",
        "--require-healthy-mirror",
        "--require-healthy-timescale",
        "--lookback-minutes",
        str(env.get("TIMESCALE_TELEMETRY_VALIDATE_LOOKBACK_MINUTES") or "5"),
        "--max-count-delta",
        str(env.get("TIMESCALE_TELEMETRY_MAX_COUNT_DELTA") or "0"),
        "--max-last-ts-lag-ms",
        str(env.get("TIMESCALE_TELEMETRY_MAX_LAST_TS_LAG_MS") or "5000"),
        "--json",
    ]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the canonical repository validation workflow.")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Include runtime-coupled smoke checks that require a running operator and engine.",
    )
    args = parser.parse_args(argv)

    env = dict(os.environ)
    env.setdefault("PYTHONPATH", str(ROOT))
    _load_dotenv_defaults(env)
    if args.live:
        _load_compose_live_defaults(env)
        env.setdefault("TRADING_VALIDATE_REPO_LIVE", "1")
        env.setdefault("TRADING_VALIDATION_REQUIRE_PROD_DEPS", "1")
    else:
        env.setdefault("TRADING_VALIDATE_REPO_LIVE", "0")
    env.setdefault("TS_PG_SCHEMA_PER_DB_PATH", "1")
    env.setdefault("TS_PG_POOL_SIZE", "12")
    env.setdefault("TS_PG_POOL_MIN_SIZE", "2")

    python = _project_python()
    pytest = _project_pytest(python)
    checks: list[tuple[str, list[str]]] = [
        ("syntax", [python, "tools/syntax_check_workspace.py"]),
        ("ruff-static-release-gate", [python, "-m", "ruff", "check", "engine/", "routes/", "services/", "tests/"]),
        ("docs", [python, "tools/validate_docs.py"]),
        ("ui-asset-refs", [python, "tools/check_local_asset_refs.py"]),
        ("dependency-lock", [python, "tools/validate_dependency_lock.py"]),
        ("noop-guard", [python, "tools/noop_guard.py"]),
        ("storage-route-audit", [python, "tools/storage_route_audit.py"]),
        ("runtime-graph-startup", [python, "tools/runtime_graph_check.py", "--mode", "startup"]),
    ]
    telemetry_burnin_check = _telemetry_dual_write_burnin_command(python, env)
    if telemetry_burnin_check:
        checks.append(("telemetry-dual-write-burnin", telemetry_burnin_check))
    checks.extend(
        [
            ("unit-tests", [python, "-m", "unittest", "discover", "-s", "tests", "-v"]),
            ("pytest-tests", [*pytest, "tests/", "-v", "--tb=short"]),
            ("news-selftest", [python, "tools/news_ingestion_selftest.py"]),
        ]
    )

    if args.live:
        checks.append(("pipeline-smoke", [python, "tools/pipeline_smoke_test.py"]))

    try:
        for label, command in checks:
            try:
                _run(label, command, env)
            except subprocess.CalledProcessError as exc:
                print(f"\nValidation failed during {label}.")
                return exc.returncode or 1
    finally:
        _cleanup_validation_pg_schemas(env)

    print("\nValidation complete.")
    if not args.live:
        print(
            "Live smoke checks were skipped. "
            "Run `python tools/validate_repo.py --live` against a running stack."
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
