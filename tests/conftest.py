from __future__ import annotations

import functools
import os
import socket
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.runtime.test_isolation import (  # noqa: E402
    apply_runtime_test_defaults,
    cleanup_runtime_test_state,
    reset_runtime_test_env,
)

apply_runtime_test_defaults()
reset_runtime_test_env()

os.environ.setdefault("TS_TESTING", "1")
os.environ.setdefault("TS_STORAGE_BACKEND", "sqlite")
os.environ.setdefault("TS_PG_POOL_TIMEOUT", "0.1")
os.environ.setdefault("TS_PG_CONNECT_TIMEOUT", "1")


def pytest_configure(config):
    config.addinivalue_line("markers", "linux_only: test runs only on Linux")
    config.addinivalue_line("markers", "windows_only: test runs only on Windows")
    config.addinivalue_line(
        "markers",
        "requires_postgres: test needs a reachable Postgres instance "
        "at TS_PG_DSN; auto-skipped when unreachable",
    )
    config.addinivalue_line(
        "markers",
        "requires_redis: test needs a reachable Redis instance at "
        "TS_REDIS_URL; auto-skipped when unreachable",
    )
    config.addinivalue_line(
        "markers",
        "requires_rocm: test needs a ROCm-enabled torch runtime and visible HIP device; "
        "auto-skipped when unavailable",
    )


def _probe_tcp(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _probe_unix(path: str, timeout: float = 1.0) -> bool:
    if not hasattr(socket, "AF_UNIX"):
        return False
    if not os.path.exists(path):
        return False
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect(path)
        return True
    except OSError:
        return False
    finally:
        sock.close()


def _parse_pg_dsn(dsn: str) -> tuple[str, int] | tuple[str, None]:
    host = "127.0.0.1"
    port = 5432
    for token in dsn.split():
        if "=" not in token:
            continue
        key, _, value = token.partition("=")
        if key == "host":
            host = value
        elif key == "port":
            try:
                port = int(value)
            except ValueError:
                pass
    if host.startswith("/"):
        return host, None
    return host, port


def _platform_default_pg_target() -> tuple[str, int | None]:
    if sys.platform == "linux":
        return "/var/run/postgresql", None
    return "127.0.0.1", 5432


def _platform_default_redis_url() -> str:
    if sys.platform == "linux":
        return "unix:///var/run/redis/trading.sock"
    return "redis://127.0.0.1:6379/0"


@functools.lru_cache(maxsize=1)
def _postgres_reachable() -> bool:
    dsn = os.environ.get("TS_PG_DSN")
    if dsn:
        host, port = _parse_pg_dsn(dsn)
    else:
        host, port = _platform_default_pg_target()
    if port is None:
        return _probe_unix(host)
    return _probe_tcp(host, port)


@functools.lru_cache(maxsize=1)
def _redis_reachable() -> bool:
    url = os.environ.get("TS_REDIS_URL") or _platform_default_redis_url()
    parsed = urlparse(url)
    if parsed.scheme == "unix":
        return _probe_unix(parsed.path)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 6379
    return _probe_tcp(host, port)


@functools.lru_cache(maxsize=1)
def _rocm_reachable() -> bool:
    if str(os.environ.get("TRADING_FORCE_ROCM_TESTS", "0") or "0").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return True
    try:
        import torch

        return bool(
            getattr(getattr(torch, "version", None), "hip", None)
            and getattr(torch, "cuda", None)
            and torch.cuda.is_available()
            and torch.cuda.device_count() > 0
        )
    except Exception:
        return False


def pytest_runtest_setup(item):
    keywords = item.keywords
    if "linux_only" in keywords and sys.platform != "linux":
        pytest.skip("linux-only test")
    if "windows_only" in keywords and sys.platform != "win32":
        pytest.skip("windows-only test")
    if "requires_postgres" in keywords and not _postgres_reachable():
        pytest.skip("postgres not reachable at TS_PG_DSN")
    if "requires_redis" in keywords and not _redis_reachable():
        pytest.skip("redis not reachable at TS_REDIS_URL")
    if "requires_rocm" in keywords and not _rocm_reachable():
        pytest.skip("ROCm torch device not reachable")


@pytest.fixture(autouse=True)
def _default_test_secrets(monkeypatch, tmp_path, request):
    cleanup_runtime_test_state(timeout_s=0.5)
    reset_runtime_test_env()

    for key in (
        "PGPASSWORD",
        "TS_PG_PASSWORD",
        "TS_PG_PASSWORD_APP",
        "TS_PG_APP_PASSWORD",
        "TS_PG_PASSWORD_INGEST",
        "TS_PG_INGEST_PASSWORD",
        "TS_PG_PASSWORD_READER",
        "TS_PG_READER_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)

    node_path = str(getattr(request.node, "fspath", "") or "")
    if not node_path.endswith("test_secrets_loader.py"):
        monkeypatch.setenv("TS_CREDENTIAL_AUDIT_ENABLED", "0")
    else:
        monkeypatch.setenv("TS_CREDENTIAL_AUDIT_ENABLED", "1")
    secret_dir = tmp_path / "test_secrets"
    secret_dir.mkdir()
    for name, value in {
        "master_key": "test-master-key",
        "pg_password_app": "test-app-password",
        "pg_password_ingest": "test-ingest-password",
        "pg_password_reader": "test-reader-password",
    }.items():
        (secret_dir / name).write_text(value, encoding="utf-8")
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")
    monkeypatch.setenv("TS_DEV_SECRETS_DIR", str(secret_dir))
    monkeypatch.delenv("TS_ENV", raising=False)
    try:
        yield
    finally:
        cleanup_runtime_test_state(timeout_s=0.5)
        reset_runtime_test_env()
