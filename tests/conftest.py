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


@pytest.fixture(autouse=True)
def _default_test_secrets(monkeypatch, tmp_path):
    if os.environ.get("TS_SECRETS_PROVIDER"):
        return
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
