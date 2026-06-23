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

DEFAULT_TEST_TMP_ROOT = Path("/var/tmp") / f"trading-system-tests-{os.getuid()}" / "pytest"


def _configure_disk_backed_test_tmp() -> Path:
    configured = os.environ.get("TRADING_TEST_TMPDIR")
    tmp_root = Path(configured).expanduser() if configured else DEFAULT_TEST_TMP_ROOT
    if not tmp_root.is_absolute():
        tmp_root = ROOT / tmp_root
    tmp_root = tmp_root.resolve()
    tmp_root.mkdir(parents=True, exist_ok=True)
    for key in ("TMPDIR", "TEMP", "TMP", "PYTEST_DEBUG_TEMPROOT"):
        os.environ[key] = str(tmp_root)
    os.environ.setdefault("TRADING_TEST_TMPDIR", str(tmp_root))
    return tmp_root


TEST_TMP_ROOT = _configure_disk_backed_test_tmp()

from engine.runtime.test_isolation import (  # noqa: E402
    apply_runtime_test_defaults,
    cleanup_runtime_test_state,
    reset_runtime_test_env,
)
from engine.runtime.test_network_isolation import (  # noqa: E402
    install_socket_guard,
    live_network_opt_in_enabled,
    set_external_network_allowed_for_current_test,
    uninstall_socket_guard,
)

apply_runtime_test_defaults()
reset_runtime_test_env()

os.environ.setdefault("TS_TESTING", "1")
os.environ.setdefault("TS_STORAGE_BACKEND", "sqlite")
os.environ.setdefault("TS_PG_POOL_TIMEOUT", "0.1")
os.environ.setdefault("TS_PG_CONNECT_TIMEOUT", "1")


def pytest_configure(config):
    install_socket_guard()


def pytest_unconfigure(config):
    uninstall_socket_guard()


def pytest_collection_modifyitems(config, items):
    if live_network_opt_in_enabled():
        return
    selected = []
    deselected = []
    for item in items:
        if "live_network" in item.keywords:
            deselected.append(item)
        else:
            selected.append(item)
    if not deselected:
        return
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)


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
    return "/var/run/postgresql", None


def _platform_default_redis_url() -> str:
    return "unix:///var/run/redis/trading.sock"


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


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    set_external_network_allowed_for_current_test(
        bool("live_network" in item.keywords and live_network_opt_in_enabled())
    )
    keywords = item.keywords
    if "linux_only" in keywords and sys.platform != "linux":
        pytest.skip("linux-only test")
    if "requires_postgres" in keywords and not _postgres_reachable():
        pytest.skip("postgres not reachable at TS_PG_DSN")
    if "requires_redis" in keywords and not _redis_reachable():
        pytest.skip("redis not reachable at TS_REDIS_URL")


@pytest.hookimpl(trylast=True)
def pytest_runtest_teardown(item, nextitem):
    set_external_network_allowed_for_current_test(False)


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
        "data_source_master_key": "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=",
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
