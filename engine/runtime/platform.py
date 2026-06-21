"""Platform-aware runtime defaults for application storage paths, DSNs, and local hosts."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

LOOPBACK_HOST = "127.0.0.1"
LOCALHOST_NAME = "localhost"
LOOPBACK_HOSTS = frozenset({LOOPBACK_HOST, "::1", LOCALHOST_NAME})
POSTGRES_URL_SCHEMES = frozenset({"postgres", "postgresql", "timescale", "timescaledb"})
DEFAULT_DASHBOARD_HOST = LOOPBACK_HOST
DEFAULT_DASHBOARD_DEV_PORT = 8000
DEFAULT_IBKR_HOST = LOOPBACK_HOST


def is_linux() -> bool:
    return sys.platform.startswith("linux")


def _dsn_value(value: str) -> str:
    text = str(value)
    if not text:
        return text
    if any(ch.isspace() for ch in text) or "'" in text or "\\" in text:
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return text


def normalize_loopback_host(host: str | None) -> str:
    text = str(host or "").strip()
    if not text or text.lower() == LOCALHOST_NAME:
        return LOOPBACK_HOST
    return text


def is_loopback_host(host: str | None) -> bool:
    text = normalize_loopback_host(host).lower()
    return text in {LOOPBACK_HOST, "::1"}


def default_dashboard_host() -> str:
    return DEFAULT_DASHBOARD_HOST


def default_dashboard_dev_port() -> int:
    return int(DEFAULT_DASHBOARD_DEV_PORT)


def default_ibkr_host() -> str:
    return DEFAULT_IBKR_HOST


def default_backup_evidence_path() -> str:
    return str(Path("/") / "var" / "backups" / "trading" / "evidence" / "latest_backup_restore_evidence.json")


def default_backup_root_dir() -> str:
    return str(Path("/") / "var" / "backups" / "trading")


def default_base_backup_dir() -> str:
    return str(Path("/") / "var" / "backups" / "trading" / "base")


def default_wal_backup_dir() -> str:
    return str(Path("/") / "var" / "backups" / "trading" / "wal")


def default_restore_drill_dir() -> str:
    return str(Path("/") / "var" / "backups" / "trading" / "drills")


def default_postgres_log_path() -> str:
    return str(Path("/") / "var" / "log" / "postgresql" / "postgresql-16-main.log")


def default_container_runtime_roots() -> tuple[str, ...]:
    return (
        str(Path("/") / "var" / "lib" / "docker"),
        str(Path("/") / "var" / "lib" / "containerd"),
    )


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_local_runtime_root() -> Path:
    configured = str(
        os.environ.get("TRADING_RUNTIME_ROOT")
        or os.environ.get("TS_LOCAL_RUNTIME_ROOT")
        or ""
    ).strip()
    if configured:
        return Path(configured).expanduser()
    return repo_root() / "var"


def default_local_log_dir() -> Path:
    return default_local_runtime_root() / "log"


def default_local_db_dir() -> Path:
    return default_local_runtime_root() / "db"


def default_local_db_path() -> Path:
    return default_local_db_dir() / ("trading" + "." + "db")


def default_local_tmp_dir() -> Path:
    return default_local_runtime_root() / "tmp"


def default_local_artifacts_dir() -> Path:
    return default_local_runtime_root() / "artifacts"


def default_local_models_dir() -> Path:
    return default_local_artifacts_dir() / "models"


def default_local_audit_dir() -> Path:
    return default_local_runtime_root() / "audit"


def use_local_runtime_defaults() -> bool:
    env_raw = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "dev").strip().lower()
    env = "prod" if env_raw in {"prod", "production"} else env_raw
    engine_mode = str(os.environ.get("ENGINE_MODE") or "safe").strip().lower()
    if engine_mode == "development":
        engine_mode = "dev"
    supervised = str(os.environ.get("ENGINE_SUPERVISED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    explicit_dev_env = bool(str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip()) and env in {
        "dev",
        "test",
    }
    live_like = engine_mode in {"live", "shadow", "paper"}
    return not bool(supervised or env == "prod" or (live_like and not explicit_dev_env))


def default_pg_user() -> str:
    configured_raw = str(os.environ.get("TS_PG_ROLE") or os.environ.get("TS_PG_USER") or "").strip()
    if configured_raw:
        configured = configured_raw.lower()
        if configured in {"app", "application"}:
            return "ts_app"
        if configured in {"ingest", "ingestion"}:
            return "ts_ingest"
        if configured == "reader":
            return "ts_reader"
        return configured_raw

    process_role = str(
        os.environ.get("TS_PROCESS_ROLE")
        or os.environ.get("ENGINE_PROCESS_ROLE")
        or ""
    ).strip().lower()
    if process_role in {"ingest", "ingestion", "market-data", "market_data"}:
        return "ts_ingest"
    if process_role in {"reader", "read", "readonly", "read_only"}:
        return "ts_reader"

    job_name = str(os.environ.get("ENGINE_JOB_NAME") or os.environ.get("JOB_NAME") or "").strip().lower()
    if any(part in job_name for part in ("ingest", "stream", "poll_prices", "market")):
        return "ts_ingest"
    return "ts_app"


def pg_password_secret_name(user: str | None = None) -> str:
    role = str(user or default_pg_user()).strip()
    if role == "ts_ingest":
        return "pg_password_ingest"
    if role == "ts_reader":
        return "pg_password_reader"
    return "pg_password_app"


def _load_text_file(path: str) -> str:
    return Path(path).expanduser().read_text(encoding="utf-8").strip()


def _load_pg_password_file(user: str | None = None) -> str:
    role = pg_password_secret_name(user).removeprefix("pg_password_").upper()
    for name in (
        "TS_PG_PASSWORD_FILE",
        "TIMESCALE_PASSWORD_FILE",
        f"TS_PG_PASSWORD_{role}_FILE",
        f"TS_PG_{role}_PASSWORD_FILE",
        "PGPASSWORD_FILE",
    ):
        path = str(os.environ.get(name) or "").strip()
        if path:
            return _load_text_file(path)
    return ""


def _load_pg_password_secret_ref(user: str | None = None) -> str:
    role = pg_password_secret_name(user).removeprefix("pg_password_").upper()
    for name in (
        "TS_PG_PASSWORD_SECRET",
        "TIMESCALE_PASSWORD_SECRET",
        f"TS_PG_PASSWORD_{role}_SECRET",
        f"TS_PG_{role}_PASSWORD_SECRET",
        "PGPASSWORD_SECRET",
    ):
        secret_name = str(os.environ.get(name) or "").strip()
        if not secret_name:
            continue
        from services.secrets.loader import load_secret

        return load_secret(secret_name).decode("utf-8", "ignore").rstrip("\r\n")
    return ""


def _load_pg_password(user: str | None = None) -> str:
    configured_file = _load_pg_password_file(user)
    if configured_file:
        return configured_file

    configured_secret = _load_pg_password_secret_ref(user)
    if configured_secret:
        return configured_secret

    configured = str(os.environ.get("TS_PG_PASSWORD") or os.environ.get("TIMESCALE_PASSWORD") or "").strip()
    if configured:
        return configured

    role = pg_password_secret_name(user).removeprefix("pg_password_").upper()
    from services.secrets.loader import load_secret

    if (
        str(os.environ.get("TS_SECRETS_PROVIDER") or "").strip()
        or str(os.environ.get("TS_DEV_SECRETS_DIR") or "").strip()
        or str(os.environ.get("CREDENTIALS_DIRECTORY") or "").strip()
    ):
        return load_secret(pg_password_secret_name(user)).decode("utf-8", "ignore").rstrip("\r\n")

    for name in (
        f"TS_PG_PASSWORD_{role}",
        f"TS_PG_{role}_PASSWORD",
    ):
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value

    value = str(os.environ.get("PGPASSWORD") or "").strip()
    if value:
        return value

    return load_secret(pg_password_secret_name(user)).decode("utf-8", "ignore").rstrip("\r\n")


def _dsn_user(conninfo: str) -> str:
    match = re.search(r"(?:^|\s)user=(?P<value>'(?:\\'|[^'])*'|\S+)", str(conninfo or ""))
    if not match:
        return default_pg_user()
    value = str(match.group("value") or "").strip()
    if value.startswith("'") and value.endswith("'"):
        value = value[1:-1].replace("\\'", "'").replace("\\\\", "\\")
    return value or default_pg_user()


def dsn_with_pg_password(conninfo: str) -> str:
    raw = str(conninfo or "").strip()
    if not raw or re.search(r"(?:^|\s)password=", raw, re.IGNORECASE):
        return raw
    user = _dsn_user(raw)
    password = _load_pg_password(user)
    return f"{raw} password={_dsn_value(password)}".strip()


def _url_with_pg_password(conninfo: str) -> str:
    raw = str(conninfo or "").strip()
    if not raw:
        return raw
    parsed = urlsplit(raw)
    if parsed.password or not parsed.scheme or parsed.scheme.lower() not in POSTGRES_URL_SCHEMES or not parsed.hostname:
        return raw
    user = str(parsed.username or default_pg_user())
    password = _load_pg_password(user)
    if not password:
        return raw
    host = str(parsed.hostname or "")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    return urlunsplit((parsed.scheme, auth + host, parsed.path, parsed.query, parsed.fragment))


def connection_info_with_pg_password(conninfo: str) -> str:
    raw = str(conninfo or "").strip()
    if not raw:
        return raw
    try:
        parsed = urlsplit(raw)
    except ValueError:
        return dsn_with_pg_password(raw)
    if parsed.scheme and parsed.scheme.lower() in POSTGRES_URL_SCHEMES:
        return _url_with_pg_password(raw)
    return dsn_with_pg_password(raw)


def _pg_port(default: str) -> str:
    return str(os.environ.get("TS_PG_PORT") or default).strip() or str(default)


def default_pg_dsn() -> str:
    user = default_pg_user()
    if is_linux():
        # Linux defaults to PgBouncer's socket port; TS_PG_PORT=5432 opts into
        # direct Postgres without replacing the whole DSN.
        host = str(Path("/") / "var" / "run" / "postgresql")
        default_port = "6432"
    else:
        host = LOOPBACK_HOST
        default_port = "5432"
    parts = [f"host={host}", f"port={_pg_port(default_port)}", f"user={user}", "dbname=trading"]
    password = _load_pg_password(user)
    if password:
        parts.append(f"password={_dsn_value(password)}")
    return " ".join(parts)


def default_admin_pg_dsn() -> str:
    return "host=/var/run/postgresql port=5432 user=postgres dbname=postgres"


def default_redis_url() -> str:
    return "unix:///var/run/redis/trading.sock"


def default_data_root() -> Path:
    configured = str(os.environ.get("TS_DATA_ROOT") or "").strip()
    if configured:
        return Path(configured).expanduser()

    return Path("/var/lib/trading")
