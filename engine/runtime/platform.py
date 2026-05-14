"""Platform-aware runtime defaults for application storage paths and DSNs."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path


def is_linux() -> bool:
    return sys.platform.startswith("linux")


def is_windows() -> bool:
    return sys.platform.startswith("win")


def _dsn_value(value: str) -> str:
    text = str(value)
    if not text:
        return text
    if any(ch.isspace() for ch in text) or "'" in text or "\\" in text:
        return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"
    return text


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


def _load_pg_password(user: str | None = None) -> str:
    role = pg_password_secret_name(user).removeprefix("pg_password_").upper()
    for name in (
        f"TS_PG_PASSWORD_{role}",
        f"TS_PG_{role}_PASSWORD",
        "TS_PG_PASSWORD",
        "PGPASSWORD",
    ):
        value = str(os.environ.get(name) or "").strip()
        if value:
            return value

    from services.secrets.loader import load_secret

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


def _pg_port(default: str) -> str:
    return str(os.environ.get("TS_PG_PORT") or default).strip() or str(default)


def default_pg_dsn() -> str:
    user = default_pg_user()
    if is_linux():
        # Linux defaults to PgBouncer's socket port; TS_PG_PORT=5432 opts into
        # direct Postgres without replacing the whole DSN.
        parts = ["host=/var/run/postgresql", f"port={_pg_port('6432')}", f"user={user}", "dbname=trading"]
    else:
        parts = ["host=127.0.0.1", f"port={_pg_port('5432')}", f"user={user}", "dbname=trading"]
    password = _load_pg_password(user)
    if password:
        parts.append(f"password={_dsn_value(password)}")
    return " ".join(parts)


def default_admin_pg_dsn() -> str:
    if is_linux():
        return "host=/var/run/postgresql port=5432 user=postgres dbname=postgres"

    parts = ["host=127.0.0.1", "port=5432", "user=postgres", "dbname=postgres"]
    return " ".join(parts)


def default_redis_url() -> str:
    if is_linux():
        return "unix:///var/run/redis/trading.sock"
    return "redis://127.0.0.1:6379/0"


def default_data_root() -> Path:
    configured = str(os.environ.get("TS_DATA_ROOT") or "").strip()
    if configured:
        return Path(configured).expanduser()

    if is_linux():
        return Path("/var/lib/trading")

    local_app_data = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        return Path(local_app_data) / "Trading"
    return Path.home() / "AppData" / "Local" / "Trading"
