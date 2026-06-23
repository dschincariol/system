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

# ------------------------------------------------------------------
# Network access mode (loopback-only local vs. trusted-LAN exposure)
# ------------------------------------------------------------------
# A single opt-in toggle (``TRADING_NETWORK_MODE``) drives the dashboard
# bind-host default. The operator sidecar remains an internal service by
# default and is reached through the authenticated same-origin dashboard
# bridge; LAN mode must not silently publish the sidecar's :4001 control
# plane. Local mode is unchanged (loopback). LAN mode only changes the
# dashboard default bind host; explicit DASHBOARD_HOST still wins, and the
# existing startup gates (which require DASHBOARD_API_TOKEN for any
# non-loopback bind, plus the live/prod public-exposure ACK) still apply.
WILDCARD_BIND_HOST = "0.0.0.0"
NETWORK_MODE_LOCAL = "local"
NETWORK_MODE_LAN = "lan"
_LAN_MODE_ALIASES = frozenset({"lan", "host", "server", "remote", "0.0.0.0", "wildcard"})
# Documented fallback used ONLY for human-facing startup banners/logs. It is
# never injected into browser application logic (clients derive URLs from
# window.location). Override with TRADING_LAN_IP.
DEFAULT_LAN_ADVERTISE_IP = "192.168.0.165"
DEFAULT_OPERATOR_PORT = 4001


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


def is_wildcard_host(host: str | None) -> bool:
    text = str(host or "").strip().strip("[]").lower()
    return text in {"0.0.0.0", "::", "0:0:0:0:0:0:0:0"}


def resolve_network_mode(environ: dict | None = None) -> str:
    """Return ``"lan"`` when LAN exposure is requested, else ``"local"``.

    Driven by ``TRADING_NETWORK_MODE``. Unknown/empty values stay ``local`` so
    the default is always loopback-only.
    """
    env = environ if environ is not None else os.environ
    raw = str(env.get("TRADING_NETWORK_MODE") or "").strip().lower()
    if raw in _LAN_MODE_ALIASES:
        return NETWORK_MODE_LAN
    return NETWORK_MODE_LOCAL


def apply_network_mode_bind_defaults(environ: dict | None = None) -> dict:
    """Expand ``TRADING_NETWORK_MODE=lan`` into concrete bind-host defaults.

    Mutates ``environ`` in place so every downstream dashboard reader --
    startup gates, the dashboard bind, and logging -- observes the same
    resolved ``DASHBOARD_HOST``. This is what keeps the security gate honest:
    setting the wildcard bind here means ``startup_gates`` will (correctly)
    require ``DASHBOARD_API_TOKEN``.

    The operator sidecar is deliberately excluded. Compose may bind the
    sidecar to 0.0.0.0 inside the private Docker network, but host LAN access
    must go through the dashboard bridge unless an explicit reviewed design
    publishes the sidecar. Explicit, non-empty host values are never
    overwritten, and local mode is a no-op. Returns the mapping of keys this
    call set (empty when nothing changed), so callers can log precisely what
    was applied. Idempotent.
    """
    env = environ if environ is not None else os.environ
    applied: dict[str, str] = {}
    if resolve_network_mode(env) != NETWORK_MODE_LAN:
        return applied
    if not str(env.get("DASHBOARD_HOST") or "").strip():
        env["DASHBOARD_HOST"] = WILDCARD_BIND_HOST
        applied["DASHBOARD_HOST"] = WILDCARD_BIND_HOST
    return applied


def _detect_primary_lan_ip() -> str:
    """Best-effort primary outbound IPv4 address; ``""`` when undeterminable.

    Uses a connect-less UDP socket trick (no packets are sent) to ask the OS
    which local interface would route off-box. Loopback results are rejected.
    """
    import socket

    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(0.25)
        sock.connect((DEFAULT_LAN_ADVERTISE_IP, 9))
        ip = str(sock.getsockname()[0] or "").strip()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        return ""
    finally:
        try:
            if sock is not None:
                sock.close()
        except Exception:
            # no-op-guard: allow - best-effort cleanup during LAN IP detection.
            pass
    return ""


def resolve_lan_advertise_ip(environ: dict | None = None) -> str:
    """LAN IP for human-facing startup banners/logs only (never browser logic).

    Resolution order: explicit ``TRADING_LAN_IP`` -> auto-detected primary
    interface IP -> documented fallback (``192.168.0.165``).
    """
    env = environ if environ is not None else os.environ
    explicit = str(env.get("TRADING_LAN_IP") or "").strip()
    if explicit:
        return explicit
    detected = _detect_primary_lan_ip()
    if detected:
        return detected
    return DEFAULT_LAN_ADVERTISE_IP


def network_access_banner_lines(
    *,
    service: str,
    bind_host: str,
    port: int,
    environ: dict | None = None,
) -> list[str]:
    """Build operator-facing startup banner lines for a bound HTTP service.

    Prints the bind host, port, environment mode, the loopback URL, and -- when
    the service is bound to a wildcard/non-loopback host -- the LAN URL plus a
    security reminder when no dashboard API token is configured.
    """
    env = environ if environ is not None else os.environ
    mode = resolve_network_mode(env)
    engine_mode = str(env.get("ENGINE_MODE") or "safe").strip() or "safe"
    wildcard = is_wildcard_host(bind_host)
    non_loopback = wildcard or not is_loopback_host(bind_host)
    local_host = LOOPBACK_HOST if wildcard else (bind_host or LOOPBACK_HOST)
    lines = [
        f"[{service}] network_mode={mode} engine_mode={engine_mode}",
        f"[{service}] bind_host={bind_host} port={int(port)}",
        f"[{service}] local URL:  http://{local_host}:{int(port)}/",
    ]
    if non_loopback:
        lan_ip = resolve_lan_advertise_ip(env)
        lines.append(f"[{service}] LAN URL:    http://{lan_ip}:{int(port)}/")
        token_present = bool(str(env.get("DASHBOARD_API_TOKEN") or "").strip())
        if not token_present:
            lines.append(
                f"[{service}] WARNING: bound for LAN access without DASHBOARD_API_TOKEN; "
                "set it to enable authenticated mutations (startup gate enforces this)."
            )
    return lines


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
