"""Production secret-source policy and repo-local env-file inventory.

This module intentionally records only environment key names and source classes.
It must not retain or return secret values from env files or process env.
"""

from __future__ import annotations

import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlsplit, urlunsplit


_TRUTHY = {"1", "true", "yes", "on", "y"}
_FALSY = {"0", "false", "no", "off", "n"}
_POSTGRES_URL_SCHEMES = {"postgres", "postgresql", "timescale", "timescaledb"}
_REDIS_URL_SCHEMES = {"redis", "rediss"}
_DSN_PASSWORD_RE = re.compile(r"(?:^|\s)password\s*=", re.IGNORECASE)
_ENV_ASSIGN_RE = re.compile(r"^\s*(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=(.*)$")
_PLACEHOLDER_SECRET_PATHS = {"/dev/null"}


@dataclass(frozen=True)
class SecretEnvSpec:
    key: str
    description: str
    kind: str = "secret"
    file_envs: tuple[str, ...] = ()
    secret_envs: tuple[str, ...] = ()
    provider_secret_names: tuple[str, ...] = ()
    value_shape: str = "plain"


@dataclass(frozen=True)
class SecretTextFileResult:
    """Structured result for file-backed secret reads.

    The default repr intentionally omits the secret value so accidental logging
    of this object does not disclose credentials.
    """

    ok: bool
    value: str = ""
    path: str = ""
    reason: str = ""
    missing: bool = False
    empty: bool = False
    error_type: str = ""

    def __bool__(self) -> bool:
        return bool(self.ok and self.value)

    def __repr__(self) -> str:
        return (
            "SecretTextFileResult("
            f"ok={self.ok!r}, path={self.path!r}, reason={self.reason!r}, "
            f"missing={self.missing!r}, empty={self.empty!r}, error_type={self.error_type!r})"
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": bool(self.ok),
            "path": self.path,
            "reason": self.reason,
            "missing": bool(self.missing),
            "empty": bool(self.empty),
            "error_type": self.error_type,
        }


def _alts(key: str, *provider_names: str) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    return (
        (f"{key}_FILE",),
        (f"{key}_SECRET",),
        tuple(dict.fromkeys(tuple(provider_names) + (key, key.lower()))),
    )


def _spec(
    key: str,
    description: str,
    *,
    kind: str = "secret",
    provider_names: tuple[str, ...] = (),
    file_envs: tuple[str, ...] | None = None,
    secret_envs: tuple[str, ...] | None = None,
    value_shape: str = "plain",
) -> SecretEnvSpec:
    default_file_envs, default_secret_envs, default_provider_names = _alts(key, *provider_names)
    return SecretEnvSpec(
        key=key,
        description=description,
        kind=kind,
        file_envs=tuple(file_envs if file_envs is not None else default_file_envs),
        secret_envs=tuple(secret_envs if secret_envs is not None else default_secret_envs),
        provider_secret_names=tuple(default_provider_names),
        value_shape=value_shape,
    )


SECRET_ENV_SPECS: tuple[SecretEnvSpec, ...] = (
    _spec("DASHBOARD_API_TOKEN", "dashboard/API mutation token", provider_names=("dashboard_api_token",)),
    _spec("OPERATOR_API_TOKEN", "operator sidecar API token", provider_names=("operator_api_token",)),
    _spec("DATA_SOURCE_MASTER_KEY", "data-source credential encryption master key", provider_names=("master_key",), file_envs=("DATA_SOURCE_MASTER_KEY_FILE",)),
    _spec("TRADING_MASTER_KEY", "trading master key", provider_names=("trading_master_key",), file_envs=("TRADING_MASTER_KEY_FILE",)),
    _spec("APP_MASTER_KEY", "application master key", provider_names=("app_master_key",)),
    _spec("BACKUP_EVIDENCE_HMAC_KEY", "backup evidence signing key", provider_names=("backup_evidence_hmac_key",), file_envs=("BACKUP_EVIDENCE_HMAC_KEY_FILE",)),
    _spec("ALPACA_KEY_ID", "Alpaca broker key id", kind="credential", provider_names=("alpaca_key_id",)),
    _spec("ALPACA_SECRET_KEY", "Alpaca broker secret key", provider_names=("alpaca_secret_key",)),
    _spec("POLYGON_API_KEY", "Polygon market-data API key", provider_names=("polygon_api_key",)),
    _spec("POLYGON_KEY", "Polygon market-data API key alias", provider_names=("polygon_api_key",)),
    _spec("TRADIER_API_TOKEN", "Tradier API token", provider_names=("tradier_api_token",)),
    _spec("OPENAI_API_KEY", "OpenAI API key", provider_names=("openai_api_key",)),
    _spec("ANTHROPIC_API_KEY", "Anthropic API key", provider_names=("anthropic_api_key",)),
    _spec("TIMESCALE_PASSWORD", "Timescale/Postgres password", provider_names=("pg_password_app", "timescale_password")),
    _spec("TS_PG_PASSWORD", "runtime Postgres password", provider_names=("pg_password_app",)),
    _spec("PGPASSWORD", "libpq password", provider_names=("pg_password_app",)),
    _spec("TS_PG_PASSWORD_APP", "runtime Postgres app-role password", provider_names=("pg_password_app",)),
    _spec("TS_PG_APP_PASSWORD", "runtime Postgres app-role password", provider_names=("pg_password_app",)),
    _spec("TS_PG_PASSWORD_INGEST", "runtime Postgres ingest-role password", provider_names=("pg_password_ingest",)),
    _spec("TS_PG_INGEST_PASSWORD", "runtime Postgres ingest-role password", provider_names=("pg_password_ingest",)),
    _spec("TS_PG_PASSWORD_READER", "runtime Postgres reader-role password", provider_names=("pg_password_reader",)),
    _spec("TS_PG_READER_PASSWORD", "runtime Postgres reader-role password", provider_names=("pg_password_reader",)),
    _spec("REDIS_PASSWORD", "Redis password", provider_names=("redis_password",)),
    _spec("MINIO_ROOT_USER", "MinIO root user credential", kind="credential", provider_names=("minio_root_user", "object_store_access_key")),
    _spec("MINIO_ROOT_PASSWORD", "MinIO root password", provider_names=("minio_root_password", "object_store_secret_key")),
    _spec("MINIO_ACCESS_KEY", "MinIO access key alias", kind="credential", provider_names=("minio_access_key", "object_store_access_key")),
    _spec("MINIO_SECRET_KEY", "MinIO secret key alias", provider_names=("minio_secret_key", "object_store_secret_key")),
    _spec("OBJECT_STORE_ACCESS_KEY", "object-store access key", kind="credential", provider_names=("object_store_access_key",)),
    _spec("OBJECT_STORE_SECRET_KEY", "object-store secret key", provider_names=("object_store_secret_key",)),
    _spec("OBJECT_STORE_SESSION_TOKEN", "object-store session token", provider_names=("object_store_session_token", "aws_session_token")),
    _spec("AWS_ACCESS_KEY_ID", "AWS access key id", kind="credential", provider_names=("aws_access_key_id", "object_store_access_key")),
    _spec("AWS_SECRET_ACCESS_KEY", "AWS secret access key", provider_names=("aws_secret_access_key", "object_store_secret_key")),
    _spec("AWS_SESSION_TOKEN", "AWS session token", provider_names=("aws_session_token",)),
    _spec(
        "TS_PG_DSN",
        "runtime Postgres DSN",
        kind="connection_string",
        provider_names=("pg_password_app",),
        file_envs=("TS_PG_PASSWORD_FILE", "PGPASSWORD_FILE"),
        secret_envs=("TS_PG_PASSWORD_SECRET",),
        value_shape="dsn",
    ),
    _spec(
        "TIMESCALE_DSN",
        "Timescale telemetry DSN",
        kind="connection_string",
        provider_names=("pg_password_app",),
        file_envs=("TS_PG_PASSWORD_FILE", "TIMESCALE_PASSWORD_FILE", "PGPASSWORD_FILE"),
        secret_envs=("TS_PG_PASSWORD_SECRET", "TIMESCALE_PASSWORD_SECRET"),
        value_shape="dsn",
    ),
    _spec(
        "TIMESCALE_PRICES_DSN",
        "Timescale price DSN",
        kind="connection_string",
        provider_names=("pg_password_app",),
        file_envs=("TS_PG_PASSWORD_FILE", "TIMESCALE_PASSWORD_FILE", "PGPASSWORD_FILE"),
        secret_envs=("TS_PG_PASSWORD_SECRET", "TIMESCALE_PASSWORD_SECRET"),
        value_shape="dsn",
    ),
    _spec(
        "OFFLINE_TS_PG_DSN",
        "offline Timescale clone DSN",
        kind="connection_string",
        provider_names=("pg_password_app",),
        file_envs=("TS_PG_PASSWORD_FILE", "TIMESCALE_PASSWORD_FILE", "PGPASSWORD_FILE"),
        secret_envs=("TS_PG_PASSWORD_SECRET", "TIMESCALE_PASSWORD_SECRET"),
        value_shape="dsn",
    ),
    _spec(
        "REDIS_URL",
        "Redis URL",
        kind="connection_string",
        provider_names=("redis_password",),
        file_envs=("REDIS_PASSWORD_FILE",),
        secret_envs=("REDIS_PASSWORD_SECRET",),
        value_shape="url",
    ),
    _spec(
        "TS_REDIS_URL",
        "runtime Redis URL",
        kind="connection_string",
        provider_names=("redis_password",),
        file_envs=("TS_REDIS_PASSWORD_FILE", "REDIS_PASSWORD_FILE"),
        secret_envs=("TS_REDIS_PASSWORD_SECRET", "REDIS_PASSWORD_SECRET"),
        value_shape="url",
    ),
    _spec(
        "LIVE_CACHE_REDIS_URL",
        "live cache Redis URL",
        kind="connection_string",
        provider_names=("redis_password",),
        file_envs=("LIVE_CACHE_REDIS_PASSWORD_FILE", "TS_REDIS_PASSWORD_FILE", "REDIS_PASSWORD_FILE"),
        secret_envs=("LIVE_CACHE_REDIS_PASSWORD_SECRET", "TS_REDIS_PASSWORD_SECRET", "REDIS_PASSWORD_SECRET"),
        value_shape="url",
    ),
)

SECRET_ENV_SPEC_BY_KEY = {spec.key: spec for spec in SECRET_ENV_SPECS}
OPTIONAL_PROVIDER_SECRET_KEYS = frozenset(
    {
        "ALPACA_KEY_ID",
        "ALPACA_SECRET_KEY",
        "POLYGON_API_KEY",
        "POLYGON_KEY",
        "TRADIER_API_TOKEN",
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
    }
)
REPO_LOCAL_ENV_RELATIVE_PATHS = (
    ".env",
    ".env.local",
    ".env.codex-sim-paper.bak",
    ".env.codex-sim-paper-db.bak",
    "deploy/compose/.env",
    "deploy/compose/.env.codex-sim-paper.bak",
    "deploy/env/trading.env",
)


def _env_truthy(raw: object) -> bool:
    return str(raw or "").strip().lower() in _TRUTHY


def _env_falsey(raw: object) -> bool:
    return str(raw or "").strip().lower() in _FALSY


def _env_text(environ: Mapping[str, Any], name: str) -> str:
    return str(environ.get(name, "") or "").strip()


def _env_enabled(environ: Mapping[str, Any], name: str, *, default: bool = False) -> bool:
    raw = _env_text(environ, name)
    if not raw:
        return default
    if raw.lower() in _TRUTHY:
        return True
    if raw.lower() in _FALSY:
        return False
    return default


def _contains_postgres_password(text: str) -> bool:
    return bool(_DSN_PASSWORD_RE.search(str(text or "")))


def _url_has_userinfo_secret(text: str) -> bool:
    try:
        parsed = urlsplit(str(text or ""))
    except ValueError:
        return False
    if not parsed.scheme or parsed.scheme.lower() not in (_POSTGRES_URL_SCHEMES | _REDIS_URL_SCHEMES):
        return False
    return bool(parsed.password)


def _value_contains_inline_secret(spec: SecretEnvSpec, value: object) -> bool:
    text = str(value if value is not None else "").strip()
    if not text:
        return False
    if spec.value_shape == "dsn":
        if _contains_postgres_password(text):
            return True
        try:
            return bool(urlsplit(text).scheme and _url_has_userinfo_secret(text))
        except ValueError:
            return False
    if spec.value_shape == "url":
        return _url_has_userinfo_secret(text)
    return True


def strict_secret_source_policy_required(environ: Mapping[str, Any] | None = None) -> bool:
    env = os.environ if environ is None else environ
    if _env_truthy(env.get("PROD_LOCK")):
        return True
    if _env_truthy(env.get("ENGINE_SUPERVISED")):
        return True
    for name in ("ENV", "APP_ENV", "TS_ENV", "NODE_ENV"):
        if _env_text(env, name).lower() in {"prod", "production"}:
            return True
    for name in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        if _env_text(env, name).lower() == "live":
            return True
    return False


def _policy_suppressed_for_tests(environ: Mapping[str, Any]) -> bool:
    if _env_truthy(environ.get("TRADING_ENFORCE_SECRET_SOURCE_POLICY")):
        return False
    try:
        from engine.runtime.test_isolation import running_python_tests

        return bool(running_python_tests())
    except Exception:
        return False


def _repo_root(environ: Mapping[str, Any] | None = None) -> Path:
    env = os.environ if environ is None else environ
    configured = _env_text(env, "TRADING_SECRET_POLICY_REPO_ROOT")
    if configured:
        return Path(configured).expanduser().resolve()
    from engine.runtime.platform import repo_root

    return repo_root().resolve()


def _is_repo_local_file(path: Path, repo_root: Path) -> bool:
    try:
        resolved = path.resolve(strict=False)
        resolved.relative_to(repo_root)
        return True
    except ValueError:
        return False


def _iter_repo_env_paths(repo_root: Path) -> tuple[Path, ...]:
    root = Path(repo_root).resolve()
    return tuple(root / relative for relative in REPO_LOCAL_ENV_RELATIVE_PATHS)


def _parse_env_file_secret_keys(path: Path, repo_root: Path) -> tuple[str, ...]:
    if not path.exists() or not _is_repo_local_file(path, repo_root):
        return tuple()
    keys: set[str] = set()
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return tuple()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        match = _ENV_ASSIGN_RE.match(line)
        if not match:
            continue
        key = str(match.group(1) or "").strip()
        spec = SECRET_ENV_SPEC_BY_KEY.get(key)
        if spec is None:
            continue
        raw = str(match.group(2) or "").strip()
        if "#" in raw:
            raw = raw.split("#", 1)[0].strip()
        raw = raw.strip("'\"")
        if _value_contains_inline_secret(spec, raw):
            keys.add(key)
    return tuple(sorted(keys))


def repo_local_secret_key_inventory(repo_root: str | Path | None = None) -> list[dict[str, Any]]:
    root = Path(repo_root).resolve() if repo_root is not None else _repo_root()
    by_key: dict[str, set[str]] = {}
    for path in _iter_repo_env_paths(root):
        for key in _parse_env_file_secret_keys(path, root):
            try:
                display = str(path.relative_to(root))
            except ValueError:
                display = str(path)
            by_key.setdefault(key, set()).add(display)
    return [
        {
            "key": key,
            "files": sorted(files),
            "kind": SECRET_ENV_SPEC_BY_KEY[key].kind,
            "alternatives": sorted(
                set(SECRET_ENV_SPEC_BY_KEY[key].file_envs)
                | set(SECRET_ENV_SPEC_BY_KEY[key].secret_envs)
                | {f"provider:{name}" for name in SECRET_ENV_SPEC_BY_KEY[key].provider_secret_names}
            ),
        }
        for key, files in sorted(by_key.items())
    ]


def _provider_configured(environ: Mapping[str, Any]) -> bool:
    return bool(
        _env_text(environ, "TS_SECRETS_PROVIDER")
        or _env_text(environ, "CREDENTIALS_DIRECTORY")
        or _env_text(environ, "TS_DEV_SECRETS_DIR")
    )


def _configured_broker_names(environ: Mapping[str, Any]) -> set[str]:
    names: set[str] = set()
    for env_name in ("LIVE_BROKER", "BROKER_NAME", "BROKER", "BROKER_FAILOVER"):
        raw = _env_text(environ, env_name).lower()
        if not raw:
            continue
        for part in re.split(r"[\s,;:]+", raw):
            text = part.strip()
            if text:
                names.add(text)
    return names


def _required_secret_groups(environ: Mapping[str, Any]) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    if _env_enabled(environ, "POLYGON_REST_ENABLED") or _env_enabled(environ, "POLYGON_WS_ENABLED"):
        groups.append(
            {
                "name": "polygon_market_data",
                "mode": "any",
                "keys": ("POLYGON_API_KEY", "POLYGON_KEY"),
                "trigger": "POLYGON_REST_ENABLED/POLYGON_WS_ENABLED",
            }
        )
    if _env_enabled(environ, "TRADIER_ENABLED"):
        groups.append(
            {
                "name": "tradier_market_data",
                "mode": "all",
                "keys": ("TRADIER_API_TOKEN",),
                "trigger": "TRADIER_ENABLED",
            }
        )
    broker_names = _configured_broker_names(environ)
    if "alpaca" in broker_names or _env_enabled(environ, "ALPACA_ENABLED"):
        groups.append(
            {
                "name": "alpaca_broker",
                "mode": "all",
                "keys": ("ALPACA_KEY_ID", "ALPACA_SECRET_KEY"),
                "trigger": "BROKER/LIVE_BROKER=alpaca",
            }
        )
    if _env_enabled(environ, "OPENAI_ENABLED"):
        groups.append(
            {
                "name": "openai_api",
                "mode": "all",
                "keys": ("OPENAI_API_KEY",),
                "trigger": "OPENAI_ENABLED",
            }
        )
    return groups


def _required_secret_keys(groups: list[dict[str, Any]]) -> set[str]:
    keys: set[str] = set()
    for group in groups:
        keys.update(str(key) for key in tuple(group.get("keys") or ()))
    return keys


def _file_permission_issue(path: Path) -> str:
    text = str(path)
    if os.path.normpath(text) in _PLACEHOLDER_SECRET_PATHS:
        return "placeholder_path"
    docker_secret = text.startswith("/run/secrets/")
    try:
        st = path.stat()
    except FileNotFoundError:
        return "missing"
    except OSError as exc:
        return f"stat_failed:{type(exc).__name__}"
    if not path.is_file():
        return "not_regular_file"
    if st.st_size <= 0:
        return "empty"
    if not os.access(path, os.R_OK):
        return "not_readable"
    mode = stat.S_IMODE(st.st_mode)
    if docker_secret:
        return ""
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        return f"insecure_permissions:{oct(mode)}"
    current_uid = os.geteuid() if hasattr(os, "geteuid") else st.st_uid
    if st.st_uid not in {0, current_uid}:
        return f"unexpected_owner_uid:{st.st_uid}"
    return ""


def _configured_file_sources(spec: SecretEnvSpec, environ: Mapping[str, Any], *, validate_files: bool) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for env_name in spec.file_envs:
        raw_path = _env_text(environ, env_name)
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        issue = _file_permission_issue(path) if validate_files else ""
        sources.append(
            {
                "type": "file",
                "env": env_name,
                "path": str(path),
                "ok": not issue,
                "issue": issue,
            }
        )
    return sources


def _configured_secret_sources(spec: SecretEnvSpec, environ: Mapping[str, Any]) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    for env_name in spec.secret_envs:
        secret_name = _env_text(environ, env_name)
        if secret_name:
            sources.append({"type": "provider_ref", "env": env_name, "secret_name": secret_name, "ok": True})
    if _provider_configured(environ):
        for secret_name in spec.provider_secret_names:
            sources.append({"type": "provider_default", "secret_name": secret_name, "ok": True})
    return sources


def secret_source_policy_snapshot(
    *,
    environ: Mapping[str, Any] | None = None,
    repo_root: str | Path | None = None,
    validate_files: bool = False,
) -> dict[str, Any]:
    env = os.environ if environ is None else environ
    root = Path(repo_root).resolve() if repo_root is not None else _repo_root(env)
    required = strict_secret_source_policy_required(env)
    suppressed = bool(required and _policy_suppressed_for_tests(env))
    provider = str(env.get("TS_SECRETS_PROVIDER") or "systemd-creds").strip().lower()
    inventory = repo_local_secret_key_inventory(root)
    inline_env: list[dict[str, Any]] = []
    approved: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []
    sources_by_key: dict[str, list[dict[str, Any]]] = {}
    required_groups = _required_secret_groups(env)
    required_keys = _required_secret_keys(required_groups)

    if required and not suppressed and provider == "plaintext":
        violations.append(
            {
                "key": "TS_SECRETS_PROVIDER",
                "kind": "config",
                "reason": "plaintext_provider_forbidden",
                "provider": provider,
            }
        )

    for spec in SECRET_ENV_SPECS:
        value = env.get(spec.key)
        inline_present = _value_contains_inline_secret(spec, value)
        file_sources = _configured_file_sources(spec, env, validate_files=validate_files)
        secret_sources = _configured_secret_sources(spec, env)
        sources = file_sources + secret_sources
        sources_by_key[spec.key] = sources
        approved.extend({**source, "key": spec.key} for source in sources if bool(source.get("ok")))

        if inline_present:
            inline_env.append({"key": spec.key, "kind": spec.kind, "source": "process_env"})
            if required and not suppressed:
                violations.append(
                    {
                        "key": spec.key,
                        "kind": spec.kind,
                        "reason": "inline_secret_env",
                        "approved_alternatives": sorted(set(spec.file_envs) | set(spec.secret_envs)),
                    }
                )
        validate_configured_files = spec.key in required_keys or spec.key not in OPTIONAL_PROVIDER_SECRET_KEYS
        if required and not suppressed and validate_configured_files:
            for source in file_sources:
                if not bool(source.get("ok")):
                    violations.append(
                        {
                            "key": spec.key,
                            "kind": spec.kind,
                            "reason": "secret_file_invalid",
                            "source_env": source.get("env"),
                            "issue": source.get("issue"),
                        }
                    )

    if required and not suppressed:
        for group in required_groups:
            keys = tuple(str(key) for key in tuple(group.get("keys") or ()) if str(key))
            mode = str(group.get("mode") or "all")
            if mode == "any":
                if any(any(bool(source.get("ok")) for source in sources_by_key.get(key, [])) for key in keys):
                    continue
                violations.append(
                    {
                        "key": keys[0] if keys else str(group.get("name") or "unknown"),
                        "kind": "secret",
                        "reason": "required_secret_source_missing",
                        "group": str(group.get("name") or ""),
                        "trigger": str(group.get("trigger") or ""),
                        "required_any": list(keys),
                    }
                )
                continue
            for key in keys:
                if any(bool(source.get("ok")) for source in sources_by_key.get(key, [])):
                    continue
                violations.append(
                    {
                        "key": key,
                        "kind": SECRET_ENV_SPEC_BY_KEY.get(key, SecretEnvSpec(key, "")).kind,
                        "reason": "required_secret_source_missing",
                        "group": str(group.get("name") or ""),
                        "trigger": str(group.get("trigger") or ""),
                    }
                )

    repo_inline = []
    for item in inventory:
        key = str(item.get("key") or "")
        spec = SECRET_ENV_SPEC_BY_KEY.get(key)
        repo_inline.append({"key": key, "kind": item.get("kind"), "files": list(item.get("files") or [])})
        if required and not suppressed and spec is not None:
            violations.append(
                {
                    "key": key,
                    "kind": spec.kind,
                    "reason": "repo_local_inline_secret",
                    "files": list(item.get("files") or []),
                    "approved_alternatives": sorted(set(spec.file_envs) | set(spec.secret_envs)),
                }
            )

    blockers = [
        f"{item['reason']}:{item['key']}"
        for item in violations
        if str(item.get("reason") or "")
        in {
            "inline_secret_env",
            "repo_local_inline_secret",
            "secret_file_invalid",
            "required_secret_source_missing",
            "plaintext_provider_forbidden",
        }
    ]
    blockers = list(dict.fromkeys(blockers))
    return {
        "ok": not blockers,
        "required": bool(required),
        "suppressed_for_tests": bool(suppressed),
        "reason": "ok" if not blockers else blockers[0],
        "blockers": blockers,
        "violations": violations,
        "inline_env": inline_env,
        "repo_local_inline": repo_inline,
        "approved_sources": approved,
        "required_secret_groups": required_groups,
        "inventory": inventory,
    }


def format_secret_source_policy_error(snapshot: Mapping[str, Any]) -> str:
    blockers = [str(item) for item in list(snapshot.get("blockers") or [])]
    if not blockers:
        return "secret source policy invalid"
    return (
        "production secret source policy invalid: "
        + "; ".join(blockers)
        + "; use *_FILE, *_SECRET, systemd credentials, Docker Compose secrets, or root-owned 0600 files"
    )


def validate_secret_source_policy(
    *,
    environ: Mapping[str, Any] | None = None,
    repo_root: str | Path | None = None,
    validate_files: bool = False,
) -> dict[str, Any]:
    snapshot = secret_source_policy_snapshot(
        environ=environ,
        repo_root=repo_root,
        validate_files=validate_files,
    )
    if not bool(snapshot.get("ok")):
        raise RuntimeError(format_secret_source_policy_error(snapshot))
    return snapshot


def read_secret_text_file(path: str | Path) -> SecretTextFileResult:
    candidate = Path(path).expanduser()
    path_text = str(candidate)
    try:
        st = candidate.stat()
    except FileNotFoundError:
        return SecretTextFileResult(
            ok=False,
            path=path_text,
            reason="missing",
            missing=True,
            error_type="FileNotFoundError",
        )
    except OSError as exc:
        return SecretTextFileResult(
            ok=False,
            path=path_text,
            reason="stat_failed",
            error_type=type(exc).__name__,
        )
    if not candidate.is_file():
        return SecretTextFileResult(ok=False, path=path_text, reason="not_regular")
    if st.st_size <= 0:
        return SecretTextFileResult(ok=False, path=path_text, reason="empty", missing=True, empty=True)
    if not os.access(candidate, os.R_OK):
        return SecretTextFileResult(ok=False, path=path_text, reason="not_readable")
    try:
        value = candidate.read_text(encoding="utf-8").strip()
    except OSError as exc:
        return SecretTextFileResult(
            ok=False,
            path=path_text,
            reason="read_failed",
            error_type=type(exc).__name__,
        )
    if not value:
        return SecretTextFileResult(ok=False, path=path_text, reason="empty", missing=True, empty=True)
    return SecretTextFileResult(ok=True, value=value, path=path_text, reason="ok")


def read_secret_text_from_env(
    key: str,
    *,
    environ: Mapping[str, Any] | None = None,
    file_envs: tuple[str, ...] | None = None,
    secret_envs: tuple[str, ...] | None = None,
    provider_secret_names: tuple[str, ...] | None = None,
) -> str:
    env = os.environ if environ is None else environ
    spec = SECRET_ENV_SPEC_BY_KEY.get(str(key))
    file_names = tuple(file_envs if file_envs is not None else (spec.file_envs if spec else (f"{key}_FILE",)))
    secret_names = tuple(secret_envs if secret_envs is not None else (spec.secret_envs if spec else (f"{key}_SECRET",)))
    provider_names = tuple(
        provider_secret_names
        if provider_secret_names is not None
        else (spec.provider_secret_names if spec else (str(key), str(key).lower()))
    )
    for env_name in file_names:
        path = _env_text(env, env_name)
        if path:
            return read_secret_text_file(path).value
    if env is not os.environ:
        return ""
    import importlib

    secret_loader = importlib.import_module("services.secrets.loader")
    load_secret = secret_loader.load_secret
    secret_not_available = getattr(secret_loader, "SecretNotAvailable", RuntimeError)

    for env_name in secret_names:
        secret_name = _env_text(env, env_name)
        if not secret_name:
            continue
        return load_secret(secret_name).decode("utf-8", "ignore").rstrip("\r\n")
    if _provider_configured(env):
        for secret_name in provider_names:
            try:
                value = load_secret(secret_name).decode("utf-8", "ignore").rstrip("\r\n")
            except secret_not_available:
                continue
            if value:
                return value
    return ""


def url_with_password(url: str, password: str) -> str:
    text = str(url or "").strip()
    if not text or not password:
        return text
    parsed = urlsplit(text)
    if parsed.password or not parsed.scheme or not parsed.hostname:
        return text
    user = quote(str(parsed.username or ""), safe="")
    host = str(parsed.hostname or "")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = f"{user}:{quote(password, safe='')}@" if user else f":{quote(password, safe='')}@"
    return urlunsplit((parsed.scheme, auth + host, parsed.path, parsed.query, parsed.fragment))


__all__ = [
    "REPO_LOCAL_ENV_RELATIVE_PATHS",
    "SECRET_ENV_SPECS",
    "SECRET_ENV_SPEC_BY_KEY",
    "SecretEnvSpec",
    "SecretTextFileResult",
    "format_secret_source_policy_error",
    "read_secret_text_file",
    "read_secret_text_from_env",
    "repo_local_secret_key_inventory",
    "secret_source_policy_snapshot",
    "strict_secret_source_policy_required",
    "url_with_password",
    "validate_secret_source_policy",
]
