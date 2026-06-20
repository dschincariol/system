from __future__ import annotations

"""Staging harness for capturing redacted production-preflight evidence."""

import argparse
import datetime as _dt
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit


REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIRM_PRODUCTION_PHRASE = "I_UNDERSTAND_THIS_USES_PRODUCTION_CREDENTIALS"
NON_PROD_TARGETS = frozenset(
    {
        "dev",
        "development",
        "local",
        "non-prod",
        "nonprod",
        "paper",
        "pre-prod",
        "preprod",
        "qa",
        "sandbox",
        "stage",
        "staging",
        "test",
        "uat",
    }
)
PRODUCTION_TARGETS = frozenset({"live", "prd", "prod", "production"})
PRODUCTION_MARKERS = frozenset({"live", "prd", "prod", "production"})
NON_PROD_MARKERS = NON_PROD_TARGETS | frozenset({"nonproduction", "nonprd"})
SAFE_BASE_ENV_KEYS = frozenset(
        {
        "HOME",
        "LANG",
        "LC_ALL",
        "PATH",
        "PYTHONPATH",
        "TEMP",
        "TMP",
        "TMPDIR",
        "USER",
        "USERNAME",
        "VIRTUAL_ENV",
        "WINDIR",
    }
)
REDACT_KEYWORDS = (
    "access_key",
    "api_key",
    "apikey",
    "auth",
    "credential",
    "hmac_key",
    "key_file",
    "master_key",
    "password",
    "passwd",
    "pgpass",
    "secret",
    "session_token",
    "token",
)
DSN_OR_URL_KEYS = (
    "dsn",
    "url",
    "uri",
    "endpoint",
)
ENV_SNAPSHOT_KEYS = (
    "ALLOW_TRAINING",
    "APP_ENV",
    "BACKUP_EVIDENCE_HMAC_KEY_FILE",
    "BACKUP_EVIDENCE_PATH",
    "BACKUP_EVIDENCE_SIGNATURE_MAX_AGE_S",
    "CREDENTIALS_DIRECTORY",
    "DATA_SOURCE_MASTER_KEY",
    "DASHBOARD_API_TOKEN",
    "DB_PATH",
    "DEPLOYMENT_ENV",
    "DISABLE_LIVE_EXECUTION",
    "ENGINE_MODE",
    "ENV",
    "EXECUTION_MODE",
    "KILL_SWITCH_GLOBAL",
    "LIVE_BROKER",
    "LIVE_CACHE_REDIS_URL",
    "NODE_ENV",
    "OBJECT_STORE_ACCESS_KEY",
    "OBJECT_STORE_ENDPOINT",
    "OBJECT_STORE_SECRET_KEY",
    "OPERATOR_MODE",
    "PGDATABASE",
    "PGHOST",
    "PGPASSWORD",
    "PGPORT",
    "PGUSER",
    "PREFLIGHT_REQUIRE_BACKUP_EVIDENCE",
    "PREFLIGHT_REQUIRE_OBJECT_STORAGE",
    "PREFLIGHT_REQUIRE_REDIS",
    "PREFLIGHT_REQUIRE_TIMESCALE",
    "PROD_LOCK",
    "REDIS_PASSWORD",
    "REDIS_URL",
    "STAGING_PREFLIGHT_TARGET_ENV",
    "STAGING_PREFLIGHT_TARGET_ID",
    "TIMESCALE_DSN",
    "TIMESCALE_PRICES_DSN",
    "TRADING_ENV",
    "TS_DEV_SECRETS_DIR",
    "TS_ENV",
    "TS_PG_DSN",
    "TS_PG_PASSWORD",
    "TS_PG_ROLE",
    "TS_SECRETS_PROVIDER",
    "TS_STORAGE_BACKEND",
)
TOKEN_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)
KEY_VALUE_SECRET_RE = re.compile(
    r"(?P<key>(?:password|passwd|secret|token|api[_-]?key|access[_-]?key|session[_-]?token))="
    r"(?P<quote>['\"]?)(?P<value>[^'\"\s]+)(?P=quote)",
    re.IGNORECASE,
)


class GuardrailError(RuntimeError):
    """Raised when the staging harness refuses to launch preflight."""


def _utc_now() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.timezone.utc)


def _utc_stamp(now: _dt.datetime | None = None) -> str:
    return (now or _utc_now()).strftime("%Y%m%dT%H%M%SZ")


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _strip_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            raise GuardrailError(f"{path}:{line_number}: expected KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            raise GuardrailError(f"{path}:{line_number}: empty environment key")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise GuardrailError(f"{path}:{line_number}: invalid environment key: {key}")
        values[key] = _strip_quotes(value)
    return values


def _safe_base_env(base_env: Mapping[str, str]) -> dict[str, str]:
    env = {key: str(value) for key, value in base_env.items() if key in SAFE_BASE_ENV_KEYS and value}
    env["PYTHONPATH"] = str(REPO_ROOT) if not env.get("PYTHONPATH") else f"{REPO_ROOT}{os.pathsep}{env['PYTHONPATH']}"
    return env


def load_child_env(
    env_files: Sequence[Path],
    *,
    base_env: Mapping[str, str] | None = None,
    allow_ambient_env: bool = False,
) -> tuple[dict[str, str], list[str]]:
    base = dict(base_env or os.environ)
    child_env = dict(base) if allow_ambient_env else _safe_base_env(base)
    loaded: list[str] = []
    for env_file in env_files:
        path = env_file.expanduser()
        if not path.exists():
            raise GuardrailError(f"env file does not exist: {path}")
        if not path.is_file():
            raise GuardrailError(f"env file is not a regular file: {path}")
        child_env.update(parse_env_file(path))
        loaded.append(str(path))

    child_env.setdefault("TS_STORAGE_BACKEND", "postgres")
    child_env.setdefault("ENGINE_SUPERVISED", "1")
    child_env.setdefault("PYTHONUNBUFFERED", "1")
    child_env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    child_env.setdefault("PROD_LOCK", "1")
    child_env.setdefault("ALLOW_TRAINING", "0")
    child_env.setdefault("DISABLE_LIVE_EXECUTION", "1")
    child_env.setdefault("KILL_SWITCH_GLOBAL", "1")
    child_env.setdefault("PREFLIGHT_ISOLATE_SMOKE_DB", "1")
    return child_env, loaded


def _normalize_target(raw: str | None) -> str:
    return str(raw or "").strip().lower().replace("_", "-")


def _tokens(value: str) -> set[str]:
    return {match.group(0).lower() for match in TOKEN_RE.finditer(str(value or ""))}


def _has_prod_marker(value: str) -> bool:
    tokens = _tokens(value)
    if tokens & NON_PROD_MARKERS:
        return False
    return bool(tokens & PRODUCTION_MARKERS)


def _is_sensitive_key(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(marker in lowered for marker in REDACT_KEYWORDS)


def _is_dsn_or_url_key(key: str) -> bool:
    lowered = str(key or "").lower()
    return any(marker in lowered for marker in DSN_OR_URL_KEYS)


def _redaction_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "surrogatepass")).hexdigest()[:12]


def _redacted_marker(value: str) -> str:
    return f"<redacted:{_redaction_digest(value)}>"


def _redact_url(value: str) -> str:
    text = str(value or "")
    try:
        parts = urlsplit(text)
    except ValueError:
        return text
    if not parts.scheme or not parts.netloc or "@" not in parts.netloc:
        return text
    host = parts.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parts.port:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, f"<redacted>@{host}", parts.path, parts.query, parts.fragment))


def _redact_conninfo(value: str) -> str:
    text = str(value or "")
    text = _redact_url(text)
    return KEY_VALUE_SECRET_RE.sub(lambda match: f"{match.group('key')}={_redacted_marker(match.group('value'))}", text)


def sensitive_values(env: Mapping[str, str]) -> set[str]:
    values: set[str] = set()
    for key, value in env.items():
        text = str(value or "")
        if not text:
            continue
        if _is_sensitive_key(key):
            values.add(text)
        elif _is_dsn_or_url_key(key) and _redact_conninfo(text) != text:
            values.add(text)
            values.update(match.group("value") for match in KEY_VALUE_SECRET_RE.finditer(text))
    return values


def redact_string(value: str, known_sensitive_values: set[str] | None = None) -> str:
    text = _redact_conninfo(str(value or ""))
    for secret in sorted(known_sensitive_values or set(), key=len, reverse=True):
        if secret and len(secret) >= 4:
            text = text.replace(secret, _redacted_marker(secret))
    return text


def redact_json(value: Any, known_sensitive_values: set[str] | None = None, *, key: str = "") -> Any:
    if isinstance(value, Mapping):
        return {
            str(item_key): redact_json(item_value, known_sensitive_values, key=str(item_key))
            for item_key, item_value in value.items()
        }
    if isinstance(value, list):
        return [redact_json(item, known_sensitive_values, key=key) for item in value]
    if isinstance(value, tuple):
        return [redact_json(item, known_sensitive_values, key=key) for item in value]
    if isinstance(value, str):
        if _is_sensitive_key(key):
            return _redacted_marker(value) if value else ""
        if _is_dsn_or_url_key(key):
            return redact_string(value, known_sensitive_values)
        return redact_string(value, known_sensitive_values)
    return value


def redacted_env_snapshot(env: Mapping[str, str]) -> dict[str, str]:
    known = sensitive_values(env)
    keys = sorted(set(ENV_SNAPSHOT_KEYS) | {key for key in env if key.startswith("STAGING_PREFLIGHT_")})
    return {
        key: str(redact_json(str(env.get(key) or ""), known, key=key))
        for key in keys
        if key in env
    }


def _parse_key_value_conninfo(dsn: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    try:
        parts = shlex.split(str(dsn or ""))
    except ValueError:
        parts = str(dsn or "").split()
    for part in parts:
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        parsed[key.strip().lower()] = value.strip()
    return parsed


def postgres_target_snapshot(env: Mapping[str, str]) -> dict[str, Any]:
    dsn = str(env.get("TS_PG_DSN") or "")
    snapshot: dict[str, Any] = {"dsn_configured": bool(dsn.strip())}
    if not dsn.strip():
        return snapshot

    known = sensitive_values(env)
    snapshot["dsn"] = redact_string(dsn, known)
    parts = urlsplit(dsn)
    if parts.scheme in {"postgres", "postgresql"}:
        snapshot.update(
            {
                "scheme": parts.scheme,
                "host": parts.hostname,
                "port": parts.port,
                "database": parts.path.lstrip("/"),
                "user": parts.username,
            }
        )
        return snapshot

    kv = _parse_key_value_conninfo(dsn)
    for key, value in kv.items():
        if key in {"host", "hostaddr", "port", "dbname", "user", "sslmode", "target_session_attrs"}:
            snapshot[key] = value
    return snapshot


def _production_signal_findings(env: Mapping[str, str], target_env: str) -> list[str]:
    findings: list[str] = []
    if target_env in PRODUCTION_TARGETS:
        findings.append(f"target environment is production-like: {target_env}")

    for key in ("APP_ENV", "ENV", "TRADING_ENV", "DEPLOYMENT_ENV", "TS_ENV"):
        value = str(env.get(key) or "").strip()
        if value and _normalize_target(value) in PRODUCTION_TARGETS:
            findings.append(f"{key}={value} is production-like")

    for key in ("ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        value = str(env.get(key) or "").strip().lower()
        if value == "live":
            findings.append(f"{key}=live")

    for key in ("TS_PG_DSN", "TIMESCALE_DSN", "TIMESCALE_PRICES_DSN", "REDIS_URL", "LIVE_CACHE_REDIS_URL"):
        value = str(env.get(key) or "").strip()
        if value and _has_prod_marker(value):
            findings.append(f"{key} contains a production-like marker")

    etc_root = Path("/") / "etc"
    default_prod_paths = {
        str(etc_root / "credstore.encrypted"),
        str(etc_root / "trading"),
        str(etc_root / "trading" / "trading.env"),
    }
    for key in ("CREDENTIALS_DIRECTORY", "TS_DEV_SECRETS_DIR", "TRADING_ENV_FILE"):
        value = str(env.get(key) or "").strip()
        normalized = value.rstrip("/")
        if normalized in default_prod_paths:
            findings.append(f"{key} points at the default production path: {value}")

    return findings


def validate_guardrails(
    env: Mapping[str, str],
    *,
    target_env: str,
    allow_production_target: bool = False,
    production_confirmation: str = "",
) -> list[str]:
    normalized_target = _normalize_target(target_env)
    if not normalized_target:
        raise GuardrailError(
            "target environment is required; pass --target-env staging or set STAGING_PREFLIGHT_TARGET_ENV in the env file"
        )
    if normalized_target not in NON_PROD_TARGETS and normalized_target not in PRODUCTION_TARGETS:
        raise GuardrailError(
            "target environment must be explicit and recognized as non-prod "
            f"({', '.join(sorted(NON_PROD_TARGETS))}) or intentionally confirmed production"
        )

    backend = str(env.get("TS_STORAGE_BACKEND") or "").strip().lower()
    if backend not in {"postgres", "pg"}:
        raise GuardrailError(f"staging prod preflight requires TS_STORAGE_BACKEND=postgres, got {backend or '<unset>'}")

    if not str(env.get("TS_PG_DSN") or "").strip():
        raise GuardrailError("staging prod preflight requires TS_PG_DSN for the target Postgres database")

    findings = _production_signal_findings(env, normalized_target)
    if findings:
        confirmed = bool(allow_production_target and production_confirmation == CONFIRM_PRODUCTION_PHRASE)
        if not confirmed:
            rendered = "; ".join(findings)
            raise GuardrailError(
                "refusing to run staging prod preflight with production-like target signals: "
                f"{rendered}. Pass --allow-production-target and "
                f"--confirm-production-target {CONFIRM_PRODUCTION_PHRASE!r} only when this is intentional."
            )
        return findings

    if normalized_target in PRODUCTION_TARGETS:
        raise GuardrailError("production target confirmation was not accepted")
    return []


def _evidence_path(evidence_dir: Path, target_env: str, label: str | None, now: _dt.datetime | None = None) -> Path:
    safe_target = re.sub(r"[^A-Za-z0-9_.-]+", "_", _normalize_target(target_env) or "unknown")
    safe_label = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(label or "").strip())
    stem = f"prod_preflight_{_utc_stamp(now)}"
    if safe_label:
        stem = f"{stem}_{safe_label}"
    return evidence_dir / safe_target / f"{stem}.json"


def _write_evidence(path: Path, evidence: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.parent.chmod(0o700)
    except OSError:
        pass
    tmp_path = path.with_name(f".{path.name}.tmp")
    tmp_path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    try:
        tmp_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    tmp_path.replace(path)


def _parse_stdout_json(stdout: str) -> tuple[dict[str, Any] | None, str | None]:
    text = str(stdout or "").strip()
    if not text:
        return None, "empty_stdout"
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        return None, f"json_parse_failed:{exc}"
    if not isinstance(parsed, dict):
        return None, "json_output_not_object"
    return parsed, None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run engine/runtime/prod_preflight.py --json against an explicit staging Postgres target."
    )
    parser.add_argument("--env-file", action="append", default=[], help="Env file to load; may be supplied more than once.")
    parser.add_argument("--target-env", default="", help="Explicit target environment, usually 'staging'.")
    parser.add_argument("--evidence-dir", default=str(REPO_ROOT / "var" / "artifacts" / "preflight"))
    parser.add_argument("--evidence-label", default="")
    parser.add_argument("--timeout-s", type=int, default=int(os.environ.get("PREFLIGHT_SMOKE_TIMEOUT_S", "900")))
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument(
        "--allow-ambient-env",
        action="store_true",
        help="Start from the full ambient environment instead of only safe process variables.",
    )
    parser.add_argument(
        "--allow-production-target",
        action="store_true",
        help="Permit production-like target signals only when the confirmation phrase is also supplied.",
    )
    parser.add_argument("--confirm-production-target", default="")
    return parser


def run(argv: Sequence[str] | None = None, *, base_env: Mapping[str, str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    env_files = [Path(item) for item in args.env_file]
    if not env_files and not args.allow_ambient_env:
        parser.error("--env-file is required unless --allow-ambient-env is supplied")

    started = time.monotonic()
    generated_at = _utc_now()
    evidence_dir = Path(args.evidence_dir).expanduser()
    child_env: dict[str, str] = {}
    loaded_env_files: list[str] = []
    guardrail_findings: list[str] = []
    guardrail_error = ""
    target_env = ""
    known_sensitive: set[str] = set()
    prod_preflight_json: dict[str, Any] | None = None
    stdout_text = ""
    stderr_text = ""
    returncode = 3

    try:
        child_env, loaded_env_files = load_child_env(
            env_files,
            base_env=base_env,
            allow_ambient_env=bool(args.allow_ambient_env),
        )
        target_env = _normalize_target(args.target_env or child_env.get("STAGING_PREFLIGHT_TARGET_ENV"))
        if target_env:
            child_env["STAGING_PREFLIGHT_TARGET_ENV"] = target_env
            child_env["PREFLIGHT_TARGET_ENV"] = target_env
        known_sensitive = sensitive_values(child_env)
        guardrail_findings = validate_guardrails(
            child_env,
            target_env=target_env,
            allow_production_target=bool(args.allow_production_target),
            production_confirmation=str(
                args.confirm_production_target
                or child_env.get("STAGING_PREFLIGHT_CONFIRM_PRODUCTION_TARGET")
                or ""
            ),
        )

        command = [str(args.python), "engine/runtime/prod_preflight.py", "--json", "--timeout_s", str(args.timeout_s)]
        proc = subprocess.run(
            command,
            cwd=str(REPO_ROOT),
            env=child_env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=max(int(args.timeout_s) + 30, int(args.timeout_s)),
            check=False,
        )
        returncode = int(proc.returncode)
        stdout_text = proc.stdout or ""
        stderr_text = proc.stderr or ""
        prod_preflight_json, parse_error = _parse_stdout_json(stdout_text)
        if parse_error:
            guardrail_findings.append(f"prod_preflight_stdout_{parse_error}")
    except GuardrailError as exc:
        guardrail_error = str(exc)
        stderr_text = guardrail_error
        returncode = 3
    except subprocess.TimeoutExpired as exc:
        stderr_text = f"staging prod preflight timed out after {exc.timeout}s"
        stdout_text = exc.stdout or ""
        if isinstance(stdout_text, bytes):
            stdout_text = stdout_text.decode("utf-8", "replace")
        returncode = 124

    duration_ms = int((time.monotonic() - started) * 1000)
    evidence_path = _evidence_path(evidence_dir, target_env or "unknown", args.evidence_label, generated_at)
    evidence = {
        "schema_version": 1,
        "generated_at": generated_at.isoformat(),
        "duration_ms": duration_ms,
        "target": {
            "env": target_env,
            "id": str(child_env.get("STAGING_PREFLIGHT_TARGET_ID") or ""),
            "env_files": loaded_env_files,
            "allow_ambient_env": bool(args.allow_ambient_env),
            "production_override": bool(args.allow_production_target),
        },
        "guardrails": {
            "ok": not bool(guardrail_error),
            "error": guardrail_error,
            "findings": guardrail_findings,
            "production_confirmation_required": CONFIRM_PRODUCTION_PHRASE,
        },
        "command": {
            "argv": [str(args.python), "engine/runtime/prod_preflight.py", "--json", "--timeout_s", str(args.timeout_s)],
            "cwd": str(REPO_ROOT),
            "timeout_s": int(args.timeout_s),
        },
        "environment": {
            "redacted": redacted_env_snapshot(child_env),
            "postgres_target": postgres_target_snapshot(child_env),
        },
        "process": {
            "returncode": returncode,
        },
        "prod_preflight": redact_json(prod_preflight_json, known_sensitive) if prod_preflight_json is not None else None,
        "stdout": redact_string(stdout_text, known_sensitive),
        "stderr": redact_string(stderr_text, known_sensitive),
    }
    _write_evidence(evidence_path, evidence)

    print(
        json.dumps(
            {
                "ok": returncode == 0,
                "returncode": returncode,
                "target_env": target_env,
                "evidence_path": _repo_relative(evidence_path),
                "guardrail_error": guardrail_error,
            },
            sort_keys=True,
        )
    )
    return returncode


def main() -> int:
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
