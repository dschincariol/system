"""Runtime checks for external dependency readiness.

These checks are intentionally lightweight and dependency-minimal so production
preflight can verify that the configured Timescale/Postgres, Redis, and object
storage endpoints are reachable before runtime smoke jobs start.
"""

from __future__ import annotations

import os
import socket
import hashlib
import hmac
from pathlib import Path
from typing import Any
from datetime import datetime, timezone
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import quote, urlparse, urlunparse

from engine.runtime.artifact_store import OBJECT_STORAGE_SCHEMES

_TIMESCALE_SCHEMES = frozenset({"postgres", "postgresql"})
_REDIS_SCHEMES = frozenset({"redis", "rediss"})
_HTTP_SCHEMES = frozenset({"http", "https"})


def _env_bool(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if raw == "":
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if raw == "":
        return float(default)
    try:
        return float(raw)
    except Exception:
        return float(default)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _secret_text_from_env(*env_names: str) -> str:
    secret_name = ""
    for env_name in env_names:
        name = _clean_text(env_name)
        file_env_names = [name] if name.endswith("_FILE") else []
        if name.endswith("_SECRET"):
            file_env_names.append(f"{name.removesuffix('_SECRET')}_FILE")
        for file_env_name in file_env_names:
            path = _clean_text(os.environ.get(file_env_name))
            if path:
                from engine.runtime.secret_sources import read_secret_text_file

                return read_secret_text_file(path)
        if name.endswith("_FILE"):
            continue
        secret_name = _clean_text(os.environ.get(name))
        if secret_name:
            break
    if not secret_name:
        return ""
    from services.secrets.loader import load_secret

    return load_secret(secret_name).decode("utf-8", "ignore").rstrip("\r\n")


def _object_scheme(value: Any) -> str:
    text = _clean_text(value)
    if "://" not in text:
        return ""
    return str(urlparse(text).scheme or "").strip().lower()


def _timescale_backend_requires_service(value: Any) -> bool:
    backend = _clean_text(value).lower()
    return backend in {"postgres", "postgresql", "pg", "timescale", "timescaledb"}


def _probe_socket(host: str, port: int, *, timeout_s: float) -> tuple[bool, str | None]:
    try:
        with socket.create_connection((str(host), int(port)), timeout=float(timeout_s)):
            return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _probe_postgres(dsn: str, *, timeout_s: float) -> tuple[bool, str | None]:
    try:
        import psycopg

        timeout = max(1, int(float(timeout_s)))
        con = psycopg.connect(str(dsn), connect_timeout=timeout)
        try:
            with con.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
            if not row or int(row[0]) != 1:
                return False, "postgres validation query returned unexpected result"
        finally:
            con.close()
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _postgres_probe_dsn(dsn: str) -> str:
    """Return a probe DSN with the configured runtime credential attached."""

    raw = str(dsn or "").strip()
    parsed = urlparse(raw)
    scheme = str(parsed.scheme or "").strip().lower()
    if scheme in _TIMESCALE_SCHEMES and parsed.hostname:
        if parsed.password:
            return raw
        from engine.runtime.platform import connection_info_with_pg_password

        return connection_info_with_pg_password(raw)

    from engine.runtime.platform import connection_info_with_pg_password

    return connection_info_with_pg_password(raw)


def _redis_command(*parts: str) -> bytes:
    encoded = [str(part).encode("utf-8") for part in parts]
    payload = [f"*{len(encoded)}\r\n".encode("ascii")]
    for part in encoded:
        payload.append(f"${len(part)}\r\n".encode("ascii"))
        payload.append(part)
        payload.append(b"\r\n")
    return b"".join(payload)


def _url_with_password(url: str, password: str) -> str:
    text = _clean_text(url)
    if not text or not password:
        return text
    parsed = urlparse(text)
    if parsed.password:
        return text
    if not parsed.scheme or not parsed.hostname:
        return text
    user = quote(str(parsed.username or ""), safe="")
    host = str(parsed.hostname or "")
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port:
        host = f"{host}:{parsed.port}"
    auth = f"{user}:{quote(password, safe='')}@" if user else f":{quote(password, safe='')}@"
    return urlunparse((parsed.scheme, auth + host, parsed.path, parsed.params, parsed.query, parsed.fragment))


def _redis_probe_url(redis_url: str) -> str:
    password = _secret_text_from_env(
        "LIVE_CACHE_REDIS_PASSWORD_SECRET",
        "TS_REDIS_PASSWORD_SECRET",
        "REDIS_PASSWORD_SECRET",
    )
    return _url_with_password(redis_url, password)


def _redis_ok(response: bytes, expected: bytes) -> bool:
    return response.startswith(expected)


def _probe_redis(redis_url: str, *, timeout_s: float) -> tuple[bool, str | None]:
    try:
        parsed = urlparse(str(redis_url))
        host = _clean_text(parsed.hostname)
        port = int(parsed.port or 6379)
        username = _clean_text(parsed.username)
        password = _clean_text(parsed.password)
        if not host:
            return False, "redis URL missing host"

        with socket.create_connection((host, port), timeout=float(timeout_s)) as sock:
            sock.settimeout(float(timeout_s))
            if password:
                if username:
                    sock.sendall(_redis_command("AUTH", username, password))
                else:
                    sock.sendall(_redis_command("AUTH", password))
                auth_response = sock.recv(4096)
                if not _redis_ok(auth_response, b"+OK"):
                    return False, "redis AUTH failed"

            sock.sendall(_redis_command("PING"))
            ping_response = sock.recv(4096)
            if not _redis_ok(ping_response, b"+PONG"):
                return False, "redis PING failed"
        return True, None
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    key_date = hmac.new(("AWS4" + secret_key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    key_region = hmac.new(key_date, region.encode("utf-8"), hashlib.sha256).digest()
    key_service = hmac.new(key_region, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()


def _object_bucket_url(endpoint: str, bucket: str) -> str:
    parsed = urlparse(endpoint if "://" in endpoint else f"http://{endpoint}")
    base_path = str(parsed.path or "").rstrip("/")
    bucket_path = "/" + "/".join(part for part in (base_path.strip("/"), quote(bucket, safe="")) if part)
    return urlunparse((parsed.scheme or "http", parsed.netloc, bucket_path, "", "", ""))


def _probe_object_storage_bucket(
    *,
    endpoint: str,
    bucket: str,
    access_key: str,
    secret_key: str,
    region: str,
    timeout_s: float,
) -> tuple[bool, str | None]:
    try:
        url = _object_bucket_url(endpoint, bucket)
        parsed = urlparse(url)
        host = str(parsed.netloc or "").strip()
        if not host:
            return False, "object storage endpoint missing host"

        now = datetime.now(timezone.utc)
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        region_name = str(region or "").strip() or "us-east-1"
        payload_hash = hashlib.sha256(b"").hexdigest()
        canonical_uri = quote(str(parsed.path or "/"), safe="/~")
        headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        token = _clean_text(
            os.environ.get("OBJECT_STORE_SESSION_TOKEN")
            or os.environ.get("AWS_SESSION_TOKEN")
        )
        if token:
            headers["x-amz-security-token"] = token

        signed_header_names = sorted(headers)
        canonical_headers = "".join(f"{name}:{headers[name]}\n" for name in signed_header_names)
        signed_headers = ";".join(signed_header_names)
        canonical_request = "\n".join(
            [
                "HEAD",
                canonical_uri,
                "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{region_name}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signature = hmac.new(
            _signing_key(secret_key, date_stamp, region_name, "s3"),
            string_to_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers["Authorization"] = (
            "AWS4-HMAC-SHA256 "
            f"Credential={access_key}/{credential_scope},"
            f"SignedHeaders={signed_headers},"
            f"Signature={signature}"
        )

        req = urllib_request.Request(url, headers=headers, method="HEAD")
        with urllib_request.urlopen(req, timeout=float(timeout_s)) as resp:
            status = int(getattr(resp, "status", 0) or 0)
        if status in {200, 204}:
            return True, None
        return False, f"object storage bucket check returned HTTP {status}"
    except urllib_error.HTTPError as exc:
        status = int(getattr(exc, "code", 0) or 0)
        if status == 404:
            return False, "object storage bucket not found"
        if status in {401, 403}:
            return False, "object storage credentials rejected"
        return False, f"object storage bucket check returned HTTP {status}"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"


def _network_target(value: Any, *, default_scheme: str, default_port: int) -> tuple[str | None, int | None, str | None]:
    text = _clean_text(value)
    if not text:
        return None, None, None
    parsed = urlparse(text if "://" in text else f"{default_scheme}://{text}")
    host = _clean_text(parsed.hostname)
    if not host:
        return None, None, None
    port = int(parsed.port or default_port)
    target = f"{host}:{port}"
    return host, port, target


def _service_status(
    *,
    name: str,
    required: bool,
    configured: bool,
    target: str | None = None,
) -> dict[str, Any]:
    return {
        "name": str(name),
        "required": bool(required),
        "configured": bool(configured),
        "target": str(target) if target else None,
        "reachable": None,
        "ok": False,
        "notes": [],
        "warnings": [],
        "errors": [],
    }


def _append_summary(result: dict[str, Any], status: dict[str, Any]) -> None:
    result["services"].append(status)
    result["notes"].extend(list(status.get("notes") or []))
    result["warnings"].extend(list(status.get("warnings") or []))
    result["errors"].extend(list(status.get("errors") or []))


def _check_timescale_service(*, name: str, dsn: Any, required: bool, timeout_s: float) -> dict[str, Any] | None:
    dsn_text = _clean_text(dsn)
    if not dsn_text and not required:
        return None
    host, port, target = _network_target(dsn_text, default_scheme="postgresql", default_port=5432)
    status = _service_status(name=name, required=required, configured=bool(host and port), target=target)
    if not dsn_text:
        status["errors"].append(f"{name} required but DSN is missing")
        return status
    scheme = str(urlparse(dsn_text).scheme or "").strip().lower()
    if scheme and scheme not in _TIMESCALE_SCHEMES:
        status["errors"].append(f"{name} has unsupported DSN scheme: {scheme}")
        return status
    if not host or not port:
        status["errors"].append(f"{name} DSN missing host or port")
        return status
    try:
        probe_dsn = _postgres_probe_dsn(dsn_text)
    except Exception as exc:
        status["reachable"] = False
        message = f"{name} credential resolution failed target={target} error={type(exc).__name__}: {exc}"
        if required:
            status["errors"].append(message)
        else:
            status["warnings"].append(message)
        return status

    reachable, error = _probe_postgres(probe_dsn, timeout_s=timeout_s)
    status["reachable"] = bool(reachable)
    if reachable:
        status["ok"] = True
        status["notes"].append(f"{name} authenticated query ok target={target}")
    else:
        message = f"{name} unreachable target={target} error={error or 'unknown'}"
        if required:
            status["errors"].append(message)
        else:
            status["warnings"].append(message)
    return status


def _check_redis_service(*, required: bool, timeout_s: float) -> dict[str, Any] | None:
    backend = _clean_text(os.environ.get("LIVE_CACHE_BACKEND", "auto")).lower() or "auto"
    redis_url = _clean_text(
        os.environ.get("LIVE_CACHE_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or os.environ.get("REDIS_CACHE_URL")
    )
    active = required or backend == "redis"
    if not active:
        return None

    host, port, target = _network_target(redis_url, default_scheme="redis", default_port=6379)
    status = _service_status(name="live_cache_redis", required=(required or backend == "redis"), configured=bool(host and port), target=target)
    if not redis_url:
        status["errors"].append("live_cache_redis required but URL is missing")
        return status
    scheme = str(urlparse(redis_url).scheme or "").strip().lower()
    if scheme and scheme not in _REDIS_SCHEMES:
        status["errors"].append(f"live_cache_redis has unsupported URL scheme: {scheme}")
        return status
    if not host or not port:
        status["errors"].append("live_cache_redis URL missing host or port")
        return status
    try:
        probe_url = _redis_probe_url(redis_url)
    except Exception as exc:
        status["reachable"] = False
        message = f"live_cache_redis credential resolution failed target={target} error={type(exc).__name__}: {exc}"
        if status["required"]:
            status["errors"].append(message)
        else:
            status["warnings"].append(message)
        return status

    reachable, error = _probe_redis(probe_url, timeout_s=timeout_s)
    status["reachable"] = bool(reachable)
    if reachable:
        status["ok"] = True
        status["notes"].append(f"live_cache_redis ping ok target={target} backend={backend}")
        if backend == "auto":
            status["warnings"].append("live_cache_redis configured while backend=auto; runtime can still fall back to memory")
    else:
        message = f"live_cache_redis unreachable target={target} error={error or 'unknown'}"
        if status["required"]:
            status["errors"].append(message)
        else:
            status["warnings"].append(message)
    return status


def _check_object_storage_service(*, required: bool, timeout_s: float) -> dict[str, Any] | None:
    endpoint = _clean_text(
        os.environ.get("OBJECT_STORE_ENDPOINT")
        or os.environ.get("MINIO_ENDPOINT")
        or os.environ.get("S3_ENDPOINT")
    )
    bucket = _clean_text(
        os.environ.get("OBJECT_STORE_BUCKET")
        or os.environ.get("MINIO_BUCKET")
        or os.environ.get("S3_BUCKET")
    )
    access_key = _clean_text(
        os.environ.get("OBJECT_STORE_ACCESS_KEY")
        or os.environ.get("MINIO_ACCESS_KEY")
        or os.environ.get("AWS_ACCESS_KEY_ID")
    )
    if not access_key:
        try:
            access_key = _secret_text_from_env(
                "OBJECT_STORE_ACCESS_KEY_FILE",
                "MINIO_ACCESS_KEY_FILE",
                "AWS_ACCESS_KEY_ID_FILE",
                "OBJECT_STORE_ACCESS_KEY_SECRET",
                "MINIO_ACCESS_KEY_SECRET",
                "AWS_ACCESS_KEY_ID_SECRET",
            )
        except Exception:
            access_key = ""
    dataset_prefix = _clean_text(os.environ.get("TRAINING_DATASET_URI_PREFIX"))
    object_scheme = _object_scheme(dataset_prefix)
    active = required or object_scheme in OBJECT_STORAGE_SCHEMES
    if not active:
        return None

    secret_key = _clean_text(
        os.environ.get("OBJECT_STORE_SECRET_KEY")
        or os.environ.get("MINIO_SECRET_KEY")
        or os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    if not secret_key:
        try:
            secret_key = _secret_text_from_env(
                "OBJECT_STORE_SECRET_KEY_FILE",
                "MINIO_SECRET_KEY_FILE",
                "AWS_SECRET_ACCESS_KEY_FILE",
                "OBJECT_STORE_SECRET_KEY_SECRET",
                "MINIO_SECRET_KEY_SECRET",
                "AWS_SECRET_ACCESS_KEY_SECRET",
            )
        except Exception as exc:
            secret_key = ""
            secret_key_error = f"{type(exc).__name__}: {exc}"
        else:
            secret_key_error = ""
    else:
        secret_key_error = ""

    host, port, target = _network_target(endpoint, default_scheme="http", default_port=9000)
    status = _service_status(
        name="object_storage",
        required=bool(required or object_scheme in OBJECT_STORAGE_SCHEMES),
        configured=bool(host and port and bucket and access_key and secret_key),
        target=target,
    )
    if not endpoint:
        status["errors"].append("object_storage required but endpoint is missing")
    else:
        scheme = str(urlparse(endpoint if "://" in endpoint else f"http://{endpoint}").scheme or "").strip().lower()
        if scheme and scheme not in _HTTP_SCHEMES:
            status["errors"].append(f"object_storage has unsupported endpoint scheme: {scheme}")
    if not bucket:
        status["errors"].append("object_storage bucket is missing")
    if not access_key:
        status["errors"].append("object_storage access key is missing")
    if not secret_key:
        if secret_key_error:
            status["errors"].append(f"object_storage secret key credential resolution failed: {secret_key_error}")
        else:
            status["errors"].append("object_storage secret key is missing")
    if not host or not port:
        status["errors"].append("object_storage endpoint missing host or port")
    if status["errors"]:
        status["reachable"] = False
    else:
        reachable, error = _probe_object_storage_bucket(
            endpoint=endpoint,
            bucket=bucket,
            access_key=access_key,
            secret_key=secret_key,
            region=_clean_text(os.environ.get("OBJECT_STORE_REGION") or os.environ.get("AWS_REGION")),
            timeout_s=timeout_s,
        )
        status["reachable"] = bool(reachable)
        if reachable:
            status["ok"] = True
            status["notes"].append(f"object_storage bucket check ok target={target} bucket={bucket}")
        else:
            message = f"object_storage unreachable target={target} error={error or 'unknown'}"
            if status["required"]:
                status["errors"].append(message)
            else:
                status["warnings"].append(message)

    mirror_root = _clean_text(os.environ.get("ARTIFACT_STORE_MIRROR_ROOT"))
    if mirror_root:
        mirror_path = Path(mirror_root).expanduser()
        try:
            mirror_path.mkdir(parents=True, exist_ok=True)
            status["notes"].append(f"artifact_mirror_root ready path={mirror_path}")
        except Exception as exc:
            status["errors"].append(f"artifact_mirror_root unavailable path={mirror_path} error={type(exc).__name__}: {exc}")
            status["ok"] = False
    elif status["required"]:
        status["errors"].append("artifact_mirror_root missing for required object storage")
        status["ok"] = False

    return status


def check_external_service_readiness() -> dict[str, Any]:
    timeout_s = max(0.1, _env_float("PREFLIGHT_EXTERNAL_TIMEOUT_S", 2.0))
    result: dict[str, Any] = {
        "ok": True,
        "notes": [],
        "warnings": [],
        "errors": [],
        "services": [],
    }

    require_timescale = _env_bool("PREFLIGHT_REQUIRE_TIMESCALE", False)
    require_redis = _env_bool("PREFLIGHT_REQUIRE_REDIS", False)
    require_object_storage = _env_bool("PREFLIGHT_REQUIRE_OBJECT_STORAGE", False)

    telemetry_required = require_timescale or _env_bool("TIMESCALE_ENABLED", False) or _timescale_backend_requires_service(
        os.environ.get("TELEMETRY_READ_BACKEND")
    )
    prices_required = require_timescale or _env_bool("TIMESCALE_PRICES_ENABLED", False) or _timescale_backend_requires_service(
        os.environ.get("PRICE_READ_BACKEND")
    )

    for status in (
        _check_timescale_service(
            name="timescale_primary",
            dsn=os.environ.get("TIMESCALE_DSN"),
            required=telemetry_required,
            timeout_s=timeout_s,
        ),
        _check_timescale_service(
            name="timescale_prices",
            dsn=(os.environ.get("TIMESCALE_PRICES_DSN") or os.environ.get("TIMESCALE_DSN")),
            required=prices_required,
            timeout_s=timeout_s,
        ),
        _check_redis_service(required=require_redis, timeout_s=timeout_s),
        _check_object_storage_service(required=require_object_storage, timeout_s=timeout_s),
    ):
        if status is None:
            continue
        _append_summary(result, status)

    result["ok"] = not bool(result["errors"])
    return result


__all__ = [
    "check_external_service_readiness",
]
