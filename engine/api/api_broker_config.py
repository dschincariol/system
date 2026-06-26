"""Broker configuration control-plane API.

These handlers keep broker selection and credentials in runtime storage rather
than mutating process environment files. Secrets are encrypted with the same
credential helper used by the data-source control plane and masked on reads.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
import urllib.error
import urllib.request
from typing import Any, Dict

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import connect_ro, run_write_txn
from services.credential_encryption import (
    DEFAULT_MASTER_KEY_NAME,
    decrypt_credentials,
    encrypt_credentials,
    mask_credentials,
)


ROUTE_SPECS_BROKER_CONFIG = [
    ("GET", "/api/broker/config", "api_get_broker_config"),
    ("POST", "/api/broker/config", "api_post_broker_config"),
    ("POST", "/api/broker/test_connection", "api_post_broker_test_connection"),
    ("GET", "/api/broker/audit", "api_get_broker_audit"),
]

_CONFIG_KEY = "broker.config"
_CREDENTIALS_KEY = "broker.credentials_enc"
_CREDENTIALS_VERSION_KEY = "broker.credentials_key_version"
_LAST_TEST_KEY = "broker.last_test"
_AUDIT_LIMIT = 200
_DEFAULT_TEST_MAX_AGE_S = 24 * 60 * 60
LOG = logging.getLogger(__name__)
_FINGERPRINT_CONFIG_KEYS = (
    "active_broker",
    "paper_live_mode",
    "failover_order",
    "base_url",
    "host",
    "port",
    "client_id",
    "timeout_s",
    "retry_policy",
)
_BROKER_CONNECTION_PROBES: Dict[str, Any] = {}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _json_loads(raw: Any, default: Any) -> Any:
    try:
        parsed = json.loads(str(raw or ""))
        return parsed if parsed is not None else default
    except Exception:
        return default


def _broker_test_max_age_ms() -> int:
    raw = str(os.environ.get("BROKER_CONNECTION_TEST_MAX_AGE_S") or "").strip()
    if not raw:
        return int(_DEFAULT_TEST_MAX_AGE_S * 1000)
    try:
        value = float(raw)
    except Exception:
        return int(_DEFAULT_TEST_MAX_AGE_S * 1000)
    return int(max(0.0, value) * 1000.0)


def _int_or_zero(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _float_or_default(value: Any, default: float, *, minimum: float = 0.1, maximum: float = 120.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        parsed = float(default)
    return max(float(minimum), min(float(maximum), float(parsed)))


def _runtime_execution_mode() -> str:
    raw = str(os.environ.get("EXECUTION_MODE") or os.environ.get("ENGINE_MODE") or "safe").strip().lower()
    aliases = {
        "": "safe",
        "dev": "safe",
        "development": "safe",
        "test": "safe",
        "testing": "safe",
        "sim": "safe",
        "simulation": "safe",
        "sim-paper": "paper",
        "sim_paper": "paper",
    }
    return aliases.get(raw, raw or "safe")


def _broker_test_live_probe_allowed() -> bool:
    return _runtime_execution_mode() != "safe"


def _safe_mode_broker_probe_result(broker: str) -> Dict[str, Any]:
    return {
        "ok": True,
        "state": "safe_mode_live_probe_skipped",
        "reasons": [],
        "test_kind": "safe_mode_no_live_broker_probe",
        "live_probe_skipped": True,
        "activation_eligible": False,
        "detail": "runtime_safe_mode_does_not_open_live_broker_socket",
        "effective_execution_mode": _runtime_execution_mode(),
        "broker": str(broker or ""),
    }


def _credential_value(credentials: Dict[str, Any], *names: str) -> str:
    lowered = {str(key or "").strip().lower(): value for key, value in dict(credentials or {}).items()}
    for name in names:
        value = lowered.get(str(name or "").strip().lower())
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _credential_status(credentials: Dict[str, Any]) -> Dict[str, Any]:
    keys = sorted(str(key) for key in dict(credentials or {}).keys() if str(key).strip())
    return {
        "configured": bool(keys),
        "keys": keys,
        "masked": mask_credentials(dict(credentials or {})),
    }


def _broker_config_fingerprint(config: Dict[str, Any]) -> str:
    payload = {key: config.get(key) for key in _FINGERPRINT_CONFIG_KEYS}
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _default_config() -> Dict[str, Any]:
    broker = str(os.environ.get("BROKER_NAME") or os.environ.get("BROKER") or "sim").strip().lower() or "sim"
    mode = str(os.environ.get("EXECUTION_MODE") or os.environ.get("ENGINE_MODE") or "safe").strip().lower() or "safe"
    return {
        "active_broker": broker,
        "paper_live_mode": "live" if mode == "live" else ("paper" if mode in {"paper", "sim-paper", "sim_paper"} else "safe"),
        "active": broker == "sim",
        "disabled": broker != "sim",
        "failover_order": [broker, "sim"] if broker != "sim" else ["sim"],
        "base_url": str(os.environ.get("BROKER_BASE_URL") or os.environ.get("ALPACA_BASE_URL") or "").strip(),
        "host": str(os.environ.get("IBKR_HOST") or "").strip(),
        "port": str(os.environ.get("IBKR_PORT") or "").strip(),
        "client_id": str(os.environ.get("IBKR_CLIENT_ID") or "").strip(),
        "timeout_s": float(os.environ.get("BROKER_TIMEOUT_S") or 5.0),
        "retry_policy": {
            "max_attempts": int(float(os.environ.get("BROKER_RETRY_ATTEMPTS") or 3)),
            "backoff_s": float(os.environ.get("BROKER_RETRY_BACKOFF_S") or 0.5),
        },
    }


def _table_exists(con, name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception:
        try:
            row = con.execute("SELECT to_regclass(?)", (str(name),)).fetchone()
            return bool(row and row[0])
        except Exception:
            return False


def _read_meta_value(key: str) -> str:
    con = connect_ro()
    try:
        if not _table_exists(con, "broker_meta"):
            return ""
        row = con.execute("SELECT value FROM broker_meta WHERE key=? LIMIT 1", (str(key),)).fetchone()
        return str(row[0] or "") if row else ""
    finally:
        con.close()


def _ensure_schema(con) -> None:
    is_sqlite = True
    try:
        con.execute("SELECT sqlite_version()").fetchone()
    except Exception:
        is_sqlite = False
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS broker_meta (
          key TEXT PRIMARY KEY,
          value TEXT,
          updated_ts_ms BIGINT
        )
        """
    )
    id_decl = "INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "BIGSERIAL PRIMARY KEY"
    detail_default = "'{}'" if is_sqlite else "'{}'::jsonb"
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS broker_config_audit (
          id {id_decl},
          ts_ms BIGINT NOT NULL,
          action TEXT NOT NULL,
          actor TEXT NOT NULL,
          active_broker TEXT,
          success BIGINT NOT NULL,
          message TEXT,
          detail_json {'TEXT' if is_sqlite else 'JSONB'} NOT NULL DEFAULT {detail_default}
        )
        """
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_broker_config_audit_ts ON broker_config_audit(ts_ms DESC)"
    )


def _write_meta(con, key: str, value: Any, now_ms: int) -> None:
    raw = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), sort_keys=True)
    con.execute(
        """
        INSERT INTO broker_meta(key, value, updated_ts_ms)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value=excluded.value,
          updated_ts_ms=excluded.updated_ts_ms
        """,
        (str(key), str(raw), int(now_ms)),
    )


def _audit(action: str, *, actor: str = "operator", success: bool = True, message: str = "", detail: Dict[str, Any] | None = None) -> None:
    try:
        now_ms = _now_ms()

        def _write(con):
            _ensure_schema(con)
            broker = str((detail or {}).get("active_broker") or (detail or {}).get("broker") or "")
            con.execute(
                """
                INSERT INTO broker_config_audit
                (ts_ms, action, actor, active_broker, success, message, detail_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ms,
                    str(action or ""),
                    str(actor or "operator"),
                    broker,
                    1 if success else 0,
                    str(message or ""),
                    json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
                ),
            )

        run_write_txn(_write)
    except Exception:
        return


def _rollback_after_broker_audit_write_failure(con, exc: BaseException) -> None:
    try:
        rollback = getattr(con, "rollback", None)
        if callable(rollback):
            rollback()
    except Exception:
        LOG.debug("broker_config_audit_rollback_failed", exc_info=True)
    log_failure(
        LOG,
        event="broker_config_audit_write_failed",
        code="BROKER_CONFIG_AUDIT_WRITE_FAILED",
        message="broker config audit write failed after broker connection test",
        error=exc,
        level=logging.WARNING,
        component="engine.api.api_broker_config",
        extra={"phase": "broker_test_connection_audit_write"},
        include_health=False,
    )


def _stored_config() -> Dict[str, Any]:
    config = _default_config()
    stored = _json_loads(_read_meta_value(_CONFIG_KEY), {})
    if isinstance(stored, dict):
        config.update(stored)
    return config


def _stored_credentials() -> tuple[Dict[str, Any], str]:
    blob = _read_meta_value(_CREDENTIALS_KEY)
    key_version = _read_meta_value(_CREDENTIALS_VERSION_KEY) or DEFAULT_MASTER_KEY_NAME
    if not blob:
        return {}, key_version
    return decrypt_credentials(blob, key_name=key_version), key_version


def _public_payload(config: Dict[str, Any] | None = None) -> Dict[str, Any]:
    cfg = dict(config or _stored_config())
    try:
        credentials, key_version = _stored_credentials()
        credential_error = ""
    except Exception as exc:
        credentials, key_version, credential_error = {}, DEFAULT_MASTER_KEY_NAME, f"{type(exc).__name__}:{exc}"
    last_test = _json_loads(_read_meta_value(_LAST_TEST_KEY), {})
    cfg["credentials_configured"] = bool(credentials)
    cfg["masked_credentials"] = mask_credentials(credentials)
    cfg["credential_age"] = {
        "key_version": key_version,
        "credential_error": credential_error,
    }
    cfg["last_test_result"] = last_test if isinstance(last_test, dict) else {}
    cfg["config_fingerprint"] = _broker_config_fingerprint(cfg)
    cfg["secrets_masked"] = True
    return {"ok": True, "ts_ms": _now_ms(), "config": cfg}


def _normalize_config(body: Dict[str, Any], current: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "active_broker",
        "paper_live_mode",
        "active",
        "disabled",
        "failover_order",
        "base_url",
        "host",
        "port",
        "client_id",
        "timeout_s",
        "retry_policy",
    }
    out = dict(current or {})
    for key in allowed:
        if key in body:
            out[key] = body.get(key)
    out["active_broker"] = str(out.get("active_broker") or "sim").strip().lower() or "sim"
    out["paper_live_mode"] = str(out.get("paper_live_mode") or "safe").strip().lower() or "safe"
    out["active"] = bool(out.get("active"))
    out["disabled"] = bool(out.get("disabled"))
    if not isinstance(out.get("failover_order"), list):
        out["failover_order"] = [out["active_broker"], "sim"] if out["active_broker"] != "sim" else ["sim"]
    out["failover_order"] = [str(item).strip().lower() for item in out["failover_order"] if str(item).strip()]
    try:
        out["timeout_s"] = max(0.1, min(120.0, float(out.get("timeout_s") or 5.0)))
    except Exception:
        out["timeout_s"] = 5.0
    if not isinstance(out.get("retry_policy"), dict):
        out["retry_policy"] = {"max_attempts": 3, "backoff_s": 0.5}
    return out


def _alpaca_account_probe(config: Dict[str, Any], credentials: Dict[str, Any]) -> Dict[str, Any]:
    base_url = str(config.get("base_url") or os.environ.get("ALPACA_BASE_URL") or "").strip().rstrip("/")
    key_id = _credential_value(credentials, "api_key", "key_id", "alpaca_key_id", "ALPACA_KEY_ID")
    secret = _credential_value(credentials, "secret", "secret_key", "alpaca_secret_key", "ALPACA_SECRET_KEY")
    timeout_s = _float_or_default(config.get("timeout_s"), 5.0)
    if not base_url:
        return {
            "ok": False,
            "state": "configuration_invalid",
            "reasons": ["missing_alpaca_base_url"],
            "test_kind": "alpaca_account_read",
        }
    if not key_id or not secret:
        return {
            "ok": False,
            "state": "configuration_invalid",
            "reasons": ["missing_alpaca_credentials"],
            "test_kind": "alpaca_account_read",
        }
    req = urllib.request.Request(
        f"{base_url}/v2/account",
        headers={
            "APCA-API-KEY-ID": key_id,
            "APCA-API-SECRET-KEY": secret,
            "Content-Type": "application/json",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        status = int(getattr(exc, "code", 0) or 0)
        return {
            "ok": False,
            "state": "auth_failed" if status in (401, 403) else "http_failed",
            "reasons": ["alpaca_auth_failed" if status in (401, 403) else f"alpaca_http_{status}"],
            "http_status": status,
            "test_kind": "alpaca_account_read",
        }
    account = _json_loads(raw, {}) if raw else {}
    return {
        "ok": True,
        "state": "connected",
        "reasons": [],
        "account_status": str((account or {}).get("status") or ""),
        "trading_blocked": bool((account or {}).get("trading_blocked", False)),
        "account_blocked": bool((account or {}).get("account_blocked", False)),
        "test_kind": "alpaca_account_read",
    }


def _ibkr_gateway_probe(config: Dict[str, Any], _credentials: Dict[str, Any]) -> Dict[str, Any]:
    host = str(config.get("host") or "").strip()
    port = _int_or_zero(config.get("port"))
    client_id = _int_or_zero(config.get("client_id"))
    timeout_s = _float_or_default(config.get("timeout_s"), 5.0)
    retry_policy = config.get("retry_policy") if isinstance(config.get("retry_policy"), dict) else {}
    retries = _int_or_zero((retry_policy or {}).get("max_attempts") or 1)
    if not host or port <= 0 or client_id < 0:
        return {
            "ok": False,
            "state": "configuration_invalid",
            "reasons": ["missing_ibkr_host_port_client_id"],
            "test_kind": "ibkr_session_ping",
        }
    from engine.execution.broker_ibkr_gateway import ping_broker_connection

    result = ping_broker_connection(
        timeout_s=timeout_s,
        retries=max(1, retries),
        host=host,
        port=port,
        client_id=client_id,
    )
    out = dict(result or {})
    out["test_kind"] = "ibkr_session_ping"
    if not out.get("reasons"):
        reason = str(out.get("reason") or out.get("status") or out.get("error") or "").strip()
        out["reasons"] = [reason] if reason and not bool(out.get("ok")) else []
    return out


def _probe_for_broker(broker: str):
    override = _BROKER_CONNECTION_PROBES.get(str(broker or "").strip().lower())
    if callable(override):
        return override
    if broker == "alpaca":
        return _alpaca_account_probe
    if broker == "ibkr":
        return _ibkr_gateway_probe
    return None


def _test_connection(config: Dict[str, Any], credentials: Dict[str, Any] | None = None) -> Dict[str, Any]:
    started = time.monotonic()
    broker = str(config.get("active_broker") or "sim").strip().lower()
    credentials = dict(credentials or {})
    reasons: list[str] = []
    if not broker:
        reasons.append("missing_broker")
    probe_detail: Dict[str, Any] = {}
    if broker == "sim":
        ok = True
        probe_detail = {"state": "connected", "reasons": [], "test_kind": "sim_local"}
    elif not _broker_test_live_probe_allowed():
        ok = True
        probe_detail = _safe_mode_broker_probe_result(broker)
    else:
        probe = _probe_for_broker(broker)
        if probe is None:
            ok = bool(credentials or str(config.get("base_url") or config.get("host") or "").strip())
            probe_detail = {
                "state": "configuration_only_passed" if ok else "configuration_invalid",
                "reasons": [] if ok else ["missing_broker_endpoint_or_credentials"],
                "test_kind": "generic_configuration",
            }
        else:
            try:
                probe_detail = dict(probe(config, credentials) or {})
                ok = bool(probe_detail.get("ok"))
            except Exception as exc:
                ok = False
                probe_detail = {
                    "state": "probe_exception",
                    "reasons": [f"{type(exc).__name__}: {exc}"],
                    "test_kind": f"{broker}_probe_exception",
                }
    reasons.extend(str(item) for item in list(probe_detail.get("reasons") or []) if str(item).strip())
    latency_ms = round((time.monotonic() - started) * 1000.0, 3)
    state = str(probe_detail.get("state") or ("passed" if ok else "failed"))
    return {
        "ok": bool(ok),
        "broker": broker,
        "state": state,
        "latency_ms": latency_ms,
        "reasons": reasons,
        "tested_ts_ms": _now_ms(),
        "test_kind": str(probe_detail.get("test_kind") or ("read_only_broker_probe" if broker != "sim" else "sim_local")),
        "non_mutating": True,
        "live_probe_skipped": bool(probe_detail.get("live_probe_skipped", False)),
        "activation_eligible": bool(probe_detail.get("activation_eligible", True)),
        "effective_execution_mode": str(probe_detail.get("effective_execution_mode") or _runtime_execution_mode()),
        "mode": str(config.get("paper_live_mode") or ""),
        "endpoint": {
            "base_url": str(config.get("base_url") or ""),
            "host": str(config.get("host") or ""),
            "port": str(config.get("port") or ""),
            "client_id": str(config.get("client_id") or ""),
        },
        "credential_status": _credential_status(credentials),
        "config_fingerprint": _broker_config_fingerprint(config),
    }


def _persist_test_connection_result(
    *,
    result: Dict[str, Any],
    config: Dict[str, Any],
    actor: str,
    now_ms: int,
) -> None:
    def _write(con):
        try:
            _ensure_schema(con)
            _write_meta(con, _LAST_TEST_KEY, result, now_ms)
            con.execute(
                """
                INSERT INTO broker_config_audit
                (ts_ms, action, actor, active_broker, success, message, detail_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_ms,
                    "test_connection",
                    actor,
                    str(config.get("active_broker") or ""),
                    1 if result.get("ok") else 0,
                    str(result.get("state") or ""),
                    json.dumps(result, separators=(",", ":"), sort_keys=True),
                ),
            )
        except Exception as exc:
            _rollback_after_broker_audit_write_failure(con, exc)
            raise

    run_write_txn(
        _write,
        table="broker_config_audit",
        operation="broker_test_connection",
    )


def api_get_broker_config(_parsed=None, _body=None, _ctx=None):
    return _public_payload()


def api_post_broker_config(_parsed=None, body=None, _ctx=None):
    if not isinstance(body, dict):
        return {"ok": False, "error": "invalid_body", "meta": {"status": 400}}
    actor = str(body.get("actor") or "operator").strip() or "operator"
    current = _stored_config()
    next_config = _normalize_config(body, current)
    wants_activation = bool(next_config.get("active")) and not bool(next_config.get("disabled"))
    last_test = _json_loads(_read_meta_value(_LAST_TEST_KEY), {})
    if wants_activation and str(next_config.get("active_broker") or "sim") != "sim":
        tested_ts_ms = _int_or_zero((last_test or {}).get("tested_ts_ms")) if isinstance(last_test, dict) else 0
        max_age_ms = _broker_test_max_age_ms()
        test_stale = tested_ts_ms <= 0 or (_now_ms() - int(tested_ts_ms)) > int(max_age_ms)
        expected_fingerprint = _broker_config_fingerprint(next_config)
        test_matches = bool(
            isinstance(last_test, dict)
            and bool(last_test.get("ok"))
            and bool(last_test.get("activation_eligible", True))
            and not bool(last_test.get("live_probe_skipped", False))
            and str(last_test.get("broker") or "") == str(next_config.get("active_broker") or "")
            and bool(last_test.get("non_mutating", False))
            and str(last_test.get("config_fingerprint") or "") == str(expected_fingerprint)
        )
        if not test_matches or test_stale:
            message = "broker_test_stale" if test_matches and test_stale else "broker_test_required"
            _audit(
                "activation_blocked",
                actor=actor,
                success=False,
                message=message,
                detail={**next_config, "expected_config_fingerprint": expected_fingerprint},
            )
            return {
                "ok": False,
                "error": message,
                "message": "Run a fresh passing broker connection test before broker activation.",
                "test_max_age_ms": int(max_age_ms),
                "expected_config_fingerprint": expected_fingerprint,
                "meta": {"status": 422},
            }

    credentials = body.get("credentials")
    now_ms = _now_ms()
    current_disabled = bool(current.get("disabled", False))
    current_failover = [str(item).strip().lower() for item in list(current.get("failover_order") or [])]
    next_failover = [str(item).strip().lower() for item in list(next_config.get("failover_order") or [])]
    current_active = bool(current.get("active", False)) and not current_disabled
    same_current_broker = str(current.get("active_broker") or "").strip().lower() == str(next_config.get("active_broker") or "").strip().lower()
    if wants_activation:
        audit_action = "activation"
        audit_message = "broker_config_activated"
    elif bool(next_config.get("disabled")) and current_active and same_current_broker:
        audit_action = "deactivation"
        audit_message = "broker_config_deactivated"
    elif next_failover != current_failover:
        audit_action = "failover_update"
        audit_message = "broker_failover_updated"
    else:
        audit_action = "config_update"
        audit_message = "broker_config_updated"

    def _write(con):
        _ensure_schema(con)
        _write_meta(con, _CONFIG_KEY, next_config, now_ms)
        if isinstance(credentials, dict):
            _write_meta(con, _CREDENTIALS_KEY, encrypt_credentials(credentials), now_ms)
            _write_meta(con, _CREDENTIALS_VERSION_KEY, DEFAULT_MASTER_KEY_NAME, now_ms)
        con.execute(
            """
            INSERT INTO broker_config_audit
            (ts_ms, action, actor, active_broker, success, message, detail_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                now_ms,
                audit_action,
                actor,
                str(next_config.get("active_broker") or ""),
                1,
                audit_message,
                json.dumps(
                    {
                        **next_config,
                        "credentials_supplied": isinstance(credentials, dict),
                        "config_fingerprint": _broker_config_fingerprint(next_config),
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                ),
            ),
        )

    run_write_txn(_write)
    return _public_payload(next_config)


def api_post_broker_test_connection(_parsed=None, body=None, _ctx=None):
    payload = body if isinstance(body, dict) else {}
    actor = str(payload.get("actor") or "operator").strip() or "operator"
    config = _normalize_config(payload.get("config") if isinstance(payload.get("config"), dict) else payload, _stored_config())
    try:
        stored_credentials, _key_version = _stored_credentials()
    except Exception:
        stored_credentials = {}
    credentials = payload.get("credentials") if isinstance(payload.get("credentials"), dict) else stored_credentials
    result = _test_connection(config, credentials)
    now_ms = _now_ms()
    try:
        _persist_test_connection_result(result=result, config=config, actor=actor, now_ms=now_ms)
        result["audit_persisted"] = True
    except Exception as exc:
        clean = dict(result)
        clean.update(
            {
                "ok": False,
                "state": "audit_write_failed",
                "audit_persisted": False,
                "audit_error": type(exc).__name__,
                "reasons": list(result.get("reasons") or []) + ["broker_test_audit_write_failed"],
                "meta": {"status": 500},
            }
        )
        return clean
    return result


def api_get_broker_audit(_parsed=None, _body=None, _ctx=None):
    con = connect_ro()
    try:
        if not _table_exists(con, "broker_config_audit"):
            return {"ok": True, "rows": []}
        rows = con.execute(
            """
            SELECT ts_ms, action, actor, active_broker, success, message, detail_json
              FROM broker_config_audit
             ORDER BY ts_ms DESC, id DESC
             LIMIT ?
            """,
            (_AUDIT_LIMIT,),
        ).fetchall() or []
        out = []
        for row in rows:
            detail = _json_loads(row[6], {})
            if isinstance(detail, dict):
                detail.pop("credentials", None)
            out.append({
                "ts_ms": int(row[0] or 0),
                "action": str(row[1] or ""),
                "actor": str(row[2] or ""),
                "active_broker": str(row[3] or ""),
                "success": bool(row[4]),
                "message": str(row[5] or ""),
                "detail": detail if isinstance(detail, dict) else {},
            })
        return {"ok": True, "rows": out}
    finally:
        con.close()
