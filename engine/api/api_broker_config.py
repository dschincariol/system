"""Broker configuration control-plane API.

These handlers keep broker selection and credentials in runtime storage rather
than mutating process environment files. Secrets are encrypted with the same
credential helper used by the data-source control plane and masked on reads.
"""

from __future__ import annotations

import json
import os
import time
from typing import Any, Dict

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


def _test_connection(config: Dict[str, Any], credentials: Dict[str, Any] | None = None) -> Dict[str, Any]:
    started = time.monotonic()
    broker = str(config.get("active_broker") or "sim").strip().lower()
    credentials = dict(credentials or {})
    reasons: list[str] = []
    if not broker:
        reasons.append("missing_broker")
    if broker == "sim":
        ok = True
    elif broker == "ibkr":
        ok = bool(str(config.get("host") or "").strip() and str(config.get("port") or "").strip() and str(config.get("client_id") or "").strip())
        if not ok:
            reasons.append("missing_ibkr_host_port_client_id")
    elif broker == "alpaca":
        ok = bool(str(config.get("base_url") or "").strip() and credentials)
        if not ok:
            reasons.append("missing_alpaca_base_url_or_credentials")
    else:
        ok = bool(credentials or str(config.get("base_url") or config.get("host") or "").strip())
        if not ok:
            reasons.append("missing_broker_endpoint_or_credentials")
    latency_ms = round((time.monotonic() - started) * 1000.0, 3)
    return {
        "ok": bool(ok),
        "broker": broker,
        "state": "passed" if ok else "failed",
        "latency_ms": latency_ms,
        "reasons": reasons,
        "tested_ts_ms": _now_ms(),
    }


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
        test_matches = bool(
            isinstance(last_test, dict)
            and bool(last_test.get("ok"))
            and str(last_test.get("broker") or "") == str(next_config.get("active_broker") or "")
        )
        if not test_matches or test_stale:
            message = "broker_test_stale" if test_matches and test_stale else "broker_test_required"
            _audit("activation_blocked", actor=actor, success=False, message=message, detail=next_config)
            return {
                "ok": False,
                "error": message,
                "message": "Run a fresh passing broker connection test before broker activation.",
                "test_max_age_ms": int(max_age_ms),
                "meta": {"status": 422},
            }

    credentials = body.get("credentials")
    now_ms = _now_ms()

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
                "config_update",
                actor,
                str(next_config.get("active_broker") or ""),
                1,
                "broker_config_updated",
                json.dumps({**next_config, "credentials_supplied": isinstance(credentials, dict)}, separators=(",", ":"), sort_keys=True),
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

    def _write(con):
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

    run_write_txn(_write)
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
