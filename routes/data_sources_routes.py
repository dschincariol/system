"""Route handlers for the data-source control plane and source-specific logs.

These routes sit beside the core API package because they are mounted as a
focused control-plane surface that coordinates the data source manager with the
runtime job manager.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Any, Dict

from engine.api.http_parsing import qs as _qs
from engine.api.auth_config import safe_dev_localhost_fallback_enabled
from services.data_source_manager import get_manager


ROUTE_SPECS_DATA_SOURCES = [
    ("GET", "/api/data_sources", "api_get_data_sources"),
    ("GET", "/api/data_sources/logs", "api_get_data_source_logs"),
    ("POST", "/api/data_sources/create", "api_post_data_source_create"),
    ("POST", "/api/data_sources/update", "api_post_data_source_update"),
    ("POST", "/api/data_sources/delete", "api_post_data_source_delete"),
    ("POST", "/api/data_sources/enable", "api_post_data_source_enable"),
    ("POST", "/api/data_sources/disable", "api_post_data_source_disable"),
    ("POST", "/api/data_sources/test", "api_post_data_source_test"),
    ("POST", "/api/data_sources/populate_now", "api_post_data_source_populate_now"),
    ("POST", "/api/data_sources/test_save", "api_post_data_source_test_save"),
    ("POST", "/api/data_sources/accounts/update", "api_post_data_source_account_update"),
]


def _jobs_from(ctx):
    try:
        if isinstance(ctx, dict):
            return ctx.get("JOBS")
    except Exception as e:
        sys.stderr.write(f"[data_sources_routes] jobs_from_failed: {type(e).__name__}: {e}\n")
        sys.stderr.flush()
        return None
    return None


def _body_source_key(parsed, body) -> str:
    query = _qs(parsed) or {}
    source_key = str(query.get("source_key") or "").strip()
    if not source_key and isinstance(body, dict):
        source_key = str(body.get("source_key") or "").strip()
    return source_key


def _body_actor(body) -> str:
    if isinstance(body, dict):
        return str(body.get("actor") or "").strip() or "operator"
    return "operator"


def _body_client_ip(body) -> str:
    if isinstance(body, dict):
        return str(body.get("client_ip") or "").strip()
    return ""


def _data_source_refusal_status(payload: Dict[str, Any]) -> int:
    classification = str(payload.get("classification") or "").strip().lower()
    error = str(payload.get("error") or payload.get("message") or "").strip().lower()
    status_code = None
    evidence = payload.get("evidence")
    if isinstance(evidence, dict):
        try:
            status_code = int(evidence.get("status_code") or 0) or None
        except Exception:
            status_code = None

    if classification == "wrong_credentials" or error.endswith("_credentials_rejected"):
        return 401
    if classification == "entitlement_missing" or error.endswith("_entitlement_missing"):
        return 403
    if classification == "rate_limited":
        return 429
    if classification in {"missing_credentials", "missing_settings"} or error.endswith("_credentials_missing"):
        return 422
    if classification == "unsupported":
        return 422
    if classification in {"empty_payload", "malformed_payload"}:
        return 502
    if classification == "provider_unreachable":
        if status_code and 400 <= status_code < 500:
            return 422
        return 503
    if error in {"source_not_found"}:
        return 404
    if error.startswith("missing_") or error.startswith("invalid_") or error.startswith("masked_"):
        return 400
    return 422


def _with_refusal_status(payload: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(payload or {})
    if bool(out.get("ok", True)):
        return out

    nested_test = out.get("test")
    status_payload = nested_test if isinstance(nested_test, dict) and not bool(nested_test.get("ok", True)) else out
    status = _data_source_refusal_status(status_payload)
    reason_code = str(
        status_payload.get("classification")
        or status_payload.get("reason_code")
        or status_payload.get("error")
        or out.get("error")
        or "data_source_refused"
    )
    provider_reason = str(status_payload.get("error") or status_payload.get("message") or reason_code)
    message = str(out.get("message") or status_payload.get("message") or provider_reason)

    out.setdefault("reason_code", reason_code)
    out.setdefault("provider_reason_code", provider_reason)
    out.setdefault("message", message)
    out.setdefault("http_status", int(status))
    meta = out.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    meta.setdefault("status", int(status))
    meta.setdefault("reason_code", reason_code)
    out["meta"] = meta
    return out


def api_get_data_sources(parsed, _body=None, ctx=None):
    """Return source catalog, templates, runtime status, and auth requirements.

    Parameters
    ----------
    parsed : Any
        Accepted for handler signature compatibility and ignored.
    _body : Any, optional
        Unused request body placeholder.
    ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        Control-plane payload containing ``sources``, ``templates``,
        ``runtime``, ``auth``, and ``desired_ingestion_jobs``. Timestamps are
        reported in epoch milliseconds.
    """
    manager = get_manager()
    manager.initialize()
    runtime = manager.get_runtime_snapshot()
    desired_jobs = list(runtime.get("desired_ingestion_jobs") or manager.get_desired_ingestion_jobs())
    job_states = dict(runtime.get("jobs") or {})
    sources = manager.attach_runtime_states_to_sources(
        manager.list_sources(),
        runtime_snapshot=runtime,
        desired_jobs=desired_jobs,
        job_states=job_states,
    )
    return {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "sources": sources,
        "templates": manager.list_source_templates(),
        "provider_accounts": manager.list_provider_accounts(),
        "provider_account_templates": manager.list_provider_account_templates(),
        "runtime": runtime,
        "auth": {
            "token_required": bool(str(os.environ.get("DASHBOARD_API_TOKEN") or "").strip())
            or not safe_dev_localhost_fallback_enabled(),
            "actor_required": True,
        },
        "desired_ingestion_jobs": desired_jobs,
    }


def api_get_data_source_logs(parsed, _body=None, _ctx=None):
    """Return recent control-plane log rows for a specific source.

    Parameters
    ----------
    parsed : Any
        Parsed request whose query string may include ``source_key`` and
        ``limit``.
    _body : Any, optional
        Unused request body placeholder.
    _ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        ``{"ok": True, "source_key": ..., "logs": [...]}`` when the source key
        is present. ``limit`` is clamped to the inclusive range ``[1, 1000]``.
    """
    manager = get_manager()
    source_key = _body_source_key(parsed, None)
    if not source_key:
        return {"ok": False, "error": "missing_source_key"}
    query = _qs(parsed) or {}
    try:
        limit = max(1, min(int(query.get("limit") or 100), 1000))
    except Exception:
        limit = 100
    source = manager.get_source(source_key)
    return {
        "ok": True,
        "source_key": source_key,
        "source": source,
        "runnable_state": str((source or {}).get("runnable_state") or ""),
        "job_name": str((source or {}).get("job_name") or ""),
        "logs": manager.list_logs(source_key, limit=limit),
    }


def api_post_data_source_create(parsed, body=None, ctx=None):
    """Create a new configurable data source and reconcile runtime lifecycle.

    Parameters
    ----------
    parsed : Any
        Accepted for handler signature compatibility and ignored.
    body : dict, optional
        Source payload forwarded to the data source manager.
    ctx : dict, optional
        Optional request context used to locate the jobs manager for lifecycle
        reconciliation.

    Returns
    -------
    dict
        Success responses include the persisted ``source`` payload and a
        ``lifecycle`` summary. Validation failures return ``ok=False`` with a
        machine-readable ``error`` string.
    """
    if not isinstance(body, dict):
        return {"ok": False, "error": "invalid_body"}
    manager = get_manager()
    try:
        source = manager.create_source(body)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    lifecycle = manager.manage_lifecycle(reason=f"api_create:{source.get('source_key')}", jobs_manager=_jobs_from(ctx))
    return {"ok": True, "source": source, "lifecycle": lifecycle}


def api_post_data_source_update(parsed, body=None, ctx=None):
    """Update an existing data source and reconcile ingestion lifecycle.

    Parameters
    ----------
    parsed : Any
        Accepted for handler signature compatibility and ignored.
    body : dict, optional
        Source payload forwarded to the manager update path.
    ctx : dict, optional
        Optional request context used to locate the jobs manager for lifecycle
        reconciliation.

    Returns
    -------
    dict
        Success responses include the updated ``source`` payload and a
        ``lifecycle`` summary. Validation failures return ``ok=False`` with a
        machine-readable ``error`` string.
    """
    if not isinstance(body, dict):
        return {"ok": False, "error": "invalid_body"}
    manager = get_manager()
    try:
        source = manager.update_source(body)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    lifecycle = manager.manage_lifecycle(reason=f"api_update:{source.get('source_key')}", jobs_manager=_jobs_from(ctx))
    return {"ok": True, "source": source, "lifecycle": lifecycle}


def api_post_data_source_delete(parsed, body=None, ctx=None):
    """Delete a configurable source and mark runtime configuration dirty.

    Parameters
    ----------
    parsed : Any
        Parsed request whose query string may provide ``source_key``.
    body : dict, optional
        Optional body used to resolve ``source_key``, ``actor``, and
        ``client_ip``.
    ctx : dict, optional
        Optional request context used to locate the jobs manager for lifecycle
        reconciliation.

    Returns
    -------
    dict
        Success responses include the delete acknowledgement and a ``lifecycle``
        summary. Missing or unknown source keys return ``ok=False`` with an
        ``error`` code.
    """
    manager = get_manager()
    source_key = _body_source_key(parsed, body)
    if not source_key:
        return {"ok": False, "error": "missing_source_key"}
    try:
        deleted = manager.delete_source(
            source_key,
            actor=_body_actor(body),
            client_ip=_body_client_ip(body),
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    lifecycle = manager.manage_lifecycle(reason=f"api_delete:{source_key}", jobs_manager=_jobs_from(ctx))
    return {"ok": True, "deleted": deleted, "lifecycle": lifecycle}


def api_post_data_source_enable(parsed, body=None, ctx=None):
    """Enable a source and reconcile the ingestion job set.

    Parameters
    ----------
    parsed : Any
        Parsed request whose query string may provide ``source_key``.
    body : dict, optional
        Optional body used to resolve ``source_key``, ``actor``, and
        ``client_ip``.
    ctx : dict, optional
        Optional request context used to locate the jobs manager for lifecycle
        reconciliation.

    Returns
    -------
    dict
        Success responses include the updated ``source`` payload and a
        ``lifecycle`` summary. Validation failures return ``ok=False``.
    """
    manager = get_manager()
    source_key = _body_source_key(parsed, body)
    if not source_key:
        return {"ok": False, "error": "missing_source_key"}
    try:
        source = manager.set_enabled(
            source_key,
            True,
            actor=_body_actor(body),
            client_ip=_body_client_ip(body),
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    lifecycle = manager.manage_lifecycle(reason=f"api_enable:{source_key}", jobs_manager=_jobs_from(ctx))
    return {"ok": True, "source": source, "lifecycle": lifecycle}


def api_post_data_source_disable(parsed, body=None, ctx=None):
    """Disable a source and reconcile the ingestion job set.

    Parameters
    ----------
    parsed : Any
        Parsed request whose query string may provide ``source_key``.
    body : dict, optional
        Optional body used to resolve ``source_key``, ``actor``, and
        ``client_ip``.
    ctx : dict, optional
        Optional request context used to locate the jobs manager for lifecycle
        reconciliation.

    Returns
    -------
    dict
        Success responses include the updated ``source`` payload and a
        ``lifecycle`` summary. Validation failures return ``ok=False``.
    """
    manager = get_manager()
    source_key = _body_source_key(parsed, body)
    if not source_key:
        return {"ok": False, "error": "missing_source_key"}
    try:
        source = manager.set_enabled(
            source_key,
            False,
            actor=_body_actor(body),
            client_ip=_body_client_ip(body),
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    lifecycle = manager.manage_lifecycle(reason=f"api_disable:{source_key}", jobs_manager=_jobs_from(ctx))
    return {"ok": True, "source": source, "lifecycle": lifecycle}


def api_post_data_source_test(parsed, body=None, _ctx=None):
    """Run the manager's connectivity test for a specific source.

    Parameters
    ----------
    parsed : Any
        Parsed request whose query string may provide ``source_key``.
    body : dict, optional
        Optional body used to resolve ``source_key``, ``actor``, and
        ``client_ip``.
    _ctx : Any, optional
        Unused request context placeholder.

    Returns
    -------
    dict
        Provider-specific test result from the data source manager. Unknown
        sources or malformed input return ``ok=False`` with an ``error`` code.
    """
    manager = get_manager()
    source_key = _body_source_key(parsed, body)
    if not source_key:
        return _with_refusal_status({"ok": False, "error": "missing_source_key"})
    try:
        return _with_refusal_status(
            manager.test_connection(
                source_key,
                actor=_body_actor(body),
                client_ip=_body_client_ip(body),
            )
        )
    except ValueError as exc:
        return _with_refusal_status({"ok": False, "error": str(exc)})


def api_post_data_source_populate_now(parsed, body=None, _ctx=None):
    """Run a bounded one-shot provider populate and storage-contract check."""
    manager = get_manager()
    source_key = _body_source_key(parsed, body)
    if not source_key:
        return _with_refusal_status({"ok": False, "error": "missing_source_key"})
    try:
        return manager.populate_now(
            source_key,
            actor=_body_actor(body),
            client_ip=_body_client_ip(body),
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}


def api_post_data_source_test_save(parsed, body=None, ctx=None):
    """Save source input and immediately run the provider liveness test."""
    if not isinstance(body, dict):
        return _with_refusal_status({"ok": False, "error": "invalid_body"})
    manager = get_manager()
    try:
        result = manager.test_and_save_source(
            body,
            actor=_body_actor(body),
            client_ip=_body_client_ip(body),
        )
    except ValueError as exc:
        return _with_refusal_status({"ok": False, "saved": False, "error": str(exc)})
    except Exception as exc:
        return {
            "ok": False,
            "saved": False,
            "error": f"test_save_failed:{type(exc).__name__}",
            "reason_code": "data_source_test_save_failed",
            "message": "Data-source test-and-save failed unexpectedly.",
            "detail": type(exc).__name__,
            "http_status": 500,
            "meta": {"status": 500, "reason_code": "data_source_test_save_failed"},
        }
    lifecycle = manager.manage_lifecycle(
        reason=f"api_test_save:{result.get('source_key')}",
        jobs_manager=_jobs_from(ctx),
    )
    return _with_refusal_status({**result, "lifecycle": lifecycle})


def api_post_data_source_account_update(parsed, body=None, ctx=None):
    """Update a shared provider-account credential set and reconcile runtime."""
    if not isinstance(body, dict):
        return {"ok": False, "error": "invalid_body"}
    manager = get_manager()
    try:
        account = manager.update_provider_account(body)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    lifecycle = manager.manage_lifecycle(
        reason=f"api_provider_account_update:{account.get('account_key')}",
        jobs_manager=_jobs_from(ctx),
    )
    return {"ok": True, "provider_account": account, "lifecycle": lifecycle}
