"""Route handlers for the data-source control plane and source-specific logs.

These routes sit beside the core API package because they are mounted as a
focused control-plane surface that coordinates the data source manager with the
runtime job manager.
"""

from __future__ import annotations

import os
import sys
import time

from engine.api.http_parsing import qs as _qs
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
    return {
        "ok": True,
        "ts_ms": int(time.time() * 1000),
        "sources": manager.list_sources(),
        "templates": manager.list_source_templates(),
        "runtime": manager.get_runtime_snapshot(),
        "auth": {
            "token_required": bool(str(os.environ.get("DASHBOARD_API_TOKEN") or "").strip()),
            "actor_required": True,
        },
        "desired_ingestion_jobs": manager.get_desired_ingestion_jobs(),
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
    return {
        "ok": True,
        "source_key": source_key,
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
        return {"ok": False, "error": "missing_source_key"}
    try:
        return manager.test_connection(
            source_key,
            actor=_body_actor(body),
            client_ip=_body_client_ip(body),
        )
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
