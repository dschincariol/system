"""Small psycopg connection hygiene helpers shared by runtime pools."""

from __future__ import annotations

import logging
from typing import Any

try:
    from psycopg.pq import TransactionStatus
except Exception:  # pragma: no cover - psycopg is optional for sqlite-only runs.
    TransactionStatus = None  # type: ignore[assignment]


def transaction_status_name(conn: Any) -> str:
    """Return a stable transaction-status name without requiring psycopg at import time."""

    try:
        status = conn.info.transaction_status
    except Exception:
        return "unknown"
    name = str(getattr(status, "name", "") or "").strip()
    if name:
        return name
    return str(status or "unknown")


def _transaction_is_idle(conn: Any) -> bool:
    try:
        status = conn.info.transaction_status
    except Exception:
        return True
    if TransactionStatus is not None:
        try:
            return status == TransactionStatus.IDLE
        except Exception:
            return transaction_status_name(conn).upper().endswith("IDLE")
    return transaction_status_name(conn).upper().endswith("IDLE")


def rollback_if_in_transaction(
    conn: Any,
    *,
    logger: Any = None,
    context: str = "",
    suppress: bool = False,
) -> bool:
    """Rollback a pooled connection only when psycopg reports non-idle state."""

    if conn is None or _transaction_is_idle(conn):
        return False
    try:
        conn.rollback()
        return True
    except Exception:
        log = logger if logger is not None else logging.getLogger(__name__)
        try:
            log.warning(
                "Postgres connection rollback failed before pool reuse.",
                exc_info=True,
                extra={"context": str(context or "pg_connection_hygiene")},
            )
        except Exception:
            if suppress:
                return False
            raise
        if suppress:
            return False
        raise
