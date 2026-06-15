"""Audit hash-chain verifier."""

from __future__ import annotations
import logging

import json
import re
from dataclasses import dataclass, field
from typing import Any

try:
    from psycopg import Error as PsycopgError
    from psycopg.errors import UndefinedFunction
except ModuleNotFoundError:
    class PsycopgError(Exception):  # type: ignore[no-redef]
        pass

    class UndefinedFunction(Exception):  # type: ignore[no-redef]
        pass

from engine.audit.chain import coerce_row_for_hash, order_by_clause, row_identifier, table_columns
from engine.audit.hashing import compute_row_hash
from engine.runtime.dbapi_compat import is_sqlite_error
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.metrics import emit_counter
from engine.runtime.observability import record_component_health

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChainFinding:
    table_name: str
    row_id: int | None
    finding: str
    expected_hash: bytes | None
    actual_hash: bytes | None
    payload_excerpt: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VerifyResult:
    table_name: str
    rows_verified: int
    findings: tuple[ChainFinding, ...]

    @property
    def ok(self) -> bool:
        return not self.findings


def verify_table(
    table: str,
    conn,
    *,
    from_id: int | None = None,
    to_id: int | None = None,
    batch_size: int = 10000,
    emit_findings: bool = True,
) -> VerifyResult:
    """Verify a table's hash chain.

    For id-windowed checks with ``from_id > 1``, the verifier includes
    ``from_id - 1`` in the scan so the boundary row is rehashed instead of
    trusted as an opaque seed. The smallest meaningful id window is therefore
    two contiguous rows: the requested first row and its predecessor.
    """

    table_name = _ident(table)
    columns = table_columns(conn, table_name)
    names = {col.name for col in columns}
    requested_from_id = int(from_id) if from_id is not None else None
    lower_id = requested_from_id
    findings: list[ChainFinding] = []
    if "id" in names and requested_from_id is not None and requested_from_id > 1:
        boundary_id = requested_from_id - 1
        if _row_id_exists(conn, table_name, boundary_id):
            lower_id = boundary_id
        else:
            findings.append(
                ChainFinding(
                    table_name=table_name,
                    row_id=boundary_id,
                    finding="window_boundary_missing",
                    expected_hash=None,
                    actual_hash=None,
                    payload_excerpt={"from_id": requested_from_id},
                )
            )

    where: list[str] = []
    params: list[Any] = []
    if "id" in names and lower_id is not None:
        where.append("id >= ?")
        params.append(int(lower_id))
    if "id" in names and to_id is not None:
        where.append("id <= ?")
        params.append(int(to_id))
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""
    order_sql = order_by_clause(conn, table_name)
    col_names = _cursor_columns(conn, table_name, columns)

    prev_actual = _seed_previous_hash(conn, table_name, names, lower_id)
    verified = 0
    cursor = conn.execute(f"SELECT * FROM {table_name}{where_sql} {order_sql}", tuple(params))
    try:
        for idx, raw in enumerate(_iter_cursor(cursor, batch_size=batch_size), start=1):
            row = coerce_row_for_hash(_row_dict(raw, col_names), columns)
            row_id = row_identifier(row, idx)
            stored_prev = _bytes_or_none(row.get("prev_hash"))
            actual = _bytes_or_none(row.get("row_hash"))
            if stored_prev != prev_actual:
                findings.append(
                    ChainFinding(
                        table_name=table_name,
                        row_id=row_id,
                        finding="prev_hash_mismatch",
                        expected_hash=prev_actual,
                        actual_hash=stored_prev,
                        payload_excerpt=_payload_excerpt(row),
                    )
                )
                prev_actual = actual
                verified += 1
                continue

            expected = compute_row_hash(prev_actual, row)
            if actual != expected:
                findings.append(
                    ChainFinding(
                        table_name=table_name,
                        row_id=row_id,
                        finding="row_hash_mismatch",
                        expected_hash=expected,
                        actual_hash=actual,
                        payload_excerpt=_payload_excerpt(row),
                    )
                )
            prev_actual = actual
            verified += 1
    finally:
        try:
            cursor.close()
        except (AttributeError, PsycopgError) as exc:
            _record_verifier_degraded(
                "audit_chain_cursor_close_failed",
                exc,
                table_name=table_name,
            )
        except Exception as exc:
            if not is_sqlite_error(exc):
                raise
            _record_verifier_degraded(
                "audit_chain_cursor_close_failed",
                exc,
                table_name=table_name,
            )

    if emit_findings:
        for finding in findings:
            _emit_finding(conn, finding)

    return VerifyResult(table_name=table_name, rows_verified=int(verified), findings=tuple(findings))


def verify_all(
    conn,
    *,
    table: str | None = None,
    from_id: int | None = None,
    to_id: int | None = None,
    batch_size: int = 10000,
    emit_findings: bool = True,
) -> list[VerifyResult]:
    from engine.runtime.schema.table_classification import audit_tables

    tables = [_ident(table)] if table else list(audit_tables())
    existing = set(_existing_tables(conn))
    out: list[VerifyResult] = []
    for table_name in tables:
        if table_name not in existing:
            continue
        out.append(
            verify_table(
                table_name,
                conn,
                from_id=from_id,
                to_id=to_id,
                batch_size=batch_size,
                emit_findings=emit_findings,
            )
        )
    return out


def _iter_cursor(cursor, *, batch_size: int):
    """Yield rows without forcing a full-table ``fetchall`` into memory."""

    fetchmany = getattr(cursor, "fetchmany", None)
    if callable(fetchmany):
        size = max(1, int(batch_size or 1))
        while True:
            rows = fetchmany(size) or []
            if not rows:
                break
            for row in rows:
                yield row
        return

    for row in cursor:
        yield row


def _seed_previous_hash(conn, table: str, names: set[str], from_id: int | None) -> bytes | None:
    if from_id is None or "id" not in names:
        return None
    row = conn.execute(
        f"SELECT row_hash FROM {table} WHERE id < ? {order_by_clause(conn, table, descending=True)} LIMIT 1",
        (int(from_id),),
    ).fetchone()
    return _bytes_or_none(row[0]) if row else None


def _row_id_exists(conn, table: str, row_id: int) -> bool:
    row = conn.execute(f"SELECT 1 FROM {table} WHERE id = ? LIMIT 1", (int(row_id),)).fetchone()
    return row is not None


def _emit_finding(conn, finding: ChainFinding) -> None:
    try:
        conn.execute(
            """
            INSERT INTO audit_chain_findings(
              table_name, row_id, finding, expected_hash, actual_hash, payload_excerpt
            )
            VALUES (?,?,?,?,?,?)
            """,
            (
                finding.table_name,
                finding.row_id,
                finding.finding,
                finding.expected_hash,
                finding.actual_hash,
                json.dumps(finding.payload_excerpt, separators=(",", ":"), sort_keys=True),
            ),
        )
    except (TypeError, ValueError, PsycopgError) as exc:
        log_failure(
            LOG,
            event="audit_chain_finding_emit_failed",
            code="AUDIT_CHAIN_FINDING_EMIT_FAILED",
            message="failed to persist audit chain verification finding",
            error=exc,
            component="engine.audit.verifier",
            extra={
                "table_name": finding.table_name,
                "row_id": finding.row_id,
                "finding": finding.finding,
            },
            persist=False,
        )
        raise
    except Exception as exc:
        if not is_sqlite_error(exc):
            raise
        log_failure(
            LOG,
            event="audit_chain_finding_emit_failed",
            code="AUDIT_CHAIN_FINDING_EMIT_FAILED",
            message="failed to persist audit chain verification finding",
            error=exc,
            component="engine.audit.verifier",
            extra={
                "table_name": finding.table_name,
                "row_id": finding.row_id,
                "finding": finding.finding,
            },
            persist=False,
        )
        raise


def _existing_tables(conn) -> list[str]:
    try:
        rows = conn.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ANY (current_schemas(false))
            """
        ).fetchall()
        return [str(row[0]) for row in rows or []]
    except UndefinedFunction:
        # fallback: SQLite has no information_schema/current_schemas catalog.
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() or []
        return [str(row[0]) for row in rows]
    except Exception as exc:
        if not is_sqlite_error(exc, "OperationalError"):
            raise
        # fallback: SQLite has no information_schema/current_schemas catalog.
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() or []
        return [str(row[0]) for row in rows]


def _cursor_columns(conn, table: str, columns) -> list[str]:
    del conn, table
    return [col.name for col in columns]


def _row_dict(row, columns: list[str]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        try:
            return {str(key): row[key] for key in row.keys()}
        except (AttributeError, TypeError, KeyError, IndexError):
            # fallback: some DBAPI rows expose incomplete mapping access but still support positional reads.
            return _row_sequence_dict(row, columns)
    return _row_sequence_dict(row, columns)


def _row_sequence_dict(row, columns: list[str]) -> dict[str, Any]:
    return {str(columns[idx]): row[idx] for idx in range(min(len(columns), len(row)))}


def _payload_excerpt(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if key in {"prev_hash", "row_hash"}:
            continue
        if isinstance(value, (bytes, bytearray, memoryview)):
            out[key] = bytes(value).hex()[:128]
        elif isinstance(value, str):
            out[key] = value[:256]
        else:
            out[key] = value
        if len(out) >= 12:
            break
    return out


def _bytes_or_none(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return bytes(value)
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return bytes(value)
    if isinstance(value, str):
        try:
            return bytes.fromhex(value)
        except ValueError:
            return value.encode("utf-8")
    return bytes(value)


def _record_verifier_degraded(event: str, error: BaseException, **extra: Any) -> None:
    LOG.warning(event, exc_info=(type(error), error, error.__traceback__), extra={"audit_extra": dict(extra or {})})
    record_component_health(
        "audit_chain_verifier",
        ok=False,
        status="degraded",
        detail=str(event),
        extra={
            **dict(extra or {}),
            "error_type": type(error).__name__,
        },
    )
    emit_counter(
        "audit_chain_verifier_degraded",
        1,
        component="engine.audit.verifier",
        extra_tags={
            "event": str(event),
            "error_type": type(error).__name__,
        },
    )


def _ident(name: str) -> str:
    text = str(name or "")
    if not _IDENT_RE.match(text):
        raise ValueError(f"invalid_identifier:{text}")
    return text
