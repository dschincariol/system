"""Helpers for offline-safe data-source credential rotation."""

from __future__ import annotations
import logging
import sqlite3

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from cryptography.exceptions import InvalidTag

from engine.runtime.metrics import emit_counter
from services.credential_encryption import (
    DecryptionError,
    decrypt_credentials,
    encrypt_credentials,
)

LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class RotationResult:
    scanned: int
    rotated: int
    skipped: int
    verified: int
    old_key_deleted: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "scanned": int(self.scanned),
            "rotated": int(self.rotated),
            "skipped": int(self.skipped),
            "verified": int(self.verified),
            "old_key_deleted": int(self.old_key_deleted),
        }


class SecretRotationError(RuntimeError):
    """Base class for credential rotation failures."""


class SecretRotationPartialFailure(SecretRotationError):
    """Raised when one or more rows fail during credential rotation."""

    def __init__(self, failures: list[dict[str, Any]]) -> None:
        self.failures = [dict(item) for item in failures]
        row_ids = ",".join(str(item.get("row_id")) for item in self.failures)
        super().__init__(f"credential_rotation_partial_failure:rows={row_ids}")


def _row_value(row: Any, key: str, index: int, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    if hasattr(row, "keys") and key in set(row.keys()):
        return row[key]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        if 0 <= int(index) < len(row):
            return row[index]
    return default


def _original_error_class(exc: BaseException) -> str:
    cause = exc.__cause__
    return type(cause).__name__ if cause is not None else type(exc).__name__


def _record_rotation_row_failure(
    *,
    row_id: int,
    exc: BaseException,
    phase: str,
) -> dict[str, Any]:
    failure = {
        "row_id": int(row_id),
        "phase": str(phase),
        "error_class": _original_error_class(exc),
        "reported_error_class": type(exc).__name__,
    }
    emit_counter(
        "credential_rotation_row_failures",
        1,
        component="services.secrets.rotation",
        extra_tags=failure,
    )
    LOG.error(
        "credential_rotation_row_failed row_id=%s phase=%s error_class=%s reported_error_class=%s",
        failure["row_id"],
        failure["phase"],
        failure["error_class"],
        failure["reported_error_class"],
    )
    return failure


def _is_optional_provider_accounts_query_error(exc: BaseException) -> bool:
    if isinstance(exc, (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn)):
        return True
    if isinstance(exc, sqlite3.OperationalError):
        message = str(exc).lower()
        if "data_source_provider_accounts" not in message:
            return False
        return "no such table" in message or "no such column" in message
    return False


def _record_provider_accounts_scan_skipped(*, exc: BaseException, phase: str) -> None:
    error_class = type(exc).__name__
    emit_counter(
        "credential_rotation_provider_accounts_scan_skipped",
        1,
        component="services.secrets.rotation",
        extra_tags={"phase": str(phase), "error_class": error_class},
    )
    LOG.warning(
        "credential_rotation_provider_accounts_scan_skipped phase=%s error_class=%s",
        str(phase),
        error_class,
    )


def re_encrypt_blob(blob: str, *, old_key_name: str, new_key_name: str) -> str:
    payload = decrypt_credentials(str(blob or ""), key_name=str(old_key_name))
    return encrypt_credentials(payload, key_name=str(new_key_name))


def _delete_verified_old_key(
    *,
    old_key_name: str,
    new_key_name: str,
    target_key_version: str,
    rotated: int,
    verified: int,
) -> int:
    if rotated <= 0:
        return 0
    if str(old_key_name) in {str(new_key_name), str(target_key_version)}:
        return 0
    if verified < rotated:
        raise SecretRotationError(
            "credential_rotation_old_key_delete_blocked:verification_incomplete:"
            f"rotated={int(rotated)}:verified={int(verified)}"
        )
    try:
        from services.secrets.loader import delete_secret

        deleted = bool(delete_secret(str(old_key_name)))
    except Exception as exc:
        emit_counter(
            "credential_rotation_old_key_delete_failures",
            1,
            component="services.secrets.rotation",
            extra_tags={
                "old_key_name": str(old_key_name),
                "new_key_name": str(new_key_name),
                "error_class": type(exc).__name__,
            },
        )
        LOG.error(
            "credential_rotation_old_key_delete_failed old_key=%s new_key=%s error_class=%s error=%s",
            str(old_key_name),
            str(new_key_name),
            type(exc).__name__,
            exc,
        )
        raise SecretRotationError(
            f"credential_rotation_old_key_delete_failed:{old_key_name}:{type(exc).__name__}:{exc}"
        ) from exc
    if deleted:
        emit_counter(
            "credential_rotation_old_key_deleted",
            1,
            component="services.secrets.rotation",
            extra_tags={"old_key_name": str(old_key_name), "new_key_name": str(new_key_name)},
        )
        LOG.info(
            "credential_rotation_old_key_deleted old_key=%s new_key=%s",
            str(old_key_name),
            str(new_key_name),
        )
        return 1
    return 0


def re_encrypt_data_sources(
    *,
    old_key_name: str = "master_key",
    new_key_name: str = "master_key.next",
    final_key_version: str | None = None,
    delete_old_key: bool = True,
    storage_module: Any | None = None,
) -> dict[str, int]:
    """Re-encrypt every populated data-source credential row.

    This covers both per-source ``data_sources.credentials_enc`` blobs and
    shared ``data_source_provider_accounts.credentials_enc`` blobs. Rows whose
    ``key_version`` does not match ``old_key_name`` are left in place so an
    interrupted rotation can be retried safely.
    """
    if storage_module is None:
        from engine.runtime import storage as storage_module

    with storage_module.connect_ro_direct() as con:
        rows = [
            ("data_sources", row)
            for row in (con.execute(
            """
            SELECT id, source_key, credentials_enc, key_version
            FROM data_sources
            ORDER BY id
            """
            ).fetchall() or [])
        ]
        try:
            rows.extend(
                (
                    "data_source_provider_accounts",
                    row,
                )
                for row in (
                    con.execute(
                        """
                        SELECT id, account_key AS source_key, credentials_enc, key_version
                        FROM data_source_provider_accounts
                        ORDER BY id
                        """
                    ).fetchall()
                    or []
                )
            )
        except Exception as exc:
            if not _is_optional_provider_accounts_query_error(exc):
                raise
            _record_provider_accounts_scan_skipped(exc=exc, phase="rotate_scan")

    target_key_version = str(final_key_version or new_key_name)
    updates: list[tuple[str, str, str, int]] = []
    failures: list[dict[str, Any]] = []
    skipped = 0
    for table_name, row in rows:
        row_id = int(_row_value(row, "id", 0, 0) or 0)
        blob = str(_row_value(row, "credentials_enc", 2, "") or "")
        key_version = str(_row_value(row, "key_version", 3, old_key_name) or old_key_name)
        if not blob.strip() or key_version != str(old_key_name):
            skipped += 1
            continue
        try:
            encrypted = re_encrypt_blob(blob, old_key_name=old_key_name, new_key_name=new_key_name)
        except (InvalidTag, DecryptionError) as exc:
            failures.append(_record_rotation_row_failure(row_id=row_id, exc=exc, phase="decrypt"))
            continue
        updates.append((str(table_name), encrypted, target_key_version, row_id))

    if failures:
        raise SecretRotationPartialFailure(failures)

    def _write(con) -> None:
        for table_name, encrypted, key_version, row_id in updates:
            try:
                if table_name == "data_source_provider_accounts":
                    con.execute(
                        """
                        UPDATE data_source_provider_accounts
                           SET credentials_enc = ?, key_version = ?
                         WHERE id = ?
                        """,
                        (encrypted, key_version, int(row_id)),
                    )
                else:
                    con.execute(
                        """
                        UPDATE data_sources
                           SET credentials_enc = ?, key_version = ?
                         WHERE id = ?
                        """,
                        (encrypted, key_version, int(row_id)),
                    )
            except psycopg.Error as exc:
                failure = _record_rotation_row_failure(row_id=row_id, exc=exc, phase="write")
                raise SecretRotationPartialFailure([failure]) from exc

    if updates:
        storage_module.run_write_txn(_write)

    verified = verify_data_sources_key(
        new_key_name=target_key_version,
        decrypt_key_name=new_key_name,
        storage_module=storage_module,
    )
    old_key_deleted = (
        _delete_verified_old_key(
            old_key_name=old_key_name,
            new_key_name=new_key_name,
            target_key_version=target_key_version,
            rotated=len(updates),
            verified=verified,
        )
        if delete_old_key
        else 0
    )
    return RotationResult(
        scanned=len(rows),
        rotated=len(updates),
        skipped=skipped,
        verified=verified,
        old_key_deleted=old_key_deleted,
    ).as_dict()


def verify_data_sources_key(
    *,
    new_key_name: str,
    decrypt_key_name: str | None = None,
    storage_module: Any | None = None,
) -> int:
    if storage_module is None:
        from engine.runtime import storage as storage_module

    with storage_module.connect_ro_direct() as con:
        rows = [
            ("data_sources", row)
            for row in (con.execute(
            """
            SELECT credentials_enc, key_version
            FROM data_sources
            WHERE COALESCE(credentials_enc, '') <> ''
            ORDER BY id
            """
            ).fetchall() or [])
        ]
        try:
            rows.extend(
                (
                    "data_source_provider_accounts",
                    row,
                )
                for row in (
                    con.execute(
                        """
                        SELECT credentials_enc, key_version
                        FROM data_source_provider_accounts
                        WHERE COALESCE(credentials_enc, '') <> ''
                        ORDER BY id
                        """
                    ).fetchall()
                    or []
                )
            )
        except Exception as exc:
            if not _is_optional_provider_accounts_query_error(exc):
                raise
            _record_provider_accounts_scan_skipped(exc=exc, phase="verify_scan")

    key_name = str(decrypt_key_name or new_key_name)
    verified = 0
    for _table_name, row in rows:
        blob = str(_row_value(row, "credentials_enc", 0, "") or "")
        key_version = str(_row_value(row, "key_version", 1, "") or "")
        if key_version != str(new_key_name):
            continue
        decrypt_credentials(blob, key_name=key_name)
        verified += 1
    return verified


__all__ = [
    "RotationResult",
    "SecretRotationError",
    "SecretRotationPartialFailure",
    "re_encrypt_blob",
    "re_encrypt_data_sources",
    "verify_data_sources_key",
]
