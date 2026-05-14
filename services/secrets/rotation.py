"""Helpers for offline-safe data-source credential rotation."""

from __future__ import annotations
import logging

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

    def as_dict(self) -> dict[str, int]:
        return {
            "scanned": int(self.scanned),
            "rotated": int(self.rotated),
            "skipped": int(self.skipped),
            "verified": int(self.verified),
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


def re_encrypt_blob(blob: str, *, old_key_name: str, new_key_name: str) -> str:
    payload = decrypt_credentials(str(blob or ""), key_name=str(old_key_name))
    return encrypt_credentials(payload, key_name=str(new_key_name))


def re_encrypt_data_sources(
    *,
    old_key_name: str = "master_key",
    new_key_name: str = "master_key.next",
    final_key_version: str | None = None,
    storage_module: Any | None = None,
) -> dict[str, int]:
    """Re-encrypt every populated ``data_sources.credentials_enc`` row.

    Rows whose ``key_version`` does not match ``old_key_name`` are left in
    place so an interrupted rotation can be retried safely.
    """
    if storage_module is None:
        from engine.runtime import storage as storage_module

    with storage_module.connect_ro_direct() as con:
        rows = con.execute(
            """
            SELECT id, source_key, credentials_enc, key_version
            FROM data_sources
            ORDER BY id
            """
        ).fetchall() or []

    target_key_version = str(final_key_version or new_key_name)
    updates: list[tuple[str, str, int]] = []
    failures: list[dict[str, Any]] = []
    skipped = 0
    for row in rows:
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
        updates.append((encrypted, target_key_version, row_id))

    if failures:
        raise SecretRotationPartialFailure(failures)

    def _write(con) -> None:
        for encrypted, key_version, row_id in updates:
            try:
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
    return RotationResult(
        scanned=len(rows),
        rotated=len(updates),
        skipped=skipped,
        verified=verified,
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
        rows = con.execute(
            """
            SELECT credentials_enc, key_version
            FROM data_sources
            WHERE COALESCE(credentials_enc, '') <> ''
            ORDER BY id
            """
        ).fetchall() or []

    key_name = str(decrypt_key_name or new_key_name)
    verified = 0
    for row in rows:
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
