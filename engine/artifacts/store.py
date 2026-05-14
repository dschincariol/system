"""Local content-addressed artifact store."""

from __future__ import annotations
import errno
import hashlib
import io
import json
import logging
import os
import shutil
import tempfile
from contextlib import contextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterator, Protocol

from engine.artifacts.paths import artifacts_root, object_path, validate_sha256
from engine.artifacts.refs import ArtifactRef

CHUNK_SIZE = 1024 * 1024
_LOG = logging.getLogger(__name__)


class ArtifactStore(Protocol):
    def put(
        self,
        data: bytes,
        *,
        content_type: str,
        kind: str,
        alias: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        ...

    def put_path(
        self,
        path: Path,
        *,
        content_type: str,
        kind: str,
        alias: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        ...

    def get_bytes(self, ref: ArtifactRef, *, verify: bool = True) -> bytes:
        ...

    def open(self, ref: ArtifactRef, *, verify: bool = True) -> BinaryIO:
        ...

    def resolve(self, alias: str) -> ArtifactRef | None:
        ...

    def set_alias(self, alias: str, ref: ArtifactRef) -> None:
        ...

    def list_aliases(self, prefix: str | None = None) -> list[str]:
        ...

    def list_versions(self, alias: str) -> list[ArtifactRef]:
        ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso_ts(value: datetime | None = None) -> str:
    return (value or _utc_now()).astimezone(timezone.utc).isoformat()


def _json_dumps(value: Any) -> str:
    return json.dumps(dict(value or {}), separators=(",", ":"), sort_keys=True, default=str)


def _json_param(con: Any, value: Any) -> Any:
    payload = dict(value or {})
    module = type(con).__module__
    if "sqlite" in module:
        return _json_dumps(payload)
    if module.startswith("engine.runtime.storage_pg"):
        return payload
    try:
        from psycopg.types.json import Jsonb

        return Jsonb(payload)
    except Exception:
        return _json_dumps(payload)


def _json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        out = json.loads(raw)
    except Exception:
        return {}
    return dict(out) if isinstance(out, dict) else {}


def _parse_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text = str(value or "").strip()
    if not text:
        return _utc_now()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return _utc_now()


def _row_value(row: Any, key: str, index: int, default: Any = None) -> Any:
    if row is None:
        return default
    try:
        return row[key]
    except Exception:
        try:
            return row[index]
        except Exception:
            return default


def _connect_default():
    from engine.runtime.storage import connect

    return connect()


class ArtifactCorruption(IOError):
    def __init__(self, *, expected_sha256: str, actual_sha256: str, path: Path) -> None:
        self.expected_sha256 = validate_sha256(expected_sha256)
        self.actual_sha256 = validate_sha256(actual_sha256)
        self.path = Path(path)
        super().__init__(
            "artifact_corruption:"
            f" expected_sha256={self.expected_sha256}"
            f" actual_sha256={self.actual_sha256}"
            f" path={self.path}"
        )


class LocalArtifactStore:
    def __init__(
        self,
        *,
        root: Path | str | None = None,
        connect_factory: Callable[[], Any] | None = None,
        ensure_schema: bool = True,
    ) -> None:
        self.root = Path(root).expanduser().resolve() if root is not None else artifacts_root()
        self.connect_factory = connect_factory or _connect_default
        if ensure_schema:
            self.ensure_schema()

    def ensure_schema(self) -> None:
        with self._connection(readonly=False) as con:
            self._ensure_schema(con)
            self._commit(con)

    def put(
        self,
        data: bytes,
        *,
        content_type: str,
        kind: str,
        alias: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        payload = bytes(data or b"")
        with tempfile.NamedTemporaryFile(dir=self._temp_dir(), delete=False) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        try:
            return self._put_staged_file(
                tmp_path,
                content_type=content_type,
                kind=kind,
                alias=alias,
                metadata=metadata,
                known_size=len(payload),
                known_sha=hashlib.sha256(payload).hexdigest(),
            )
        finally:
            self._unlink_quietly(tmp_path)

    def put_path(
        self,
        path: Path,
        *,
        content_type: str,
        kind: str,
        alias: str | None = None,
        metadata: dict | None = None,
    ) -> ArtifactRef:
        source = Path(path).expanduser()
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(str(source))
        tmp_dir = self._temp_dir()
        digest = hashlib.sha256()
        size = 0
        with tempfile.NamedTemporaryFile(dir=tmp_dir, delete=False) as tmp:
            tmp_path = Path(tmp.name)
            with source.open("rb") as handle:
                while True:
                    chunk = handle.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    digest.update(chunk)
                    size += len(chunk)
                    tmp.write(chunk)
        try:
            return self._put_staged_file(
                tmp_path,
                content_type=content_type,
                kind=kind,
                alias=alias,
                metadata=metadata,
                known_size=int(size),
                known_sha=digest.hexdigest(),
            )
        finally:
            self._unlink_quietly(tmp_path)

    def get_bytes(self, ref: ArtifactRef, *, verify: bool = True) -> bytes:
        digest = validate_sha256(ref.sha256)
        path = object_path(digest, root=self.root)
        payload = path.read_bytes()
        if verify:
            self._verify_payload_hash(payload, expected_sha256=digest, path=path)
        return payload

    def open(self, ref: ArtifactRef, *, verify: bool = True) -> BinaryIO:
        digest = validate_sha256(ref.sha256)
        path = object_path(digest, root=self.root)
        if verify:
            handle = io.BytesIO(self.get_bytes(ref, verify=True))
            handle.name = str(path)  # type: ignore[attr-defined]
            return handle
        return path.open("rb")

    def resolve(self, alias: str) -> ArtifactRef | None:
        alias_text = self._normalize_alias(alias)
        with self._connection(readonly=True) as con:
            row = con.execute(
                """
                SELECT a.sha256, a.size_bytes, a.content_type, a.kind, a.created_ts, a.metadata
                FROM artifact_aliases aa
                JOIN artifacts a ON a.sha256 = aa.sha256
                WHERE aa.alias=?
                ORDER BY aa.set_at DESC
                LIMIT 1
                """,
                (alias_text,),
            ).fetchone()
            return self._ref_from_row(row) if row else None

    def set_alias(self, alias: str, ref: ArtifactRef) -> None:
        alias_text = self._normalize_alias(alias)
        digest = validate_sha256(ref.sha256)

        def _write(con) -> None:
            existing = con.execute(
                """
                SELECT sha256
                FROM artifact_aliases
                WHERE alias=?
                ORDER BY set_at DESC
                LIMIT 1
                """,
                (alias_text,),
            ).fetchone()
            previous_sha = str(_row_value(existing, "sha256", 0, "") or "").strip().lower() if existing else ""
            con.execute(
                """
                INSERT INTO artifact_aliases(alias, sha256, set_at)
                VALUES(?,?,?)
                """,
                (alias_text, digest, _iso_ts()),
            )
            if previous_sha != digest:
                con.execute(
                    "UPDATE artifacts SET ref_count=ref_count + 1 WHERE sha256=?",
                    (digest,),
                )
                if previous_sha:
                    con.execute(
                        "UPDATE artifacts SET ref_count=CASE WHEN ref_count > 0 THEN ref_count - 1 ELSE 0 END WHERE sha256=?",
                        (previous_sha,),
                    )

        self._run_write(_write)

    def list_aliases(self, prefix: str | None = None) -> list[str]:
        with self._connection(readonly=True) as con:
            if prefix is None:
                rows = con.execute("SELECT DISTINCT alias FROM artifact_aliases ORDER BY alias").fetchall()
            else:
                rows = con.execute(
                    "SELECT DISTINCT alias FROM artifact_aliases WHERE alias LIKE ? ORDER BY alias",
                    (f"{str(prefix)}%",),
                ).fetchall()
        return [str(_row_value(row, "alias", 0, "") or "") for row in rows or []]

    def list_versions(self, alias: str) -> list[ArtifactRef]:
        alias_text = self._normalize_alias(alias)
        with self._connection(readonly=True) as con:
            rows = con.execute(
                """
                SELECT a.sha256, a.size_bytes, a.content_type, a.kind, a.created_ts, a.metadata
                FROM artifact_aliases aa
                JOIN artifacts a ON a.sha256 = aa.sha256
                WHERE aa.alias=?
                ORDER BY aa.set_at DESC
                """,
                (alias_text,),
            ).fetchall()
        return [self._ref_from_row(row) for row in rows or []]

    def object_path(self, ref_or_sha: ArtifactRef | str) -> Path:
        sha = ref_or_sha.sha256 if isinstance(ref_or_sha, ArtifactRef) else str(ref_or_sha)
        return object_path(sha, root=self.root)

    def _put_staged_file(
        self,
        tmp_path: Path,
        *,
        content_type: str,
        kind: str,
        alias: str | None,
        metadata: dict | None,
        known_size: int,
        known_sha: str,
    ) -> ArtifactRef:
        digest = validate_sha256(known_sha)
        dest = object_path(digest, root=self.root)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            self._finalize_staged_file(tmp_path, dest)
        created_ts = _utc_now()
        metadata_dict = dict(metadata or {})

        def _write(con) -> ArtifactRef:
            con.execute(
                """
                INSERT INTO artifacts(sha256, size_bytes, content_type, kind, created_ts, metadata, ref_count)
                VALUES(?,?,?,?,?,?,0)
                ON CONFLICT(sha256) DO NOTHING
                """,
                (
                    digest,
                    int(known_size),
                    str(content_type),
                    str(kind),
                    _iso_ts(created_ts),
                    _json_param(con, metadata_dict),
                ),
            )
            row = con.execute(
                """
                SELECT sha256, size_bytes, content_type, kind, created_ts, metadata
                FROM artifacts
                WHERE sha256=?
                LIMIT 1
                """,
                (digest,),
            ).fetchone()
            return self._ref_from_row(row)

        ref = self._run_write(_write)
        if alias:
            self.set_alias(str(alias), ref)
        return ref

    @staticmethod
    def _normalize_alias(alias: str) -> str:
        text = str(alias or "").strip()
        if not text:
            raise ValueError("artifact_alias_required")
        return text

    @staticmethod
    def _ref_from_row(row: Any) -> ArtifactRef:
        if row is None:
            raise KeyError("artifact_row_missing")
        return ArtifactRef(
            sha256=validate_sha256(str(_row_value(row, "sha256", 0, ""))),
            size=int(_row_value(row, "size_bytes", 1, 0) or 0),
            content_type=str(_row_value(row, "content_type", 2, "") or ""),
            kind=str(_row_value(row, "kind", 3, "") or ""),
            created_ts=_parse_ts(_row_value(row, "created_ts", 4, None)),
            metadata=_json_loads(_row_value(row, "metadata", 5, None)),
        )

    def _temp_dir(self) -> Path:
        path = Path(self.root).expanduser().resolve() / "temp"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _finalize_staged_file(self, tmp_path: Path, dest: Path) -> None:
        if not self._same_device(tmp_path, dest.parent):
            self._log_cross_device_fallback(tmp_path, dest)
            self._copy_staged_file_for_atomic_replace(tmp_path, dest)
            return
        try:
            os.replace(str(tmp_path), str(dest))
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            self._log_cross_device_fallback(tmp_path, dest)
            self._copy_staged_file_for_atomic_replace(tmp_path, dest)

    @staticmethod
    def _same_device(left: Path, right: Path) -> bool:
        return os.stat(str(left)).st_dev == os.stat(str(right)).st_dev

    def _copy_staged_file_for_atomic_replace(self, tmp_path: Path, dest: Path) -> None:
        fd = -1
        dest_tmp_path: Path | None = None
        try:
            fd, dest_tmp_path = self._create_dest_tmp_path(dest)
            out_handle = os.fdopen(fd, "wb")
            fd = -1
            with tmp_path.open("rb") as source, out_handle as out:
                shutil.copyfileobj(source, out, length=CHUNK_SIZE)
                out.flush()
                os.fsync(out.fileno())
            os.replace(str(dest_tmp_path), str(dest))
            self._unlink_quietly(tmp_path)
        except Exception:
            if fd >= 0:
                with suppress(OSError):
                    os.close(fd)
            if dest_tmp_path is not None:
                self._unlink_quietly(dest_tmp_path)
            raise

    @staticmethod
    def _create_dest_tmp_path(dest: Path) -> tuple[int, Path]:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
        for index in range(1000):
            candidate = dest.with_name(f"{dest.name}.tmp_{index:03d}")
            try:
                return os.open(str(candidate), flags, 0o666), candidate
            except FileExistsError:
                continue
        raise FileExistsError(f"no_available_artifact_tmp_path:{dest}")

    @staticmethod
    def _log_cross_device_fallback(tmp_path: Path, dest: Path) -> None:
        _LOG.warning(
            "Artifact staged file and destination are on different filesystems; "
            "using copy-and-atomic-replace fallback",
            extra={
                "artifact_tmp_path": str(tmp_path),
                "artifact_dest_path": str(dest),
            },
        )

    def _all_object_paths(self) -> Iterator[Path]:
        objects = self.root / "objects"
        if not objects.exists():
            return iter(())
        return (path for path in objects.glob("*/*/*") if path.is_file())

    @staticmethod
    def _verify_payload_hash(payload: bytes, *, expected_sha256: str, path: Path) -> None:
        actual_sha256 = hashlib.sha256(payload).hexdigest()
        if actual_sha256 != expected_sha256:
            raise ArtifactCorruption(
                expected_sha256=expected_sha256,
                actual_sha256=actual_sha256,
                path=path,
            )

    @staticmethod
    def _unlink_quietly(path: Path) -> None:
        try:
            if Path(path).exists():
                Path(path).unlink()
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

    @staticmethod
    def _commit(con: Any) -> None:
        commit = getattr(con, "commit", None)
        if callable(commit):
            commit()

    @staticmethod
    def _rollback(con: Any) -> None:
        rollback = getattr(con, "rollback", None)
        if callable(rollback):
            rollback()

    @staticmethod
    def _close(con: Any) -> None:
        close = getattr(con, "close", None)
        if callable(close):
            close()

    @contextmanager
    def _connection(self, *, readonly: bool = False):
        del readonly
        con = self.connect_factory()
        try:
            yield con
        finally:
            self._close(con)

    def _run_write(self, fn: Callable[[Any], Any]) -> Any:
        with self._connection(readonly=False) as con:
            try:
                self._ensure_schema(con)
                result = fn(con)
                self._commit(con)
                return result
            except Exception:
                self._rollback(con)
                raise

    @staticmethod
    def _ensure_schema(con: Any) -> None:
        if LocalArtifactStore._schema_is_migration_owned(con):
            return
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS artifacts (
              sha256 TEXT PRIMARY KEY,
              size_bytes BIGINT NOT NULL,
              content_type TEXT NOT NULL,
              kind TEXT NOT NULL,
              created_ts TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
              metadata JSONB NOT NULL DEFAULT '{}',
              ref_count INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS artifacts_kind_created ON artifacts (kind, created_ts DESC)")
        try:
            con.execute("CREATE INDEX IF NOT EXISTS artifacts_metadata_gin ON artifacts USING GIN (metadata jsonb_path_ops)")
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS artifact_aliases (
              alias TEXT NOT NULL,
              sha256 TEXT NOT NULL REFERENCES artifacts(sha256),
              set_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (alias, set_at)
            )
            """
        )
        con.execute("CREATE INDEX IF NOT EXISTS artifact_aliases_current ON artifact_aliases (alias, set_at DESC)")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS artifact_fsck_findings (
              id INTEGER PRIMARY KEY,
              checked_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
              severity TEXT NOT NULL,
              finding_type TEXT NOT NULL,
              sha256 TEXT,
              path TEXT,
              detail_json JSONB NOT NULL DEFAULT '{}'
            )
            """
        )

    @staticmethod
    def _schema_is_migration_owned(con: Any) -> bool:
        module = type(con).__module__
        return module.startswith("engine.runtime.storage_pg") or module.startswith("psycopg")

    def remove_object_file(self, sha256: str) -> bool:
        path = object_path(validate_sha256(sha256), root=self.root)
        if not path.exists():
            return False
        path.unlink()
        return True


_DEFAULT_STORE: LocalArtifactStore | None = None


def default_store() -> LocalArtifactStore:
    global _DEFAULT_STORE
    if _DEFAULT_STORE is None:
        _DEFAULT_STORE = LocalArtifactStore()
    return _DEFAULT_STORE


def copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dst)


__all__ = [
    "ArtifactCorruption",
    "ArtifactStore",
    "CHUNK_SIZE",
    "LocalArtifactStore",
    "copy_file",
    "default_store",
]
