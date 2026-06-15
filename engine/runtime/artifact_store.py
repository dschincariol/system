"""Runtime helpers for model artifact manifests and local materialization."""

from __future__ import annotations

import os
import re
import logging
from pathlib import Path, PurePosixPath
from typing import Any, Mapping
from urllib.parse import parse_qs, unquote, urlparse

LOG = logging.getLogger(__name__)

OBJECT_STORAGE_SCHEMES = frozenset({"artifact", "az", "azure", "gs", "minio", "s3"})
ARTIFACT_MIRROR_ROOT_ENV = "ARTIFACT_STORE_MIRROR_ROOT"
_IDENTITY_SANITIZE_RE = re.compile(r"[^A-Za-z0-9._-]+")
_LOCAL_ARTIFACT_ALIAS_PREFIXES = ("model:", "filing:", "transcript:", "news:", "artifact:")


def _clean_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        LOG.debug("artifact_manifest_int_parse_failed value=%r", value, exc_info=True)
        return None


def _looks_like_windows_path(value: str) -> bool:
    text = str(value or "").strip()
    return len(text) >= 3 and text[1] == ":" and text[2] in {"/", "\\"}


def _looks_like_local_artifact_alias(value: str) -> bool:
    text = str(value or "").strip()
    if not text or "://" in text or _looks_like_windows_path(text):
        return False
    return text.startswith(_LOCAL_ARTIFACT_ALIAS_PREFIXES)


def _resolve_local_path(value: str) -> Path:
    path = Path(str(value or "")).expanduser()
    if path.is_absolute():
        return path
    return (Path.cwd() / path).resolve()


def _query_value(params: Mapping[str, list[str]], *names: str) -> str | None:
    for name in names:
        values = params.get(str(name), [])
        if values:
            text = _clean_text(values[0])
            if text:
                return text
    return None


def _manifest_identity(*, version_id: str | None, sha256: str | None, etag: str | None) -> str | None:
    return sha256 or version_id or etag


def _sanitize_identity(value: str | None) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    return _IDENTITY_SANITIZE_RE.sub("_", text)


def _object_mirror_path(*, scheme: str, bucket: str, key: str, identity: str | None) -> Path | None:
    root = _clean_text(os.environ.get(ARTIFACT_MIRROR_ROOT_ENV))
    if not root:
        return None
    posix_key = PurePosixPath(str(key))
    base = Path(root).expanduser() / str(scheme) / str(bucket)
    if posix_key.parent != PurePosixPath("."):
        base = base.joinpath(*posix_key.parent.parts)
    filename = posix_key.name or "artifact.bin"
    if identity:
        name_path = Path(filename)
        filename = f"{name_path.stem}__{identity}{name_path.suffix}"
    return base / filename


def _strip_runtime_fields(manifest: Mapping[str, Any]) -> dict[str, Any]:
    persisted: dict[str, Any] = {}
    for key, value in dict(manifest or {}).items():
        if key in {"local_mirror_path", "local_path"}:
            continue
        if value in (None, "", [], {}, ()):
            continue
        persisted[str(key)] = value
    return persisted


def build_artifact_manifest(
    artifact_uri: Any,
    metadata: Mapping[str, Any] | None = None,
    *,
    require_immutable_object: bool = False,
) -> dict[str, Any] | None:
    metadata_dict = dict(metadata or {})
    existing = metadata_dict.get("artifact_manifest")
    existing_manifest = dict(existing) if isinstance(existing, Mapping) else {}
    artifact_uri_text = _clean_text(artifact_uri) or _clean_text(existing_manifest.get("artifact_uri"))
    if not artifact_uri_text:
        return None

    if _looks_like_local_artifact_alias(artifact_uri_text):
        return {
            "artifact_uri": str(artifact_uri_text),
            "storage_backend": "artifact",
            "scheme": "artifact",
            "alias": str(artifact_uri_text),
            "immutable": False,
            "sha256": _clean_text(existing_manifest.get("sha256")),
            "size_bytes": _safe_int(existing_manifest.get("size_bytes")),
            "content_type": _clean_text(existing_manifest.get("content_type")),
        }

    if _looks_like_windows_path(artifact_uri_text) or "://" not in artifact_uri_text:
        local_path = _resolve_local_path(artifact_uri_text)
        size_bytes = _safe_int(existing_manifest.get("size_bytes"))
        if size_bytes is None and local_path.exists():
            try:
                size_bytes = int(local_path.stat().st_size)
            except OSError:
                LOG.warning(
                    "artifact_manifest_local_size_failed path=%s",
                    local_path,
                    exc_info=True,
                )
                size_bytes = None
        return {
            "artifact_uri": str(artifact_uri_text),
            "storage_backend": "local",
            "scheme": "file",
            "immutable": False,
            "sha256": _clean_text(existing_manifest.get("sha256")),
            "etag": _clean_text(existing_manifest.get("etag")),
            "version_id": _clean_text(existing_manifest.get("version_id")),
            "size_bytes": size_bytes,
            "content_type": _clean_text(existing_manifest.get("content_type")),
            "local_path": str(local_path),
        }

    parsed = urlparse(str(artifact_uri_text))
    scheme = str(parsed.scheme or "").strip().lower()
    if scheme == "file":
        local_path_text = unquote(parsed.path or "")
        if parsed.netloc:
            local_path_text = f"//{parsed.netloc}{local_path_text}"
        local_path = _resolve_local_path(local_path_text)
        size_bytes = _safe_int(existing_manifest.get("size_bytes"))
        if size_bytes is None and local_path.exists():
            try:
                size_bytes = int(local_path.stat().st_size)
            except OSError:
                LOG.warning(
                    "artifact_manifest_file_size_failed path=%s",
                    local_path,
                    exc_info=True,
                )
                size_bytes = None
        return {
            "artifact_uri": str(artifact_uri_text),
            "storage_backend": "local",
            "scheme": "file",
            "immutable": False,
            "sha256": _clean_text(existing_manifest.get("sha256")),
            "etag": _clean_text(existing_manifest.get("etag")),
            "version_id": _clean_text(existing_manifest.get("version_id")),
            "size_bytes": size_bytes,
            "content_type": _clean_text(existing_manifest.get("content_type")),
            "local_path": str(local_path),
        }

    if scheme not in OBJECT_STORAGE_SCHEMES:
        raise ValueError(f"unsupported_artifact_uri_scheme:{scheme}")

    bucket = _clean_text(parsed.netloc) or _clean_text(existing_manifest.get("bucket"))
    key = unquote(str(parsed.path or "").lstrip("/")) or _clean_text(existing_manifest.get("key"))
    if not bucket or not key:
        raise ValueError("object_storage_artifact_requires_bucket_and_key")

    query = parse_qs(parsed.query, keep_blank_values=False)
    version_id = _clean_text(existing_manifest.get("version_id")) or _query_value(query, "version_id", "version")
    sha256 = _clean_text(existing_manifest.get("sha256")) or _query_value(query, "sha256", "digest")
    etag = _clean_text(existing_manifest.get("etag")) or _query_value(query, "etag")
    identity = _manifest_identity(version_id=version_id, sha256=sha256, etag=etag)
    immutable = bool(existing_manifest.get("immutable", False) or identity)
    if require_immutable_object and not immutable:
        raise ValueError("object_storage_artifact_requires_immutable_identity")

    local_mirror_path = _clean_text(existing_manifest.get("local_mirror_path"))
    if not local_mirror_path:
        mirror_path = _object_mirror_path(
            scheme=scheme,
            bucket=str(bucket),
            key=str(key),
            identity=_sanitize_identity(identity),
        )
        local_mirror_path = str(mirror_path) if mirror_path is not None else None

    return {
        "artifact_uri": str(artifact_uri_text),
        "storage_backend": "object",
        "scheme": str(scheme),
        "bucket": str(bucket),
        "key": str(key),
        "immutable": bool(immutable),
        "version_id": version_id,
        "sha256": sha256,
        "etag": etag,
        "size_bytes": _safe_int(existing_manifest.get("size_bytes")),
        "content_type": _clean_text(existing_manifest.get("content_type")),
        "local_mirror_path": local_mirror_path,
    }


def normalize_artifact_registration(
    *,
    artifact_uri: Any,
    metadata: Mapping[str, Any] | None = None,
) -> tuple[str | None, dict[str, Any], dict[str, Any] | None]:
    metadata_dict = dict(metadata or {})
    manifest = build_artifact_manifest(
        artifact_uri,
        metadata_dict,
        require_immutable_object=True,
    )
    if manifest is not None:
        metadata_dict["artifact_manifest"] = _strip_runtime_fields(manifest)
        artifact_uri = manifest.get("artifact_uri")
    artifact_uri_text = _clean_text(artifact_uri)
    return artifact_uri_text, metadata_dict, (dict(manifest) if manifest is not None else None)


def get_artifact_manifest(record: Mapping[str, Any]) -> dict[str, Any] | None:
    record_dict = dict(record or {})
    metadata = dict(record_dict.get("metadata") or {})
    explicit = record_dict.get("artifact_manifest")
    if isinstance(explicit, Mapping):
        metadata["artifact_manifest"] = dict(explicit)
    return build_artifact_manifest(record_dict.get("artifact_uri"), metadata, require_immutable_object=False)


def artifact_cache_key(record: Mapping[str, Any], *, manifest: Mapping[str, Any] | None = None) -> str:
    manifest_dict = dict(manifest or get_artifact_manifest(record) or {})
    cache_parts = [
        _clean_text(manifest_dict.get("artifact_uri")),
        _clean_text(manifest_dict.get("version_id")),
        _clean_text(manifest_dict.get("sha256")),
        _clean_text(manifest_dict.get("etag")),
        _clean_text(manifest_dict.get("local_path")),
        _clean_text(manifest_dict.get("local_mirror_path")),
    ]
    compact = [part for part in cache_parts if part]
    if compact:
        return "|".join(compact)
    return str(record.get("artifact_uri") or "").strip()


def resolve_artifact_read_path(record: Mapping[str, Any]) -> tuple[dict[str, Any], Path]:
    manifest = get_artifact_manifest(record)
    if manifest is None:
        raise FileNotFoundError("model_artifact_missing")

    backend = str(manifest.get("storage_backend") or "").strip().lower()
    if backend == "local":
        local_path = _clean_text(manifest.get("local_path"))
        if not local_path:
            raise FileNotFoundError("model_artifact_missing")
        path = Path(local_path)
    elif backend == "artifact":
        alias = _clean_text(manifest.get("alias")) or _clean_text(manifest.get("artifact_uri"))
        if not alias:
            raise FileNotFoundError("artifact_alias_missing")
        from engine.artifacts.store import LocalArtifactStore

        store = LocalArtifactStore()
        ref = store.resolve(alias)
        if ref is None:
            raise FileNotFoundError(f"artifact_alias_not_found:{alias}")
        path = store.object_path(ref)
    elif backend == "object":
        mirror_path = _clean_text(manifest.get("local_mirror_path"))
        if not mirror_path:
            raise FileNotFoundError(f"model_artifact_mirror_unconfigured:{manifest.get('artifact_uri')}")
        path = Path(mirror_path)
    else:
        raise ValueError(f"unsupported_artifact_backend:{backend}")

    if not path.exists():
        raise FileNotFoundError(f"model_artifact_not_found:{path}")
    return dict(manifest), path


def resolve_artifact_write_path(record: Mapping[str, Any]) -> tuple[dict[str, Any], Path] | None:
    manifest = get_artifact_manifest(record)
    if manifest is None:
        return None
    if str(manifest.get("storage_backend") or "").strip().lower() != "local":
        return None
    local_path = _clean_text(manifest.get("local_path"))
    if not local_path:
        return None
    return dict(manifest), Path(local_path)


__all__ = [
    "ARTIFACT_MIRROR_ROOT_ENV",
    "OBJECT_STORAGE_SCHEMES",
    "artifact_cache_key",
    "build_artifact_manifest",
    "get_artifact_manifest",
    "normalize_artifact_registration",
    "resolve_artifact_read_path",
    "resolve_artifact_write_path",
]
