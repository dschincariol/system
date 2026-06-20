"""Helpers for encrypting, decrypting, and masking data-source credentials.

The data-source control plane stores provider secrets encrypted at rest and
returns masked copies to browser clients so operators can rotate configuration
without exposing raw credentials in the UI.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import logging
import os
import stat
from pathlib import Path
from typing import Any, Dict

from cryptography.exceptions import InvalidTag

from services.secrets.loader import load_secret

DEFAULT_MASTER_KEY_NAME = "master_key"
LOG = logging.getLogger(__name__)
_CANONICAL_MASTER_KEY_BYTES = 32
_MIN_DEV_RAW_MASTER_KEY_BYTES = 16
_MASTER_KEY_PLACEHOLDER_TEXT = {
    "changeme",
    "changemenow",
    "change-me",
    "change_me",
    "default",
    "dummy",
    "example",
    "generate",
    "generated",
    "masterkey",
    "master-key",
    "none",
    "null",
    "password",
    "placeholder",
    "sample",
    "secret",
    "test",
    "todo",
    "unset",
    "yourkey",
}
_MASTER_KEY_KNOWN_DEFAULT_TEXT = {
    "__generate_on_install__",
    "<generate-with-openssl-rand-base64-32>",
    "data_source_master_key",
    "data-source-master-key",
    "master-secret",
    "unit-test-master-key",
    "your-data-source-master-key",
    "your_data_source_master_key",
}
_KNOWN_DEFAULT_BINARY_KEYS = {
    bytes(32),
    b"0" * 32,
    b"1" * 32,
    b"a" * 32,
    b"x" * 32,
    b"test" * 8,
}


class MasterKeyLoadError(RuntimeError):
    """Raised when configured master-key material cannot be decoded safely."""


class DecryptionError(RuntimeError):
    """Raised when a stored credential blob cannot be decrypted or parsed."""


def _compact_master_key_text(raw: object) -> str:
    text = str(raw if raw is not None else "").strip().strip("'\"").lower()
    return "".join(ch for ch in text if ch.isalnum())


def _production_like_runtime() -> bool:
    env_input = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "").strip()
    env_raw = env_input.lower()
    env = "prod" if env_raw in {"prod", "production"} else env_raw
    engine_mode = str(os.environ.get("ENGINE_MODE") or "").strip().lower()
    explicit_dev_env = bool(env_input and env in {"dev", "test", "development"})
    supervised = str(os.environ.get("ENGINE_SUPERVISED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    return bool(supervised or env == "prod" or (engine_mode == "live" and not explicit_dev_env))


def _is_placeholder_or_default_text(raw: object) -> bool:
    text = str(raw if raw is not None else "").strip().strip("'\"").lower()
    compact = _compact_master_key_text(text)
    placeholder_compact = {_compact_master_key_text(item) for item in _MASTER_KEY_PLACEHOLDER_TEXT}
    default_compact = {_compact_master_key_text(item) for item in _MASTER_KEY_KNOWN_DEFAULT_TEXT}
    if text in _MASTER_KEY_PLACEHOLDER_TEXT or text in _MASTER_KEY_KNOWN_DEFAULT_TEXT:
        return True
    if compact in placeholder_compact or compact in default_compact:
        return True
    if text.startswith("<") and text.endswith(">"):
        return True
    return bool("generate" in compact and "openssl" in compact)


def _repeated_pattern(raw: bytes) -> bool:
    if not raw:
        return True
    for size in (1, 2, 4, 8, 16):
        if len(raw) % size == 0 and raw == raw[:size] * (len(raw) // size):
            return True
    return False


def _raw_key_material_issue(raw: bytes, *, production: bool) -> str | None:
    text = raw.decode("utf-8", errors="ignore")
    if not raw:
        return "empty"
    if _is_placeholder_or_default_text(text):
        return "placeholder_or_known_default"
    if len(raw) < (32 if production else _MIN_DEV_RAW_MASTER_KEY_BYTES):
        return f"short:{len(raw)}"
    if _repeated_pattern(raw) or len(set(raw)) < (16 if production else 8):
        return "low_entropy"
    if production:
        return "raw_text_forbidden_in_production"
    return None


def _decode_base64_master_key(raw: bytes) -> bytes:
    try:
        text = raw.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise MasterKeyLoadError(
            f"data_source_master_key_decode_failed:{type(exc).__name__}:{exc}"
        ) from exc
    try:
        decoded = base64.b64decode(text.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError) as exc:
        raise MasterKeyLoadError(
            f"data_source_master_key_decode_failed:{type(exc).__name__}:{exc}"
        ) from exc
    if base64.b64encode(decoded).decode("ascii") != text:
        raise MasterKeyLoadError("data_source_master_key_malformed_base64")
    if len(decoded) != _CANONICAL_MASTER_KEY_BYTES:
        raise MasterKeyLoadError(f"data_source_master_key_invalid_length:{len(decoded)}")
    if decoded in _KNOWN_DEFAULT_BINARY_KEYS or _repeated_pattern(decoded) or len(set(decoded)) < 16:
        raise MasterKeyLoadError("data_source_master_key_low_entropy")
    return decoded


def _looks_like_encoded_master_key(raw: bytes | str) -> bool:
    if isinstance(raw, str):
        value = raw.strip()
    else:
        try:
            value = raw.decode("ascii").strip()
        except UnicodeDecodeError:
            return False
    return len(value) == 44 and value.endswith("=")


def _strict_file_permission_issue(path: Path) -> str | None:
    try:
        st = path.stat()
    except OSError:
        return None
    mode = stat.S_IMODE(st.st_mode)
    if str(path).startswith("/run/secrets/"):
        return None
    if mode & (stat.S_IWGRP | stat.S_IWOTH):
        return f"insecure_permissions:{oct(mode)}"
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        return f"insecure_permissions:{oct(mode)}"
    return None


def _read_master_key_file(path: Path, *, production: bool) -> bytes:
    try:
        st = path.stat()
    except FileNotFoundError as exc:
        raise MasterKeyLoadError(f"data_source_master_key_file_missing:{path}") from exc
    except OSError as exc:
        raise MasterKeyLoadError(
            f"data_source_master_key_file_stat_failed:{type(exc).__name__}:{exc}"
        ) from exc
    if not path.is_file():
        raise MasterKeyLoadError(f"data_source_master_key_file_not_regular:{path}")
    if st.st_size <= 0:
        raise MasterKeyLoadError("data_source_master_key_file_empty")
    if not os.access(path, os.R_OK):
        raise MasterKeyLoadError(f"data_source_master_key_file_not_readable:{path}")
    if production:
        permission_issue = _strict_file_permission_issue(path)
        if permission_issue:
            raise MasterKeyLoadError(f"data_source_master_key_file_{permission_issue}")
    return path.read_bytes().strip()


def _validated_master_key_material(raw: bytes, *, production: bool, source: str) -> bytes:
    stripped = bytes(raw or b"").strip()
    if not stripped:
        raise MasterKeyLoadError(f"data_source_master_key_empty:{source}")
    text = stripped.decode("utf-8", errors="ignore")
    if _is_placeholder_or_default_text(text):
        raise MasterKeyLoadError(f"data_source_master_key_placeholder_or_known_default:{source}")
    if _looks_like_encoded_master_key(stripped):
        return _decode_base64_master_key(stripped)
    raw_issue = _raw_key_material_issue(stripped, production=production)
    if raw_issue:
        raise MasterKeyLoadError(f"data_source_master_key_{raw_issue}:{source}")
    return stripped


def data_source_master_key_validation_snapshot(
    *,
    production: bool | None = None,
    require_present: bool | None = None,
) -> Dict[str, Any]:
    """Return validation state for the default data-source master key.

    Production/live runtimes require canonical base64 text for 32 bytes of key
    material. Raw text is accepted only for non-production local development.
    """

    strict = _production_like_runtime() if production is None else bool(production)
    required = strict if require_present is None else bool(require_present)
    source = "unset"
    path_text = ""
    try:
        raw_env = os.environ.get("DATA_SOURCE_MASTER_KEY")
        if raw_env is not None and str(raw_env).strip():
            source = "env"
            raw = str(raw_env).strip().encode("utf-8")
        else:
            file_path = str(os.environ.get("DATA_SOURCE_MASTER_KEY_FILE") or "").strip()
            if file_path:
                source = "file"
                path = Path(file_path).expanduser()
                path_text = str(path)
                raw = _read_master_key_file(path, production=strict)
            elif required:
                source = "secret"
                raw = load_secret(DEFAULT_MASTER_KEY_NAME)
            else:
                return {
                    "ok": True,
                    "required": False,
                    "production": strict,
                    "source": source,
                    "raw_text_allowed": True,
                }
        material = _validated_master_key_material(raw, production=strict, source=source)
        encoded = _looks_like_encoded_master_key(raw)
        return {
            "ok": True,
            "required": required,
            "production": strict,
            "source": source,
            "path": path_text,
            "format": "base64_32" if encoded else "raw_dev_text",
            "raw_text_allowed": not strict,
            "key_material_bytes": len(material),
        }
    except Exception as exc:
        if isinstance(exc, MasterKeyLoadError):
            reason = str(exc)
        else:
            reason = f"{type(exc).__name__}:{exc}"
        return {
            "ok": False,
            "required": required,
            "production": strict,
            "source": source,
            "path": path_text,
            "reason": reason,
            "raw_text_allowed": not strict,
        }


def validate_data_source_master_key(
    *,
    production: bool | None = None,
    require_present: bool | None = None,
) -> Dict[str, Any]:
    snapshot = data_source_master_key_validation_snapshot(
        production=production,
        require_present=require_present,
    )
    if not bool(snapshot.get("ok")):
        raise MasterKeyLoadError(f"data_source_master_key_invalid:{snapshot.get('reason')}")
    return snapshot


def _env_master_key_material(key_name: str) -> bytes | None:
    if str(key_name or DEFAULT_MASTER_KEY_NAME) != DEFAULT_MASTER_KEY_NAME:
        return None

    production = _production_like_runtime()
    raw_env = os.environ.get("DATA_SOURCE_MASTER_KEY")
    if raw_env is not None and str(raw_env).strip():
        return _validated_master_key_material(
            str(raw_env).strip().encode("utf-8"),
            production=production,
            source="env",
        )

    raw: bytes = b""
    if raw_env is None or not str(raw_env).strip():
        file_path = str(os.environ.get("DATA_SOURCE_MASTER_KEY_FILE") or "").strip()
        if file_path:
            try:
                raw = _read_master_key_file(Path(file_path).expanduser(), production=production)
            except MasterKeyLoadError as exc:
                if production or "file_missing" not in str(exc):
                    raise
                LOG.info("data_source_master_key_file_missing path=%s", file_path)
                raw = b""

    if not raw:
        return None
    return _validated_master_key_material(raw, production=production, source="file")


def _hash_master_key_material(raw: bytes) -> bytes:
    end = len(raw)
    while end > 0 and raw[end - 1] in (10, 13):
        end -= 1
    view = memoryview(raw)
    digest_view = view[:end]
    try:
        return hashlib.sha256(digest_view).digest()
    finally:
        digest_view.release()
        view.release()


def _master_key_bytes(key_name: str = DEFAULT_MASTER_KEY_NAME) -> bytes:
    raw: bytes | None = None
    normalized_key_name = str(key_name or DEFAULT_MASTER_KEY_NAME)
    try:
        raw = _env_master_key_material(normalized_key_name)
        if raw is None:
            raw = load_secret(normalized_key_name)
            if normalized_key_name == DEFAULT_MASTER_KEY_NAME:
                raw = _validated_master_key_material(
                    raw,
                    production=_production_like_runtime(),
                    source="secret",
                )
        return _hash_master_key_material(raw)
    finally:
        # load_secret returns immutable bytes. Keep the plaintext master key
        # local to this function and drop the reference immediately after use.
        raw = None


def _aesgcm(key_name: str = DEFAULT_MASTER_KEY_NAME):
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    except ImportError as exc:
        raise RuntimeError(
            "credential_encryption_dependency_missing: install cryptography"
        ) from exc
    return AESGCM(_master_key_bytes(key_name))


def encrypt_credentials(
    payload: Dict[str, Any] | None,
    *,
    key_name: str = DEFAULT_MASTER_KEY_NAME,
) -> str:
    """Encrypt a credential payload for storage at rest.

    Parameters
    ----------
    payload : dict, optional
        Credential mapping to encrypt. Keys and values must be JSON
        serializable.

    Returns
    -------
    str
        Base64-encoded blob containing a 12-byte AES-GCM nonce followed by the
        ciphertext. Empty payloads return an empty string.

    Raises
    ------
    RuntimeError
        If the cryptography dependency or master key configuration is missing.
    """
    data = dict(payload or {})
    if not data:
        return ""
    nonce = os.urandom(12)
    plaintext = json.dumps(data, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ciphertext = _aesgcm(key_name).encrypt(nonce, plaintext, None)
    return base64.b64encode(nonce + ciphertext).decode("ascii")


def decrypt_credentials(
    blob: str | None,
    *,
    key_name: str = DEFAULT_MASTER_KEY_NAME,
) -> Dict[str, Any]:
    """Decrypt a stored credential blob back into a mapping.

    Parameters
    ----------
    blob : str, optional
        Base64-encoded blob produced by :func:`encrypt_credentials`.

    Returns
    -------
    dict
        Decrypted credential mapping. Empty or obviously invalid blobs return an
        empty dictionary.

    Raises
    ------
    DecryptionError
        Raised for non-empty blobs when the payload cannot be authenticated or
        parsed.
    """
    raw = str(blob or "").strip()
    if not raw:
        return {}
    try:
        packed = base64.b64decode(raw.encode("ascii"), validate=True)
        if len(packed) < 13:
            return {}
        nonce = packed[:12]
        ciphertext = packed[12:]
        plaintext = _aesgcm(key_name).decrypt(nonce, ciphertext, None)
        decoded = json.loads(plaintext.decode("utf-8"))
    except (
        binascii.Error,
        InvalidTag,
        json.JSONDecodeError,
        UnicodeEncodeError,
        UnicodeDecodeError,
    ) as exc:
        raise DecryptionError(f"credential_decryption_failed:{type(exc).__name__}:{exc}") from exc
    return decoded if isinstance(decoded, dict) else {}


def mask_credentials(payload: Dict[str, Any] | None) -> Dict[str, str]:
    """Mask credential values for operator-facing responses.

    Parameters
    ----------
    payload : dict, optional
        Raw credential mapping.

    Returns
    -------
    dict of str to str
        Same keys as the input mapping with values replaced by short masked
        strings. Values of length four or fewer are fully masked.
    """
    out: Dict[str, str] = {}
    for key, value in dict(payload or {}).items():
        raw = str(value or "")
        if not raw:
            out[str(key)] = ""
        elif len(raw) <= 4:
            out[str(key)] = "*" * len(raw)
        else:
            out[str(key)] = f"{raw[:2]}{'*' * max(2, len(raw) - 4)}{raw[-2:]}"
    return out
