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
from pathlib import Path
from typing import Any, Dict

from cryptography.exceptions import InvalidTag

from services.secrets.loader import load_secret

DEFAULT_MASTER_KEY_NAME = "master_key"
LOG = logging.getLogger(__name__)


class MasterKeyLoadError(RuntimeError):
    """Raised when configured master-key material cannot be decoded safely."""


class DecryptionError(RuntimeError):
    """Raised when a stored credential blob cannot be decrypted or parsed."""


def _looks_like_encoded_master_key(raw: str) -> bool:
    value = str(raw or "").strip()
    return len(value) == 44 and value.endswith("=")


def _env_master_key_material(key_name: str) -> bytes | None:
    if str(key_name or DEFAULT_MASTER_KEY_NAME) != DEFAULT_MASTER_KEY_NAME:
        return None

    raw = str(os.environ.get("DATA_SOURCE_MASTER_KEY") or "").strip()
    if not raw:
        file_path = str(os.environ.get("DATA_SOURCE_MASTER_KEY_FILE") or "").strip()
        if file_path:
            try:
                raw = Path(file_path).expanduser().read_text(encoding="utf-8").strip()
            except FileNotFoundError:
                LOG.info("data_source_master_key_file_missing path=%s", file_path)
                raw = ""

    if not raw:
        return None

    if _looks_like_encoded_master_key(raw):
        try:
            decoded = base64.b64decode(raw.encode("ascii"), validate=True)
        except (binascii.Error, UnicodeEncodeError) as exc:
            raise MasterKeyLoadError(
                f"data_source_master_key_decode_failed:{type(exc).__name__}:{exc}"
            ) from exc
        if len(decoded) != 32:
            raise MasterKeyLoadError(f"data_source_master_key_invalid_length:{len(decoded)}")
        return decoded
    return raw.encode("utf-8")


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
    try:
        raw = _env_master_key_material(str(key_name or DEFAULT_MASTER_KEY_NAME))
        if raw is None:
            raw = load_secret(str(key_name or DEFAULT_MASTER_KEY_NAME))
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
