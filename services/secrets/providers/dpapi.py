"""Windows DPAPI secret provider."""

from __future__ import annotations

import ctypes
import os
import sys
from pathlib import Path

from services.secrets.loader import SecretNotAvailable, validate_secret_name


def _secrets_dir() -> Path:
    configured = str(os.environ.get("TS_DPAPI_SECRETS_DIR") or "").strip()
    if configured:
        return Path(configured).expanduser()
    local_app_data = str(os.environ.get("LOCALAPPDATA") or "").strip()
    if local_app_data:
        return Path(local_app_data) / "Trading" / "secrets"
    return Path.home() / "AppData" / "Local" / "Trading" / "secrets"


def _secret_path(name: str) -> Path:
    secret_name = validate_secret_name(name)
    return _secrets_dir() / f"{secret_name}.dpapi"


def _win32crypt():
    if not sys.platform.startswith("win"):
        raise SecretNotAvailable("dpapi_windows_only")
    try:
        import win32crypt
    except Exception as exc:
        raise SecretNotAvailable(f"pywin32_unavailable:{type(exc).__name__}:{exc}") from exc
    return win32crypt


def _copy_plaintext(data: object) -> bytearray:
    return bytearray(data)


def _zero_bytearray(data: bytearray | None) -> None:
    if not data:
        return
    try:
        addr = ctypes.addressof(ctypes.c_char.from_buffer(data))
        ctypes.memset(addr, 0, len(data))
    except (TypeError, ValueError):
        for idx in range(len(data)):
            data[idx] = 0


def _bytes_buffer_address(data: bytes) -> int | None:
    try:
        py_bytes_as_string = ctypes.pythonapi.PyBytes_AsString
        py_bytes_as_string.argtypes = [ctypes.py_object]
        py_bytes_as_string.restype = ctypes.c_void_p
        addr = py_bytes_as_string(data)
    except Exception:
        return None
    return int(addr or 0) or None


def _writable_buffer_address(data: object) -> int | None:
    try:
        return ctypes.addressof(ctypes.c_char.from_buffer(data))
    except (TypeError, ValueError):
        return None


def _zero_original_buffer(data: object) -> bool:
    try:
        length = len(data)  # type: ignore[arg-type]
    except TypeError:
        return False
    if length <= 0:
        return True

    addr: int | None
    if isinstance(data, bytes):
        # CPython exposes the bytes payload address, but this is intentionally
        # best-effort: pywin32 and alternate runtimes may not expose a safely
        # writable buffer, and perfect memory hygiene is not achievable here.
        addr = _bytes_buffer_address(data)
    else:
        addr = _writable_buffer_address(data)
    if not addr:
        return False
    try:
        ctypes.memset(addr, 0, length)
    except Exception:
        return False
    return True


def protect_secret(name: str, data: bytes, *, secrets_dir: str | os.PathLike[str] | None = None) -> Path:
    """Encrypt and store a secret for tests and developer provisioning."""
    win32crypt = _win32crypt()
    secret_name = validate_secret_name(name)
    path = (Path(secrets_dir).expanduser() if secrets_dir is not None else _secret_path(secret_name).parent) / f"{secret_name}.dpapi"
    path.parent.mkdir(parents=True, exist_ok=True)
    encrypted = win32crypt.CryptProtectData(bytes(data), str(secret_name), None, None, None, 0)
    path.write_bytes(encrypted)
    return path


def load(name: str) -> bytes:
    plaintext: bytearray | None = None
    try:
        win32crypt = _win32crypt()
        path = _secret_path(name)
        try:
            encrypted = path.read_bytes()
        except FileNotFoundError as exc:
            raise SecretNotAvailable(f"secret_missing:{name}") from exc
        except OSError as exc:
            raise SecretNotAvailable(f"secret_read_failed:{name}:{type(exc).__name__}:{exc}") from exc
        try:
            _description, data = win32crypt.CryptUnprotectData(encrypted, None, None, None, 0)
            plaintext = _copy_plaintext(data)
            _zero_original_buffer(data)
            return bytes(plaintext)
        except Exception as exc:
            raise SecretNotAvailable(f"secret_decrypt_failed:{name}:{type(exc).__name__}:{exc}") from exc
    finally:
        _zero_bytearray(plaintext)


def delete(name: str) -> bool:
    path = _secret_path(name)
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise SecretNotAvailable(f"secret_delete_failed:{name}:{type(exc).__name__}:{exc}") from exc


__all__ = ["delete", "load", "protect_secret"]
