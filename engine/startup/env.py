"""Environment bootstrap helpers for ``start_system.py``.

This module owns pure startup environment parsing and local bootstrap-file
helpers. The root ``start_system.py`` entrypoint remains the compatibility
facade and supplies logging callbacks plus process-global paths.
"""

import base64
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Callable, Optional


def _log_nonfatal(
    warn: Callable[..., None] | None,
    event: str,
    error: BaseException,
    **extra: Any,
) -> None:
    if warn is None:
        return
    try:
        warn(event, error, **extra)
    except Exception:
        return


def env_file_has_nonempty_value(
    env_path: Path,
    key: str,
    *,
    warn: Callable[..., None] | None = None,
) -> bool:
    """Return whether an env file contains a non-empty value for ``key``."""
    if not env_path.exists():
        return False
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            if name.strip() == key and value.strip():
                return True
    except Exception as e:
        _log_nonfatal(warn, "START_SYSTEM_ENV_FILE_READ_FAILED", e, path=str(env_path), key=str(key))
    return False


def append_env_line(env_path: Path, line: str) -> None:
    """Append one normalized line to an env file, preserving missing newline behavior."""
    existing = ""
    try:
        existing = env_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        existing = ""
    with env_path.open("a", encoding="utf-8", newline="") as fh:
        if existing and not existing.endswith(("\n", "\r")):
            fh.write("\n")
        fh.write(str(line).rstrip("\r\n") + "\n")


def ensure_local_secret_file(
    path: Path,
    *,
    warn: Callable[..., None] | None = None,
) -> None:
    """Create the local secret file if it is missing or empty."""
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path.parent, 0o700)
    except Exception as e:
        _log_nonfatal(warn, "START_SYSTEM_LOCAL_SECRET_DIR_CHMOD_FAILED", e, path=str(path.parent))

    needs_secret = True
    try:
        if path.exists() and path.read_text(encoding="utf-8").strip():
            needs_secret = False
    except Exception as e:
        _log_nonfatal(warn, "START_SYSTEM_LOCAL_SECRET_FILE_READ_FAILED", e, path=str(path))

    if needs_secret:
        key = base64.b64encode(os.urandom(32)).decode("ascii")
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="ascii", newline="\n") as fh:
            fh.write(key + "\n")

    try:
        os.chmod(path, 0o600)
    except Exception as e:
        _log_nonfatal(warn, "START_SYSTEM_LOCAL_SECRET_FILE_CHMOD_FAILED", e, path=str(path))


def strict_runtime_requires_explicit_db_path(environ: Mapping[str, str] | None = None) -> bool:
    """Return whether runtime safety requires callers to set DB_PATH explicitly."""
    try:
        from engine.runtime.config_schema import get_runtime_safety_context

        requires_explicit = bool(get_runtime_safety_context().get("strict_runtime"))
    except Exception:
        source = os.environ if environ is None else environ
        env_raw = str(source.get("ENV") or source.get("NODE_ENV") or "dev").strip().lower()
        env = "prod" if env_raw in {"prod", "production"} else env_raw
        engine_mode = str(source.get("ENGINE_MODE") or "safe").strip().lower()
        supervised = str(source.get("ENGINE_SUPERVISED") or "").strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        explicit_dev_env = bool(str(source.get("ENV") or source.get("NODE_ENV") or "").strip()) and env in {
            "dev",
            "test",
        }
        requires_explicit = bool(
            supervised or env == "prod" or (engine_mode in {"live", "shadow", "paper"} and not explicit_dev_env)
        )
    return bool(requires_explicit)


def ensure_local_env_file(
    base_dir: Path,
    local_secret_relpath: Path,
    *,
    warn: Callable[..., None] | None = None,
) -> None:
    """Ensure the local ``.env`` exists and points to a local master-key file."""
    env_path = Path(base_dir) / ".env"
    example_path = Path(base_dir) / ".env.example"

    if not env_path.exists():
        if example_path.exists():
            env_path.write_text(example_path.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            env_path.write_text("", encoding="utf-8")

    if env_file_has_nonempty_value(env_path, "DATA_SOURCE_MASTER_KEY", warn=warn):
        return
    if not env_file_has_nonempty_value(env_path, "DATA_SOURCE_MASTER_KEY_FILE", warn=warn):
        secret_path = Path(base_dir) / local_secret_relpath
        ensure_local_secret_file(secret_path, warn=warn)
        append_env_line(env_path, f"DATA_SOURCE_MASTER_KEY_FILE={local_secret_relpath.as_posix()}")


def env_int(
    environ: Mapping[str, str],
    name: str,
    default: int,
    *,
    minimum: Optional[int] = None,
    maximum: Optional[int] = None,
) -> int:
    raw = environ.get(name)
    try:
        value = int(float(str(raw if raw is not None else default).strip()))
    except Exception:
        value = int(default)
    if minimum is not None:
        value = max(int(minimum), value)
    if maximum is not None:
        value = min(int(maximum), value)
    return value


def env_float(
    environ: Mapping[str, str],
    name: str,
    default: float,
    *,
    minimum: Optional[float] = None,
    maximum: Optional[float] = None,
) -> float:
    raw = environ.get(name)
    try:
        value = float(str(raw if raw is not None else default).strip())
    except Exception:
        value = float(default)
    if minimum is not None:
        value = max(float(minimum), value)
    if maximum is not None:
        value = min(float(maximum), value)
    return value


def env_bool(environ: Mapping[str, str], name: str, default: bool) -> bool:
    raw = str(environ.get(name, "1" if default else "0") or "").strip().lower()
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(default)
