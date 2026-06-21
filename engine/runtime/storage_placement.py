from __future__ import annotations

"""Production storage placement checks for high-growth runtime state."""

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


_PRODUCTION_VALUES = {"prod", "production", "live"}
_TRUTHY = {"1", "true", "yes", "on", "y"}
_FALSY = {"0", "false", "no", "off", "n"}
_DEFAULT_ALLOWED_FS_TYPES = ("zfs",)


def _system_path(*parts: str) -> str:
    return str(Path(os.sep).joinpath(*parts))


_DEFAULT_DOCKER_DATA_ROOT = _system_path("var", "lib", "docker")
_DEFAULT_FORBIDDEN_PREFIXES = (_DEFAULT_DOCKER_DATA_ROOT, _system_path("var", "lib", "containerd"))
_TIMESCALE_CONTAINER_DATA = _system_path("var", "lib", "postgresql", "data")
_BACKUP_CONTAINER_ROOT = _system_path("var", "backups", "trading")
_DEFAULT_BACKUP_ROOT = _BACKUP_CONTAINER_ROOT


@dataclass(frozen=True)
class StorageTarget:
    name: str
    description: str
    host_env: str
    host_path: str
    container_path: str
    visible_in_runtime: bool


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _truthy(value: Any) -> bool:
    return _clean(value).lower() in _TRUTHY


def _falsey(value: Any) -> bool:
    return _clean(value).lower() in _FALSY


def _split_csv(value: Any) -> list[str]:
    return [part.strip() for part in re.split(r"[\s,]+", _clean(value)) if part.strip()]


def _production_like(env: Mapping[str, str]) -> bool:
    for name in ("PROD_LOCK", "ENGINE_SUPERVISED"):
        if _truthy(env.get(name)):
            return True
    for name in ("ENV", "APP_ENV", "TS_ENV", "ENGINE_MODE", "EXECUTION_MODE", "OPERATOR_MODE"):
        if _clean(env.get(name)).lower() in _PRODUCTION_VALUES:
            return True
    return False


def _checks_enabled(env: Mapping[str, str]) -> bool:
    raw = env.get("PREFLIGHT_REQUIRE_ZFS_STORAGE")
    if raw is not None and _clean(raw):
        return _truthy(raw)
    raw = env.get("PREFLIGHT_CHECK_STORAGE_PLACEMENT")
    if raw is not None and _clean(raw):
        return _truthy(raw)
    return _production_like(env)


def _env_path(env: Mapping[str, str], name: str) -> str:
    return _clean(env.get(name))


def _join_path(parent: str, child: str) -> str:
    if not parent:
        return ""
    return str(Path(parent).expanduser() / child)


def _target_specs(env: Mapping[str, str]) -> list[StorageTarget]:
    timescale_data = _env_path(env, "TRADING_TIMESCALE_DATA")
    backup_root = _env_path(env, "TRADING_BACKUP_ROOT")
    return [
        StorageTarget(
            name="timescale_pgdata",
            description="Timescale PGDATA",
            host_env="TRADING_TIMESCALE_DATA",
            host_path=timescale_data,
            container_path=_TIMESCALE_CONTAINER_DATA,
            visible_in_runtime=False,
        ),
        StorageTarget(
            name="timescale_pg_wal",
            description="Timescale pg_wal",
            host_env="TRADING_TIMESCALE_DATA",
            host_path=_env_path(env, "TRADING_TIMESCALE_WAL") or _join_path(timescale_data, "pg_wal"),
            container_path=_join_path(_TIMESCALE_CONTAINER_DATA, "pg_wal"),
            visible_in_runtime=False,
        ),
        StorageTarget(
            name="redis_appendonly",
            description="Redis appendonly data",
            host_env="TRADING_REDIS_DATA",
            host_path=_env_path(env, "TRADING_REDIS_DATA"),
            container_path="/data",
            visible_in_runtime=False,
        ),
        StorageTarget(
            name="minio_data",
            description="MinIO object data",
            host_env="TRADING_MINIO_DATA",
            host_path=_env_path(env, "TRADING_MINIO_DATA"),
            container_path="/data",
            visible_in_runtime=False,
        ),
        StorageTarget(
            name="runtime_data",
            description="runtime data",
            host_env="TRADING_RUNTIME_DATA",
            host_path=_env_path(env, "TRADING_RUNTIME_DATA"),
            container_path=_env_path(env, "TRADING_DATA") or _env_path(env, "DB_PATH") or "/app/data",
            visible_in_runtime=True,
        ),
        StorageTarget(
            name="runtime_logs",
            description="runtime logs",
            host_env="TRADING_RUNTIME_LOGS",
            host_path=_env_path(env, "TRADING_RUNTIME_LOGS"),
            container_path=_env_path(env, "TRADING_LOGS") or "/app/logs",
            visible_in_runtime=True,
        ),
        StorageTarget(
            name="artifact_mirror",
            description="artifact mirror",
            host_env="TRADING_ARTIFACT_MIRROR",
            host_path=_env_path(env, "TRADING_ARTIFACT_MIRROR"),
            container_path=_env_path(env, "ARTIFACT_STORE_MIRROR_ROOT") or "/app/artifact_mirror",
            visible_in_runtime=True,
        ),
        StorageTarget(
            name="training_datasets",
            description="training dataset cache",
            host_env="TRADING_TRAINING_DATASETS",
            host_path=_env_path(env, "TRADING_TRAINING_DATASETS"),
            container_path=_env_path(env, "TRAINING_DATASET_STORE_ROOT") or "/app/training_datasets",
            visible_in_runtime=True,
        ),
        StorageTarget(
            name="backup_root",
            description="backup root",
            host_env="TRADING_BACKUP_ROOT",
            host_path=backup_root,
            container_path=_BACKUP_CONTAINER_ROOT,
            visible_in_runtime=True,
        ),
        StorageTarget(
            name="backup_wal",
            description="backup WAL archive",
            host_env="TRADING_BACKUP_WAL_DIR",
            host_path=_env_path(env, "TRADING_BACKUP_WAL_DIR") or _join_path(backup_root, "wal"),
            container_path=_join_path(_BACKUP_CONTAINER_ROOT, "wal"),
            visible_in_runtime=True,
        ),
    ]


def _decode_mount_field(value: str) -> str:
    return str(value or "").replace("\\040", " ")


def _parse_mountinfo(text: str) -> list[dict[str, str]]:
    mounts: list[dict[str, str]] = []
    for line in str(text or "").splitlines():
        left, sep, right = line.partition(" - ")
        if not sep:
            continue
        left_fields = left.split()
        right_fields = right.split()
        if len(left_fields) < 5 or len(right_fields) < 3:
            continue
        mounts.append(
            {
                "mount_point": _decode_mount_field(left_fields[4]),
                "mount_root": _decode_mount_field(left_fields[3]),
                "filesystem_type": _decode_mount_field(right_fields[0]).lower(),
                "mount_source": _decode_mount_field(right_fields[1]),
                "super_options": _decode_mount_field(right_fields[2]),
            }
        )
    return mounts


def _read_mountinfo() -> str:
    try:
        return Path("/proc/self/mountinfo").read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""


def _norm_path(value: str) -> str:
    text = _clean(value)
    if not text:
        return ""
    return os.path.normpath(str(Path(text).expanduser()))


def _path_under(path: str, prefix: str) -> bool:
    path_norm = _norm_path(path)
    prefix_norm = _norm_path(prefix)
    if not path_norm or not prefix_norm:
        return False
    return path_norm == prefix_norm or path_norm.startswith(prefix_norm.rstrip("/") + "/")


def _best_mount(path: str, mounts: Iterable[Mapping[str, str]]) -> dict[str, str]:
    path_norm = _norm_path(path)
    best: dict[str, str] = {}
    best_len = -1
    for mount in mounts:
        mount_point = _norm_path(str(mount.get("mount_point") or ""))
        if not mount_point:
            continue
        if not _path_under(path_norm, mount_point):
            continue
        mount_len = len(mount_point)
        if mount_len <= best_len:
            continue
        best_len = mount_len
        best = {str(k): str(v) for k, v in dict(mount).items()}
    return best


def _mount_mentions_forbidden(mount: Mapping[str, str], forbidden_prefixes: Iterable[str]) -> str:
    for field in ("mount_point", "mount_root", "mount_source"):
        value = str(mount.get(field) or "")
        for prefix in forbidden_prefixes:
            if _path_under(value, prefix) or prefix in value:
                return f"{field}={value}"
    return ""


def _root_mount(mount: Mapping[str, str]) -> bool:
    return _norm_path(str(mount.get("mount_point") or "")) == "/"


def _first_visible_mount(
    *,
    target: StorageTarget,
    mounts: Iterable[Mapping[str, str]],
    path_exists: Callable[[str], bool],
) -> tuple[str, dict[str, str]]:
    candidates: list[str] = []
    if target.visible_in_runtime and target.container_path:
        candidates.append(target.container_path)
    if target.host_path:
        candidates.append(target.host_path)
        parent = str(Path(target.host_path).expanduser().parent)
        if parent and parent != target.host_path:
            candidates.append(parent)
    for candidate in candidates:
        try:
            exists = bool(path_exists(candidate))
        except Exception:
            exists = False
        if not exists:
            continue
        mount = _best_mount(candidate, mounts)
        if mount:
            return candidate, mount
    return "", {}


def _default_path_exists(path: str) -> bool:
    return Path(path).expanduser().exists()


def _allowed_prefixes(env: Mapping[str, str]) -> list[str]:
    configured = _split_csv(env.get("TRADING_ALLOWED_STORAGE_PREFIXES"))
    if configured:
        return configured
    prefixes = []
    zfs_root = _clean(env.get("TRADING_ZFS_ROOT")) or "/zpool"
    backup_root = _clean(env.get("TRADING_BACKUP_ROOT")) or _DEFAULT_BACKUP_ROOT
    prefixes.extend([zfs_root, backup_root])
    return prefixes


def _require_visible_host_paths(env: Mapping[str, str]) -> bool:
    if _production_like(env):
        return True
    raw = env.get("PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS")
    return bool(raw is not None and _clean(raw) and _truthy(raw))


def storage_pressure_paths(env: Mapping[str, str] | None = None) -> list[tuple[str, Path]]:
    """Return host/container paths that should be part of disk-pressure checks."""

    source_env: Mapping[str, str] = env if env is not None else os.environ
    paths: list[tuple[str, Path]] = []
    zfs_root = _clean(source_env.get("TRADING_ZFS_ROOT")) or "/zpool"
    paths.append(("zfs_pool", Path(zfs_root)))
    for target in _target_specs(source_env):
        if target.name == "backup_root":
            continue
        if target.host_path:
            paths.append((target.name, Path(target.host_path).expanduser()))
        if target.visible_in_runtime and target.container_path:
            paths.append((f"{target.name}:mount", Path(target.container_path).expanduser()))
    docker_root = _clean(source_env.get("DOCKER_DATA_ROOT")) or _DEFAULT_DOCKER_DATA_ROOT
    paths.append(("docker:data_root", Path(docker_root)))
    paths.append(("docker:volumes", Path(docker_root) / "volumes"))
    for idx, raw in enumerate(_split_csv(source_env.get("DISK_PRESSURE_DOCKER_VOLUME_PATHS"))):
        paths.append((f"docker:volume_{idx}", Path(raw).expanduser()))
    return paths


def check_storage_placement(
    env: Mapping[str, str] | None = None,
    *,
    mountinfo_text: str | None = None,
    path_exists: Callable[[str], bool] | None = None,
) -> dict[str, Any]:
    source_env: Mapping[str, str] = env if env is not None else os.environ
    state: dict[str, Any] = {
        "checked": False,
        "production_like": _production_like(source_env),
        "ok": True,
        "notes": [],
        "warnings": [],
        "errors": [],
        "targets": [],
        "policy": {},
    }
    if not _checks_enabled(source_env):
        state["notes"].append("storage placement check skipped")
        return state

    allowed_fs_types = {
        item.lower()
        for item in (_split_csv(source_env.get("TRADING_ALLOWED_STORAGE_FS_TYPES")) or list(_DEFAULT_ALLOWED_FS_TYPES))
    }
    allowed_prefixes = _allowed_prefixes(source_env)
    forbidden_prefixes = (
        _split_csv(source_env.get("TRADING_FORBIDDEN_STORAGE_PREFIXES"))
        or list(_DEFAULT_FORBIDDEN_PREFIXES)
    )
    require_visible = _require_visible_host_paths(source_env)
    exists = path_exists or _default_path_exists
    mounts = _parse_mountinfo(mountinfo_text if mountinfo_text is not None else _read_mountinfo())
    errors: list[str] = []
    warnings: list[str] = []
    targets: list[dict[str, Any]] = []

    state["checked"] = True
    state["policy"] = {
        "allowed_fs_types": sorted(allowed_fs_types),
        "allowed_prefixes": allowed_prefixes,
        "forbidden_prefixes": forbidden_prefixes,
        "require_visible_host_paths": bool(require_visible),
        "require_non_root_mount": bool(require_visible),
    }

    for target in _target_specs(source_env):
        target_state: dict[str, Any] = {
            "name": target.name,
            "description": target.description,
            "host_env": target.host_env,
            "host_path": target.host_path,
            "container_path": target.container_path,
            "visible_in_runtime": target.visible_in_runtime,
            "ok": False,
            "reason": "",
        }
        targets.append(target_state)
        host_path = _clean(target.host_path)
        if not host_path:
            target_state["reason"] = "missing_host_path_env"
            errors.append(
                f"storage placement invalid target={target.name} reason=missing_host_path_env env={target.host_env}"
            )
            continue
        if not Path(host_path).expanduser().is_absolute():
            target_state["reason"] = "host_path_not_absolute"
            errors.append(
                f"storage placement invalid target={target.name} reason=host_path_not_absolute path={host_path}"
            )
            continue
        forbidden_path = next((prefix for prefix in forbidden_prefixes if _path_under(host_path, prefix)), "")
        if forbidden_path:
            target_state["reason"] = "forbidden_host_prefix"
            target_state["forbidden_prefix"] = forbidden_path
            errors.append(
                f"storage placement invalid target={target.name} reason=forbidden_host_prefix "
                f"path={host_path} prefix={forbidden_path}"
            )
            continue

        approved_prefix = next((prefix for prefix in allowed_prefixes if _path_under(host_path, prefix)), "")
        target_state["approved_prefix"] = approved_prefix
        if not approved_prefix:
            target_state["reason"] = "host_path_not_under_allowed_prefix"
            errors.append(
                f"storage placement invalid target={target.name} reason=host_path_not_under_allowed_prefix "
                f"path={host_path} allowed={','.join(allowed_prefixes)}"
            )
            continue

        visible_path, mount = _first_visible_mount(target=target, mounts=mounts, path_exists=exists)
        target_state["checked_path"] = visible_path
        target_state["mount"] = mount
        if mount:
            if require_visible and _root_mount(mount):
                target_state["reason"] = "root_backed_mount"
                errors.append(
                    f"storage placement invalid target={target.name} reason=root_backed_mount "
                    f"path={host_path} checked_path={visible_path}"
                )
                continue
            forbidden_mount = _mount_mentions_forbidden(mount, forbidden_prefixes)
            if forbidden_mount:
                target_state["reason"] = "forbidden_mount_source"
                target_state["forbidden_mount"] = forbidden_mount
                errors.append(
                    f"storage placement invalid target={target.name} reason=forbidden_mount_source {forbidden_mount}"
                )
                continue
            fs_type = str(mount.get("filesystem_type") or "").lower()
            if fs_type not in allowed_fs_types:
                target_state["reason"] = "mount_filesystem_not_allowed"
                target_state["filesystem_type"] = fs_type
                errors.append(
                    f"storage placement invalid target={target.name} reason=mount_filesystem_not_allowed "
                    f"fstype={fs_type or 'unknown'} path={visible_path}"
                )
                continue
            target_state["ok"] = True
            target_state["reason"] = "verified_mount"
            target_state["filesystem_type"] = fs_type
            continue

        if require_visible:
            target_state["reason"] = "host_path_not_visible"
            errors.append(
                f"storage placement invalid target={target.name} reason=host_path_not_visible path={host_path}"
            )
            continue
        target_state["ok"] = True
        target_state["reason"] = "approved_prefix_unverified"
        warnings.append(
            f"storage placement target={target.name} approved by host path prefix but mount not visible path={host_path}"
        )

    if errors:
        state["ok"] = False
    state["warnings"] = warnings
    state["errors"] = errors
    state["targets"] = targets
    if not errors:
        verified = sum(1 for item in targets if item.get("reason") == "verified_mount")
        prefix_only = sum(1 for item in targets if item.get("reason") == "approved_prefix_unverified")
        state["notes"].append(
            "storage placement ok "
            f"targets={len(targets)} verified_mounts={verified} prefix_only={prefix_only}"
        )
    return state


__all__ = ["check_storage_placement", "storage_pressure_paths"]
