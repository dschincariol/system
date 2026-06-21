from __future__ import annotations

import unittest

from engine.runtime.storage_placement import check_storage_placement


def _safe_env() -> dict[str, str]:
    return {
        "ENV": "prod",
        "PREFLIGHT_REQUIRE_ZFS_STORAGE": "1",
        "TRADING_ZFS_ROOT": "/zpool",
        "TRADING_ALLOWED_STORAGE_PREFIXES": "/zpool,/var/backups/trading",
        "TRADING_ALLOWED_STORAGE_FS_TYPES": "zfs",
        "TRADING_FORBIDDEN_STORAGE_PREFIXES": "/var/lib/docker,/var/lib/containerd",
        "TRADING_TIMESCALE_DATA": "/zpool/trading/timescaledb/data",
        "TRADING_REDIS_DATA": "/zpool/trading/redis/data",
        "TRADING_MINIO_DATA": "/zpool/trading/minio/data",
        "TRADING_RUNTIME_DATA": "/zpool/trading/runtime/data",
        "TRADING_RUNTIME_LOGS": "/zpool/trading/runtime/logs",
        "TRADING_ARTIFACT_MIRROR": "/zpool/trading/runtime/artifact_mirror",
        "TRADING_TRAINING_DATASETS": "/zpool/trading/runtime/training_datasets",
        "TRADING_BACKUP_ROOT": "/var/backups/trading",
        "TRADING_BACKUP_WAL_DIR": "/var/backups/trading/wal",
    }


SAFE_MOUNTINFO = """
22 1 0:20 / / rw - ext4 /dev/root rw
33 22 0:33 / /zpool rw - zfs zpool rw
34 22 0:34 / /var/backups/trading rw - zfs zpool/backups/trading rw
""".strip()


class StoragePlacementTests(unittest.TestCase):
    def test_safe_zfs_host_paths_pass(self) -> None:
        def exists(path: str) -> bool:
            return path == "/" or path.startswith("/zpool") or path.startswith("/var/backups/trading")

        state = check_storage_placement(_safe_env(), mountinfo_text=SAFE_MOUNTINFO, path_exists=exists)

        self.assertTrue(state["checked"])
        self.assertTrue(state["ok"], state)
        self.assertEqual(state["errors"], [])
        self.assertTrue(any("storage placement ok" in note for note in state["notes"]))

    def test_production_missing_explicit_host_paths_fails_closed(self) -> None:
        state = check_storage_placement({"ENV": "prod"}, mountinfo_text=SAFE_MOUNTINFO, path_exists=lambda _path: False)

        self.assertFalse(state["ok"])
        rendered = "\n".join(state["errors"])
        self.assertIn("target=timescale_pgdata reason=missing_host_path_env", rendered)
        self.assertIn("target=runtime_logs reason=missing_host_path_env", rendered)

    def test_production_rejects_prefix_only_unverified_storage(self) -> None:
        env = _safe_env()
        env["PREFLIGHT_STORAGE_REQUIRE_VISIBLE_HOST_PATHS"] = "0"

        state = check_storage_placement(env, mountinfo_text=SAFE_MOUNTINFO, path_exists=lambda _path: False)

        self.assertFalse(state["ok"])
        self.assertTrue(state["policy"]["require_visible_host_paths"])
        self.assertTrue(
            any("target=timescale_pgdata reason=host_path_not_visible" in item for item in state["errors"]),
            state["errors"],
        )

    def test_prod_lock_requires_visible_storage_even_without_env_prod(self) -> None:
        env = _safe_env()
        env.pop("ENV")
        env["PROD_LOCK"] = "1"

        state = check_storage_placement(env, mountinfo_text=SAFE_MOUNTINFO, path_exists=lambda _path: False)

        self.assertFalse(state["ok"])
        self.assertTrue(state["production_like"])
        self.assertTrue(state["policy"]["require_visible_host_paths"])

    def test_non_production_can_use_prefix_only_when_explicitly_checked(self) -> None:
        env = _safe_env()
        env["ENV"] = "dev"
        env["PROD_LOCK"] = "0"
        env["PREFLIGHT_REQUIRE_ZFS_STORAGE"] = "1"

        state = check_storage_placement(env, mountinfo_text="", path_exists=lambda _path: False)

        self.assertTrue(state["ok"], state)
        self.assertFalse(state["production_like"])
        self.assertFalse(state["policy"]["require_visible_host_paths"])
        self.assertTrue(state["warnings"])
        self.assertTrue(all(item.get("reason") == "approved_prefix_unverified" for item in state["targets"]))

    def test_var_lib_docker_host_path_is_rejected(self) -> None:
        env = _safe_env()
        env["TRADING_TIMESCALE_DATA"] = "/var/lib/docker/volumes/timescaledb-data/_data"

        state = check_storage_placement(env, mountinfo_text=SAFE_MOUNTINFO, path_exists=lambda _path: False)

        self.assertFalse(state["ok"])
        self.assertTrue(
            any("target=timescale_pgdata reason=forbidden_host_prefix" in item for item in state["errors"]),
            state["errors"],
        )

    def test_root_backed_docker_named_volume_mount_is_rejected(self) -> None:
        mountinfo = SAFE_MOUNTINFO + "\n35 22 0:35 /var/lib/docker/volumes/trading-data/_data /app/data rw - ext4 /dev/root rw"

        def exists(path: str) -> bool:
            return path in {"/app/data"} or path.startswith("/zpool") or path.startswith("/var/backups/trading")

        state = check_storage_placement(_safe_env(), mountinfo_text=mountinfo, path_exists=exists)

        self.assertFalse(state["ok"])
        self.assertTrue(
            any("target=runtime_data reason=forbidden_mount_source" in item for item in state["errors"]),
            state["errors"],
        )

    def test_root_mount_is_rejected_even_when_root_filesystem_is_zfs(self) -> None:
        mountinfo = "22 1 0:20 / / rw - zfs rpool/root rw"

        def exists(path: str) -> bool:
            return path == "/" or path.startswith("/zpool") or path.startswith("/var/backups/trading")

        state = check_storage_placement(_safe_env(), mountinfo_text=mountinfo, path_exists=exists)

        self.assertFalse(state["ok"])
        self.assertTrue(
            any("target=timescale_pgdata reason=root_backed_mount" in item for item in state["errors"]),
            state["errors"],
        )

    def test_mounted_zfs_path_must_still_be_under_allowed_storage_prefix(self) -> None:
        env = _safe_env()
        env["TRADING_TIMESCALE_DATA"] = "/srv/timescale/data"
        mountinfo = SAFE_MOUNTINFO + "\n35 22 0:35 / /srv rw - zfs zpool/srv rw"

        def exists(path: str) -> bool:
            return path.startswith("/srv") or path.startswith("/zpool") or path.startswith("/var/backups/trading")

        state = check_storage_placement(env, mountinfo_text=mountinfo, path_exists=exists)

        self.assertFalse(state["ok"])
        self.assertTrue(
            any("target=timescale_pgdata reason=host_path_not_under_allowed_prefix" in item for item in state["errors"]),
            state["errors"],
        )


if __name__ == "__main__":
    unittest.main()
