from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from engine.runtime import storage_placement
from engine.runtime.storage_placement import check_storage_placement


def _safe_env() -> dict[str, str]:
    return {
        "ENV": "prod",
        "PREFLIGHT_REQUIRE_ZFS_STORAGE": "1",
        "TRADING_ZFS_ROOT": "/zpool",
        "TRADING_ALLOWED_STORAGE_PREFIXES": "/zpool,/dbpool,/auxpool,/var/backups/trading",
        "TRADING_ALLOWED_STORAGE_FS_TYPES": "zfs",
        "TRADING_FORBIDDEN_STORAGE_PREFIXES": "/var/lib/docker,/var/lib/containerd",
        "TRADING_TIMESCALE_DATA": "/dbpool/trading/timescaledb/data",
        "TRADING_REDIS_DATA": "/auxpool/trading/redis",
        "TRADING_MINIO_DATA": "/auxpool/trading/minio",
        "TRADING_RUNTIME_DATA": "/auxpool/trading/runtime/data",
        "TRADING_RUNTIME_LOGS": "/auxpool/trading/runtime/logs",
        "TRADING_ARTIFACT_MIRROR": "/auxpool/trading/runtime/artifact_mirror",
        "TRADING_TRAINING_DATASETS": "/auxpool/trading/runtime/training_datasets",
        "TRADING_BACKUP_ROOT": "/var/backups/trading",
        "TRADING_BACKUP_WAL_DIR": "/var/backups/trading/wal",
    }


SAFE_MOUNTINFO = """
22 1 0:20 / / rw - ext4 /dev/root rw
33 22 0:33 / /zpool rw - zfs zpool rw
34 22 0:34 / /dbpool rw - zfs dbpool rw
35 22 0:35 / /auxpool rw - zfs auxpool rw
36 22 0:36 / /var/backups/trading rw - zfs zpool/trading-backups rw
""".strip()


class StoragePlacementTests(unittest.TestCase):
    def test_safe_zfs_host_paths_pass(self) -> None:
        def exists(path: str) -> bool:
            return (
                path == "/"
                or path.startswith("/zpool")
                or path.startswith("/dbpool")
                or path.startswith("/auxpool")
                or path.startswith("/var/backups/trading")
            )

        state = check_storage_placement(_safe_env(), mountinfo_text=SAFE_MOUNTINFO, path_exists=exists)

        self.assertTrue(state["checked"])
        self.assertTrue(state["ok"], state)
        self.assertEqual(state["errors"], [])
        self.assertTrue(any("storage placement ok" in note for note in state["notes"]))
        self.assertEqual(state["evidence_status"], "satisfied")
        self.assertEqual(state["summary"]["verified_mounts"], 10)
        self.assertEqual(state["summary"]["needs_evidence_targets"], 0)

    def test_production_missing_explicit_host_paths_fails_closed(self) -> None:
        state = check_storage_placement({"ENV": "prod"}, mountinfo_text=SAFE_MOUNTINFO, path_exists=lambda _path: False)

        self.assertFalse(state["ok"])
        self.assertEqual(state["evidence_status"], "needs_evidence")
        self.assertGreater(state["summary"]["needs_evidence_targets"], 0)
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

    def test_cli_exits_nonzero_for_bad_storage_preflight(self) -> None:
        with patch.object(
            storage_placement,
            "check_storage_placement",
            return_value={
                "ok": False,
                "errors": [
                    "storage placement invalid target=timescale_pgdata "
                    "reason=forbidden_host_prefix path=/var/lib/docker/volumes/timescaledb-data/_data"
                ],
            },
        ):
            self.assertEqual(storage_placement.main(["--json"]), 3)

    def test_cli_exits_zero_for_good_storage_preflight(self) -> None:
        with patch.object(
            storage_placement,
            "check_storage_placement",
            return_value={"ok": True, "notes": ["storage placement ok targets=10 verified_mounts=10 prefix_only=0"]},
        ):
            self.assertEqual(storage_placement.main(["--json"]), 0)

    def test_root_backed_docker_named_volume_mount_is_rejected(self) -> None:
        mountinfo = SAFE_MOUNTINFO + "\n35 22 0:35 /var/lib/docker/volumes/trading-data/_data /app/data rw - ext4 /dev/root rw"

        def exists(path: str) -> bool:
            return (
                path in {"/app/data"}
                or path.startswith("/zpool")
                or path.startswith("/dbpool")
                or path.startswith("/auxpool")
                or path.startswith("/var/backups/trading")
            )

        state = check_storage_placement(_safe_env(), mountinfo_text=mountinfo, path_exists=exists)

        self.assertFalse(state["ok"])
        self.assertTrue(
            any("target=runtime_data reason=forbidden_mount_source" in item for item in state["errors"]),
            state["errors"],
        )

    def test_root_mount_is_rejected_even_when_root_filesystem_is_zfs(self) -> None:
        mountinfo = "22 1 0:20 / / rw - zfs rpool/root rw"

        def exists(path: str) -> bool:
            return (
                path == "/"
                or path.startswith("/zpool")
                or path.startswith("/dbpool")
                or path.startswith("/auxpool")
                or path.startswith("/var/backups/trading")
            )

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
            return (
                path.startswith("/srv")
                or path.startswith("/zpool")
                or path.startswith("/dbpool")
                or path.startswith("/auxpool")
                or path.startswith("/var/backups/trading")
            )

        state = check_storage_placement(env, mountinfo_text=mountinfo, path_exists=exists)

        self.assertFalse(state["ok"])
        self.assertTrue(
            any("target=timescale_pgdata reason=host_path_not_under_allowed_prefix" in item for item in state["errors"]),
            state["errors"],
        )

    def test_timescale_compose_preflight_rejects_docker_backed_bind_mount(self) -> None:
        env = _safe_env()
        mountinfo = "\n".join(
            [
                "22 1 0:20 / / rw - ext4 /dev/root rw",
                "33 22 0:33 / /zpool rw - zfs zpool rw",
                "34 22 0:34 / /auxpool rw - zfs auxpool rw",
                (
                    "35 22 0:35 /var/lib/docker/volumes/timescaledb/_data "
                    "/dbpool/trading/timescaledb/data rw - ext4 /dev/root rw"
                ),
                "36 22 0:36 / /var/backups/trading rw - zfs zpool/trading-backups rw",
            ]
        )

        def exists(path: str) -> bool:
            return (
                path == "/"
                or path.startswith("/dbpool/trading/timescaledb/data")
                or path.startswith("/var/backups/trading")
                or path.startswith("/auxpool/trading/redis")
                or path.startswith("/auxpool/trading/minio")
                or path.startswith("/auxpool/trading/runtime")
            )

        state = check_storage_placement(env, mountinfo_text=mountinfo, path_exists=exists)

        self.assertFalse(state["ok"])
        rendered = "\n".join(state["errors"])
        self.assertIn("target=timescale_pgdata reason=forbidden_mount_source", rendered)
        self.assertIn("mount_root=/var/lib/docker/volumes/timescaledb/_data", rendered)

    def test_timescale_compose_preflight_accepts_real_zfs_bind_mount(self) -> None:
        env = _safe_env()
        mountinfo = "\n".join(
            [
                "22 1 0:20 / / rw - ext4 /dev/root rw",
                "33 22 0:33 / /zpool rw - zfs zpool rw",
                "35 22 0:35 / /dbpool/trading/timescaledb/data rw - zfs dbpool/trading/timescaledb/data rw",
                "36 22 0:36 / /auxpool/trading/redis rw - zfs auxpool/trading/redis rw",
                "37 22 0:37 / /auxpool/trading/minio rw - zfs auxpool/trading/minio rw",
                "38 22 0:38 / /auxpool/trading/runtime/data rw - zfs auxpool/trading/runtime/data rw",
                "39 22 0:39 / /auxpool/trading/runtime/logs rw - zfs auxpool/trading/runtime/logs rw",
                "40 22 0:40 / /auxpool/trading/runtime/artifact_mirror rw - zfs auxpool/trading/runtime/artifact_mirror rw",
                "41 22 0:41 / /auxpool/trading/runtime/training_datasets rw - zfs auxpool/trading/runtime/training_datasets rw",
                "42 22 0:42 / /var/backups/trading rw - zfs zpool/trading-backups rw",
            ]
        )

        def exists(path: str) -> bool:
            return (
                path == "/"
                or path.startswith("/zpool")
                or path.startswith("/dbpool")
                or path.startswith("/auxpool")
                or path.startswith("/var/backups/trading")
            )

        state = check_storage_placement(env, mountinfo_text=mountinfo, path_exists=exists)

        self.assertTrue(state["ok"], state)
        timescale = [item for item in state["targets"] if item.get("name") == "timescale_pgdata"][0]
        self.assertEqual(timescale["reason"], "verified_mount")
        self.assertEqual(timescale["evidence_status"], "satisfied")
        self.assertEqual(timescale["evidence_level"], "verified_mount")
        self.assertEqual(timescale["filesystem_type"], "zfs")
        self.assertEqual(timescale["mount_source"], "dbpool/trading/timescaledb/data")
        self.assertEqual(timescale["mount_point"], "/dbpool/trading/timescaledb/data")
        self.assertEqual(timescale["container_destination"], "/var/lib/postgresql/data")

    def test_health_storage_wal_guards_surface_required_blockers(self) -> None:
        from engine.runtime import health

        ctx = SimpleNamespace(out={})
        with (
            patch.dict("os.environ", {"ENGINE_MODE": "live"}, clear=False),
            patch(
                "engine.runtime.storage_placement.check_storage_placement",
                return_value={"checked": True, "ok": False, "warnings": [], "errors": ["storage placement invalid"]},
            ),
            patch(
                "engine.runtime.backup_evidence.wal_archiver_runtime_snapshot",
                return_value={
                    "required": True,
                    "ok": False,
                    "blockers": ["wal_archiver_last_archive_missing"],
                    "warnings": [],
                },
            ),
            patch(
                "engine.runtime.backup_evidence.pg_wal_disk_risk_snapshot",
                return_value={"required": True, "ok": True, "blockers": [], "warnings": []},
            ),
        ):
            health._check_storage_wal_guards(ctx)

        state = ctx.out["storage_wal_guards"]
        self.assertFalse(state["ok"])
        self.assertTrue(state["required"])
        self.assertIn("storage_placement_invalid", state["blockers"])
        self.assertIn("wal_archiver_last_archive_missing", state["blockers"])


if __name__ == "__main__":
    unittest.main()
