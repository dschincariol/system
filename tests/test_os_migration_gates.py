from __future__ import annotations

import importlib.util
import re
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
PREFLIGHT_PATH = REPO_ROOT / "ops" / "server" / "os_migration_preflight.py"
POSTFLIGHT_PATH = REPO_ROOT / "ops" / "server" / "os_migration_postflight.py"
RUNBOOK_PATH = REPO_ROOT / "docs" / "OS_MIGRATION_RUNBOOK.md"
SYSTEMD_UNIT_DIR = REPO_ROOT / "ops" / "server" / "systemd"
CI_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "validate.yml"
BACKUP_EVIDENCE_SCRIPT_PATH = REPO_ROOT / "ops" / "backup" / "backup_restore_evidence.sh"
BACKUP_EVIDENCE_INSTALLER_PATH = REPO_ROOT / "ops" / "server" / "install_backup_evidence_gate.sh"
RUNBOOK_UNIT_RE = re.compile(r"\b(?:trading|pgbouncer)[A-Za-z0-9_.@-]*\.(?:service|target|timer)\b")


def shipped_systemd_units() -> set[str]:
    return {
        path.name
        for path in SYSTEMD_UNIT_DIR.iterdir()
        if path.is_file() and path.suffix in {".service", ".target", ".timer"}
    }


def load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load module from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class OSMigrationGateTests(unittest.TestCase):
    def test_scripts_exist_are_executable_and_have_help(self) -> None:
        for path in (PREFLIGHT_PATH, POSTFLIGHT_PATH):
            with self.subTest(path=path):
                self.assertTrue(path.exists())
                mode = path.stat().st_mode
                self.assertTrue(mode & stat.S_IXUSR, f"{path} is not executable by owner")
                completed = subprocess.run(
                    [sys.executable, str(path), "--help"],
                    cwd=REPO_ROOT,
                    check=False,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    timeout=20,
                )
                self.assertEqual(completed.returncode, 0, completed.stderr)
                self.assertIn("--output", completed.stdout)

    def test_scripts_do_not_encode_upgrade_or_mutation_commands(self) -> None:
        forbidden = (
            "do-release-upgrade",
            "apt install",
            "apt upgrade",
            "apt full-upgrade",
            "zfs snapshot",
            "zfs rollback",
            "zpool upgrade",
            "docker compose up",
            "docker compose down",
            "systemctl start",
            "systemctl stop",
            "systemctl restart",
        )
        for path in (PREFLIGHT_PATH, POSTFLIGHT_PATH):
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path):
                for needle in forbidden:
                    self.assertNotIn(needle, text)
                self.assertIn('"read_only": True', text)
                self.assertIn('"mutating_actions": []', text)

    def test_preflight_apt_source_parser_classifies_third_party_sources(self) -> None:
        preflight = load_module(PREFLIGHT_PATH, "os_migration_preflight_test")
        with tempfile.TemporaryDirectory() as tmp:
            sources = Path(tmp) / "sources.list"
            sources.write_text(
                "\n".join(
                    [
                        "deb http://archive.ubuntu.com/ubuntu questing main restricted",
                        "deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu questing stable",
                    ]
                ),
                encoding="utf-8",
            )
            deb822 = Path(tmp) / "rocm.sources"
            deb822.write_text(
                "\n".join(
                    [
                        "Types: deb",
                        "URIs: https://repo.radeon.com/rocm/apt/7.2.4",
                        "Suites: noble",
                        "Components: main",
                    ]
                ),
                encoding="utf-8",
            )

            result = preflight.collect_apt_sources([sources, deb822])

        self.assertEqual(result["third_party_count"], 2)
        classifications = {entry["uri"]: entry["classification"] for entry in result["entries"]}
        self.assertEqual(classifications["http://archive.ubuntu.com/ubuntu"], "ubuntu")
        self.assertEqual(classifications["https://download.docker.com/linux/ubuntu"], "third_party")
        self.assertEqual(classifications["https://repo.radeon.com/rocm/apt/7.2.4"], "third_party")

    def test_preflight_docker_inspect_sanitizer_drops_env_values(self) -> None:
        preflight = load_module(PREFLIGHT_PATH, "os_migration_preflight_sanitize_test")
        sanitized = preflight.sanitize_container_inspect(
            {
                "Id": "abc",
                "Name": "/trading-runtime",
                "Image": "sha256:image",
                "Config": {
                    "Image": "runtime:local",
                    "Env": ["BROKER_SECRET=should-not-appear"],
                    "Labels": {"com.docker.compose.service": "runtime"},
                },
                "State": {"Status": "running", "Health": {"Status": "healthy"}},
                "Mounts": [{"Type": "bind", "Source": "/zpool/trading/runtime/data", "Destination": "/app/data"}],
            }
        )

        self.assertNotIn("Env", sanitized["Config"])
        self.assertNotIn("should-not-appear", repr(sanitized))
        self.assertEqual(sanitized["Config"]["Image"], "runtime:local")
        self.assertEqual(sanitized["Mounts"][0]["Source"], "/zpool/trading/runtime/data")

    def test_postflight_kernel_gate_distinguishes_target_lts(self) -> None:
        postflight = load_module(POSTFLIGHT_PATH, "os_migration_postflight_test")
        with (
            patch.object(postflight, "read_os_release", return_value={"VERSION_CODENAME": "resolute"}),
            patch.object(postflight.platform, "release", return_value="7.0.0-24-generic"),
        ):
            result = postflight.check_os_and_kernel("resolute")
        self.assertEqual(result["status"], "PASS")

        with (
            patch.object(postflight, "read_os_release", return_value={"VERSION_CODENAME": "resolute"}),
            patch.object(postflight.platform, "release", return_value="6.17.0-22-generic"),
        ):
            result = postflight.check_os_and_kernel("resolute")
        self.assertEqual(result["status"], "FAIL")

    def test_postflight_rocm_is_pass_when_not_required_and_absent(self) -> None:
        postflight = load_module(POSTFLIGHT_PATH, "os_migration_postflight_rocm_test")
        with patch.object(postflight, "rocm_marker_present", return_value=False):
            result = postflight.check_rocm(False, "gfx1151")
        self.assertEqual(result["status"], "PASS")
        self.assertIn("not required", result["detail"])

    def test_postflight_rocm_marker_without_requirement_is_not_enforced(self) -> None:
        postflight = load_module(POSTFLIGHT_PATH, "os_migration_postflight_rocm_optional_test")
        with (
            patch.object(postflight, "rocm_marker_present", return_value=True),
            patch.object(postflight.shutil, "which", return_value=None),
        ):
            result = postflight.check_rocm(False, "gfx1151")
        self.assertEqual(result["status"], "PASS")
        self.assertIn("not required", result["detail"])

    def test_backup_evidence_scripts_grant_operator_read_access(self) -> None:
        installer = BACKUP_EVIDENCE_INSTALLER_PATH.read_text(encoding="utf-8")
        evidence_script = BACKUP_EVIDENCE_SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertIn("TRADING_OPERATOR_USER", installer)
        self.assertIn("resolve_compose_path", installer)
        self.assertIn("grant_operator_backup_evidence_access", installer)
        self.assertIn('setfacl -m "u:${TRADING_OPERATOR_USER}:--x" "$BACKUP_ROOT"', installer)
        self.assertIn('chmod o+x "$BACKUP_ROOT"', installer)
        self.assertIn('chmod o+rx "$BACKUP_EVIDENCE_DIR"', installer)
        self.assertIn("TS_BACKUP_EVIDENCE_OPERATOR_USER", installer)
        self.assertIn("grant_evidence_file_read_access", evidence_script)
        self.assertIn('setfacl -m "u:${user}:r--" "$path"', evidence_script)
        self.assertIn("grant_evidence_operator_access", evidence_script)
        self.assertIn('setfacl -m "u:${user}:--x" "$backup_root"', evidence_script)
        self.assertIn('chmod o+x "$backup_root"', evidence_script)
        self.assertIn('chmod o+rx "$evidence_dir"', evidence_script)
        self.assertIn("mode_matches_wal_target_path", evidence_script)
        self.assertIn('mode_matches "$actual" "$(mode_with_other_execute "$expected")"', evidence_script)
        self.assertIn("grant_evidence_operator_access\n  wal_target_verified_at=", evidence_script)
        self.assertIn('chmod o+r "$path"', evidence_script)

    def test_postflight_backup_evidence_requires_wal_archive_target(self) -> None:
        postflight = POSTFLIGHT_PATH.read_text(encoding="utf-8")

        self.assertIn('"wal_archive_target"', postflight)

    def test_preflight_systemd_inventory_matches_shipped_units(self) -> None:
        preflight = load_module(PREFLIGHT_PATH, "os_migration_preflight_units_test")
        shipped = shipped_systemd_units()
        self.assertTrue(shipped)
        self.assertEqual(set(preflight.TRADING_UNITS), shipped)

    def test_postflight_backup_timers_are_shipped_timer_units(self) -> None:
        postflight = load_module(POSTFLIGHT_PATH, "os_migration_postflight_timers_test")
        shipped_timers = {unit for unit in shipped_systemd_units() if unit.endswith(".timer")}
        self.assertTrue(set(postflight.BACKUP_TIMERS).issubset(shipped_timers))

    def test_runbook_systemd_units_are_shipped_units(self) -> None:
        text = RUNBOOK_PATH.read_text(encoding="utf-8")
        referenced = set(RUNBOOK_UNIT_RE.findall(text))
        missing = referenced - shipped_systemd_units()
        self.assertFalse(missing, f"runbook references missing systemd units: {sorted(missing)}")

    def test_ci_runs_os_migration_gate_tests(self) -> None:
        workflow = CI_WORKFLOW_PATH.read_text(encoding="utf-8")
        self.assertIn("tests/test_os_migration_gates.py", workflow)

    def test_runbook_contains_required_operator_gates(self) -> None:
        text = RUNBOOK_PATH.read_text(encoding="utf-8")
        required = [
            "os_migration_preflight.py",
            "os_migration_postflight.py",
            "25.10",
            "26.04 LTS",
            "24.04 LTS",
            "trading-backup-evidence.timer",
            "docker compose",
            "zfs snapshot -r",
            "zfs rollback",
            "do-release-upgrade",
            "zpool upgrade",
            "gfx1151",
            "PASS",
            "NO-GO",
        ]
        for needle in required:
            with self.subTest(needle=needle):
                self.assertIn(needle, text)


if __name__ == "__main__":
    unittest.main()
