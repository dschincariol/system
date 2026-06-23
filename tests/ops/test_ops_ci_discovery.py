from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
OPS_DIR = REPO_ROOT / "tests" / "ops"
CI_WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "validate.yml"
OPS_GATE_STEP_NAME = "Run ops/server pytest and shell test gate"

REQUIRED_OPS_TEST_SURFACE = {
    "rocm": {"test_rocm_acceleration.py"},
    "idle_nvme": {"test_idle_nvme_assessment.py", "test_idle_nvme_reclaim_guards.sh"},
    "os_migration": {"test_os_migration_gates.py"},
    "zfs_tuning": {"test_zfs_tuning.sh"},
    "cpu_power": {"test_cpu_power_policy.sh"},
    "disk_remediation_relocate": {"test_disk_remediation_relocate_docker.sh"},
    "storage_provisioning": {"test_provision_storage_pools.sh"},
    "backup_retention": {"test_backup_prune_retention.sh"},
    "hygiene": {"test_repo_artifact_hygiene.py"},
}


def _ops_python_tests() -> list[str]:
    return sorted(path.name for path in OPS_DIR.glob("test_*.py") if path.is_file())


def _ops_shell_tests() -> list[str]:
    return sorted(path.name for path in OPS_DIR.glob("test_*.sh") if path.is_file())


def _ops_gate_step() -> str:
    workflow = CI_WORKFLOW_PATH.read_text(encoding="utf-8")
    marker = f"      - name: {OPS_GATE_STEP_NAME}\n"
    start = workflow.find(marker)
    assert start != -1, f"{OPS_GATE_STEP_NAME!r} step is missing from validate workflow"
    end = workflow.find("\n      - name:", start + len(marker))
    return workflow[start:] if end == -1 else workflow[start:end]


def test_ops_test_inventory_includes_required_residual_surface() -> None:
    actual = set(_ops_python_tests()) | set(_ops_shell_tests())
    missing = {
        category: sorted(required - actual)
        for category, required in REQUIRED_OPS_TEST_SURFACE.items()
        if required - actual
    }

    assert not missing


def test_validate_workflow_runs_hygiene_and_directory_based_ops_pytest_gate() -> None:
    workflow = CI_WORKFLOW_PATH.read_text(encoding="utf-8")
    step = _ops_gate_step()

    assert "python tools/check_repo_artifact_hygiene.py" in workflow
    assert 'python -m pytest -q -m "not requires_rocm" tests/ops' in step
    assert not re.search(r"tests/ops/test_[A-Za-z0-9_]+\.py", step)


def test_ci_pytest_command_collects_every_python_ops_test() -> None:
    completed = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-m", "not requires_rocm", "tests/ops"],
        cwd=REPO_ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    output = completed.stdout.replace("\\", "/")

    assert completed.returncode == 0, completed.stderr
    for test_file in _ops_python_tests():
        assert f"tests/ops/{test_file}" in output


def test_ci_shell_find_discovers_every_shell_ops_test() -> None:
    step = _ops_gate_step()
    completed = subprocess.run(
        ["find", "tests/ops", "-maxdepth", "1", "-type", "f", "-name", "*.sh"],
        cwd=REPO_ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
    )
    discovered = sorted(Path(line).name for line in completed.stdout.splitlines() if line)
    actual = _ops_shell_tests()

    assert completed.returncode == 0, completed.stderr
    assert actual
    assert discovered == actual
    assert "find tests/ops -maxdepth 1 -type f -name '*.sh' | sort" in step
    assert 'if [ "${#ops_shell_tests[@]}" -eq 0 ]; then' in step
    assert 'for test_script in "${ops_shell_tests[@]}"; do' in step
