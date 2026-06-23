from __future__ import annotations

import stat
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "ops" / "server" / "disk_remediation.sh"
RUNBOOK = ROOT / "docs" / "DISK_RETENTION_RUNBOOK.md"
GLOSSARY = ROOT / "docs" / "REFERENCE_CONFIGURATION_GLOSSARY.md"
BOOTSTRAP = ROOT / "ops" / "server" / "bootstrap.sh"
VERIFY = ROOT / "ops" / "server" / "verify.sh"
WORKFLOW = ROOT / ".github" / "workflows" / "validate.yml"
OLD_LOCAL_PATH = "/home/david/gitsandbox" + "/disk" + "-remediation.sh"


def test_disk_remediation_script_lives_under_repo_ops_server() -> None:
    assert SCRIPT.exists()
    assert SCRIPT.stat().st_mode & stat.S_IXUSR

    text = SCRIPT.read_text(encoding="utf-8")
    assert OLD_LOCAL_PATH not in text
    assert 'SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"' in text
    assert 'REPO_ROOT="${TRADING_REPO_ROOT:-$DEFAULT_REPO_ROOT}"' in text
    assert '"${DEFAULT_REPO_ROOT}/app/deploy/compose/docker-compose.stack.yml"' in text


def test_disk_retention_docs_use_deployed_remediation_path() -> None:
    runbook = RUNBOOK.read_text(encoding="utf-8")
    glossary = GLOSSARY.read_text(encoding="utf-8")

    assert OLD_LOCAL_PATH not in runbook
    assert "/opt/trading/ops/server/disk_remediation.sh" in runbook
    assert "ops/server/disk_remediation.sh" in glossary
    assert OLD_LOCAL_PATH not in glossary


def test_bootstrap_installs_and_verify_checks_server_ops_scripts() -> None:
    bootstrap = BOOTSTRAP.read_text(encoding="utf-8")
    verify = VERIFY.read_text(encoding="utf-8")

    assert 'SERVER_SCRIPT_SRC_DIR="${REPO_ROOT}/ops/server"' in bootstrap
    assert 'SERVER_SCRIPT_DST_DIR="${INSTALL_ROOT}/ops/server"' in bootstrap
    assert "install_server_ops_scripts()" in bootstrap
    assert "disk_remediation.sh" in bootstrap
    assert "provision_storage_pools.sh" in bootstrap
    assert "zfs_tuning.sh" in bootstrap
    assert "install_server_ops_scripts" in bootstrap.split("main() {", 1)[1]

    assert 'SERVER_SCRIPT_DIR="${TRADING_SERVER_SCRIPT_DIR:-${INSTALL_ROOT}/ops/server}"' in verify
    assert "check_server_ops_assets()" in verify
    assert "disk_remediation.sh provision_storage_pools.sh zfs_tuning.sh" in verify
    assert 'bash -n "${SERVER_SCRIPT_DIR}/${script}"' in verify


def test_verify_accepts_root_owned_runtime_config_directory() -> None:
    verify = VERIFY.read_text(encoding="utf-8")

    assert 'check_dir_owner_mode "$ETC_DIR" root "$TRADING_GROUP" 750' in verify
    assert '"$ETC_DIR"' not in verify.split('for dir in \\', 1)[1].split("do", 1)[0]


def test_validate_workflow_runs_disk_remediation_ops_tests() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "Run ops/server pytest and shell test gate" in workflow
    assert "python -m pytest -q -m \"not requires_rocm\" tests/ops" in workflow
    assert "find tests/ops -maxdepth 1 -type f -name '*.sh' | sort" in workflow
    assert 'bash "${test_script}"' in workflow
    assert "bash tests/ops/test_zfs_tuning.sh" not in workflow
    assert "bash tests/ops/test_disk_remediation_relocate_docker.sh" not in workflow
