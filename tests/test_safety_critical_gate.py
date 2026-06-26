from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

EXPECTED_SAFETY_CRITICAL_SUITES = {
    "tests/test_kill_switch_regressions.py",
    "tests/test_broker_router_dry_run_gates.py",
    "tests/test_broker_order_idempotency_regressions.py",
    "tests/test_broker_apply_orders_modes.py",
    "tests/test_drawdown_fail_closed.py",
    "tests/test_risk_invariants_property.py",
    "tests/test_real_capital_safety_e2e.py",
    "tests/test_position_reconcile_safety.py",
    "tests/test_live_prelive_reconcile_policy.py",
}


def _safety_job_sources() -> set[str]:
    workflow = (ROOT / ".github/workflows/validate.yml").read_text(encoding="utf-8")
    lines = workflow.splitlines()
    in_job = False
    job_lines: list[str] = []
    for line in lines:
        if line == "  safety-critical-money-path:":
            in_job = True
            continue
        if in_job and re.match(r"^  [A-Za-z0-9_-]+:", line):
            break
        if in_job:
            job_lines.append(line)
    return set(re.findall(r"tests/test_[A-Za-z0-9_]+\.py", "\n".join(job_lines)))


def _node_contains_safety_marker(node: ast.AST) -> bool:
    if isinstance(node, ast.Attribute) and node.attr == "safety_critical":
        return True
    return any(_node_contains_safety_marker(child) for child in ast.iter_child_nodes(node))


def _has_module_level_safety_marker(path: Path) -> bool:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for statement in module.body:
        if not isinstance(statement, ast.Assign):
            continue
        if not any(isinstance(target, ast.Name) and target.id == "pytestmark" for target in statement.targets):
            continue
        if _node_contains_safety_marker(statement.value):
            return True
    return False


def test_safety_critical_marker_declared_in_pyproject() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    markers = data["tool"]["pytest"]["ini_options"]["markers"]

    assert any(marker.startswith("safety_critical:") for marker in markers)


def test_safety_critical_workflow_names_all_required_suites() -> None:
    assert _safety_job_sources() == EXPECTED_SAFETY_CRITICAL_SUITES


def test_safety_critical_suites_exist_and_use_module_level_marker() -> None:
    for relative_path in sorted(EXPECTED_SAFETY_CRITICAL_SUITES):
        path = ROOT / relative_path
        assert path.exists(), relative_path
        assert _has_module_level_safety_marker(path), relative_path
