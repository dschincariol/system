from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_engine_rl_does_not_import_broker_router():
    rl_root = REPO_ROOT / "engine" / "rl"
    offenders: list[str] = []
    for path in rl_root.glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = str(alias.name)
                    if name == "engine.execution.broker_router" or name.endswith(".broker_router"):
                        offenders.append(f"{path.name}:{name}")
            elif isinstance(node, ast.ImportFrom):
                module = str(node.module or "")
                if module == "engine.execution.broker_router" or module.endswith(".broker_router"):
                    offenders.append(f"{path.name}:{module}")
    assert offenders == []


def test_broker_router_rejects_rl_sourced_orders_before_routing():
    from engine.execution import broker_router

    result = broker_router.submit_order(
        {"symbol": "AAPL", "qty": 1.0, "source": "rl.portfolio_env"},
        dry_run=True,
        broker="sim",
    )
    assert result["ok"] is False
    assert result["status"] == "rl_source_forbidden"
    assert result["blocked_orders"][0]["source"] == "rl.portfolio_env"


def test_router_guardrail_is_present_in_source():
    source = (REPO_ROOT / "engine" / "execution" / "broker_router.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    function_names = {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef)
    }
    assert "_rl_source_block" in function_names
    # The guard checks for both `rl.` and `rl_` prefixes (the tuple-form
    # `startswith(("rl.", "rl_"))` was added in the L4-EXEC-01 fix to
    # close the mixed-case bypass). Either form is acceptable as long
    # as both prefixes are checked.
    has_dot = "rl." in source
    has_underscore = "rl_" in source
    assert has_dot and has_underscore, (
        "broker_router.py must reject both `rl.` and `rl_` source "
        "prefixes. Tuple-form `startswith((\"rl.\", \"rl_\"))` is "
        "the canonical pattern; legacy `startswith(\"rl.\")` alone "
        "is incomplete."
    )
