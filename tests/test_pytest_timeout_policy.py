from __future__ import annotations

import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_pyproject_declares_canonical_pytest_timeout_policy() -> None:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    options = data["tool"]["pytest"]["ini_options"]

    assert options["required_plugins"] == ["pytest-timeout>=2.4"]
    assert options["timeout"] == "120"
    assert options["timeout_method"] == "thread"
    assert options["timeout_func_only"] is False


def test_pytest_loads_timeout_policy(pytestconfig) -> None:
    plugin_names = {
        str(getattr(plugin, "__name__", ""))
        for plugin in pytestconfig.pluginmanager.get_plugins()
    }

    assert "pytest_timeout" in plugin_names
    assert pytestconfig.getini("timeout") == "120"
    assert pytestconfig.getini("timeout_method") == "thread"
    assert pytestconfig.getini("timeout_func_only") is False
