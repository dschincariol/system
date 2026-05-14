from __future__ import annotations

import importlib
import sys
import sqlite3
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def test_shadow_model_cannot_skip_challenger_before_champion() -> None:
    (champion_manager,) = _reload_modules("engine.strategy.champion_manager")
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE champion_assignments (
            scope TEXT NOT NULL,
            symbol TEXT NOT NULL,
            horizon_s INTEGER NOT NULL,
            model_name TEXT NOT NULL,
            challenger_name TEXT NOT NULL,
            regime TEXT NOT NULL,
            state TEXT NOT NULL,
            assigned_ts_ms INTEGER NOT NULL,
            updated_ts_ms INTEGER NOT NULL,
            meta_json TEXT,
            PRIMARY KEY(scope, symbol, horizon_s)
        )
        """
    )
    con.execute("BEGIN")

    kwargs = {
        "con": con,
        "scope": "global",
        "symbol": "AAPL",
        "model_name": "state_machine_AAPL_1700000000008_abcdef9",
        "horizon_s": 300,
        "challenger_name": "",
        "regime": "global",
    }

    shadow = champion_manager.set_champion_assignment(**kwargs, state="shadow")
    assert shadow["state"] == "shadow"

    with pytest.raises(champion_manager.IllegalChampionTransition):
        champion_manager.set_champion_assignment(**kwargs, state="champion")

    challenger = champion_manager.set_champion_assignment(**kwargs, state="challenger")
    assert challenger["state"] == "challenger"

    champion = champion_manager.set_champion_assignment(**kwargs, state="champion")
    assert champion["state"] == "champion"
    con.commit()
    con.close()
