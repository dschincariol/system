from __future__ import annotations

import base64
import os
from pathlib import Path
from unittest.mock import patch

from engine.runtime.platform import resolve_runtime_path, resolve_runtime_paths


def test_relative_runtime_paths_resolve_under_project_root(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    env = {
        "DB_PATH": "data/runtime/trading.db",
        "TRADING_DATA": "data/runtime",
        "TRADING_LOGS": "logs",
        "SQLITE_LIVENESS_DB_PATH": "data/runtime/trading.liveness.sqlite",
    }

    resolved = resolve_runtime_paths(env, project_root=project_root)

    assert Path(env["DB_PATH"]) == project_root / "data" / "runtime" / "trading.db"
    assert Path(env["TRADING_DATA"]) == project_root / "data" / "runtime"
    assert Path(env["TRADING_LOGS"]) == project_root / "logs"
    assert Path(env["SQLITE_LIVENESS_DB_PATH"]) == project_root / "data" / "runtime" / "trading.liveness.sqlite"
    assert resolved == env


def test_already_absolute_runtime_path_passes_through(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.db"
    env = {"DB_PATH": str(db_path)}

    resolve_runtime_paths(env, project_root=tmp_path / "repo")

    assert env["DB_PATH"] == str(db_path.resolve(strict=False))


def test_home_expansion_is_handled(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "home"
    monkeypatch.setenv("HOME", str(home))
    env = {"DB_PATH": "~/runtime.db"}

    resolve_runtime_paths(env, project_root=tmp_path / "repo")

    assert Path(env["DB_PATH"]) == home / "runtime.db"


def test_pwd_anchored_profile_value_is_expanded(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    env = {
        "PWD": str(project_root),
        "DB_PATH": "${PWD}/data/runtime/trading.db",
    }

    resolve_runtime_paths(env, project_root=tmp_path / "other")

    assert Path(env["DB_PATH"]) == project_root / "data" / "runtime" / "trading.db"


def test_resolved_db_path_satisfies_strict_config_schema_absolute_check(tmp_path: Path) -> None:
    key_file = tmp_path / "master_key"
    key_file.write_text(base64.b64encode(bytes(range(32))).decode("ascii") + "\n", encoding="ascii")
    key_file.chmod(0o600)
    env = {
        "ENV": "prod",
        "ENGINE_MODE": "safe",
        "ALLOW_TRAINING": "0",
        "DB_PATH": "data/runtime/trading.db",
        "DATA_SOURCE_MASTER_KEY_FILE": str(key_file),
    }
    resolve_runtime_paths(env, project_root=tmp_path / "repo")

    from engine.runtime.config_schema import load_runtime_config

    with patch.dict(os.environ, env, clear=True):
        config = load_runtime_config()

    assert Path(config.db_path).is_absolute()
    assert Path(config.db_path) == tmp_path / "repo" / "data" / "runtime" / "trading.db"


def test_empty_optional_liveness_path_stays_empty(tmp_path: Path) -> None:
    env = {"SQLITE_LIVENESS_DB_PATH": ""}

    resolve_runtime_paths(env, project_root=tmp_path / "repo")

    assert env["SQLITE_LIVENESS_DB_PATH"] == ""
    assert resolve_runtime_path("", project_root=tmp_path / "repo") == ""
