from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from engine.strategy.feature_registry import expected_columns

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_expected_columns_order_is_stable_across_repeated_calls() -> None:
    first = expected_columns()
    assert first
    for _ in range(100):
        assert expected_columns() == first


def _write_test_secrets(secret_dir) -> None:
    secret_dir.mkdir(parents=True, exist_ok=True)
    for name, value in {
        "master_key": "test-master-key",
        "pg_password_app": "test-app-password",
        "pg_password_ingest": "test-ingest-password",
        "pg_password_reader": "test-reader-password",
    }.items():
        (secret_dir / name).write_text(value, encoding="utf-8")


def _json_line(stdout: str) -> list[str]:
    for line in reversed(str(stdout or "").splitlines()):
        text = line.strip()
        if text.startswith("["):
            return list(json.loads(text))
    raise AssertionError(f"no JSON list in subprocess stdout: {stdout!r}")


def test_expected_columns_order_is_stable_across_hash_seeds(tmp_path) -> None:
    secret_dir = tmp_path / "secrets"
    _write_test_secrets(secret_dir)
    script = (
        "import json; "
        "from engine.strategy.feature_registry import expected_columns; "
        "print(json.dumps(expected_columns()))"
    )
    outputs = []
    for seed in ("1", "77", "12345"):
        env = dict(os.environ)
        env.update(
            {
                "PYTHONHASHSEED": seed,
                "TS_SECRETS_PROVIDER": "plaintext",
                "TS_DEV_SECRETS_DIR": str(secret_dir),
                "TS_PG_DSN": "host=127.0.0.1 port=1 dbname=postgres user=postgres password=test",
                "DB_PATH": str(tmp_path / f"runtime_{seed}.db"),
                "TRADING_FAILURE_DIAGNOSTICS_PERSIST": "0",
            }
        )
        result = subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(REPO_ROOT),
            env=env,
            text=True,
            capture_output=True,
            check=True,
        )
        outputs.append(_json_line(result.stdout))

    assert outputs[0]
    assert outputs[1:] == [outputs[0], outputs[0]]
