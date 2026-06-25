from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _run_node(script: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_operator_engine_env_builder_passes_dsn_and_file_pointers_without_inline_secrets():
    script = textwrap.dedent(
        """
        const assert = require("node:assert/strict");
        const {
          ENGINE_ENV_PASSTHROUGH_KEYS,
          buildEngineChildEnv,
          isEngineEnvPassthroughKey,
          pickEnginePassthroughEnv
        } = require("./boot/operator_engine_env");

        const env = buildEngineChildEnv({
          TS_PG_DSN: "host=db port=5432 user=ts_app dbname=trading",
          TS_PG_PORT: "5432",
          TS_PG_PASSWORD_FILE: "/run/secrets/pg_password_app",
          TIMESCALE_PASSWORD_FILE: "/run/secrets/timescale_password",
          DATA_SOURCE_MASTER_KEY_FILE: "/run/secrets/data_source_master_key",
          REDIS_PASSWORD_FILE: "/run/secrets/redis_password",
          OBJECT_STORE_SECRET_KEY_FILE: "/run/secrets/object_store_secret_key",
          DB_PATH: "/var/lib/trading",
          TRADING_DATA: "/var/lib/trading/data",
          TRADING_LOGS: "/var/log/trading",
          TS_PG_PASSWORD: "inline-pg-password",
          TIMESCALE_PASSWORD: "inline-timescale-password"
        }, {
          baseEnv: {
            PATH: process.env.PATH || "",
            TS_PG_PASSWORD: "ambient-inline-pg-password",
            TIMESCALE_PASSWORD: "ambient-inline-timescale-password"
          },
          extraEnv: { PYTHONPATH: "/repo" }
        });

        assert.equal(env.TS_PG_DSN, "host=db port=5432 user=ts_app dbname=trading");
        assert.equal(env.TS_PG_PASSWORD_FILE, "/run/secrets/pg_password_app");
        assert.equal(env.TIMESCALE_PASSWORD_FILE, "/run/secrets/timescale_password");
        assert.equal(env.DATA_SOURCE_MASTER_KEY_FILE, "/run/secrets/data_source_master_key");
        assert.equal(env.REDIS_PASSWORD_FILE, "/run/secrets/redis_password");
        assert.equal(env.OBJECT_STORE_SECRET_KEY_FILE, "/run/secrets/object_store_secret_key");
        assert.equal(env.DB_PATH, "/var/lib/trading");
        assert.equal(env.TRADING_DATA, "/var/lib/trading/data");
        assert.equal(env.TRADING_LOGS, "/var/log/trading");
        assert.equal(env.PYTHONPATH, "/repo");
        assert.equal(Object.prototype.hasOwnProperty.call(env, "TS_PG_PASSWORD"), false);
        assert.equal(Object.prototype.hasOwnProperty.call(env, "TIMESCALE_PASSWORD"), false);

        assert.equal(ENGINE_ENV_PASSTHROUGH_KEYS.includes("TS_PG_DSN"), true);
        assert.equal(ENGINE_ENV_PASSTHROUGH_KEYS.includes("TS_PG_PASSWORD_FILE"), true);
        assert.equal(isEngineEnvPassthroughKey("OBJECT_STORE_SECRET_KEY_FILE"), true);
        assert.equal(isEngineEnvPassthroughKey("TS_PG_PASSWORD"), false);

        const picked = pickEnginePassthroughEnv({
          TS_PG_DSN: "dsn",
          TS_PG_PASSWORD: "inline",
          OBJECT_STORE_ACCESS_KEY_FILE: "/run/secrets/object_store_access_key"
        });
        assert.equal(picked.TS_PG_DSN, "dsn");
        assert.equal(picked.OBJECT_STORE_ACCESS_KEY_FILE, "/run/secrets/object_store_access_key");
        assert.equal(Object.prototype.hasOwnProperty.call(picked, "TS_PG_PASSWORD"), false);

        console.log(JSON.stringify({ ok: true, allowlist: ENGINE_ENV_PASSTHROUGH_KEYS }));
        """
    )

    result = _run_node(script)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["ok"] is True
    assert "TS_PG_DSN" in payload["allowlist"]
    assert "TS_PG_PASSWORD_FILE" in payload["allowlist"]


def test_operator_server_uses_shared_engine_env_builder_on_spawn_paths():
    text = (ROOT / "boot" / "operator_server.js").read_text(encoding="utf-8")

    assert 'require("./operator_engine_env")' in text
    assert "function buildOperatorEngineEnv" in text
    assert "const boot = runPythonBootstrap(python, sanitized);" in text
    assert "env: buildOperatorEngineEnv(sanitized)" in text
    assert 'env: buildOperatorEngineEnv(sanitized, { TRADING_VALIDATION_MODE: "startup" })' in text
    assert "runPythonBootstrap(pythonCmd, sanitized = {})" in text
    assert "env: { ...process.env, ...sanitized, PYTHONPATH: ROOT }" not in text


def test_engine_password_resolution_uses_file_pointer_without_inline_secret(tmp_path):
    password_file = tmp_path / "pg_password_app"
    password_file.write_text("file-secret-password\n", encoding="utf-8")
    password_file.chmod(0o600)

    env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(ROOT),
        "TS_PG_DSN": "host=127.0.0.1 port=5432 user=ts_app dbname=trading",
        "TS_PG_PASSWORD_FILE": str(password_file),
        "TS_SECRETS_PROVIDER": "systemd-creds",
    }
    script = (
        "import os; "
        "from engine.runtime.platform import connection_info_with_pg_password; "
        "print(connection_info_with_pg_password(os.environ['TS_PG_DSN']))"
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "password=file-secret-password" in result.stdout
    assert "credentials_directory_missing" not in (result.stdout + result.stderr)
