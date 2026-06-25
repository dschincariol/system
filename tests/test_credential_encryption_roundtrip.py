from __future__ import annotations

import base64
import importlib
import json
import os
import sqlite3
import uuid


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _isolated_routes(monkeypatch, tmp_path):
    for key in (
        "POLYGON_API_KEY",
        "POLYGON_API_KEY_FILE",
        "POLYGON_KEY",
        "POLYGON_KEY_FILE",
        "TS_SECRETS_PROVIDER",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "credential_roundtrip.sqlite"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("ALLOW_CREDENTIAL_DATA_PROVIDERS_IN_SAFE", "1")
    monkeypatch.setenv("ENGINE_SUPERVISED", "0")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
    monkeypatch.setenv("TELEMETRY_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
    storage, = _reload_modules("engine.runtime.storage")
    storage.init_db()
    _data_source_manager, routes, credential_encryption = _reload_modules(
        "services.data_source_manager",
        "routes.data_sources_routes",
        "services.credential_encryption",
    )
    return routes, credential_encryption


def test_api_provisioning_encrypts_at_rest_and_masks_roundtrip(monkeypatch, tmp_path) -> None:
    routes, credential_encryption = _isolated_routes(monkeypatch, tmp_path)
    canary = f"codex-roundtrip-{uuid.uuid4().hex}"

    result = routes.api_post_data_source_update(
        None,
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        },
        {},
    )
    rendered_result = json.dumps(result, sort_keys=True, default=str)

    assert result["ok"] is True
    assert canary not in rendered_result
    assert result["source"]["credentials_stored"] is True
    assert result["source"]["masked_credentials"]["api_key"] == credential_encryption.mask_credentials(
        {"api_key": canary}
    )["api_key"]

    with sqlite3.connect(os.environ["DB_PATH"]) as con:
        con.row_factory = sqlite3.Row
        row = con.execute(
            "SELECT credentials_enc, key_version FROM data_sources WHERE source_key = ?",
            ("polygon",),
        ).fetchone()

    assert row is not None
    ciphertext = str(row["credentials_enc"] or "")
    assert ciphertext
    assert ciphertext != canary
    assert canary not in ciphertext
    assert row["key_version"] == credential_encryption.DEFAULT_MASTER_KEY_NAME
    assert credential_encryption.decrypt_credentials(
        ciphertext,
        key_name=str(row["key_version"]),
    ) == {"api_key": canary}

    payload = routes.api_get_data_sources(None)
    rendered_payload = json.dumps(payload, sort_keys=True, default=str)
    polygon = next(row for row in payload["sources"] if row["source_key"] == "polygon")
    assert canary not in rendered_payload
    assert polygon["credentials_stored"] is True
    assert polygon["credentials_configured"] is True
    assert polygon["runtime_credentialed"] is True
    assert polygon["status"] != "needs_credentials"
