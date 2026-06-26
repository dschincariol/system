from __future__ import annotations

import base64
import importlib
import json
from pathlib import Path
import re
from urllib.parse import urlparse
import uuid


VALID_DATA_SOURCE_MASTER_KEY = base64.b64encode(bytes(range(32))).decode("ascii")


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _isolated_data_source_modules(monkeypatch, tmp_path):
    for key in (
        "ALPACA_BASE_URL",
        "ALPACA_KEY_ID",
        "ALPACA_KEY_ID_FILE",
        "ALPACA_SECRET_KEY",
        "ALPACA_SECRET_KEY_FILE",
        "ALPACA_STREAM_URL",
        "ALPACA_TRADE_UPDATES_WS_ENABLED",
        "DATA_SOURCE_ALLOW_LIVE_ALPACA_BROKER_DATA",
        "DATA_SOURCE_MANAGER_PROJECTED_KEYS",
    ):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("DB_PATH", str(tmp_path / "data_source_catalog_metadata.sqlite"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DATA_SOURCE_MASTER_KEY", VALID_DATA_SOURCE_MASTER_KEY)
    monkeypatch.delenv("DATA_SOURCE_MASTER_KEY_FILE", raising=False)
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setenv("EXECUTION_MODE", "safe")
    monkeypatch.setenv("DISABLE_LIVE_EXECUTION", "1")
    monkeypatch.setenv("KILL_SWITCH_GLOBAL", "1")
    monkeypatch.setenv("DATA_SOURCE_CONNECTION_TEST_MIN_INTERVAL_S", "0")
    monkeypatch.setenv("ENGINE_SUPERVISED", "0")
    monkeypatch.setenv("TIMESCALE_ENABLED", "0")
    monkeypatch.setenv("TIMESCALE_TELEMETRY_MIRROR_ENABLED", "0")
    monkeypatch.setenv("TELEMETRY_READ_BACKEND", "sqlite")
    monkeypatch.setenv("TELEMETRY_READ_REQUIRE_VALIDATION", "0")
    storage, = _reload_modules("engine.runtime.storage")
    storage.init_db()
    data_source_manager, routes = _reload_modules(
        "services.data_source_manager",
        "routes.data_sources_routes",
    )
    return storage, data_source_manager, routes


def test_template_payload_shape_carries_enriched_metadata() -> None:
    data_source_manager, = _reload_modules("services.data_source_manager")
    manager = data_source_manager.DataSourceManager()

    polygon = manager._template_payload("polygon", manager._catalog["polygon"])
    assert polygon["guide"]["category"] == "Market Data"
    assert polygon["guide"]["docs_url"].startswith("https://")
    assert polygon["guide"]["signup_url"].startswith("https://")
    assert polygon["guide"]["plan_note"]
    assert polygon["default_enabled"] is True
    assert polygon["storage_tables"]
    assert "price_router" in polygon["consumers"]
    assert polygon["safe_to_auto_enable"] is False
    assert polygon["runtime_runnable"] is True

    field = polygon["credential_fields"][0]
    for key in (
        "field",
        "env_var",
        "label",
        "help_text",
        "docs_url",
        "signup_url",
        "plan_note",
        "required",
        "required_state",
        "secret",
        "validation_hint",
        "validation_regex",
        "placeholder",
        "safety_warning",
        "input_type",
    ):
        assert key in field
    assert field["field"] == "api_key"
    assert field["env_var"] == "POLYGON_API_KEY"
    assert field["label"] == "API Key"
    assert field["required"] is True
    assert field["secret"] is True
    assert field["validation_regex"]

    ibkr = manager._template_payload("ibkr", manager._catalog["ibkr"])
    port_field = next(item for item in ibkr["setting_fields"] if item["field"] == "port")
    assert port_field["env_var"] == "IBKR_PORT"
    assert port_field["secret"] is False
    assert port_field["validation_regex"] == r"^\d+$"
    assert "read-only" in port_field["safety_warning"]


def test_catalog_seeds_all_discovered_feed_sources_and_operational_metadata(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)

    payload = routes.api_get_data_sources(None)
    sources = {item["source_key"]: item for item in payload["sources"]}
    templates = {item["template_key"]: item for item in payload["templates"]}

    expected_defaults = {
        "etf_flows": False,
        "inst_13f": False,
        "cftc_cot": False,
        "finra_short_volume": False,
        "finra_short_interest": False,
        "crypto_funding": False,
        "polygon_options": True,
        "news_flow": True,
        "llm_event_extraction": False,
        "alpaca_broker_data": False,
    }
    for source_key, default_enabled in expected_defaults.items():
        assert source_key in sources
        assert source_key in templates
        source = sources[source_key]
        template = templates[source_key]
        assert source["provider_name"] == template["provider_name"]
        assert source["job_name"] == template["job_name"]
        assert source["default_enabled"] is default_enabled
        assert template["default_enabled"] is default_enabled
        assert source["enabled"] is default_enabled
        assert source["storage_tables"]
        assert source["consumers"]
        assert template["storage_tables"] == source["storage_tables"]
        assert template["consumers"] == source["consumers"]
        expected_safe_to_auto_enable = source_key == "news_flow"
        assert source["safe_to_auto_enable"] is expected_safe_to_auto_enable
        assert template["safe_to_auto_enable"] is expected_safe_to_auto_enable

    keyless = {"cftc_cot", "finra_short_volume", "finra_short_interest", "crypto_funding"}
    for source_key in keyless:
        assert templates[source_key]["credential_fields"] == []
        assert templates[source_key]["supports_test"] is True

    alpaca = templates["alpaca_broker_data"]
    assert alpaca["runtime_runnable"] is False
    assert sorted(field["env_var"] for field in alpaca["credential_fields"]) == [
        "ALPACA_KEY_ID",
        "ALPACA_SECRET_KEY",
    ]
    accounts = {item["account_key"]: item for item in payload["provider_account_templates"]}
    assert "alpaca_data" in accounts
    assert "alpaca_broker_data" in {
        item["source_key"] for item in accounts["alpaca_data"]["used_by"] if item["kind"] == "source"
    }

    for template_key, template in templates.items():
        if template_key == "rss_feed":
            continue
        assert template["storage_tables"], template_key
        assert template["consumers"], template_key


def test_get_data_credential_callers_have_catalog_or_account_guidance() -> None:
    data_source_manager, = _reload_modules("services.data_source_manager")
    manager = data_source_manager.DataSourceManager()
    catalog_envs: set[str] = set()
    for definition in manager._catalog.values():
        catalog_envs.update(str(value) for value in (definition.credential_env or {}).values() if str(value))
    for definition in manager._account_catalog.values():
        catalog_envs.update(str(value) for value in (definition.credential_env or {}).values() if str(value))

    pattern = re.compile(r"get_data_credential\(\s*['\"]([A-Z0-9_]+)['\"]")
    callers: set[str] = set()
    for root in (Path("engine"), Path("services")):
        for path in root.rglob("*.py"):
            callers.update(pattern.findall(path.read_text(encoding="utf-8", errors="ignore")))

    assert callers
    assert callers - catalog_envs == set()


def test_new_keyless_sources_project_enable_flags_only_when_enabled(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()

    source_to_flag = {
        "cftc_cot": ("ingest_cftc_cot", "INGEST_CFTC_COT_ENABLED"),
        "finra_short_volume": ("ingest_finra_short_volume", "INGEST_FINRA_SHORT_VOLUME_ENABLED"),
        "finra_short_interest": ("ingest_finra_short_interest", "INGEST_FINRA_SHORT_INTEREST_ENABLED"),
        "crypto_funding": ("ingest_crypto_funding", "INGEST_CRYPTO_FUNDING_ENABLED"),
    }

    for source_key, (job_name, flag_name) in source_to_flag.items():
        assert manager.build_job_environment(job_name)[flag_name] == "0"
        assert job_name not in set(manager.get_desired_ingestion_jobs())

        manager.update_source({"source_key": source_key, "enabled": True, "actor": "unit-test"})
        assert manager.build_job_environment(job_name)[flag_name] == "1"
        assert job_name in set(manager.get_desired_ingestion_jobs())

        manager.update_source({"source_key": source_key, "enabled": False, "actor": "unit-test"})
        assert manager.build_job_environment(job_name)[flag_name] == "0"
        assert job_name not in set(manager.get_desired_ingestion_jobs())


def test_polygon_options_source_controls_options_poll_projection(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = f"codex-canary-polygon-options-{uuid.uuid4().hex}"

    manager.update_provider_account(
        {
            "account_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    manager.update_source({"source_key": "tradier", "enabled": False, "actor": "unit-test"})
    manager.update_source({"source_key": "polygon_options", "enabled": False, "actor": "unit-test"})

    env = manager.build_job_environment("options_poll")
    assert "options_poll" not in set(manager.get_desired_ingestion_jobs())
    assert "POLYGON_API_KEY" not in env
    assert env["POLYGON_REST_ENABLED"] == "0"
    assert env["TRADIER_ENABLED"] == "0"

    manager.update_source({"source_key": "polygon_options", "enabled": True, "actor": "unit-test"})
    env = manager.build_job_environment("options_poll")
    assert "options_poll" in set(manager.get_desired_ingestion_jobs())
    assert env["POLYGON_API_KEY"] == canary
    assert env["OPTIONS_PROVIDER_CHAIN"] == "polygon"
    assert env["POLYGON_REST_ENABLED"] == "1"
    assert env["TRADIER_ENABLED"] == "0"


def test_alpaca_broker_data_is_readonly_and_not_runtime_scheduled(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    key_canary = f"codex-canary-alpaca-key-{uuid.uuid4().hex}"
    secret_canary = f"codex-canary-alpaca-secret-{uuid.uuid4().hex}"

    manager.update_provider_account(
        {
            "account_key": "alpaca_data",
            "credentials": {"key_id": key_canary, "secret_key": secret_canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )
    manager.update_source({"source_key": "alpaca_broker_data", "enabled": True, "actor": "unit-test"})

    payload = routes.api_get_data_sources(None)
    rendered = json.dumps(payload, sort_keys=True, default=str)
    assert key_canary not in rendered
    assert secret_canary not in rendered
    assert "alpaca_broker_data_readonly" not in set(manager.get_desired_ingestion_jobs())
    assert manager.build_job_environment("alpaca_broker_data_readonly") == {}

    calls: list[str] = []

    class _Response:
        status_code = 200
        headers: dict[str, str] = {}

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def _fake_get(url, **kwargs):
        headers = kwargs.get("headers") or {}
        if headers.get("APCA-API-KEY-ID") != key_canary:
            raise AssertionError("alpaca_key_header_mismatch")
        if headers.get("APCA-API-SECRET-KEY") != secret_canary:
            raise AssertionError("alpaca_secret_header_mismatch")
        path = urlparse(str(url)).path
        calls.append(path)
        if path == "/v2/account":
            return _Response({"id": "canary-account"})
        if path == "/v2/positions":
            return _Response([])
        raise AssertionError("unexpected_alpaca_readonly_path")

    monkeypatch.setattr(data_source_manager.requests, "get", _fake_get)
    monkeypatch.setattr(data_source_manager.requests, "post", lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("broker_http_post_forbidden")))
    result = manager.test_connection("alpaca_broker_data", actor="unit-test")
    assert result["ok"] is True
    assert result["evidence"]["broker_data_readonly"] is True
    assert result["evidence"]["order_authority"] is False
    assert calls == ["/v2/account", "/v2/positions"]
    assert "/v2/orders" not in calls


def test_route_payload_uses_catalog_metadata_and_never_exposes_secret_canary(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.get_manager()
    canary = f"codex-canary-secret-{uuid.uuid4().hex}"
    manager.update_source(
        {
            "source_key": "polygon",
            "credentials": {"api_key": canary},
            "replace_credentials": True,
            "actor": "unit-test",
        }
    )

    payload = routes.api_get_data_sources(None)
    text = json.dumps(payload, sort_keys=True, default=str)
    assert payload["ok"] is True
    assert canary not in text
    assert "credential_fields" in text
    assert "help_text" in text
    assert "validation_regex" in text
    polygon_template = next(item for item in payload["templates"] if item["template_key"] == "polygon")
    assert polygon_template["guide"]["summary"].startswith("Polls Polygon REST")
    assert polygon_template["credential_fields"][0]["secret"] is True
    polygon_source = next(item for item in payload["sources"] if item["source_key"] == "polygon")
    assert "credentials" not in polygon_source
    assert canary not in json.dumps(polygon_source.get("masked_credentials") or {}, sort_keys=True)


def test_update_validation_rejects_invalid_fields_and_formats(monkeypatch, tmp_path) -> None:
    _storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.DataSourceManager()
    manager.initialize()

    try:
        manager.update_source({"source_key": "ibkr", "settings": {"unexpected": "1"}})
    except ValueError as exc:
        assert str(exc) == "unexpected_setting_fields:unexpected"
    else:
        raise AssertionError("invalid setting field was accepted")

    try:
        manager.update_source({"source_key": "ibkr", "settings": {"port": "not-a-port"}})
    except ValueError as exc:
        assert str(exc) == "invalid_setting_format:port"
    else:
        raise AssertionError("invalid setting format was accepted")

    canary = f"codex-canary-secret-{uuid.uuid4().hex}"
    try:
        manager.update_source(
            {
                "source_key": "polygon",
                "credentials": {"api_key": f"{canary}\nsecond-line"},
                "replace_credentials": True,
            }
        )
    except ValueError as exc:
        assert str(exc) == "invalid_credential_format:api_key"
        assert canary not in str(exc)
    else:
        raise AssertionError("invalid credential format was accepted")

    try:
        manager.update_source(
            {
                "source_key": "polygon",
                "clear_credential_fields": ["not_a_catalog_field"],
            }
        )
    except ValueError as exc:
        assert str(exc) == "unexpected_credential_fields:not_a_catalog_field"
    else:
        raise AssertionError("invalid clear credential field was accepted")


def test_existing_rows_with_legacy_extra_settings_can_update_unrelated_fields(monkeypatch, tmp_path) -> None:
    storage, data_source_manager, _routes = _isolated_data_source_modules(monkeypatch, tmp_path)
    manager = data_source_manager.DataSourceManager()
    manager.initialize()

    legacy_settings = {"legacy_extra": "kept-for-compatibility", "host": "127.0.0.1"}
    con = storage.connect_rw_direct()
    try:
        con.execute(
            "UPDATE data_sources SET settings_json = ? WHERE source_key = ?",
            (json.dumps(legacy_settings, sort_keys=True), "ibkr"),
        )
        con.commit()
    finally:
        con.close()

    updated = manager.update_source({"source_key": "ibkr", "enabled": False})
    assert updated["enabled"] is False
    assert updated["settings"]["legacy_extra"] == "kept-for-compatibility"


def test_data_sources_ui_renders_backend_metadata_without_provider_guide_map() -> None:
    js = Path("ui/data_sources.js").read_text(encoding="utf-8")
    css = Path("ui/data_sources.css").read_text(encoding="utf-8")

    assert "SOURCE_GUIDES" not in js
    assert "templateForSource(source)?.guide" in js
    assert "field.help_text" in js
    assert "field.validation_regex" in js
    assert "fieldDocsLinks" in js
    assert "honorRequired: false" in js
    assert "aria-invalid" in js
    assert "Polygon API key" not in js
    assert ".field-error" in css
    assert "input.is-invalid" in css
    assert "This broker-data source is enabled for read-only status visibility" in js
    assert "use broker execution controls for any trading authority" in js
