"""Characterization tests for the runtime health decomposition facade."""

from __future__ import annotations

import inspect
from pathlib import Path
from unittest.mock import patch

import pytest

import engine.runtime.health as health


EXPECTED_HEALTH_SIGNATURES = {
    "get_disk_pressure_snapshot": "(paths: Optional[Iterable[tuple[str, str | pathlib.Path]]] = None) -> Dict[str, Any]",
    "get_startup_validation_snapshot": "(*, health: Optional[Dict[str, Any]] = None, db_validation: Optional[Dict[str, Any]] = None) -> Dict[str, Any]",
    "get_schema_audit": "()",
    "get_health_snapshot": "()",
    "get_readiness_snapshot": "(health: Optional[Dict[str, Any]] = None, preflight: Optional[Dict[str, Any]] = None, system_state: Optional[Dict[str, Any]] = None, graph: Optional[Dict[str, Any]] = None) -> Dict[str, Any]",
    "run_preflight": "() -> Dict",
    "preflight_cached": "(max_age_s: float = 30.0) -> Dict",
    "_sqlite_wal_path": "(db_path: pathlib.Path) -> Optional[pathlib.Path]",
    "_int_or": "(value: Any, default: int = 0) -> int",
    "_float_or": "(value: Any, default: float = 0.0) -> float",
    "_dedupe_strs": "(values: List[str]) -> List[str]",
    "_dict_or_empty": "(value: Any) -> Dict[str, Any]",
    "_json_dict_or_empty": "(raw: Any) -> Dict[str, Any]",
    "_json_list_or_empty": "(raw: Any) -> List[Any]",
    "_health_snapshot_pending_payload": "(*, now_ms: int, reason: str, cached_ts_ms: int = 0) -> Dict[str, Any]",
    "_stale_health_snapshot_payload": "(payload: Dict[str, Any], *, now_ms: int, cached_ts_ms: int) -> Dict[str, Any]",
    "_new_health_snapshot_payload": "(now_ms: int) -> Dict[str, Any]",
    "_build_health_snapshot_context": "(con: Any, now_ms: int) -> engine.runtime.health.HealthSnapshotContext",
    "_run_health_checks": "(ctx: engine.runtime.health.HealthSnapshotContext, checks: Iterable[engine.runtime.health.HealthSnapshotCheck]) -> None",
    "_finalize_health_snapshot": "(ctx: engine.runtime.health.HealthSnapshotContext) -> Dict[str, Any]",
}


def test_runtime_health_public_facade_signatures_are_stable() -> None:
    for name, expected in EXPECTED_HEALTH_SIGNATURES.items():
        assert hasattr(health, name), name
        assert str(inspect.signature(getattr(health, name))) == expected


def test_runtime_health_normalization_helpers_preserve_shapes_and_failure_tolerance() -> None:
    warnings: list[tuple[str, str]] = []

    def _capture(scope: str, err: Exception, **_extra) -> None:
        warnings.append((scope, type(err).__name__))

    with patch.object(health, "_warn", side_effect=_capture):
        assert health._sqlite_wal_path(Path("data")) == Path("data-wal")
        assert health._sqlite_wal_path(Path("trading.db")) == Path("trading.db-wal")
        assert health._int_or("bad", 7) == 7
        assert health._float_or("bad", 2.5) == 2.5
        assert health._dedupe_strs(["", "alpha", "alpha", " beta ", None, "beta"]) == ["alpha", "beta"]
        assert health._dict_or_empty({"a": 1}) == {"a": 1}
        assert health._dict_or_empty(["not", "dict"]) == {}
        assert health._json_dict_or_empty('{"ok": true, "n": 2}') == {"ok": True, "n": 2}
        assert health._json_dict_or_empty("[1, 2]") == {}
        assert health._json_dict_or_empty("{bad") == {}
        assert health._json_list_or_empty("[1, 2]") == [1, 2]
        assert health._json_list_or_empty('{"not": "list"}') == []
        assert health._json_list_or_empty("{bad") == []

    warning_scopes = [scope for scope, _name in warnings]
    assert "health.int_or" in warning_scopes
    assert "health.float_or" in warning_scopes
    assert "health.json_dict_or_empty.decode" in warning_scopes
    assert "health.json_list_or_empty.decode" in warning_scopes


def test_runtime_health_schema_audit_preserves_success_and_error_shapes() -> None:
    validation = {
        "ok": False,
        "missing_tables": ["prices"],
        "missing_columns": {"price_quotes": ["bid"]},
        "missing_indexes": ["idx_price_quotes_ts"],
        "have_tables": ["runtime_meta"],
        "schema_version": 2,
        "expected_schema_version": 3,
        "schema_status": "mismatch",
        "schema_version_notes": "unit-test",
        "schema_version_ok": False,
        "backend": "sqlite",
        "storage": "sqlite",
        "quick_check": "skipped",
        "owned_schema_ok": False,
        "owned_drift_tables": ["old_table"],
    }

    with patch.object(health, "get_db_validation_snapshot", return_value=validation) as get_validation:
        audit = health.get_schema_audit()

    get_validation.assert_called_once_with(include_quick_check=False)
    assert audit["ok"] is False
    assert audit["missing_tables"] == ["prices"]
    assert audit["missing_cols"] == {"price_quotes": ["bid"]}
    assert audit["missing_columns"] == {"price_quotes": ["bid"]}
    assert audit["missing_indexes"] == ["idx_price_quotes_ts"]
    assert audit["schema_version"] == 2
    assert audit["expected_schema_version"] == 3
    assert audit["schema_version_status"] == "mismatch"
    assert audit["schema_status"] == "mismatch"
    assert audit["schema_version_ok"] is False
    assert audit["owned_schema_ok"] is False
    assert audit["owned_drift_tables"] == ["old_table"]

    with patch.object(health, "get_db_validation_snapshot", side_effect=RuntimeError("db down")):
        degraded = health.get_schema_audit()

    assert degraded["ok"] is False
    assert degraded["missing_tables"] == []
    assert degraded["missing_cols"] == {}
    assert degraded["missing_columns"] == {}
    assert degraded["missing_indexes"] == []
    assert degraded["schema_version"] is None
    assert degraded["expected_schema_version"] == health.STORAGE_SCHEMA_VERSION
    assert degraded["schema_status"] == "unavailable"
    assert degraded["schema_version_status"] == "unavailable"
    assert degraded["schema_version_ok"] is False
    assert "RuntimeError: db down" in degraded["error"]


def test_runtime_health_snapshot_runner_preserves_pending_stale_and_check_failure_shapes() -> None:
    class FakeCon:
        def execute(self, *_args, **_kwargs):
            raise AssertionError("not used")

    ctx = health._build_health_snapshot_context(FakeCon(), 123456)
    assert ctx.now_ms == 123456
    assert ctx.out["ts_ms"] == 123456
    assert ctx.out["db_file"]["path"] == str(health.DB_PATH)

    def _ok(check_ctx):
        check_ctx.out["unit_test_probe"] = {"ok": True}

    def _bad(_check_ctx):
        raise ValueError("boom")

    health._run_health_checks(
        ctx,
        [
            health.HealthSnapshotCheck("ok_probe", _ok),
            health.HealthSnapshotCheck("bad_probe", _bad),
        ],
    )

    assert ctx.out["unit_test_probe"] == {"ok": True}
    assert ctx.check_failures == ["health_check_failed:bad_probe:ValueError"]

    pending = health._health_snapshot_pending_payload(
        now_ms=2000,
        reason="refreshing",
        cached_ts_ms=1250,
    )
    assert pending["ok"] is False
    assert pending["status"] == "DEGRADED"
    assert pending["warming_up"] is True
    assert pending["cache"]["source"] == "runtime_health_singleflight"
    assert pending["cache"]["stale"] is True
    assert pending["cache"]["age_ms"] == 750
    assert pending["execution_barrier"]["reason"] == "refreshing"

    stale = health._stale_health_snapshot_payload(
        {"ok": True, "cache": {"previous": True}, "nested": {"value": 1}},
        now_ms=3000,
        cached_ts_ms=2500,
    )
    assert stale["ok"] is True
    assert stale["cache"]["previous"] is True
    assert stale["cache"]["source"] == "runtime_health_singleflight"
    assert stale["cache"]["stale"] is True
    assert stale["cache"]["age_ms"] == 500
    stale["nested"]["value"] = 2


def test_runtime_health_finalize_preserves_severity_reasons_and_critical_blockers() -> None:
    class FakeCon:
        pass

    out = health._new_health_snapshot_payload(100000)
    out.update(
        {
            "db": {"ok": True, "initialized": True, "exists": True},
            "event_log": {"ok": True},
            "prices": {"ok": True},
            "events": {"ok": True},
            "job_summary": {"ok": True, "total": 4, "stale": 0},
            "providers": {"ok": True, "healthy": 1, "total": 1},
            "labels": {"ok": True},
            "provider_readiness": {
                "required": True,
                "ok": False,
                "blockers": ["provider:polygon:missing_credentials"],
            },
            "competition": {"ok": True},
            "attribution": {"ok": True, "orphans": {"orphan_row_count": 0}},
            "options_ingestion": {"ok": True},
            "startup_validation": {"ok": True, "reasons": []},
            "timeseries_storage": {"ok": True},
            "timescale": {"degraded_reasons": []},
            "feature_store": {"degraded_reasons": []},
            "portfolio_runtime": {"degraded": False},
            "position_reconcile": {"ok": True, "status": "ok"},
            "execution_degraded": {
                "active": True,
                "severity": "CRITICAL",
                "reason": "broker_reconcile_failed",
                "reason_codes": ["broker_reconcile_failed"],
            },
            "ingestion_runtime": {"running": True, "stale": False},
            "ingestion_freshness": {
                "critical_ok": True,
                "runtime_reason_codes": [],
                "advisory_reason_codes": [],
            },
            "data_pipeline_gates": {"ok": True, "failed_gates": []},
            "execution_barrier": {"allowed": True},
            "execution_supervisor": {"ok": True, "state": "ok", "failed_gates": [], "alerts": []},
            "broker_connection": {"ok": True, "state": "connected", "broker": "sim"},
        }
    )
    ctx = health.HealthSnapshotContext(con=FakeCon(), now_ms=100000, out=out)

    with patch.dict(health.os.environ, {"ENGINE_MODE": "live"}, clear=False):
        with patch.object(health, "_lc_get_state", return_value={"state": "LIVE", "detail": "unit-test"}):
            finalized = health._finalize_health_snapshot(ctx)

    assert finalized["ok"] is False
    assert finalized["startup"]["provider_readiness_ok"] is False
    assert finalized["startup"]["execution_degraded"] is True
    assert "provider_readiness_not_ok" in finalized["reasons"]
    assert "provider:polygon:missing_credentials" in finalized["reasons"]
    assert "execution_degraded:broker_reconcile_failed" in finalized["reasons"]
    assert "broker_reconcile_failed" in finalized["reasons"]
    assert "provider_readiness_not_ok" in finalized["critical_blockers"]
    assert "execution_degraded" in finalized["critical_blockers"]
    assert finalized["system_stage"] == "EXECUTION"
    assert finalized["lifecycle"]["state"] == "LIVE"


def test_runtime_health_readiness_snapshot_preserves_issue_levels_waiting_on_and_steps() -> None:
    health_payload = {
        "prices": {"ok": True, "age_s": 1.2, "last_ts_ms": 1000, "max_age_s": 120},
        "providers": {"ok": True, "healthy": 1, "total": 1},
        "provider_readiness": {
            "required": True,
            "ok": False,
            "required_providers": ["polygon"],
            "blockers": ["provider:polygon:missing_credentials"],
        },
        "labels": {"ok": True, "count": 12},
        "model": {"ok": True, "support_n": 12},
        "execution_barrier": {"allowed": True, "reason": "ok"},
        "broker_connection": {"ok": True, "state": "connected", "broker": "sim"},
        "db": {"ok": True, "initialized": True, "exists": True, "db_path": "/tmp/unit.db"},
        "job_summary": {"ok": True, "total": 3, "stale": 0, "stale_jobs": []},
        "startup_validation": {"ok": True, "blocking_gates": [], "reasons": []},
        "timeseries_storage": {"enabled": True, "ok": True},
        "feature_store": {"enabled": False, "ok": True},
        "portfolio_runtime": {"degraded": False},
        "position_reconcile": {"ok": True, "available": True, "status": "ok", "broker": "sim"},
        "execution_degraded": {
            "active": True,
            "severity": "WARNING",
            "reason": "latency_high",
            "reason_codes": ["execution_latency_high"],
        },
        "execution_supervisor": {"ok": True, "state": "ok", "failed_gates": []},
    }

    with patch.dict(health.os.environ, {"ENGINE_MODE": "live"}, clear=False):
        snapshot = health.get_readiness_snapshot(
            health=health_payload,
            preflight={"ok": True, "notes": []},
            system_state={"state": "LIVE", "mode": "live"},
            graph={"ok": False, "error": "graph drift"},
        )

    assert snapshot["ok"] is False
    assert snapshot["ready"] is False
    assert snapshot["status"] == "DEGRADED"
    assert snapshot["mode"] == "live"
    assert snapshot["data_feed_ok"] is False
    assert snapshot["provider_readiness_ok"] is False
    assert snapshot["risk_ok"] is True
    assert snapshot["broker_ok"] is True
    assert snapshot["timeseries_ok"] is True
    assert snapshot["system_live"] is True
    assert "provider_readiness" in snapshot["waiting_on"]
    assert "graph" in snapshot["waiting_on"]

    issue_levels = {item["code"]: item["level"] for item in snapshot["issues"]}
    assert issue_levels["provider_readiness_failed"] == "error"
    assert issue_levels["data_feed_not_ready"] == "error"
    assert issue_levels["execution_degraded"] == "warn"
    assert issue_levels["graph_invalid"] == "warn"

    reason_codes = [item["code"] for item in snapshot["reasons"]]
    assert "provider_readiness_failed" in reason_codes
    assert "data_feed_not_ready" in reason_codes
    assert "execution_degraded" not in reason_codes
    assert "graph_invalid" not in reason_codes

    step_by_id = {step["id"]: step for step in snapshot["steps"]}
    assert step_by_id["provider_readiness"]["blocked"] is True
    assert step_by_id["execution_health"]["blocked"] is False
    assert step_by_id["enable_trading"]["blocked"] is True
    assert "waiting_on=data_feed,provider_readiness,graph" in step_by_id["enable_trading"]["detail"]
