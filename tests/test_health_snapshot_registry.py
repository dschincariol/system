from engine.runtime import health


class _FakeConnection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


def _healthy_minimal_payload() -> dict:
    return {
        "ok": False,
        "reasons": [],
        "db": {"ok": True},
        "event_log": {"ok": True},
        "prices": {"ok": True},
        "events": {"ok": True},
        "labels": {"ok": True},
        "job_summary": {"ok": True},
        "providers": {"ok": True},
        "provider_readiness": {"ok": True, "required": False},
        "competition": {"ok": True},
        "attribution": {"ok": True},
        "options_ingestion": {"ok": True},
        "startup_validation": {"ok": True},
        "timeseries_storage": {"ok": True},
        "timescale": {},
        "feature_store": {},
        "portfolio_runtime": {"degraded": False},
        "position_reconcile": {"ok": True},
        "execution_degraded": {"active": False},
        "ingestion_runtime": {"running": True, "stale": False},
        "ingestion_freshness": {
            "critical_ok": True,
            "runtime_reason_codes": [],
            "advisory_reason_codes": [],
        },
        "data_pipeline_gates": {"ok": True, "failed_gates": []},
        "execution_barrier": {"allowed": True},
        "execution_supervisor": {"ok": True},
        "broker_connection": {"ok": True, "state": "connected"},
    }


def test_health_registry_isolates_probe_failure_and_continues(monkeypatch):
    calls = []
    ctx = health.HealthSnapshotContext(con=object(), now_ms=123, out={"ok": False, "reasons": []})

    def failing_check(_ctx):
        calls.append("failing")
        raise RuntimeError("boom")

    def later_check(later_ctx):
        calls.append("later")
        later_ctx.out["later_ran"] = True

    monkeypatch.setattr(health, "_warn", lambda *args, **kwargs: None)

    health._run_health_checks(
        ctx,
        (
            health.HealthSnapshotCheck("failing", failing_check),
            health.HealthSnapshotCheck("later", later_check),
        ),
    )

    assert calls == ["failing", "later"]
    assert ctx.out["later_ran"] is True
    assert ctx.check_failures == ["health_check_failed:failing:RuntimeError"]


def test_registry_failure_forces_final_snapshot_fail_closed(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "safe")
    monkeypatch.setattr(health, "_lc_get_state", lambda: {"state": "SAFE", "detail": ""})
    ctx = health.HealthSnapshotContext(
        con=object(),
        now_ms=123,
        out=_healthy_minimal_payload(),
        check_failures=["health_check_failed:failing:RuntimeError"],
    )

    snapshot = health._finalize_health_snapshot(ctx)

    assert snapshot["ok"] is False
    assert "health_check_failed:failing:RuntimeError" in snapshot["reasons"]
    assert "health_check_failed:failing:RuntimeError" in snapshot["critical_blockers"]
    assert snapshot["data_flow_ok"] is False


def test_critical_disk_pressure_blocks_final_health_in_paper_mode(monkeypatch):
    monkeypatch.setenv("ENGINE_MODE", "paper")
    monkeypatch.setattr(health, "_lc_get_state", lambda: {"state": "PAPER", "detail": ""})
    out = _healthy_minimal_payload()
    out["disk_pressure"] = {
        "ok": False,
        "status": "critical",
        "critical": ["root:disk_critical:free_bytes=1024:free_pct=0.01"],
        "warnings": [],
        "paths": [
            {
                "label": "root",
                "critical": True,
                "detail": "disk_critical:free_bytes=1024:free_pct=0.01",
            }
        ],
    }
    ctx = health.HealthSnapshotContext(con=object(), now_ms=123, out=out)

    snapshot = health._finalize_health_snapshot(ctx)

    assert snapshot["ok"] is False
    assert snapshot["startup"]["disk_pressure_ok"] is False
    assert "disk_pressure_critical" in snapshot["reasons"]
    assert "disk_pressure:root:disk_critical:free_bytes=1024:free_pct=0.01" in snapshot["reasons"]
    assert "disk_pressure_critical" in snapshot["critical_blockers"]
    assert "disk_pressure:root:disk_critical:free_bytes=1024:free_pct=0.01" in snapshot["critical_blockers"]
    assert snapshot["data_flow_ok"] is False


def test_get_health_snapshot_uses_registered_checks_and_closes_connection(monkeypatch):
    fake_con = _FakeConnection()

    def sentinel_check(ctx):
        ctx.out["sentinel"] = "ran"
        ctx.out["ok"] = True

    def finalize(ctx):
        assert ctx.out["sentinel"] == "ran"
        return ctx.out

    monkeypatch.setattr(health, "_HEALTH_SNAPSHOT_CACHE_TTL_MS", 0)
    monkeypatch.setattr(health, "_db_connect", lambda: fake_con)
    monkeypatch.setattr(
        health,
        "_HEALTH_SNAPSHOT_CHECKS",
        (health.HealthSnapshotCheck("sentinel", sentinel_check),),
    )
    monkeypatch.setattr(health, "_finalize_health_snapshot", finalize)

    snapshot = health.get_health_snapshot()

    assert snapshot["sentinel"] == "ran"
    assert fake_con.closed is True
