from engine.runtime import health
from engine.runtime import memory_pressure


class _CaptureLogger:
    def __init__(self) -> None:
        self.records = []

    def info(self, message, *, extra=None):
        self.records.append({"message": message, "extra": dict(extra or {})})


def test_memory_pressure_trace_missing_ok_defaults_false(monkeypatch):
    logger = _CaptureLogger()

    monkeypatch.setattr(health, "_HEALTH_SNAPSHOT_TRACE", True)
    monkeypatch.setattr(health, "log", logger)
    monkeypatch.setattr(health, "_warn", lambda *args, **kwargs: None)
    monkeypatch.setattr(memory_pressure, "host_memory_pressure_snapshot", lambda: {})

    ctx = health.HealthSnapshotContext(con=object(), now_ms=123, out={})

    health._check_memory_pressure(ctx)

    assert ctx.out["memory_pressure"] == {}
    assert logger.records[-1]["message"] == "health_snapshot_section"
    payload = logger.records[-1]["extra"]["extra_json"]
    assert payload["section"] == "memory_pressure"
    assert payload["ok"] is False


def test_memory_pressure_trace_preserves_explicit_true_ok(monkeypatch):
    logger = _CaptureLogger()

    monkeypatch.setattr(health, "_HEALTH_SNAPSHOT_TRACE", True)
    monkeypatch.setattr(health, "log", logger)
    monkeypatch.setattr(health, "_warn", lambda *args, **kwargs: None)
    monkeypatch.setattr(memory_pressure, "host_memory_pressure_snapshot", lambda: {"ok": True})

    ctx = health.HealthSnapshotContext(con=object(), now_ms=123, out={})

    health._check_memory_pressure(ctx)

    assert ctx.out["memory_pressure"] == {"ok": True}
    payload = logger.records[-1]["extra"]["extra_json"]
    assert payload["section"] == "memory_pressure"
    assert payload["ok"] is True
