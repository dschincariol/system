from __future__ import annotations

import importlib
import json
import sys
import time
from pathlib import Path
from typing import Any

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


@pytest.fixture()
def registry_stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "promotion_barriers.db"))
    monkeypatch.setenv("RUNTIME_METRICS_BUFFER_ENABLED", "0")
    monkeypatch.setenv("PROMOTION_COOLDOWN_S", "21600")
    modules = _reload_modules(
        "engine.runtime.db_guard",
        "engine.runtime.storage",
        "engine.runtime.runtime_meta",
        "engine.strategy.promotion_audit",
        "engine.strategy.model_marketplace",
        "engine.strategy.promotion_guard",
        "engine.model_registry",
        "engine.strategy.model_lifecycle",
    )
    storage = modules[1]
    storage.init_db()
    try:
        yield modules
    finally:
        storage.close_pooled_connections()


def _record_pass(promotion_audit: Any, *, model_id: str) -> None:
    promotion_audit.record_statistical_evidence(
        model_id=str(model_id),
        test_name="white_reality_check",
        p_value=0.01,
        decision="pass",
        payload={"source": "unit"},
    )


def _set_replay(runtime_meta: Any, *, model_id: str, model_kind: str, model_ts_ms: int, regime: str = "global") -> None:
    now_ms = int(time.time() * 1000)
    payload = {
        "ok": True,
        "updated_ts_ms": int(now_ms),
        "models": {
            f"{model_id}|AAPL|300|{regime}": {
                "model_name": str(model_id),
                "model_id": str(model_id),
                "symbol": "AAPL",
                "horizon_s": 300,
                "regime": str(regime),
                "model_kind": str(model_kind),
                "model_ts_ms": int(model_ts_ms),
                "approved": True,
            }
        },
    }
    status = {"ok": True, "status": "ready", "updated_ts_ms": int(now_ms)}
    runtime_meta.meta_set("competition_replay_validation", json.dumps(payload, separators=(",", ":"), sort_keys=True))
    runtime_meta.meta_set(
        "competition_replay_validation_status",
        json.dumps(status, separators=(",", ":"), sort_keys=True),
    )


def _audit_rows(storage: Any) -> list[dict[str, Any]]:
    con = storage.connect()
    try:
        rows = con.execute(
            """
            SELECT action, model_name, to_model_kind, to_model_ts_ms, reason_json
            FROM model_promotion_audit
            ORDER BY id ASC
            """
        ).fetchall()
    finally:
        con.close()
    out: list[dict[str, Any]] = []
    for action, model_name, to_kind, to_ts_ms, reason_json in rows:
        reason = json.loads(reason_json) if isinstance(reason_json, str) and reason_json else {}
        out.append(
            {
                "action": str(action),
                "model_name": str(model_name),
                "to_kind": str(to_kind or ""),
                "to_ts_ms": int(to_ts_ms or 0),
                "reason": reason if isinstance(reason, dict) else {},
            }
        )
    return out


def test_registry_promotion_requires_stat_replay_guard_and_audits(registry_stack: tuple[Any, ...]) -> None:
    _, storage, runtime_meta, promotion_audit, _, _, model_registry, _ = registry_stack
    model_id = "barrier_AAPL_1700000000000_abcdef1"
    model_registry.register_model(
        model_name=model_id,
        model_kind="test_kind",
        model_ts_ms=1700000000000,
        stage="challenger",
        metrics={"score": 1.0},
        regime="global",
    )

    with pytest.raises(RuntimeError, match="latest statistical evidence"):
        model_registry.promote_to_champion(model_id, "test_kind", 1700000000000, regime="global")

    _record_pass(promotion_audit, model_id=model_id)
    with pytest.raises(RuntimeError, match="replay validation"):
        model_registry.promote_to_champion(model_id, "test_kind", 1700000000000, regime="global")

    _set_replay(runtime_meta, model_id=model_id, model_kind="test_kind", model_ts_ms=1700000000001)
    with pytest.raises(RuntimeError, match="missing fresh approved replay"):
        model_registry.promote_to_champion(model_id, "test_kind", 1700000000000, regime="global")

    _set_replay(runtime_meta, model_id=model_id, model_kind="test_kind", model_ts_ms=1700000000000)
    model_registry.promote_to_champion(model_id, "test_kind", 1700000000000, regime="global")

    champion = model_registry.get_stage_latest(model_id, "champion", regime="global")
    assert champion is not None
    assert champion["model_kind"] == "test_kind"
    rows = _audit_rows(storage)
    assert [row["action"] for row in rows].count("block") >= 3
    promote_rows = [row for row in rows if row["action"] == "promote"]
    assert len(promote_rows) == 1
    assert promote_rows[0]["reason"]["statistical_evidence"]["decision"] == "pass"
    assert promote_rows[0]["reason"]["replay_validation"]["model_ts_ms"] == 1700000000000

    cooldown_model_id = "cooldown_AAPL_1700000000002_abcdef2"
    model_registry.register_model(
        model_name=cooldown_model_id,
        model_kind="test_kind",
        model_ts_ms=1700000000002,
        stage="challenger",
        metrics={"score": 2.0},
        regime="global",
    )
    _record_pass(promotion_audit, model_id=cooldown_model_id)
    _set_replay(runtime_meta, model_id=cooldown_model_id, model_kind="test_kind", model_ts_ms=1700000000002)
    with pytest.raises(RuntimeError, match="promotion guard blocked"):
        model_registry.promote_to_champion(cooldown_model_id, "test_kind", 1700000000002, regime="global")

    latest_block = [row for row in _audit_rows(storage) if row["model_name"] == cooldown_model_id][-1]
    assert latest_block["action"] == "block"
    assert "cooldown" in latest_block["reason"]["detail"]


def test_direct_champion_registration_and_lifecycle_live_are_blocked(registry_stack: tuple[Any, ...]) -> None:
    _, _, runtime_meta, promotion_audit, _, _, model_registry, model_lifecycle = registry_stack
    model_id = "lifecycle_AAPL_1700000000003_abcdef3"

    with pytest.raises(RuntimeError, match="direct champion registration"):
        model_registry.register_model(
            model_name=model_id,
            model_kind="test_kind",
            model_ts_ms=1700000000003,
            stage="champion",
            metrics={"score": 1.0},
            regime="global",
        )

    model_lifecycle.register_model_version(
        model_name=model_id,
        model_version="v1",
        model_kind="test_kind",
        stage="challenger",
        status="validated",
        live_ready=False,
    )
    with pytest.raises(RuntimeError, match="before registry promotion"):
        model_lifecycle.mark_version_live(model_id, "v1", stage="champion")

    model_registry.register_model(
        model_name=model_id,
        model_kind="test_kind",
        model_ts_ms=1700000000003,
        stage="challenger",
        metrics={"score": 1.0},
        regime="global",
    )
    _record_pass(promotion_audit, model_id=model_id)
    _set_replay(runtime_meta, model_id=model_id, model_kind="test_kind", model_ts_ms=1700000000003)
    model_registry.promote_to_champion(model_id, "test_kind", 1700000000003, regime="global")
    model_lifecycle.mark_version_live(model_id, "v1", stage="champion")

    version = model_lifecycle.get_model_version(model_id, "v1")
    assert version is not None
    assert version["stage"] == "champion"
    assert version["status"] == "live"
    assert version["live_ready"] is True
