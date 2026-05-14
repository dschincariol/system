from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.requires_postgres

LUCKY_MODEL_ID = "lucky_AAPL_1700000000000_abcdef1"
FEATURE_MODEL_ID = "feature_AAPL_1700000000001_abcdef2"
PASSING_MODEL_ID = "passing_AAPL_1700000000002_abcdef3"
FACTOR_MODEL_ID = "factor_AAPL_1700000000003_abcdef4"
DIRECT_MODEL_ID = "direct_AAPL_1700000000004_abcdef5"
DIAGNOSTIC_MODEL_ID = "diagnostic_AAPL_1700000000005_abcdef6"
DUPLICATE_MODEL_ID = "duplicate_AAPL_1700000000006_abcdef7"


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def test_assess_challenger_blocks_lucky_reality_check_failure() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "promotion_guard_fdr.db")
        _db_guard, storage, promotion_audit, promotion_guard = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.strategy.promotion_guard",
        )
        storage.init_db()

        returns = [0.01, -0.01] * 25
        passed, diagnostics = promotion_guard.assess_challenger(
            model_id=LUCKY_MODEL_ID,
            model_name=LUCKY_MODEL_ID,
            challenger_returns=returns,
            champion_returns=returns,
            bootstrap_samples=199,
            random_state=42,
        )

        rows = promotion_audit.fetch_latest_statistical_evidence(model_id=LUCKY_MODEL_ID)
        assert not passed
        assert diagnostics["tests"]["white_reality_check"]["p_value"] > 0.10
        assert len(rows) == 1
        assert rows[0]["test_name"] == "white_reality_check"
        assert rows[0]["decision"] == "fail"
        storage.close_pooled_connections()


def test_assess_challenger_requires_fdr_and_factor_threshold_for_new_features() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "promotion_guard_features.db")
        _db_guard, storage, promotion_audit, promotion_guard = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.strategy.promotion_guard",
        )
        storage.init_db()

        passed, diagnostics = promotion_guard.assess_challenger(
            model_id=FEATURE_MODEL_ID,
            model_name=FEATURE_MODEL_ID,
            challenger_returns=[0.20] * 40,
            champion_returns=[0.0] * 40,
            new_features=["factor.good", "factor.bad"],
            feature_p_values={"factor.good": 0.001, "factor.bad": 0.50},
            feature_t_stats={"factor.good": 4.0, "factor.bad": 4.0},
            bootstrap_samples=199,
            random_state=7,
        )

        rows = promotion_audit.fetch_latest_statistical_evidence(model_id=FEATURE_MODEL_ID)
        latest_ts = rows[0]["ts"]
        latest = [row for row in rows if row["ts"] == latest_ts]

        assert not passed
        assert not diagnostics["tests"]["benjamini_hochberg_fdr"]["passed"]
        assert len(latest) == 4
        assert {row["test_name"] for row in latest} == {
            "white_reality_check",
            "benjamini_hochberg_fdr",
            "harvey_liu_zhu_factor_threshold",
        }
        assert any(row["feature_id"] == "factor.bad" and row["decision"] == "fail" for row in latest)
        storage.close_pooled_connections()


def test_assess_challenger_passes_and_persists_reconstructable_payloads() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "promotion_guard_pass.db")
        _db_guard, storage, promotion_audit, promotion_guard = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.strategy.promotion_guard",
        )
        storage.init_db()

        passed, diagnostics = promotion_guard.assess_challenger(
            model_id=PASSING_MODEL_ID,
            model_name=PASSING_MODEL_ID,
            challenger_returns=[0.25] * 40,
            champion_returns=[0.0] * 40,
            new_features=["factor.good"],
            feature_p_values={"factor.good": 0.001},
            feature_t_stats={"factor.good": 4.5},
            bootstrap_samples=199,
            random_state=9,
        )

        decision = promotion_audit.latest_statistical_evidence_decision(model_id=PASSING_MODEL_ID)
        rows = list(decision.get("rows") or [])

        assert passed
        assert diagnostics["passed"]
        assert decision["decision"] == "pass"
        assert len(rows) == 3
        reality = next(row for row in rows if row["test_name"] == "white_reality_check")
        assert "bootstrap_distribution" in reality["payload"]
        assert len(reality["payload"]["bootstrap_distribution"]) == 199
        json.dumps(decision, default=str)
        storage.close_pooled_connections()


def test_assess_challenger_rejects_duplicate_model_evidence() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "promotion_guard_duplicate.db")
        _db_guard, storage, promotion_audit, promotion_guard = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.strategy.promotion_guard",
        )
        storage.init_db()

        kwargs = {
            "model_id": DUPLICATE_MODEL_ID,
            "model_name": DUPLICATE_MODEL_ID,
            "challenger_returns": [0.02 + (idx % 5) * 0.001 for idx in range(40)],
            "champion_returns": [0.001 * (idx % 3) for idx in range(40)],
            "bootstrap_samples": 199,
            "random_state": 11,
        }
        promotion_guard.assess_challenger(**kwargs)

        with pytest.raises(promotion_audit.EvidenceConflict) as exc:
            promotion_guard.assess_challenger(**kwargs)

        assert exc.value.original_ts_ms > 0
        assert exc.value.model_id == DUPLICATE_MODEL_ID
        assert exc.value.evidence_kind == "white_reality_check"
        storage.close_pooled_connections()


def test_feature_registry_requires_passing_evidence_for_unknown_factor_features() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "feature_registry_factor_evidence.db")
        _db_guard, storage, promotion_audit, feature_registry = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.strategy.feature_registry",
        )
        storage.init_db()

        promotion_audit.record_statistical_evidence(
            model_id=FACTOR_MODEL_ID,
            feature_id="factor.accepted",
            test_name="harvey_liu_zhu_factor_threshold",
            t_stat=4.0,
            p_value=0.001,
            q_value=0.05,
            decision="pass",
            payload={"feature_id": "factor.accepted"},
        )
        promotion_audit.record_statistical_evidence(
            model_id=FACTOR_MODEL_ID,
            feature_id="factor.rejected",
            test_name="harvey_liu_zhu_factor_threshold",
            t_stat=4.0,
            p_value=0.001,
            q_value=0.20,
            decision="pass",
            payload={"feature_id": "factor.rejected"},
        )

        resolved = feature_registry.resolve_feature_ids(
            ["factor.accepted", "factor.rejected", "factor.missing"],
            fallback_to_default=False,
        )

        assert resolved == ["factor.accepted"]
        storage.close_pooled_connections()


def test_model_registry_refuses_direct_promotion_without_passing_evidence() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "model_registry_evidence.db")
        _db_guard, storage, promotion_audit, model_registry = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.model_registry",
        )
        storage.init_db()

        model_registry.register_model(
            model_name=DIRECT_MODEL_ID,
            model_kind="test_kind",
            model_ts_ms=123,
            stage="challenger",
            metrics={"score": 1.0},
            regime="global",
        )

        with pytest.raises(RuntimeError, match="latest statistical evidence"):
            model_registry.promote_to_champion(DIRECT_MODEL_ID, "test_kind", 123, regime="global")

        promotion_audit.record_statistical_evidence(
            model_id=DIRECT_MODEL_ID,
            test_name="white_reality_check",
            p_value=0.01,
            decision="pass",
            payload={"test": "registry_guard"},
        )
        model_registry.promote_to_champion(DIRECT_MODEL_ID, "test_kind", 123, regime="global")
        champion = model_registry.get_stage_latest(DIRECT_MODEL_ID, "champion", regime="global")

        assert champion is not None
        assert champion["model_kind"] == "test_kind"
        storage.close_pooled_connections()


def test_api_model_diagnostics_exposes_statistical_evidence_alongside_legacy_hypotheses() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "api_statistical_evidence.db")
        _db_guard, storage, promotion_audit, api_read_advanced = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "engine.strategy.promotion_audit",
            "engine.api.api_read_advanced",
        )
        storage.init_db()

        promotion_audit.record_statistical_evidence(
            model_id=DIAGNOSTIC_MODEL_ID,
            test_name="white_reality_check",
            t_stat=1.23,
            p_value=0.01,
            bootstrap_samples=199,
            decision="pass",
            payload={"source": "unit"},
        )

        rows = storage.fetch_recent_promotion_statistical_evidence(model_id=DIAGNOSTIC_MODEL_ID)
        diagnostics = api_read_advanced.get_model_diagnostics()

        assert rows
        assert rows[0]["model_id"] == DIAGNOSTIC_MODEL_ID
        assert rows[0]["test_name"] == "white_reality_check"
        assert rows[0]["payload"] == {"source": "unit"}
        assert "promotion_hypotheses" in diagnostics
        assert diagnostics["promotion_statistical_evidence"]
        assert diagnostics["promotion_statistical_evidence"][0]["model_id"] == DIAGNOSTIC_MODEL_ID
        storage.close_pooled_connections()


def test_train_model_v2_oos_return_series_loader_emits_holdout_returns() -> None:
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as tmp:
        os.environ["DB_PATH"] = str(Path(tmp) / "train_oos_returns.db")
        _db_guard, storage, train_model_v2 = _reload_modules(
            "engine.runtime.db_guard",
            "engine.runtime.storage",
            "ops.train_model_v2",
        )
        storage.init_db()

        con = storage.connect()
        try:
            now_ms = 1_777_700_000_000
            for idx, value in enumerate([0.1, 0.2, 0.3, 0.4, 0.5], start=1):
                con.execute(
                    """
                    INSERT INTO events(id, ts_ms, title, body, source, url, event_key)
                    VALUES (?,?,?,?,?,?,?)
                    """,
                    (idx, now_ms + idx, f"event {idx}", "", "unit", "", f"event-{idx}"),
                )
                con.execute(
                    """
                    INSERT INTO labels(event_id, symbol, horizon_s, impact_z)
                    VALUES (?,?,?,?)
                    """,
                    (idx, "AAPL", 300, float(value)),
                )
            con.commit()
        finally:
            con.close()

        payload = train_model_v2._load_oos_return_series(
            symbols=["AAPL"],
            horizons=[300],
            lookback_days=365,
            holdout_fraction=0.40,
        )

        assert payload["oos_returns"] == [0.4, 0.5]
        assert payload["oos_return_count"] == 2
        storage.close_pooled_connections()
