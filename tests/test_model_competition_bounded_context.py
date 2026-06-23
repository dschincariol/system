from __future__ import annotations

import importlib
import re
import sqlite3
from pathlib import Path

from engine.strategy.model_competition import CompetitionRepository, PromotionStatGateEvaluator


REPO_ROOT = Path(__file__).resolve().parents[1]


def _create_marketplace_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE model_marketplace_scores (
          model_id TEXT NOT NULL DEFAULT 'baseline',
          model_name TEXT NOT NULL,
          symbol TEXT NOT NULL,
          horizon_s INTEGER NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          stage TEXT NOT NULL DEFAULT 'challenger',
          score REAL NOT NULL DEFAULT 0,
          trades INTEGER NOT NULL DEFAULT 0,
          wins INTEGER NOT NULL DEFAULT 0,
          losses INTEGER NOT NULL DEFAULT 0,
          gross_pnl REAL NOT NULL DEFAULT 0,
          net_pnl REAL NOT NULL DEFAULT 0,
          avg_confidence REAL NOT NULL DEFAULT 0,
          last_signal_ts_ms INTEGER,
          updated_ts_ms INTEGER NOT NULL DEFAULT 0,
          meta_json TEXT,
          PRIMARY KEY(model_id, model_name, symbol, horizon_s, regime)
        )
        """
    )


def test_repository_migrates_legacy_assignment_table_and_enforces_state_path() -> None:
    con = sqlite3.connect(":memory:")
    repo = CompetitionRepository(con)
    con.execute(
        """
        CREATE TABLE champion_assignments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          scope TEXT,
          symbol TEXT,
          horizon_s INTEGER,
          model_id TEXT,
          model_name TEXT,
          assigned_ts_ms INTEGER,
          metadata_json TEXT
        )
        """
    )

    shadow = repo.set_champion_assignment(
        scope="global",
        symbol="aapl",
        horizon_s=300,
        model_name="candidate_a",
        state="shadow",
        meta={"source": "test"},
        updated_ts_ms=100,
    )
    assert shadow["symbol"] == "AAPL"
    assert shadow["state"] == "shadow"

    repo.set_champion_assignment(
        scope="global",
        symbol="AAPL",
        horizon_s=300,
        model_name="candidate_a",
        state="challenger",
        updated_ts_ms=101,
    )
    champion = repo.set_champion_assignment(
        scope="global",
        symbol="AAPL",
        horizon_s=300,
        model_name="candidate_a",
        state="champion",
        updated_ts_ms=102,
    )

    loaded = repo.get_champion_assignment(scope="global", symbol="AAPL", horizon_s=300)
    assert champion["state"] == "champion"
    assert loaded["model_name"] == "candidate_a"
    assert loaded["state"] == "champion"


def test_repository_champion_assignment_read_path_does_not_run_schema_ddl() -> None:
    class _Cursor:
        def fetchone(self):
            return (
                "global",
                "AAPL",
                300,
                "candidate_a",
                "",
                "global",
                "champion",
                100,
                101,
                "{}",
            )

    class _Connection:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, sql: str, params=()):
            self.statements.append(str(sql))
            return _Cursor()

    con = _Connection()
    repo = CompetitionRepository(con)

    loaded = repo.get_champion_assignment(scope="global", symbol="AAPL", horizon_s=300)

    assert loaded["model_name"] == "candidate_a"
    assert con.statements
    assert not any(
        token in statement.upper()
        for statement in con.statements
        for token in ("CREATE TABLE", "ALTER TABLE", "CREATE INDEX", "CREATE UNIQUE INDEX")
    )


def test_repository_marketplace_upsert_controls_pnl_conflict_updates() -> None:
    con = sqlite3.connect(":memory:")
    _create_marketplace_table(con)
    repo = CompetitionRepository(con)

    row = {
        "model_id": "m1:v1",
        "model_name": "m1",
        "symbol": "msft",
        "horizon_s": 300,
        "regime": "global",
        "stage": "shadow",
        "score": 0.1,
        "trades": 5,
        "wins": 3,
        "losses": 2,
        "gross_pnl": 11.0,
        "net_pnl": 10.0,
        "avg_confidence": 0.7,
        "last_signal_ts_ms": 1000,
    }
    repo.upsert_marketplace_score(row, meta={"score_source": "pnl_attribution"}, updated_ts_ms=1000)
    repo.upsert_marketplace_score(
        {**row, "score": 0.9, "gross_pnl": 0.0, "net_pnl": 0.0},
        meta={"score_source": "model_oos_predictions"},
        updated_ts_ms=1001,
        update_pnl_on_conflict=False,
    )

    score, gross_pnl, net_pnl = con.execute(
        """
        SELECT score, gross_pnl, net_pnl
        FROM model_marketplace_scores
        WHERE model_id='m1:v1'
        """
    ).fetchone()
    assert score == 0.9
    assert gross_pnl == 11.0
    assert net_pnl == 10.0


def test_promotion_stat_gate_evaluator_caches_and_queues_legacy_hypothesis() -> None:
    calls: list[dict] = []
    queued: list[tuple[str, dict]] = []

    def evaluate_gate(row, n_competing_trials, *, models_returns, champion_row, con):
        calls.append(
            {
                "row": row,
                "n_competing_trials": n_competing_trials,
                "models_returns": models_returns,
                "champion_row": champion_row,
                "con": con,
            }
        )
        return True, {
            "record_legacy_hypothesis": True,
            "n_observations": 12,
            "t_statistic": 2.5,
            "deflated_sharpe": 1.2,
            "threshold_t": 1.7,
            "n_competing_trials": n_competing_trials,
            "passed": True,
        }

    def enqueue(action_name: str, **kwargs):
        queued.append((action_name, kwargs))

    evaluator = PromotionStatGateEvaluator(
        evaluate_gate=evaluate_gate,
        cache_key=lambda row: (str((row or {}).get("model_id") or ""), str((row or {}).get("version") or "")),
        candidate_version=lambda row: str((row or {}).get("version") or ""),
        enqueue_legacy_hypothesis=enqueue,
        safe_int=lambda value, default=0: int(value if value is not None else default),
        safe_float=lambda value, default=0.0: float(value if value is not None else default),
        con="db",
    )

    target = {"model_id": "m1", "model_name": "candidate_a", "version": "v1"}
    first_ok, first_payload = evaluator.evaluate(
        target,
        3,
        candidate_returns={"candidate_a": [0.1, 0.2]},
        incumbent_row={"model_name": "champion"},
    )
    second_ok, second_payload = evaluator.evaluate(
        target,
        3,
        candidate_returns={"candidate_a": [0.1, 0.2]},
        incumbent_row={"model_name": "champion"},
    )

    assert first_ok is True
    assert second_ok is True
    assert first_payload.get("cache_hit") is None
    assert second_payload["cache_hit"] is True
    assert len(calls) == 1
    assert [item[0] for item in queued] == ["record_hypothesis_result", "record_hypothesis_result"]
    assert queued[0][1]["model_name"] == "candidate_a"
    assert queued[0][1]["candidate_version"] == "v1"
    assert queued[1][1]["diagnostics"]["cache_hit"] is True


def test_model_marketplace_update_score_uses_repository_writer(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "marketplace_writer.db"))
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    importlib.reload(importlib.import_module("engine.runtime.db_guard"))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    model_marketplace = importlib.reload(importlib.import_module("engine.strategy.model_marketplace"))

    storage.init_db()
    try:
        snap = model_marketplace.update_model_score(
            model_name="repo_writer_model",
            symbol="spy",
            horizon_s=300,
            regime="global",
            stage="challenger",
            pnl_delta=2.5,
            confidence=0.8,
            won=True,
            meta={"model_id": "repo_writer_model:v1"},
        )
        assert snap["model_id"] == "repo_writer_model:v1"
        assert snap["symbol"] == "SPY"

        con = storage.connect()
        try:
            row = con.execute(
                """
                SELECT model_id, model_name, symbol, trades, wins, net_pnl
                FROM model_marketplace_scores
                WHERE model_id=? AND model_name=? AND symbol=?
                """,
                ("repo_writer_model:v1", "repo_writer_model", "SPY"),
            ).fetchone()
        finally:
            con.close()

        assert tuple(row) == ("repo_writer_model:v1", "repo_writer_model", "SPY", 1, 1, 2.5)
    finally:
        storage.close_pooled_connections()


def test_competition_tables_have_single_production_write_boundary() -> None:
    disallowed = tuple(
        re.compile(pattern, re.IGNORECASE)
        for pattern in (
            r"\bINSERT\s+INTO\s+model_marketplace_scores\b",
            r"\bUPDATE\s+model_marketplace_scores\b",
            r"\bDELETE\s+FROM\s+model_marketplace_scores\b",
            r"\bINSERT\s+INTO\s+champion_assignments\b",
            r"\bUPDATE\s+champion_assignments\b",
            r"\bDELETE\s+FROM\s+champion_assignments\b",
        )
    )
    allowed = {
        REPO_ROOT / "engine" / "strategy" / "model_competition" / "repository.py",
    }
    offenders: list[str] = []
    for path in sorted((REPO_ROOT / "engine").rglob("*.py")):
        if path in allowed:
            continue
        if "/schema/migrations/" in path.as_posix():
            continue
        text = path.read_text(encoding="utf-8")
        for pattern in disallowed:
            if pattern.search(text):
                offenders.append(f"{path.relative_to(REPO_ROOT)}: {pattern.pattern}")

    assert offenders == []
