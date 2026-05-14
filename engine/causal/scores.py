"""Persistence and monotone scoring for causal diagnostics."""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from engine.causal.dag import CausalDAG


@dataclass(frozen=True)
class CausalScoreRecord:
    feature: str
    target: str
    window: str
    ts: int
    granger_p: float
    granger_lag: int
    dowhy_effect: float | None
    dowhy_p: float | None
    score: float
    decision: str


def sigmoid(value: float) -> float:
    x = float(value)
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def causal_score(*, granger_p: float | None, dowhy_t: float | None = None) -> float:
    """Compose Granger and backdoor evidence into a score in [0, 1]."""

    try:
        p = float(granger_p)
    except Exception:
        p = 1.0
    if not math.isfinite(p) or p <= 0.0:
        p = 1e-300
    p = max(1e-300, min(1.0, p))
    try:
        t_stat = abs(float(dowhy_t))
    except Exception:
        t_stat = 0.0
    if not math.isfinite(t_stat):
        t_stat = 0.0
    score = 0.5 * sigmoid(-math.log10(p) - 1.5) + 0.5 * sigmoid(t_stat - 2.0)
    return max(0.0, min(1.0, float(score)))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return out if math.isfinite(out) else None


def ensure_causal_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS causal_scores (
            feature TEXT NOT NULL,
            target TEXT NOT NULL,
            "window" TEXT NOT NULL,
            ts INTEGER NOT NULL,
            granger_p REAL NOT NULL,
            granger_lag INTEGER NOT NULL,
            dowhy_effect REAL NULL,
            dowhy_p REAL NULL,
            score REAL NOT NULL,
            decision TEXT NOT NULL,
            PRIMARY KEY(feature, target, "window", ts)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_causal_scores_latest
          ON causal_scores(feature, target, "window", ts)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS causal_dags (
            name TEXT PRIMARY KEY,
            dag_json TEXT NOT NULL,
            created_ts INTEGER NOT NULL
        )
        """
    )


def upsert_causal_score(con, record: CausalScoreRecord) -> None:
    ensure_causal_schema(con)
    con.execute(
        """
        INSERT INTO causal_scores (
            feature, target, "window", ts, granger_p, granger_lag,
            dowhy_effect, dowhy_p, score, decision
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(feature, target, "window", ts) DO UPDATE SET
            granger_p=excluded.granger_p,
            granger_lag=excluded.granger_lag,
            dowhy_effect=excluded.dowhy_effect,
            dowhy_p=excluded.dowhy_p,
            score=excluded.score,
            decision=excluded.decision
        """,
        (
            str(record.feature),
            str(record.target),
            str(record.window),
            int(record.ts),
            float(record.granger_p),
            int(record.granger_lag),
            _finite_or_none(record.dowhy_effect),
            _finite_or_none(record.dowhy_p),
            float(record.score),
            str(record.decision),
        ),
    )


def latest_causal_scores(
    feature_ids: Iterable[str],
    *,
    target: str | None = None,
    window: str | None = None,
    con=None,
) -> dict[str, float | None]:
    """Return latest causal score by feature, preserving missing features."""

    features = [str(feature).strip() for feature in feature_ids if str(feature or "").strip()]
    out: dict[str, float | None] = {feature: None for feature in features}
    if not features:
        return out
    should_close = False
    if con is None:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=True)
        should_close = True
    try:
        for feature in features:
            where = ["feature=?"]
            params: list[Any] = [feature]
            if target is not None:
                where.append("target=?")
                params.append(str(target))
            if window is not None:
                where.append('"window"=?')
                params.append(str(window))
            try:
                row = con.execute(
                    f"""
                    SELECT score
                    FROM causal_scores
                    WHERE {' AND '.join(where)}
                    ORDER BY ts DESC
                    LIMIT 1
                    """,
                    tuple(params),
                ).fetchone()
            except Exception:
                row = None
            if row:
                out[feature] = _finite_or_none(row[0])
        return out
    finally:
        if should_close:
            con.close()


def upsert_causal_dag(con, dag: CausalDAG, *, created_ts: int | None = None) -> None:
    ensure_causal_schema(con)
    con.execute(
        """
        INSERT INTO causal_dags(name, dag_json, created_ts)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            dag_json=excluded.dag_json,
            created_ts=excluded.created_ts
        """,
        (str(dag.name), dag.to_json(), int(created_ts if created_ts is not None else _now_ms())),
    )


def load_causal_dags(con) -> dict[str, CausalDAG]:
    try:
        rows = con.execute("SELECT name, dag_json FROM causal_dags").fetchall() or []
    except Exception:
        return {}
    out: dict[str, CausalDAG] = {}
    for name, payload in rows:
        try:
            dag = CausalDAG.from_json(str(payload or "{}"))
        except Exception:
            continue
        out[str(name or dag.name)] = dag
    return out


def score_record_from_mapping(payload: Mapping[str, Any]) -> CausalScoreRecord:
    return CausalScoreRecord(
        feature=str(payload.get("feature") or ""),
        target=str(payload.get("target") or ""),
        window=str(payload.get("window") or ""),
        ts=int(payload.get("ts") or _now_ms()),
        granger_p=float(payload.get("granger_p") if payload.get("granger_p") is not None else 1.0),
        granger_lag=int(payload.get("granger_lag") or 0),
        dowhy_effect=_finite_or_none(payload.get("dowhy_effect")),
        dowhy_p=_finite_or_none(payload.get("dowhy_p")),
        score=float(payload.get("score") if payload.get("score") is not None else 0.0),
        decision=str(payload.get("decision") or ""),
    )


def record_to_json(record: CausalScoreRecord) -> str:
    return json.dumps(record.__dict__, sort_keys=True, separators=(",", ":"))
