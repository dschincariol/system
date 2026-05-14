"""Persistence for discovered feature candidates and decisions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from engine.strategy.discovery.base import CandidateFeature, EvaluationResult, now_ms, stable_json

FEATURE_STAGE_SHADOW = "shadow"
FEATURE_STAGE_LIVE = "live"
ACCEPTED_DECISION = "accepted"
REJECTION_DECISIONS = frozenset({"fdr_failed", "tstat_failed", "degenerate"})


@dataclass(frozen=True)
class CandidateRecord:
    id: int
    ts: int
    source: str
    symbol: str
    expression: str
    params: Mapping[str, Any]
    hash: str


@dataclass(frozen=True)
class FeatureRegistryRecord:
    feature_id: str
    stage: str
    source: str
    expression: str
    params: Mapping[str, Any]
    hash: str
    created_ts: int
    accepted_candidate_id: int | None = None


def ensure_discovery_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_candidates (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          ts INTEGER NOT NULL,
          source TEXT NOT NULL,
          symbol TEXT NOT NULL,
          expression TEXT NOT NULL,
          params_json TEXT NOT NULL,
          hash TEXT NOT NULL UNIQUE
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feature_candidates_source_symbol
          ON feature_candidates(source, symbol, ts)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_evaluation (
          candidate_id INTEGER NOT NULL,
          ts INTEGER NOT NULL,
          t_stat REAL,
          p_value REAL,
          q_value REAL,
          oos_ic REAL,
          decision TEXT NOT NULL,
          PRIMARY KEY(candidate_id, ts)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feature_evaluation_decision_ts
          ON feature_evaluation(decision, ts)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_registry (
          feature_id TEXT PRIMARY KEY,
          stage TEXT NOT NULL DEFAULT 'shadow' CHECK(stage IN ('shadow', 'live')),
          source TEXT NOT NULL,
          expression TEXT NOT NULL,
          params_json TEXT NOT NULL,
          hash TEXT NOT NULL UNIQUE,
          created_ts INTEGER NOT NULL,
          accepted_candidate_id INTEGER,
          metadata_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_feature_registry_stage_source
          ON feature_registry(stage, source)
        """
    )


def record_candidate(candidate: CandidateFeature, *, con=None, ts: int | None = None) -> CandidateRecord:
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        db.execute(
            """
            INSERT INTO feature_candidates(ts, source, symbol, expression, params_json, hash)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(hash) DO NOTHING
            """,
            (
                int(ts if ts is not None else now_ms()),
                str(candidate.source),
                str(candidate.symbol).upper(),
                str(candidate.expression),
                stable_json(dict(candidate.params or {})),
                str(candidate.hash),
            ),
        )
        row = db.execute(
            """
            SELECT id, ts, source, symbol, expression, params_json, hash
            FROM feature_candidates
            WHERE hash=?
            """,
            (str(candidate.hash),),
        ).fetchone()
        if owns:
            db.commit()
        if not row:
            raise RuntimeError("feature_candidate_insert_failed")
        return _candidate_record(row)
    finally:
        if owns:
            db.close()


def fetch_candidate_by_hash(candidate_hash: str, *, con=None) -> CandidateRecord | None:
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        row = db.execute(
            """
            SELECT id, ts, source, symbol, expression, params_json, hash
            FROM feature_candidates
            WHERE hash=?
            """,
            (str(candidate_hash),),
        ).fetchone()
        return None if not row else _candidate_record(row)
    finally:
        if owns:
            db.close()


def has_evaluation(candidate_id: int, *, con=None) -> bool:
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        row = db.execute(
            "SELECT 1 FROM feature_evaluation WHERE candidate_id=? LIMIT 1",
            (int(candidate_id),),
        ).fetchone()
        return bool(row)
    finally:
        if owns:
            db.close()


def record_evaluation(
    candidate_id: int,
    result: EvaluationResult,
    *,
    con=None,
    ts: int | None = None,
) -> None:
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        db.execute(
            """
            INSERT INTO feature_evaluation(
              candidate_id, ts, t_stat, p_value, q_value, oos_ic, decision
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(candidate_id, ts) DO UPDATE SET
              t_stat=excluded.t_stat,
              p_value=excluded.p_value,
              q_value=excluded.q_value,
              oos_ic=excluded.oos_ic,
              decision=excluded.decision
            """,
            (
                int(candidate_id),
                int(ts if ts is not None else now_ms()),
                _finite_or_none(result.t_stat),
                _finite_or_none(result.p_value),
                _finite_or_none(result.q_value),
                _finite_or_none(result.oos_ic),
                str(result.decision or "pending"),
            ),
        )
        if owns:
            db.commit()
    finally:
        if owns:
            db.close()


def list_evaluations(*, con=None, candidate_id: int | None = None) -> list[dict[str, Any]]:
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        params: tuple[Any, ...] = ()
        where = ""
        if candidate_id is not None:
            where = "WHERE candidate_id=?"
            params = (int(candidate_id),)
        rows = db.execute(
            f"""
            SELECT candidate_id, ts, t_stat, p_value, q_value, oos_ic, decision
            FROM feature_evaluation
            {where}
            ORDER BY ts DESC, candidate_id ASC
            """,
            params,
        ).fetchall()
        return [
            {
                "candidate_id": int(row[0] or 0),
                "ts": int(row[1] or 0),
                "t_stat": row[2],
                "p_value": row[3],
                "q_value": row[4],
                "oos_ic": row[5],
                "decision": str(row[6] or ""),
            }
            for row in rows or []
        ]
    finally:
        if owns:
            db.close()


def register_feature(
    candidate: CandidateFeature,
    *,
    candidate_id: int,
    stage: str = FEATURE_STAGE_SHADOW,
    metadata: Mapping[str, Any] | None = None,
    con=None,
    ts: int | None = None,
) -> FeatureRegistryRecord:
    stage_text = str(stage or FEATURE_STAGE_SHADOW).strip().lower()
    if stage_text not in {FEATURE_STAGE_SHADOW, FEATURE_STAGE_LIVE}:
        raise ValueError(f"invalid_feature_stage:{stage_text}")
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        db.execute(
            """
            INSERT INTO feature_registry(
              feature_id, stage, source, expression, params_json, hash,
              created_ts, accepted_candidate_id, metadata_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(feature_id) DO NOTHING
            """,
            (
                str(candidate.feature_id),
                str(stage_text),
                str(candidate.source),
                str(candidate.expression),
                stable_json(dict(candidate.params or {})),
                str(candidate.hash),
                int(ts if ts is not None else now_ms()),
                int(candidate_id),
                stable_json(dict(metadata or {})),
            ),
        )
        row = db.execute(
            """
            SELECT feature_id, stage, source, expression, params_json, hash,
                   created_ts, accepted_candidate_id
            FROM feature_registry
            WHERE feature_id=?
            """,
            (str(candidate.feature_id),),
        ).fetchone()
        if owns:
            db.commit()
        if not row:
            raise RuntimeError("feature_registry_insert_failed")
        return _feature_registry_record(row)
    finally:
        if owns:
            db.close()


def list_registered_features(
    *,
    stage: str | None = None,
    con=None,
    limit: int = 1000,
) -> list[FeatureRegistryRecord]:
    owns, db = _connection(con, readonly=False)
    try:
        ensure_discovery_schema(db)
        params: list[Any] = []
        where = ""
        if stage is not None:
            where = "WHERE stage=?"
            params.append(str(stage).strip().lower())
        params.append(max(1, min(10000, int(limit or 1000))))
        rows = db.execute(
            f"""
            SELECT feature_id, stage, source, expression, params_json, hash,
                   created_ts, accepted_candidate_id
            FROM feature_registry
            {where}
            ORDER BY created_ts DESC, feature_id ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
        return [_feature_registry_record(row) for row in rows or []]
    finally:
        if owns:
            db.close()


def _connection(con, *, readonly: bool) -> tuple[bool, Any]:
    if con is not None:
        return False, con
    from engine.runtime.storage import connect, init_db

    init_db()
    return True, connect(readonly=bool(readonly))


def _candidate_record(row) -> CandidateRecord:
    return CandidateRecord(
        id=int(row[0] or 0),
        ts=int(row[1] or 0),
        source=str(row[2] or ""),
        symbol=str(row[3] or ""),
        expression=str(row[4] or ""),
        params=_parse_json_dict(row[5]),
        hash=str(row[6] or ""),
    )


def _feature_registry_record(row) -> FeatureRegistryRecord:
    return FeatureRegistryRecord(
        feature_id=str(row[0] or ""),
        stage=str(row[1] or ""),
        source=str(row[2] or ""),
        expression=str(row[3] or ""),
        params=_parse_json_dict(row[4]),
        hash=str(row[5] or ""),
        created_ts=int(row[6] or 0),
        accepted_candidate_id=None if row[7] is None else int(row[7] or 0),
    )


def _parse_json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        parsed = json.loads(raw)
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if out == out and out not in (float("inf"), float("-inf")) else None


__all__ = [
    "ACCEPTED_DECISION",
    "CandidateRecord",
    "FEATURE_STAGE_LIVE",
    "FEATURE_STAGE_SHADOW",
    "FeatureRegistryRecord",
    "REJECTION_DECISIONS",
    "ensure_discovery_schema",
    "fetch_candidate_by_hash",
    "has_evaluation",
    "list_evaluations",
    "list_registered_features",
    "record_candidate",
    "record_evaluation",
    "register_feature",
]
