"""Persistence for discovered feature candidates and decisions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from engine.strategy.discovery.base import CandidateFeature, EvaluationResult, now_ms, stable_json
from engine.strategy.experiment_ledger import PASS_DECISIONS, fetch_experiment_ledger, record_experiment_ledger

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
        if not row:
            raise RuntimeError("feature_candidate_insert_failed")
        record = _candidate_record(row)
        _record_feature_candidate_ledger(candidate, record, con=db, ts=int(record.ts or ts or now_ms()))
        if owns:
            db.commit()
        return record
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
        record = _candidate_record(
            db.execute(
                """
                SELECT id, ts, source, symbol, expression, params_json, hash
                FROM feature_candidates
                WHERE id=?
                """,
                (int(candidate_id),),
            ).fetchone()
        )
        _record_feature_evaluation_ledger(record, result, con=db, ts=int(ts if ts is not None else now_ms()))
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
        if stage_text == FEATURE_STAGE_LIVE and str(candidate.source or "").strip().lower() == "llm_factor":
            _assert_llm_feature_live_promotion_evidence(
                candidate,
                candidate_id=int(candidate_id),
                con=db,
            )
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


def _feature_candidate_type(source: str) -> str:
    source_key = str(source or "").strip().lower()
    if source_key == "llm_factor":
        return "llm_factor"
    if source_key == "tsfresh":
        return "tsfresh_feature"
    if source_key == "pysr":
        return "search_feature"
    return "search_feature"


def _record_feature_candidate_ledger(
    candidate: CandidateFeature,
    record: CandidateRecord,
    *,
    con,
    ts: int,
) -> None:
    params = dict(candidate.params or {})
    model_name = str(params.get("model_name") or params.get("model_id") or candidate.feature_id)
    record_experiment_ledger(
        con=con,
        ts=int(ts),
        candidate_key=str(record.hash),
        candidate_name=str(candidate.feature_id),
        candidate_version=str(record.hash)[:16],
        candidate_type=_feature_candidate_type(str(candidate.source)),
        source=str(candidate.source),
        model_name=model_name,
        feature_ids=[str(candidate.feature_id), *[str(fid) for fid in list(params.get("source_feature_ids") or [])]],
        prompt_hash=str(params.get("prompt_hash") or ""),
        model_hash=str(params.get("model_id") or model_name),
        search_space={
            "expression": str(candidate.expression),
            "params": dict(params),
            "symbol": str(candidate.symbol).upper(),
        },
        trial_budget=int(params.get("max_candidates") or params.get("trial_budget") or 1),
        trial_count=max(1, _safe_int(params.get("trial_count") or params.get("trial_index"), 1)),
        promotion_decision="pending",
        status="generated",
        diagnostics={"feature_candidate_id": int(record.id)},
    )


def _record_feature_evaluation_ledger(
    record: CandidateRecord,
    result: EvaluationResult,
    *,
    con,
    ts: int,
) -> None:
    params = dict(record.params or {})
    model_name = str(params.get("model_name") or params.get("model_id") or result.feature_id or "")
    decision = str(result.decision or "pending").strip().lower()
    redundancy = dict(result.diagnostics or {}) if decision == "redundant" else {"checked": True, "decision": decision}
    record_experiment_ledger(
        con=con,
        ts=int(ts),
        candidate_key=str(record.hash),
        candidate_name=str(result.feature_id or ""),
        candidate_version=str(record.hash)[:16],
        candidate_type=_feature_candidate_type(str(record.source)),
        source=str(record.source),
        model_name=model_name,
        feature_ids=[str(result.feature_id or ""), *[str(fid) for fid in list(params.get("source_feature_ids") or [])]],
        prompt_hash=str(params.get("prompt_hash") or ""),
        model_hash=str(params.get("model_id") or model_name),
        search_space={
            "expression": str(record.expression),
            "params": dict(params),
            "symbol": str(record.symbol).upper(),
        },
        trial_budget=int(params.get("max_candidates") or params.get("trial_budget") or 1),
        trial_count=max(1, _safe_int(params.get("trial_count") or params.get("trial_index"), 1)),
        fdr={
            "p_value": result.p_value,
            "q_value": result.q_value,
            "t_stat": result.t_stat,
            "decision": str(decision),
            "n_obs": int(result.n_obs or 0),
        },
        redundancy=redundancy,
        evidence={
            "oos_ic": result.oos_ic,
            "diagnostics": dict(result.diagnostics or {}),
        },
        promotion_decision=("accepted" if decision == ACCEPTED_DECISION else "rejected"),
        status=str(decision or "evaluated"),
        diagnostics={"feature_candidate_id": int(record.id)},
    )


def _assert_llm_feature_live_promotion_evidence(
    candidate: CandidateFeature,
    *,
    candidate_id: int,
    con,
) -> None:
    row = con.execute(
        """
        SELECT decision, t_stat, p_value, q_value
        FROM feature_evaluation
        WHERE candidate_id=?
        ORDER BY ts DESC
        LIMIT 1
        """,
        (int(candidate_id),),
    ).fetchone()
    if not row:
        raise ValueError("llm_feature_live_promotion_blocked:evaluation_missing")
    decision = str(row[0] or "").strip().lower()
    if decision != ACCEPTED_DECISION:
        raise ValueError(f"llm_feature_live_promotion_blocked:evaluation_not_accepted:{decision or 'missing'}")
    rows = fetch_experiment_ledger(candidate_key=str(candidate.hash), limit=5, con=con)
    if not rows:
        raise ValueError("llm_feature_live_promotion_blocked:ledger_missing")
    latest = dict(rows[0] or {})
    ledger_decision = str(latest.get("promotion_decision") or "").strip().lower()
    trial_budget = _safe_int(latest.get("trial_budget"), 0)
    trial_count = _safe_int(latest.get("trial_count"), 0)
    fdr = dict(latest.get("fdr_json") or {})
    evidence = dict(latest.get("evidence_json") or {})
    blockers: list[str] = []
    if ledger_decision not in PASS_DECISIONS:
        blockers.append("ledger_decision_not_passing")
    if trial_budget <= 0:
        blockers.append("trial_budget_missing")
    if trial_count <= 0:
        blockers.append("trial_count_missing")
    if trial_budget > 0 and trial_count > trial_budget:
        blockers.append("trial_budget_exceeded")
    if not fdr:
        blockers.append("statistical_gate_missing")
    if fdr and str(fdr.get("decision") or "").strip().lower() != ACCEPTED_DECISION:
        blockers.append("statistical_gate_not_accepted")
    if not evidence:
        blockers.append("ledger_evidence_missing")
    if blockers:
        raise ValueError(f"llm_feature_live_promotion_blocked:{','.join(blockers)}")


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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


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
