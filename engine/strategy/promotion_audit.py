"""
FILE: promotion_audit.py

Persists an audit trail for model promotions, rollbacks, and blocked changes.
This table is the main forensic record when a staged model transition needs to
be explained later.
"""

import logging
import json
import math
import re
import time
from typing import Optional, Dict, Any

from engine.audit.chain import append_chain_row
from engine.artifacts.store import LocalArtifactStore
from engine.runtime.storage import connect, init_db, run_write_txn


_MODEL_ID_RE = re.compile(
    r"^(?P<family>[A-Za-z][A-Za-z0-9_]*?)_"
    r"(?P<symbol>[A-Z][A-Z0-9.\-]{0,15})_"
    r"(?P<train_ts_ms>[0-9]{10,17})_"
    r"(?P<short_commit_sha>[0-9a-fA-F]{7,12})$"
)


class InvalidModelId(ValueError):
    """Raised when promotion evidence does not use the structured model id."""


class EvidenceConflict(RuntimeError):
    """Raised when an evidence kind is submitted twice for the same model."""

    def __init__(
        self,
        *,
        model_id: str,
        evidence_kind: str,
        original_ts_ms: int,
        feature_id: Optional[str] = None,
    ) -> None:
        self.model_id = str(model_id)
        self.evidence_kind = str(evidence_kind)
        self.original_ts_ms = int(original_ts_ms)
        self.feature_id = str(feature_id or "")
        feature_suffix = f" feature_id={self.feature_id}" if self.feature_id else ""
        super().__init__(
            f"statistical evidence conflict for model_id={self.model_id} "
            f"evidence_kind={self.evidence_kind}{feature_suffix}; original_ts_ms={self.original_ts_ms}"
        )


def _validate_structured_model_id(model_id: str) -> str:
    model_key = str(model_id or "").strip()
    if not _MODEL_ID_RE.match(model_key):
        raise InvalidModelId(
            "model_id must match <family>_<symbol>_<train_ts_ms>_<short_commit_sha>"
        )
    return model_key


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_json_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            return dict(parsed) if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _artifact_sha_from_alias(alias: Any) -> str:
    alias_text = str(alias or "").strip()
    if not alias_text:
        return ""
    try:
        ref = LocalArtifactStore().resolve(alias_text)
    except Exception:
        return ""
    return str(ref.sha256) if ref else ""


def _lookup_artifact_sha(
    con,
    *,
    model_name: str,
    model_kind: Optional[str],
    model_ts_ms: Optional[int],
    regime: Optional[str],
) -> str:
    if not model_kind or model_ts_ms is None:
        return ""
    try:
        row = con.execute(
            """
            SELECT metrics_json, performance_metrics_json
            FROM model_registry
            WHERE model_name=? AND model_kind=? AND model_ts_ms=? AND regime=?
            ORDER BY COALESCE(updated_ts_ms, created_ts_ms) DESC, created_ts_ms DESC
            LIMIT 1
            """,
            (
                str(model_name),
                str(model_kind),
                int(model_ts_ms),
                str(regime if regime is not None else "global"),
            ),
        ).fetchone()
    except Exception:
        return ""
    if not row:
        return ""
    for payload in (_safe_json_dict(row[0]), _safe_json_dict(row[1])):
        sha = str(payload.get("artifact_sha256") or payload.get("artifact_hash") or "").strip()
        if sha:
            return sha
        alias = str(payload.get("artifact_alias") or payload.get("artifact_uri") or "").strip()
        sha = _artifact_sha_from_alias(alias)
        if sha:
            return sha
    return ""


_FEATURE_KEYS = {
    "feature",
    "feature_id",
    "feature_ids",
    "features",
    "candidate_features",
    "new_features",
    "used_features",
    "challenger_feature_ids",
    "feature_names",
}


def _add_feature_values(out: list[str], value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if text and text not in out:
            out.append(text)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, (dict, list, tuple, set)):
                _add_feature_values(out, item)
            else:
                _add_feature_values(out, key)
        return
    if isinstance(value, (list, tuple, set)):
        for item in value:
            _add_feature_values(out, item)


def _collect_feature_ids(payload: Dict[str, Any]) -> list[str]:
    out: list[str] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if str(key) in _FEATURE_KEYS:
                    _add_feature_values(out, item)
                elif isinstance(item, dict):
                    _walk(item)
                elif isinstance(item, (list, tuple)):
                    for child in item:
                        if isinstance(child, dict):
                            _walk(child)

    _walk(payload)
    return out


def _augment_reason_with_causal_scores(reason_payload: Dict[str, Any], con) -> Dict[str, Any]:
    feature_ids = _collect_feature_ids(reason_payload)
    if not feature_ids:
        return reason_payload
    try:
        from engine.causal.scores import latest_causal_scores

        latest = latest_causal_scores(feature_ids, con=con)
    except Exception:
        latest = {feature: None for feature in feature_ids}
    existing = reason_payload.get("causal_scores")
    causal_scores = dict(existing) if isinstance(existing, dict) else {}
    for feature in feature_ids:
        causal_scores.setdefault(feature, latest.get(feature))
    reason_payload["causal_scores"] = causal_scores
    return reason_payload


def audit(
    *,
    actor: str,
    action: str,
    model_name: str,
    from_kind: Optional[str] = None,
    from_ts_ms: Optional[int] = None,
    to_kind: Optional[str] = None,
    to_ts_ms: Optional[int] = None,
    from_artifact_sha256: Optional[str] = None,
    to_artifact_sha256: Optional[str] = None,
    reason: Optional[Dict[str, Any]] = None,
    regime: Optional[str] = None,
) -> None:
    init_db()
    reason_payload = dict(reason or {})
    con = connect()
    try:
        resolved_from_sha = str(from_artifact_sha256 or "").strip() or _lookup_artifact_sha(
            con,
            model_name=str(model_name),
            model_kind=from_kind,
            model_ts_ms=from_ts_ms,
            regime=regime,
        )
        resolved_to_sha = str(to_artifact_sha256 or "").strip() or _lookup_artifact_sha(
            con,
            model_name=str(model_name),
            model_kind=to_kind,
            model_ts_ms=to_ts_ms,
            regime=regime,
        )
        if resolved_from_sha:
            reason_payload["from_artifact_sha256"] = str(resolved_from_sha)
        if resolved_to_sha:
            reason_payload["to_artifact_sha256"] = str(resolved_to_sha)
        reason_payload = _augment_reason_with_causal_scores(reason_payload, con)
        append_chain_row(
            "model_promotion_audit",
            {
                "ts_ms": _now_ms(),
                "actor": str(actor),
                "action": str(action),
                "model_name": str(model_name),
                "from_model_kind": (str(from_kind) if from_kind else None),
                "from_model_ts_ms": (int(from_ts_ms) if from_ts_ms is not None else None),
                "to_model_kind": (str(to_kind) if to_kind else None),
                "to_model_ts_ms": (int(to_ts_ms) if to_ts_ms is not None else None),
                "reason_json": reason_payload,
                "regime": (str(regime) if regime is not None else None),
            },
            con,
        )
        con.commit()
    finally:
        con.close()


def _json_default(value: Any) -> Any:
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, np.generic):
            return value.item()
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
    if isinstance(value, (set, tuple)):
        return list(value)
    return str(value)


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return value


def _table_has_column(con, table_name: str, column_name: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception:
        return False
    target = str(column_name or "").strip().lower()
    return any(str(row[1] or "").strip().lower() == target for row in rows if row and len(row) > 1)


def _hash_hex(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, memoryview):
        value = value.tobytes()
    if isinstance(value, bytearray):
        value = bytes(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def record_statistical_evidence(
    *,
    model_id: str,
    test_name: str,
    decision: str,
    feature_id: Optional[str] = None,
    t_stat: Optional[float] = None,
    p_value: Optional[float] = None,
    q_value: Optional[float] = None,
    bootstrap_samples: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    ts: Optional[int] = None,
    con=None,
) -> int:
    """Persist one reconstructable statistical promotion evidence row."""

    model_key = _validate_structured_model_id(str(model_id or ""))
    test_key = str(test_name or "").strip()
    feature_key = str(feature_id).strip() if feature_id is not None and str(feature_id).strip() else ""
    evidence_kind = f"{test_key}:{feature_key}" if feature_key else str(test_key)
    ts_ms = int(ts if ts is not None else _now_ms())
    decision_text = str(decision or "").strip().lower() or "fail"
    payload_json = json.dumps(_json_safe_value(payload or {}), separators=(",", ":"), sort_keys=True, default=_json_default)

    def _write(db) -> int:
        has_evidence_kind = _table_has_column(db, "promotion_statistical_evidence", "evidence_kind")
        if has_evidence_kind:
            existing = db.execute(
                """
                SELECT ts
                FROM promotion_statistical_evidence
                WHERE model_id=?
                  AND (
                    evidence_kind=?
                    OR (evidence_kind IS NULL AND test_name=? AND COALESCE(feature_id, '')=?)
                  )
                ORDER BY ts ASC, id ASC
                LIMIT 1
                """,
                (str(model_key), str(evidence_kind), str(test_key), str(feature_key)),
            ).fetchone()
        else:
            existing = db.execute(
                """
                SELECT ts
                FROM promotion_statistical_evidence
                WHERE model_id=?
                  AND test_name=?
                  AND COALESCE(feature_id, '')=?
                ORDER BY ts ASC, id ASC
                LIMIT 1
                """,
                (str(model_key), str(test_key), str(feature_key)),
            ).fetchone()
        if existing:
            raise EvidenceConflict(
                model_id=str(model_key),
                evidence_kind=str(evidence_kind),
                feature_id=(feature_key or None),
                original_ts_ms=int(existing[0] or 0),
            )
        row_payload = {
            "ts": int(ts_ms),
            "model_id": str(model_key),
            "feature_id": (str(feature_key) if feature_key else None),
            "test_name": str(test_key),
            "t_stat": _finite_or_none(t_stat),
            "p_value": _finite_or_none(p_value),
            "q_value": _finite_or_none(q_value),
            "bootstrap_samples": (None if bootstrap_samples is None else int(bootstrap_samples)),
            "decision": str(decision_text),
            "payload_json": str(payload_json),
        }
        if has_evidence_kind:
            row_payload["evidence_kind"] = str(evidence_kind)
        result = append_chain_row(
            "promotion_statistical_evidence",
            row_payload,
            db,
        )
        return int(result.row_id or 0)

    if con is not None:
        return _write(con)

    init_db()
    return int(
        run_write_txn(
            _write,
            table="promotion_statistical_evidence",
            operation="record_statistical_evidence",
            context={"model_id": str(model_key), "test_name": str(test_key), "decision": str(decision_text)},
        )
        or 0
    )


def fetch_latest_statistical_evidence(
    *,
    model_id: str,
    limit: int = 50,
    con=None,
) -> list[Dict[str, Any]]:
    """Fetch latest statistical evidence rows for a model id."""

    model_key = str(model_id or "").strip()
    row_limit = max(1, min(500, int(limit or 50)))
    should_close = False
    if con is None:
        init_db()
        con = connect(readonly=True)
        should_close = True
    try:
        include_hashes = _table_has_column(con, "promotion_statistical_evidence", "row_hash")
        hash_select = ", prev_hash, row_hash" if include_hashes else ""
        rows = con.execute(
            f"""
            SELECT
              id, ts, model_id, feature_id, test_name, t_stat, p_value,
              q_value, bootstrap_samples, decision, payload_json{hash_select}
            FROM promotion_statistical_evidence
            WHERE model_id=?
            ORDER BY ts DESC, id DESC
            LIMIT ?
            """,
            (str(model_key), int(row_limit)),
        ).fetchall() or []
        out: list[Dict[str, Any]] = []
        for row in rows:
            try:
                payload = json.loads(row[10] or "{}")
            except Exception:
                payload = {}
            out.append(
                {
                    "id": int(row[0] or 0),
                    "ts": int(row[1] or 0),
                    "model_id": str(row[2] or ""),
                    "feature_id": (None if row[3] is None else str(row[3])),
                    "test_name": str(row[4] or ""),
                    "t_stat": row[5],
                    "p_value": row[6],
                    "q_value": row[7],
                    "bootstrap_samples": row[8],
                    "decision": str(row[9] or ""),
                    "payload": payload if isinstance(payload, dict) else {},
                    **(
                        {
                            "prev_hash": _hash_hex(row[11]),
                            "row_hash": _hash_hex(row[12]),
                        }
                        if include_hashes
                        else {}
                    ),
                }
            )
        return out
    finally:
        if should_close:
            con.close()


def latest_statistical_evidence_decision(*, model_id: str, con=None) -> Dict[str, Any]:
    """Return the latest promotion evidence attempt and aggregate pass/fail."""

    rows = fetch_latest_statistical_evidence(model_id=str(model_id or ""), limit=100, con=con)
    if not rows:
        return {"decision": "missing", "rows": [], "latest_ts": None, "passed": False}
    latest_ts = int(rows[0].get("ts") or 0)
    latest_rows = [row for row in rows if int(row.get("ts") or 0) == latest_ts]
    passed = bool(latest_rows) and all(str(row.get("decision") or "").lower() == "pass" for row in latest_rows)
    return {
        "decision": "pass" if passed else "fail",
        "rows": latest_rows,
        "latest_ts": int(latest_ts),
        "passed": bool(passed),
    }


def latest_feature_statistical_evidence_decision(
    *,
    feature_id: str,
    q_threshold: float = 0.10,
    con=None,
) -> Dict[str, Any]:
    """Return whether a feature has latest passing factor evidence."""

    feature_key = str(feature_id or "").strip()
    should_close = False
    if con is None:
        init_db()
        con = connect(readonly=True)
        should_close = True
    try:
        include_hashes = _table_has_column(con, "promotion_statistical_evidence", "row_hash")
        hash_select = ", prev_hash, row_hash" if include_hashes else ""
        row = con.execute(
            f"""
            SELECT
              id, ts, model_id, feature_id, test_name, t_stat, p_value,
              q_value, bootstrap_samples, decision, payload_json{hash_select}
            FROM promotion_statistical_evidence
            WHERE feature_id=?
              AND test_name='harvey_liu_zhu_factor_threshold'
            ORDER BY ts DESC, id DESC
            LIMIT 1
            """,
            (str(feature_key),),
        ).fetchone()
        if not row:
            return {"decision": "missing", "row": None, "passed": False}
        try:
            payload = json.loads(row[10] or "{}")
        except Exception:
            payload = {}
        q_value = _finite_or_none(row[7])
        passed = bool(
            str(row[9] or "").strip().lower() == "pass"
            and q_value is not None
            and float(q_value) < float(q_threshold)
        )
        evidence = {
            "id": int(row[0] or 0),
            "ts": int(row[1] or 0),
            "model_id": str(row[2] or ""),
            "feature_id": (None if row[3] is None else str(row[3])),
            "test_name": str(row[4] or ""),
            "t_stat": row[5],
            "p_value": row[6],
            "q_value": row[7],
            "bootstrap_samples": row[8],
            "decision": str(row[9] or ""),
            "payload": payload if isinstance(payload, dict) else {},
        }
        if include_hashes:
            evidence["prev_hash"] = _hash_hex(row[11])
            evidence["row_hash"] = _hash_hex(row[12])
        return {
            "decision": "pass" if passed else "fail",
            "row": evidence,
            "passed": bool(passed),
        }
    finally:
        if should_close:
            con.close()
