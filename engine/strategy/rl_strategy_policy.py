"""
FILE: rl_strategy_policy.py

Stores training artifacts for a lightweight RL-style policy. These artifacts
are explicitly training/shadow-only and must not drive live trading decisions.
"""

import logging
import io
import json
import time
from typing import Dict, Tuple, Optional

import numpy as np

from engine.artifacts.store import LocalArtifactStore
from engine.runtime import dbapi_compat as dbapi
from engine.runtime.storage import connect

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rl_strategy_policy_models (
  policy_name TEXT PRIMARY KEY,      -- e.g. 'v1'
  ts_ms INTEGER NOT NULL,
  n INTEGER NOT NULL,
  dim INTEGER NOT NULL,
  model_blob BLOB,                   -- legacy fallback; artifact store is source of record
  artifact_sha256 TEXT,
  artifact_alias TEXT,
  meta_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_rl_strategy_policy_models_ts ON rl_strategy_policy_models(ts_ms);

CREATE TABLE IF NOT EXISTS rl_strategy_policy_decisions (
  ts_ms INTEGER NOT NULL,
  rule_choice TEXT NOT NULL,
  rl_choice TEXT NOT NULL,
  used_choice TEXT NOT NULL,
  rl_score REAL NOT NULL,            -- signed score (positive favors conservative)
  features_json TEXT NOT NULL,
  context_json TEXT NOT NULL,
  PRIMARY KEY (ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_rl_strategy_policy_decisions_ts ON rl_strategy_policy_decisions(ts_ms);
"""

DEFAULT_POLICY_NAME = "v1"

def init_rl_policy_db() -> None:
    con = connect()
    try:
        con.executescript(_SCHEMA)
        _ensure_rl_policy_artifact_columns(con)
        con.commit()
    finally:
        con.close()


def _ensure_rl_policy_artifact_columns(con) -> None:
    for sql in (
        "ALTER TABLE rl_strategy_policy_models ADD COLUMN artifact_sha256 TEXT",
        "ALTER TABLE rl_strategy_policy_models ADD COLUMN artifact_alias TEXT",
    ):
        try:
            con.execute(sql)
        except Exception:
            logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)

def _serialize_linear(weights: np.ndarray, bias: float) -> bytes:
    w = np.asarray(weights, dtype=np.float32).reshape(-1)
    b = np.asarray([float(bias)], dtype=np.float32)
    buf = io.BytesIO()
    buf.write(np.int32(w.shape[0]).tobytes())
    buf.write(w.tobytes())
    buf.write(b.tobytes())
    return buf.getvalue()

def _deserialize_linear(blob: bytes) -> Tuple[np.ndarray, float]:
    b = memoryview(blob)
    dim = int(np.frombuffer(b[:4], dtype=np.int32)[0])
    off = 4
    w = np.frombuffer(b[off:off + dim * 4], dtype=np.float32).copy()
    off += dim * 4
    bias = float(np.frombuffer(b[off:off + 4], dtype=np.float32)[0])
    return w, bias


def _policy_artifact_alias(policy_name: str) -> str:
    name = str(policy_name or DEFAULT_POLICY_NAME).strip() or DEFAULT_POLICY_NAME
    return f"model:rl_strategy_policy:{name}:current"


def _load_artifact_blob(alias: str, sha256: str) -> bytes:
    store = LocalArtifactStore()
    ref = store.resolve(alias) if str(alias or "").strip() else None
    if ref is None and str(sha256 or "").strip():
        from datetime import datetime, timezone

        from engine.artifacts.refs import ArtifactRef

        ref = ArtifactRef(
            sha256=str(sha256).strip(),
            size=0,
            content_type="application/octet-stream",
            kind="model",
            created_ts=datetime.now(timezone.utc),
            metadata={},
        )
    if ref is None:
        return b""
    return store.get_bytes(ref)

def upsert_policy(policy_name: str, weights: np.ndarray, bias: float, n: int, feature_names) -> None:
    """
    Stores policy in SQLite. Idempotent by policy_name.
    """
    init_rl_policy_db()
    now_ms = int(time.time() * 1000)
    w = np.asarray(weights, dtype=np.float32).reshape(-1)
    blob = _serialize_linear(w, float(bias))
    meta = {
        "feature_names": list(feature_names or []),
        "type": "linear",
        "training_only": True,
        "live_execution_allowed": False,
    }
    artifact_alias = _policy_artifact_alias(policy_name)
    artifact_ref = LocalArtifactStore().put(
        blob,
        content_type="application/octet-stream",
        kind="model",
        alias=artifact_alias,
        metadata={
            "model_name": "rl_strategy_policy",
            "policy_name": str(policy_name),
            "ts_ms": int(now_ms),
            "n": int(n),
            "dim": int(w.shape[0]),
            "feature_names": list(feature_names or []),
            "training_only": True,
        },
    )

    con = connect()
    try:
        _ensure_rl_policy_artifact_columns(con)
        con.execute(
            """
            INSERT INTO rl_strategy_policy_models(
              policy_name, ts_ms, n, dim, model_blob, artifact_sha256, artifact_alias, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(policy_name) DO UPDATE SET
              ts_ms=excluded.ts_ms,
              n=excluded.n,
              dim=excluded.dim,
              model_blob=excluded.model_blob,
              artifact_sha256=excluded.artifact_sha256,
              artifact_alias=excluded.artifact_alias,
              meta_json=excluded.meta_json
            """,
            (
                str(policy_name),
                int(now_ms),
                int(n),
                int(w.shape[0]),
                dbapi.Binary(b""),
                artifact_ref.sha256,
                artifact_alias,
                json.dumps(meta, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()

def load_policy(policy_name: str = DEFAULT_POLICY_NAME) -> Optional[Dict]:
    init_rl_policy_db()
    con = connect()
    try:
        row = con.execute(
            """
            SELECT ts_ms, n, dim, model_blob, meta_json, artifact_sha256, artifact_alias
            FROM rl_strategy_policy_models
            WHERE policy_name=?
            """,
            (str(policy_name),),
        ).fetchone()
        if not row:
            return None
        ts_ms, n, dim, blob, meta_json, artifact_sha256, artifact_alias = row
        try:
            meta = json.loads(meta_json) if meta_json else {}
        except Exception:
            meta = {}
        blob_bytes = bytes(blob or b"")
        if not blob_bytes and (artifact_alias or artifact_sha256):
            blob_bytes = _load_artifact_blob(str(artifact_alias or ""), str(artifact_sha256 or ""))
        w, bias = _deserialize_linear(blob_bytes)
        return {
            "policy_name": str(policy_name),
            "ts_ms": int(ts_ms),
            "n": int(n),
            "dim": int(dim),
            "weights": w,
            "bias": float(bias),
            "feature_names": list(meta.get("feature_names") or []),
            "artifact_sha256": str(artifact_sha256 or ""),
            "artifact_alias": str(artifact_alias or ""),
        }
    finally:
        con.close()

def _build_feature_vector(features: Dict, feature_names) -> np.ndarray:
    xs = []
    for k in (feature_names or []):
        try:
            xs.append(float(features.get(k, 0.0) or 0.0))
        except Exception:
            xs.append(0.0)
    return np.asarray(xs, dtype=np.float32)

def predict_strategy(
    features: Dict,
    policy: Optional[Dict],
    *,
    threshold: float = 0.0
) -> Tuple[str, float]:
    """
    Offline/shadow helper only.

    Returns (choice, score).
    score > 0 => favors 'conservative'
    score <= 0 => favors 'baseline'
    """
    if not policy:
        # Safe deterministic fallback heuristic (shadow-only by default):
        # if drawdown is large negative, prefer conservative.
        dd = float(features.get("prev_drawdown", 0.0) or 0.0)
        score = float(-dd)  # more drawdown => higher score
        choice = "conservative" if score > float(threshold) else "baseline"
        return choice, float(score)

    x = _build_feature_vector(features, policy.get("feature_names") or [])
    w = policy["weights"]
    bias = float(policy["bias"])
    if x.shape[0] != w.shape[0]:
        # shape mismatch -> fallback
        return "baseline", 0.0

    score = float(np.dot(w, x) + bias)
    choice = "conservative" if score > float(threshold) else "baseline"
    return choice, float(score)

def log_decision(
    *,
    ts_ms: int,
    rule_choice: str,
    rl_choice: str,
    used_choice: str,
    rl_score: float,
    features: Dict,
    context: Dict,
) -> None:
    init_rl_policy_db()
    con = connect()
    try:
        

        con.execute(
            """
            INSERT OR REPLACE INTO rl_strategy_policy_decisions(
              ts_ms, rule_choice, rl_choice, used_choice, rl_score, features_json, context_json
            )
            VALUES (?,?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                str(rule_choice),
                str(rl_choice),
                str(used_choice),
                float(rl_score),
                json.dumps(features or {}, separators=(",", ":"), sort_keys=True),
                json.dumps(context or {}, separators=(",", ":"), sort_keys=True),
            ),
        )
        con.commit()
    finally:
        con.close()
