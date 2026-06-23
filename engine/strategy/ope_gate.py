"""Doubly robust off-policy evaluation gate for policy promotion.

The gate is intentionally fail-closed for policy challengers.  RL, bandit,
sizing-policy, and execution-policy candidates must provide logged decisions
with behavior propensities, outcomes, and model estimates before they can move
past shadow.
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass
from statistics import NormalDist
from typing import Any, Mapping, Sequence

from engine.audit.chain import append_chain_row
from engine.runtime.dbapi_compat import is_sqlite_connection
from engine.runtime.storage import connect, init_db, table_exists


_NORMAL = NormalDist()
_EPS = 1.0e-12

POLICY_TYPES_REQUIRING_OPE = frozenset(
    {
        "rl",
        "bandit",
        "sizing_policy",
        "execution_policy",
    }
)

_POLICY_TYPE_TOKENS: dict[str, tuple[str, ...]] = {
    "rl": (
        "rl",
        "reinforcement",
        "ppo",
        "sac",
        "a2c",
        "ddpg",
        "portfolio_env",
        "rl_portfolio",
        "rl_strategy_policy",
    ),
    "bandit": (
        "bandit",
        "contextual_bandit",
        "thompson",
        "ucb",
        "epsilon_greedy",
        "exp3",
    ),
    "sizing_policy": (
        "sizing_policy",
        "size_policy",
        "position_sizing",
        "uncertainty_sizing",
        "drawdown_policy",
        "capital_sizing",
    ),
    "execution_policy": (
        "execution_policy",
        "execution-policy",
        "execution_policy_engine",
        "routing_policy",
        "execution_slicing",
        "broker_failover_policy",
        "order_slicer",
    ),
}

_PROPENSITY_KEYS = (
    "behavior_propensity",
    "logging_propensity",
    "logged_propensity",
    "decision_propensity",
    "propensity",
)
_TARGET_PROPENSITY_KEYS = (
    "target_propensity",
    "candidate_propensity",
    "evaluation_propensity",
    "policy_propensity",
)
_OUTCOME_KEYS = (
    "outcome",
    "reward",
    "net_return",
    "net_ret",
    "realized_return",
    "realized_ret",
    "net_pnl",
    "pnl",
)
_LOGGED_MODEL_KEYS = (
    "logged_model_estimate",
    "behavior_model_estimate",
    "q_logged",
    "q_hat_logged",
    "model_estimate",
    "q_hat",
)
_TARGET_MODEL_KEYS = (
    "target_model_estimate",
    "candidate_model_estimate",
    "policy_model_estimate",
    "q_target",
    "q_hat_target",
)


@dataclass(frozen=True)
class OPEConfig:
    enabled: bool = True
    required: bool = True
    min_obs: int = 50
    min_effective_n: float = 25.0
    min_support: float = 0.80
    max_importance_weight: float = 20.0
    confidence_z: float = 1.645
    min_policy_value_lower_bound: float = 0.0
    max_standard_error: float = 0.05
    max_ci_width: float = 0.20
    max_model_optimism: float = 0.05
    lookback_ms: int = 90 * 24 * 60 * 60 * 1000
    min_behavior_propensity: float = 1.0e-6

    def to_dict(self) -> dict[str, Any]:
        return {
            "enabled": bool(self.enabled),
            "required": bool(self.required),
            "min_obs": int(self.min_obs),
            "min_effective_n": float(self.min_effective_n),
            "min_support": float(self.min_support),
            "max_importance_weight": float(self.max_importance_weight),
            "confidence_z": float(self.confidence_z),
            "min_policy_value_lower_bound": float(self.min_policy_value_lower_bound),
            "max_standard_error": float(self.max_standard_error),
            "max_ci_width": float(self.max_ci_width),
            "max_model_optimism": float(self.max_model_optimism),
            "lookback_ms": int(self.lookback_ms),
            "min_behavior_propensity": float(self.min_behavior_propensity),
        }


@dataclass(frozen=True)
class OPEObservation:
    ts_ms: int
    candidate_key: str
    model_id: str
    model_name: str
    candidate_type: str
    candidate_version: str
    symbol: str
    horizon_s: int
    regime: str
    logged_action: str
    target_action: str
    behavior_propensity: float | None
    target_propensity: float | None
    outcome: float | None
    logged_model_estimate: float | None
    target_model_estimate: float | None
    source_table: str
    source_id: str
    meta: dict[str, Any]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_bool_env(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "1" if default else "0") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "y", "on"}


def _safe_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return int(default)
    try:
        return int(raw)
    except Exception:
        return int(default)


def _safe_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if raw in (None, ""):
        return float(default)
    try:
        out = float(raw)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def ope_config_from_env(config: Mapping[str, Any] | None = None) -> OPEConfig:
    overrides = dict(config or {})
    alpha = overrides.get("alpha")
    if alpha is None:
        alpha = _safe_float_env("PROMOTION_OPE_ALPHA", 0.10)
    try:
        confidence_z = float(overrides.get("confidence_z", _NORMAL.inv_cdf(1.0 - (float(alpha) / 2.0))))
    except Exception:
        confidence_z = _safe_float_env("PROMOTION_OPE_CONFIDENCE_Z", 1.645)
    return OPEConfig(
        enabled=bool(overrides.get("enabled", _safe_bool_env("PROMOTION_OPE_ENABLED", True))),
        required=bool(overrides.get("required", _safe_bool_env("PROMOTION_OPE_REQUIRED", True))),
        min_obs=max(1, int(overrides.get("min_obs", _safe_int_env("PROMOTION_OPE_MIN_OBS", 50)))),
        min_effective_n=max(
            1.0,
            float(overrides.get("min_effective_n", _safe_float_env("PROMOTION_OPE_MIN_EFFECTIVE_N", 25.0))),
        ),
        min_support=max(
            0.0,
            min(1.0, float(overrides.get("min_support", _safe_float_env("PROMOTION_OPE_MIN_SUPPORT", 0.80)))),
        ),
        max_importance_weight=max(
            1.0,
            float(
                overrides.get(
                    "max_importance_weight",
                    _safe_float_env("PROMOTION_OPE_MAX_IMPORTANCE_WEIGHT", 20.0),
                )
            ),
        ),
        confidence_z=max(0.0, float(confidence_z)),
        min_policy_value_lower_bound=float(
            overrides.get(
                "min_policy_value_lower_bound",
                _safe_float_env("PROMOTION_OPE_MIN_POLICY_VALUE_LOWER_BOUND", 0.0),
            )
        ),
        max_standard_error=max(
            0.0,
            float(overrides.get("max_standard_error", _safe_float_env("PROMOTION_OPE_MAX_STANDARD_ERROR", 0.05))),
        ),
        max_ci_width=max(
            0.0,
            float(overrides.get("max_ci_width", _safe_float_env("PROMOTION_OPE_MAX_CI_WIDTH", 0.20))),
        ),
        max_model_optimism=max(
            0.0,
            float(overrides.get("max_model_optimism", _safe_float_env("PROMOTION_OPE_MAX_MODEL_OPTIMISM", 0.05))),
        ),
        lookback_ms=max(0, int(overrides.get("lookback_ms", _safe_int_env("PROMOTION_OPE_LOOKBACK_MS", 90 * 24 * 60 * 60 * 1000)))),
        min_behavior_propensity=max(
            _EPS,
            float(
                overrides.get(
                    "min_behavior_propensity",
                    _safe_float_env("PROMOTION_OPE_MIN_BEHAVIOR_PROPENSITY", 1.0e-6),
                )
            ),
        ),
    )


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _json_load_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        raw = value.decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) else str(value)
        obj = json.loads(raw)
    except Exception:
        return {}
    return dict(obj) if isinstance(obj, dict) else {}


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    return float(out) if math.isfinite(out) else None


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _first_float(payload: Mapping[str, Any], keys: Sequence[str]) -> float | None:
    for key in keys:
        if key in payload:
            out = _as_float(payload.get(key))
            if out is not None:
                return float(out)
    return None


def _first_text(payload: Mapping[str, Any], keys: Sequence[str]) -> str:
    for key in keys:
        value = payload.get(key)
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _nested_ope_payload(*payloads: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        out.update(dict(payload))
        for key in ("ope", "off_policy_evaluation", "off_policy", "policy_ope"):
            nested = payload.get(key)
            if isinstance(nested, Mapping):
                out.update(dict(nested))
    return out


def infer_policy_type(
    *,
    candidate_type: Any = None,
    model_kind: Any = None,
    model_name: Any = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    """Return the normalized policy type that requires OPE, or ``""``."""

    meta = dict(metadata or {})
    values: list[str] = []
    for value in (
        candidate_type,
        model_kind,
        meta.get("policy_type"),
        meta.get("candidate_type"),
        meta.get("model_kind"),
        meta.get("model_family"),
        meta.get("family"),
        meta.get("source"),
        meta.get("strategy_type"),
        model_name,
    ):
        text = str(value or "").strip().lower()
        if text:
            values.append(text)
    for policy_type, tokens in _POLICY_TYPE_TOKENS.items():
        for value in values:
            normalized = value.replace("-", "_")
            if normalized == policy_type:
                return str(policy_type)
            if any(token in normalized for token in tokens):
                return str(policy_type)
    return ""


def policy_type_requires_ope(policy_type: str) -> bool:
    return str(policy_type or "").strip().lower() in POLICY_TYPES_REQUIRING_OPE


def candidate_key_for(
    *,
    model_id: Any = None,
    model_name: Any = None,
    candidate_version: Any = None,
    metadata: Mapping[str, Any] | None = None,
) -> str:
    meta = dict(metadata or {})
    for value in (
        meta.get("candidate_key"),
        meta.get("policy_id"),
        meta.get("model_id"),
        model_id,
    ):
        text = str(value or "").strip()
        if text:
            return text
    name = str(model_name or meta.get("model_name") or "").strip()
    version = str(candidate_version or meta.get("candidate_version") or meta.get("model_version") or "").strip()
    if name and version:
        return f"{name}:{version}"
    return name


def _table_columns(con: Any, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception:
        return set()
    return {str(row[1] or "").strip() for row in rows if row and len(row) > 1 and str(row[1] or "").strip()}


def _table_exists(con: Any, table_name: str) -> bool:
    try:
        return bool(table_exists(con, str(table_name)))
    except Exception:
        return bool(_table_columns(con, str(table_name)))


def ensure_ope_schema(con: Any) -> None:
    if is_sqlite_connection(con):
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS policy_ope_observations (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              candidate_key TEXT,
              model_id TEXT,
              model_name TEXT NOT NULL,
              candidate_type TEXT NOT NULL,
              candidate_version TEXT,
              symbol TEXT,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              regime TEXT NOT NULL DEFAULT 'global',
              logged_action TEXT,
              target_action TEXT,
              behavior_propensity REAL,
              target_propensity REAL,
              outcome REAL,
              logged_model_estimate REAL,
              target_model_estimate REAL,
              source_table TEXT,
              source_id TEXT,
              meta_json TEXT NOT NULL DEFAULT '{}',
              prev_hash BLOB,
              row_hash BLOB
            );

            CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_candidate_ts
              ON policy_ope_observations(candidate_key, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_model_ts
              ON policy_ope_observations(model_id, model_name, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_scope_ts
              ON policy_ope_observations(symbol, horizon_s, regime, ts_ms DESC);

            CREATE TABLE IF NOT EXISTS policy_ope_evidence (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts_ms INTEGER NOT NULL,
              candidate_key TEXT,
              model_id TEXT,
              model_name TEXT NOT NULL,
              candidate_type TEXT NOT NULL,
              candidate_version TEXT,
              symbol TEXT,
              horizon_s INTEGER NOT NULL DEFAULT 0,
              regime TEXT NOT NULL DEFAULT 'global',
              policy_value REAL,
              standard_error REAL,
              ci_lower REAL,
              ci_upper REAL,
              n_obs INTEGER NOT NULL DEFAULT 0,
              effective_n REAL NOT NULL DEFAULT 0.0,
              support REAL NOT NULL DEFAULT 0.0,
              max_importance_weight REAL NOT NULL DEFAULT 0.0,
              confidence_z REAL NOT NULL DEFAULT 0.0,
              decision TEXT NOT NULL,
              reason TEXT NOT NULL,
              config_json TEXT NOT NULL DEFAULT '{}',
              diagnostics_json TEXT NOT NULL DEFAULT '{}',
              prev_hash BLOB,
              row_hash BLOB
            );

            CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_candidate_ts
              ON policy_ope_evidence(candidate_key, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_model_ts
              ON policy_ope_evidence(model_id, model_name, ts_ms DESC);
            CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_decision_ts
              ON policy_ope_evidence(decision, ts_ms DESC);
            """
        )
        return

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_ope_observations (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          candidate_key TEXT,
          model_id TEXT,
          model_name TEXT NOT NULL,
          candidate_type TEXT NOT NULL,
          candidate_version TEXT,
          symbol TEXT,
          horizon_s BIGINT NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          logged_action TEXT,
          target_action TEXT,
          behavior_propensity DOUBLE PRECISION,
          target_propensity DOUBLE PRECISION,
          outcome DOUBLE PRECISION,
          logged_model_estimate DOUBLE PRECISION,
          target_model_estimate DOUBLE PRECISION,
          source_table TEXT,
          source_id TEXT,
          meta_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          prev_hash BYTEA,
          row_hash BYTEA
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_candidate_ts
          ON policy_ope_observations(candidate_key, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_model_ts
          ON policy_ope_observations(model_id, model_name, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_obs_scope_ts
          ON policy_ope_observations(symbol, horizon_s, regime, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_ope_evidence (
          id BIGSERIAL PRIMARY KEY,
          ts_ms BIGINT NOT NULL,
          candidate_key TEXT,
          model_id TEXT,
          model_name TEXT NOT NULL,
          candidate_type TEXT NOT NULL,
          candidate_version TEXT,
          symbol TEXT,
          horizon_s BIGINT NOT NULL DEFAULT 0,
          regime TEXT NOT NULL DEFAULT 'global',
          policy_value DOUBLE PRECISION,
          standard_error DOUBLE PRECISION,
          ci_lower DOUBLE PRECISION,
          ci_upper DOUBLE PRECISION,
          n_obs BIGINT NOT NULL DEFAULT 0,
          effective_n DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          support DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          max_importance_weight DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          confidence_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
          decision TEXT NOT NULL,
          reason TEXT NOT NULL,
          config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          diagnostics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          prev_hash BYTEA,
          row_hash BYTEA
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_candidate_ts
          ON policy_ope_evidence(candidate_key, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_model_ts
          ON policy_ope_evidence(model_id, model_name, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_policy_ope_evidence_decision_ts
          ON policy_ope_evidence(decision, ts_ms DESC)
        """
    )


def record_policy_ope_observation(
    *,
    model_name: str,
    candidate_type: str,
    model_id: str | None = None,
    candidate_key: str | None = None,
    candidate_version: str | None = None,
    symbol: str | None = None,
    horizon_s: int = 0,
    regime: str = "global",
    logged_action: str | None = None,
    target_action: str | None = None,
    behavior_propensity: float | None = None,
    target_propensity: float | None = None,
    outcome: float | None = None,
    logged_model_estimate: float | None = None,
    target_model_estimate: float | None = None,
    source_table: str | None = None,
    source_id: str | None = None,
    meta: Mapping[str, Any] | None = None,
    ts_ms: int | None = None,
    con=None,
) -> int:
    owns = con is None
    if owns:
        init_db()
        con = connect()
    try:
        ensure_ope_schema(con)
        metadata = dict(meta or {})
        key = str(candidate_key or "").strip() or candidate_key_for(
            model_id=model_id,
            model_name=model_name,
            candidate_version=candidate_version,
            metadata=metadata,
        )
        row = {
            "ts_ms": int(ts_ms if ts_ms is not None else _now_ms()),
            "candidate_key": str(key),
            "model_id": str(model_id or ""),
            "model_name": str(model_name),
            "candidate_type": str(candidate_type),
            "candidate_version": (str(candidate_version) if candidate_version is not None else None),
            "symbol": (str(symbol).upper().strip() if symbol else None),
            "horizon_s": int(horizon_s or 0),
            "regime": str(regime or "global"),
            "logged_action": (str(logged_action) if logged_action is not None else None),
            "target_action": (str(target_action) if target_action is not None else None),
            "behavior_propensity": _as_float(behavior_propensity),
            "target_propensity": _as_float(target_propensity),
            "outcome": _as_float(outcome),
            "logged_model_estimate": _as_float(logged_model_estimate),
            "target_model_estimate": _as_float(target_model_estimate),
            "source_table": (str(source_table) if source_table else None),
            "source_id": (str(source_id) if source_id else None),
            "meta_json": metadata,
        }
        result = append_chain_row("policy_ope_observations", row, con)
        if owns:
            con.commit()
        return int(result.row_id or 0)
    finally:
        if owns and con is not None:
            con.close()


def _canonical_observations(
    con: Any,
    *,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    min_ts_ms: int,
) -> list[OPEObservation]:
    if not _table_exists(con, "policy_ope_observations"):
        return []
    where = ["ts_ms >= ?"]
    params: list[Any] = [int(min_ts_ms)]
    keys = [str(candidate_key or "").strip(), str(model_id or "").strip(), str(model_name or "").strip()]
    key_clauses: list[str] = []
    if keys[0]:
        key_clauses.append("candidate_key=?")
        params.append(keys[0])
    if keys[1]:
        key_clauses.append("model_id=?")
        params.append(keys[1])
    if keys[2]:
        key_clauses.append("model_name=?")
        params.append(keys[2])
    if key_clauses:
        where.append("(" + " OR ".join(key_clauses) + ")")
    if candidate_type:
        where.append("candidate_type=?")
        params.append(str(candidate_type))
    if candidate_version:
        where.append("(candidate_version=? OR candidate_version IS NULL OR candidate_version='')")
        params.append(str(candidate_version))
    if symbol:
        where.append("(UPPER(COALESCE(symbol,''))=UPPER(?) OR COALESCE(symbol,'')='')")
        params.append(str(symbol))
    if int(horizon_s or 0) > 0:
        where.append("(horizon_s=? OR horizon_s=0)")
        params.append(int(horizon_s))
    if regime:
        where.append("(regime=? OR regime='global' OR COALESCE(regime,'')='')")
        params.append(str(regime))
    rows = con.execute(
        f"""
        SELECT
          id, ts_ms, candidate_key, model_id, model_name, candidate_type,
          candidate_version, symbol, horizon_s, regime, logged_action,
          target_action, behavior_propensity, target_propensity, outcome,
          logged_model_estimate, target_model_estimate, source_table, source_id,
          meta_json
        FROM policy_ope_observations
        WHERE {" AND ".join(where)}
        ORDER BY ts_ms ASC, id ASC
        """,
        tuple(params),
    ).fetchall() or []
    out: list[OPEObservation] = []
    for row in rows:
        out.append(
            OPEObservation(
                ts_ms=_as_int(row[1]),
                candidate_key=str(row[2] or ""),
                model_id=str(row[3] or ""),
                model_name=str(row[4] or ""),
                candidate_type=str(row[5] or ""),
                candidate_version=str(row[6] or ""),
                symbol=str(row[7] or ""),
                horizon_s=_as_int(row[8]),
                regime=str(row[9] or "global"),
                logged_action=str(row[10] or ""),
                target_action=str(row[11] or ""),
                behavior_propensity=_as_float(row[12]),
                target_propensity=_as_float(row[13]),
                outcome=_as_float(row[14]),
                logged_model_estimate=_as_float(row[15]),
                target_model_estimate=_as_float(row[16]),
                source_table=str(row[17] or "policy_ope_observations"),
                source_id=str(row[18] or row[0] or ""),
                meta=_json_load_dict(row[19]),
            )
        )
    return out


def _target_propensity_from_actions(payload: Mapping[str, Any]) -> float | None:
    value = _first_float(payload, _TARGET_PROPENSITY_KEYS)
    if value is not None:
        return float(value)
    logged_action = _first_text(payload, ("logged_action", "behavior_action", "action", "side"))
    target_action = _first_text(payload, ("target_action", "candidate_action", "policy_action"))
    if logged_action and target_action:
        return 1.0 if logged_action.strip().lower() == target_action.strip().lower() else 0.0
    return None


def _observation_from_payload(
    *,
    ts_ms: Any,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    payload: Mapping[str, Any],
    source_table: str,
    source_id: Any,
    outcome_fallback: Any = None,
) -> OPEObservation:
    merged = _nested_ope_payload(payload)
    logged_action = _first_text(merged, ("logged_action", "behavior_action", "action", "side"))
    target_action = _first_text(merged, ("target_action", "candidate_action", "policy_action"))
    outcome = _first_float(merged, _OUTCOME_KEYS)
    if outcome is None:
        outcome = _as_float(outcome_fallback)
    logged_model_estimate = _first_float(merged, _LOGGED_MODEL_KEYS)
    target_model_estimate = _first_float(merged, _TARGET_MODEL_KEYS)
    if target_model_estimate is None and logged_action and target_action and logged_action.lower() == target_action.lower():
        target_model_estimate = logged_model_estimate
    return OPEObservation(
        ts_ms=_as_int(ts_ms),
        candidate_key=str(candidate_key),
        model_id=str(model_id),
        model_name=str(model_name),
        candidate_type=str(candidate_type),
        candidate_version=str(candidate_version),
        symbol=str(symbol or "").upper().strip(),
        horizon_s=int(horizon_s or 0),
        regime=str(regime or "global"),
        logged_action=str(logged_action),
        target_action=str(target_action),
        behavior_propensity=_first_float(merged, _PROPENSITY_KEYS),
        target_propensity=_target_propensity_from_actions(merged),
        outcome=outcome,
        logged_model_estimate=logged_model_estimate,
        target_model_estimate=target_model_estimate,
        source_table=str(source_table),
        source_id=str(source_id or ""),
        meta=dict(merged),
    )


def _json_column_value(row: Any, index: int) -> dict[str, Any]:
    try:
        return _json_load_dict(row[index])
    except Exception:
        return {}


def _shadow_prediction_observations(
    con: Any,
    *,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    min_ts_ms: int,
) -> list[OPEObservation]:
    if not _table_exists(con, "shadow_predictions"):
        return []
    cols = _table_columns(con, "shadow_predictions")
    if "extra_json" not in cols or "model_name" not in cols:
        return []
    has_labels = _table_exists(con, "labels_exec")
    outcome_sql = "le.net_ret" if has_labels else "NULL"
    join_sql = (
        """
        LEFT JOIN labels_exec le
          ON le.event_id = sp.event_id
         AND le.symbol = sp.symbol
         AND le.horizon_s = sp.horizon_s
        """
        if has_labels
        else ""
    )
    where = ["sp.ts_ms >= ?", "sp.model_name = ?"]
    params: list[Any] = [int(min_ts_ms), str(model_name)]
    if symbol:
        where.append("UPPER(sp.symbol)=UPPER(?)")
        params.append(str(symbol))
    if int(horizon_s or 0) > 0:
        where.append("sp.horizon_s=?")
        params.append(int(horizon_s))
    if regime and "regime" in cols:
        where.append("(sp.regime=? OR sp.regime='global' OR sp.regime IS NULL)")
        params.append(str(regime))
    rows = con.execute(
        f"""
        SELECT sp.id, sp.ts_ms, sp.event_id, sp.symbol, sp.horizon_s,
               {("sp.regime" if "regime" in cols else "'global'")} AS regime,
               sp.extra_json, {outcome_sql} AS outcome
        FROM shadow_predictions sp
        {join_sql}
        WHERE {" AND ".join(where)}
        ORDER BY sp.ts_ms ASC, sp.id ASC
        """,
        tuple(params),
    ).fetchall() or []
    out: list[OPEObservation] = []
    for row in rows:
        payload = _json_column_value(row, 6)
        meta = _json_load_dict(payload.get("meta"))
        payload = {**payload, **meta}
        row_policy_type = infer_policy_type(
            candidate_type=candidate_type,
            model_name=model_name,
            metadata=payload,
        )
        if row_policy_type and row_policy_type != candidate_type:
            continue
        out.append(
            _observation_from_payload(
                ts_ms=row[1],
                candidate_key=candidate_key,
                model_id=model_id,
                model_name=model_name,
                candidate_type=candidate_type,
                candidate_version=candidate_version,
                symbol=str(row[3] or ""),
                horizon_s=_as_int(row[4]),
                regime=str(row[5] or "global"),
                payload=payload,
                source_table="shadow_predictions",
                source_id=row[0],
                outcome_fallback=row[7],
            )
        )
    return out


def _execution_policy_observations(
    con: Any,
    *,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    min_ts_ms: int,
) -> list[OPEObservation]:
    if not _table_exists(con, "execution_policy_audit"):
        return []
    cols = _table_columns(con, "execution_policy_audit")
    if "policy_json" not in cols:
        return []
    where = ["ts_ms >= ?"]
    params: list[Any] = [int(min_ts_ms)]
    if "model_id" in cols and model_id:
        where.append("model_id=?")
        params.append(str(model_id))
    elif model_name:
        like = f"%{str(model_name)}%"
        json_match_terms = ["CAST(policy_json AS TEXT) LIKE ?"]
        params.append(like)
        if "decision_json" in cols:
            json_match_terms.append("CAST(decision_json AS TEXT) LIKE ?")
            params.append(like)
        where.append("(" + " OR ".join(json_match_terms) + ")")
    if symbol and "symbol" in cols:
        where.append("(UPPER(symbol)=UPPER(?) OR COALESCE(symbol,'')='')")
        params.append(str(symbol))
    select_decision = "decision_json" if "decision_json" in cols else "NULL"
    rows = con.execute(
        f"""
        SELECT id, ts_ms, symbol, side, qty, policy_json, {select_decision}
        FROM execution_policy_audit
        WHERE {" AND ".join(where)}
        ORDER BY ts_ms ASC, id ASC
        """,
        tuple(params),
    ).fetchall() or []
    out: list[OPEObservation] = []
    for row in rows:
        policy = _json_load_dict(row[5])
        decision = _json_load_dict(row[6])
        payload = {**policy, **decision}
        if infer_policy_type(candidate_type=candidate_type, model_name=model_name, metadata=payload) != candidate_type:
            continue
        payload.setdefault("logged_action", str(row[3] or ""))
        payload.setdefault("target_action", str(row[3] or ""))
        out.append(
            _observation_from_payload(
                ts_ms=row[1],
                candidate_key=candidate_key,
                model_id=model_id,
                model_name=model_name,
                candidate_type=candidate_type,
                candidate_version=candidate_version,
                symbol=str(row[2] or symbol or ""),
                horizon_s=int(horizon_s or 0),
                regime=str(regime or "global"),
                payload=payload,
                source_table="execution_policy_audit",
                source_id=row[0],
            )
        )
    return out


def _challenger_shadow_order_observations(
    con: Any,
    *,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    min_ts_ms: int,
) -> list[OPEObservation]:
    if not _table_exists(con, "challenger_shadow_orders"):
        return []
    cols = _table_columns(con, "challenger_shadow_orders")
    if "meta_json" not in cols:
        return []
    where = ["ts_ms >= ?", "model_name=?"]
    params: list[Any] = [int(min_ts_ms), str(model_name)]
    if symbol:
        where.append("UPPER(symbol)=UPPER(?)")
        params.append(str(symbol))
    if int(horizon_s or 0) > 0:
        where.append("(horizon_s=? OR horizon_s=0)")
        params.append(int(horizon_s))
    if regime:
        where.append("(regime=? OR regime='global' OR COALESCE(regime,'')='')")
        params.append(str(regime))
    rows = con.execute(
        f"""
        SELECT id, ts_ms, symbol, horizon_s, regime, side, meta_json
        FROM challenger_shadow_orders
        WHERE {" AND ".join(where)}
        ORDER BY ts_ms ASC, id ASC
        """,
        tuple(params),
    ).fetchall() or []
    out: list[OPEObservation] = []
    for row in rows:
        payload = _json_load_dict(row[6])
        if infer_policy_type(candidate_type=candidate_type, model_name=model_name, metadata=payload) != candidate_type:
            continue
        payload.setdefault("logged_action", str(row[5] or ""))
        payload.setdefault("target_action", str(row[5] or ""))
        out.append(
            _observation_from_payload(
                ts_ms=row[1],
                candidate_key=candidate_key,
                model_id=model_id,
                model_name=model_name,
                candidate_type=candidate_type,
                candidate_version=candidate_version,
                symbol=str(row[2] or ""),
                horizon_s=_as_int(row[3]),
                regime=str(row[4] or "global"),
                payload=payload,
                source_table="challenger_shadow_orders",
                source_id=row[0],
            )
        )
    return out


def _fetch_observations(
    con: Any,
    *,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    config: OPEConfig,
) -> list[OPEObservation]:
    min_ts_ms = 0
    if int(config.lookback_ms) > 0:
        min_ts_ms = int(_now_ms() - int(config.lookback_ms))
    observations: list[OPEObservation] = []
    observations.extend(
        _canonical_observations(
            con,
            candidate_key=candidate_key,
            model_id=model_id,
            model_name=model_name,
            candidate_type=candidate_type,
            candidate_version=candidate_version,
            symbol=symbol,
            horizon_s=horizon_s,
            regime=regime,
            min_ts_ms=min_ts_ms,
        )
    )
    observations.extend(
        _shadow_prediction_observations(
            con,
            candidate_key=candidate_key,
            model_id=model_id,
            model_name=model_name,
            candidate_type=candidate_type,
            candidate_version=candidate_version,
            symbol=symbol,
            horizon_s=horizon_s,
            regime=regime,
            min_ts_ms=min_ts_ms,
        )
    )
    observations.extend(
        _execution_policy_observations(
            con,
            candidate_key=candidate_key,
            model_id=model_id,
            model_name=model_name,
            candidate_type=candidate_type,
            candidate_version=candidate_version,
            symbol=symbol,
            horizon_s=horizon_s,
            regime=regime,
            min_ts_ms=min_ts_ms,
        )
    )
    observations.extend(
        _challenger_shadow_order_observations(
            con,
            candidate_key=candidate_key,
            model_id=model_id,
            model_name=model_name,
            candidate_type=candidate_type,
            candidate_version=candidate_version,
            symbol=symbol,
            horizon_s=horizon_s,
            regime=regime,
            min_ts_ms=min_ts_ms,
        )
    )
    seen: set[tuple[str, str]] = set()
    deduped: list[OPEObservation] = []
    for obs in observations:
        key = (str(obs.source_table), str(obs.source_id))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(obs)
    return deduped


def _sample_std(values: Sequence[float]) -> float:
    n = len(values)
    if n <= 1:
        return 0.0
    mean = sum(values) / float(n)
    var = sum((value - mean) ** 2 for value in values) / float(max(1, n - 1))
    return math.sqrt(max(0.0, var))


def _finite_or_none(value: float | None) -> float | None:
    if value is None:
        return None
    return float(value) if math.isfinite(float(value)) else None


def _evaluate_observations(observations: Sequence[OPEObservation], config: OPEConfig) -> dict[str, Any]:
    raw_n = int(len(observations))
    missing_behavior_propensity = 0
    missing_target_propensity = 0
    missing_outcome = 0
    missing_logged_estimate = 0
    missing_target_estimate = 0
    excessive_weights = 0
    dr_values: list[float] = []
    weights: list[float] = []
    target_estimates: list[float] = []
    correction_values: list[float] = []
    sources: dict[str, int] = {}

    for obs in observations:
        sources[obs.source_table] = sources.get(obs.source_table, 0) + 1
        behavior_propensity = obs.behavior_propensity
        if behavior_propensity is None or behavior_propensity < float(config.min_behavior_propensity):
            missing_behavior_propensity += 1
            continue
        target_propensity = obs.target_propensity
        if target_propensity is None:
            missing_target_propensity += 1
            continue
        if obs.outcome is None:
            missing_outcome += 1
            continue
        if obs.logged_model_estimate is None:
            missing_logged_estimate += 1
            continue
        if obs.target_model_estimate is None:
            missing_target_estimate += 1
            continue
        weight = max(0.0, float(target_propensity)) / max(float(config.min_behavior_propensity), float(behavior_propensity))
        if weight > float(config.max_importance_weight):
            excessive_weights += 1
        q_target = float(obs.target_model_estimate)
        correction = float(weight) * (float(obs.outcome) - float(obs.logged_model_estimate))
        dr_values.append(float(q_target + correction))
        weights.append(float(weight))
        target_estimates.append(float(q_target))
        correction_values.append(float(correction))

    n_obs = int(len(dr_values))
    weight_sum = sum(weights)
    weight_sq_sum = sum(weight * weight for weight in weights)
    effective_n = float((weight_sum * weight_sum) / weight_sq_sum) if weight_sq_sum > _EPS else 0.0
    positive_weight_n = sum(1 for weight in weights if weight > _EPS)
    support = float(positive_weight_n / float(n_obs)) if n_obs > 0 else 0.0
    max_weight = max(weights) if weights else 0.0
    policy_value = sum(dr_values) / float(n_obs) if n_obs > 0 else None
    direct_method_value = sum(target_estimates) / float(n_obs) if n_obs > 0 else None
    correction_value = sum(correction_values) / float(n_obs) if n_obs > 0 else None
    std = _sample_std(dr_values)
    stderr = None
    if n_obs > 0 and effective_n > _EPS:
        stderr = float(std / math.sqrt(max(1.0, effective_n)))
    ci_lower = None
    ci_upper = None
    if policy_value is not None and stderr is not None:
        ci_lower = float(policy_value - (float(config.confidence_z) * float(stderr)))
        ci_upper = float(policy_value + (float(config.confidence_z) * float(stderr)))
    ci_width = None
    if ci_lower is not None and ci_upper is not None:
        ci_width = float(ci_upper - ci_lower)
    model_optimism = None
    if direct_method_value is not None and policy_value is not None:
        model_optimism = float(direct_method_value - policy_value)

    blockers: list[str] = []
    if raw_n <= 0:
        blockers.append("missing_ope_evidence")
    if raw_n > 0 and missing_behavior_propensity > 0 and n_obs <= 0:
        blockers.append("missing_propensities")
    if raw_n > 0 and missing_target_propensity > 0 and n_obs <= 0:
        blockers.append("missing_target_propensities")
    if raw_n > 0 and missing_outcome > 0 and n_obs <= 0:
        blockers.append("missing_outcomes")
    if raw_n > 0 and (missing_logged_estimate > 0 or missing_target_estimate > 0) and n_obs <= 0:
        blockers.append("missing_model_estimates")
    if n_obs < int(config.min_obs):
        blockers.append("insufficient_observations")
    if effective_n < float(config.min_effective_n):
        blockers.append("insufficient_effective_sample")
    if support < float(config.min_support):
        blockers.append("insufficient_support")
    if max_weight > float(config.max_importance_weight) or excessive_weights > 0:
        blockers.append("excessive_importance_weight")
    if stderr is None or not math.isfinite(float(stderr)) or stderr > float(config.max_standard_error):
        blockers.append("statistically_weak")
    if ci_width is None or not math.isfinite(float(ci_width)) or ci_width > float(config.max_ci_width):
        blockers.append("confidence_interval_too_wide")
    if ci_lower is None or ci_lower < float(config.min_policy_value_lower_bound):
        blockers.append("confidence_bound_breached")
    if model_optimism is not None and model_optimism > float(config.max_model_optimism):
        blockers.append("optimistic_model_estimates")

    blockers = list(dict.fromkeys(blockers))
    return {
        "raw_observations": int(raw_n),
        "n_obs": int(n_obs),
        "effective_n": float(effective_n),
        "support": float(support),
        "max_importance_weight": float(max_weight),
        "policy_value": _finite_or_none(policy_value),
        "direct_method_value": _finite_or_none(direct_method_value),
        "correction_value": _finite_or_none(correction_value),
        "standard_error": _finite_or_none(stderr),
        "ci_lower": _finite_or_none(ci_lower),
        "ci_upper": _finite_or_none(ci_upper),
        "ci_width": _finite_or_none(ci_width),
        "model_optimism": _finite_or_none(model_optimism),
        "missing_behavior_propensity": int(missing_behavior_propensity),
        "missing_target_propensity": int(missing_target_propensity),
        "missing_outcome": int(missing_outcome),
        "missing_logged_model_estimate": int(missing_logged_estimate),
        "missing_target_model_estimate": int(missing_target_estimate),
        "excessive_weight_observations": int(excessive_weights),
        "source_counts": dict(sources),
        "blockers": blockers,
        "passed": not blockers,
        "status": "pass" if not blockers else str(blockers[0]),
    }


def _persist_evidence(
    con: Any,
    *,
    candidate_key: str,
    model_id: str,
    model_name: str,
    candidate_type: str,
    candidate_version: str,
    symbol: str,
    horizon_s: int,
    regime: str,
    config: OPEConfig,
    diagnostics: Mapping[str, Any],
) -> int:
    ensure_ope_schema(con)
    decision = "pass" if bool(diagnostics.get("passed")) else "fail"
    reason = str(diagnostics.get("status") or decision)
    row = {
        "ts_ms": int(_now_ms()),
        "candidate_key": str(candidate_key),
        "model_id": str(model_id or ""),
        "model_name": str(model_name),
        "candidate_type": str(candidate_type),
        "candidate_version": (str(candidate_version) if candidate_version else None),
        "symbol": (str(symbol).upper().strip() if symbol else None),
        "horizon_s": int(horizon_s or 0),
        "regime": str(regime or "global"),
        "policy_value": _as_float(diagnostics.get("policy_value")),
        "standard_error": _as_float(diagnostics.get("standard_error")),
        "ci_lower": _as_float(diagnostics.get("ci_lower")),
        "ci_upper": _as_float(diagnostics.get("ci_upper")),
        "n_obs": int(diagnostics.get("n_obs") or 0),
        "effective_n": float(diagnostics.get("effective_n") or 0.0),
        "support": float(diagnostics.get("support") or 0.0),
        "max_importance_weight": float(diagnostics.get("max_importance_weight") or 0.0),
        "confidence_z": float(config.confidence_z),
        "decision": str(decision),
        "reason": str(reason),
        "config_json": config.to_dict(),
        "diagnostics_json": dict(diagnostics),
    }
    result = append_chain_row("policy_ope_evidence", row, con)
    return int(result.row_id or 0)


def evaluate_policy_ope_gate(
    *,
    model_id: str | None = None,
    model_name: str,
    candidate_type: str | None = None,
    model_kind: str | None = None,
    candidate_version: str | None = None,
    symbol: str | None = None,
    horizon_s: int = 0,
    regime: str = "global",
    metadata: Mapping[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
    persist: bool = True,
    con=None,
) -> tuple[bool, dict[str, Any]]:
    """Evaluate and optionally persist the OPE promotion gate."""

    cfg = ope_config_from_env(config)
    meta = dict(metadata or {})
    policy_type = infer_policy_type(
        candidate_type=candidate_type,
        model_kind=model_kind,
        model_name=model_name,
        metadata=meta,
    )
    key = candidate_key_for(
        model_id=model_id,
        model_name=model_name,
        candidate_version=candidate_version,
        metadata=meta,
    )
    base_payload: dict[str, Any] = {
        "enabled": bool(cfg.enabled),
        "required": bool(cfg.required),
        "applied": False,
        "passed": True,
        "status": "not_policy_candidate",
        "candidate_key": str(key),
        "model_id": str(model_id or ""),
        "model_name": str(model_name),
        "candidate_type": str(policy_type or candidate_type or ""),
        "candidate_version": str(candidate_version or ""),
        "symbol": str(symbol or "").upper().strip(),
        "horizon_s": int(horizon_s or 0),
        "regime": str(regime or "global"),
        "config": cfg.to_dict(),
    }
    if not bool(cfg.enabled):
        base_payload.update({"applied": False, "status": "disabled", "passed": True})
        return True, base_payload
    if not policy_type_requires_ope(policy_type):
        return True, base_payload
    base_payload["applied"] = True
    base_payload["candidate_type"] = str(policy_type)
    if not bool(cfg.required):
        base_payload.update({"status": "not_required", "passed": True})
        return True, base_payload

    owns = con is None
    if owns:
        init_db()
        con = connect()
    try:
        ensure_ope_schema(con)
        observations = _fetch_observations(
            con,
            candidate_key=str(key),
            model_id=str(model_id or ""),
            model_name=str(model_name),
            candidate_type=str(policy_type),
            candidate_version=str(candidate_version or ""),
            symbol=str(symbol or "").upper().strip(),
            horizon_s=int(horizon_s or 0),
            regime=str(regime or "global"),
            config=cfg,
        )
        diagnostics = _evaluate_observations(observations, cfg)
        payload = {**base_payload, **diagnostics}
        if bool(persist):
            evidence_id = _persist_evidence(
                con,
                candidate_key=str(key),
                model_id=str(model_id or ""),
                model_name=str(model_name),
                candidate_type=str(policy_type),
                candidate_version=str(candidate_version or ""),
                symbol=str(symbol or "").upper().strip(),
                horizon_s=int(horizon_s or 0),
                regime=str(regime or "global"),
                config=cfg,
                diagnostics=payload,
            )
            payload["evidence_id"] = int(evidence_id)
        if owns:
            con.commit()
        return bool(payload.get("passed")), payload
    finally:
        if owns and con is not None:
            con.close()
