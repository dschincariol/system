"""Champion/challenger assignment logic for live model competition.

This module evaluates scored challengers, replay validation, self-critic
results, cooldowns, and realized PnL before deciding whether the current
champion should stay live, be replaced by a challenger, or be demoted because
its live behavior has degraded.
"""
import json
import os
import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.model_registry import (
    get_stage_latest,
    register_model,
    update_model_runtime,
)
from engine.runtime.storage import connect, init_db, record_hypothesis_result, run_write_txn
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.runtime_meta import meta_get, meta_set
from engine.strategy.promotion_audit import audit
from engine.strategy.promotion_guard import assess_challenger, evaluate_statistical_promotion_gate
from engine.strategy.model_marketplace import (
    compute_capital_plan,
    get_cached_replay_validation_snapshot,
    recompute_marketplace_scores,
    run_self_critic,
)

PROMOTION_MIN_SCORE = float(os.environ.get("CHAMPION_PROMOTION_MIN_SCORE", "0.0"))
PROMOTION_MIN_TRADES = int(os.environ.get("CHAMPION_PROMOTION_MIN_TRADES", "3"))
PROMOTION_SCORE_MARGIN = float(os.environ.get("CHAMPION_PROMOTION_SCORE_MARGIN", "0.25"))
DEMOTION_SCORE_MARGIN = float(os.environ.get("CHAMPION_DEMOTION_SCORE_MARGIN", "0.50"))
DEMOTION_MIN_NET_PNL = float(os.environ.get("CHAMPION_DEMOTION_MIN_NET_PNL", "-100.0"))
PROMOTION_MIN_NET_PNL_DELTA = float(os.environ.get("CHAMPION_PROMOTION_MIN_NET_PNL_DELTA", "0.0"))
MODEL_COMPETITION_WINDOW_S = int(
    os.environ.get("MODEL_COMPETITION_WINDOW_S", os.environ.get("CHAMPION_COMPETITION_WINDOW_S", "86400"))
)
PROMOTION_MIN_OBSERVATION_S = int(os.environ.get("CHAMPION_PROMOTION_MIN_OBSERVATION_S", "3600"))
PROMOTION_COOLDOWN_S = int(os.environ.get("CHAMPION_PROMOTION_COOLDOWN_S", "3600"))
PROMOTION_MIN_DELTA = float(os.environ.get("CHAMPION_PROMOTION_MIN_DELTA", os.environ.get("CHAMPION_PROMOTION_SCORE_MARGIN", "0.25")))
PROMOTION_MIN_RETURN_PCT_DELTA = float(os.environ.get("CHAMPION_PROMOTION_MIN_RETURN_PCT_DELTA", "0.0"))
STABILITY_MIN_WIN_RATE = float(os.environ.get("CHAMPION_STABILITY_MIN_WIN_RATE", "0.0"))
DEMOTION_MAX_DRAWDOWN = float(os.environ.get("CHAMPION_DEMOTION_MAX_DRAWDOWN", "250.0"))
DEMOTION_DECAY_THRESHOLD = float(os.environ.get("CHAMPION_DEMOTION_DECAY_THRESHOLD", "-50.0"))
REPLAY_FRESH_MAX_AGE_MS = int(os.environ.get("MODEL_REPLAY_FRESH_MAX_AGE_MS", str(15 * 60 * 1000)))
COMPETITION_POST_COMMIT_MAX_ACTIONS = int(os.environ.get("COMPETITION_POST_COMMIT_MAX_ACTIONS", "256"))
COMPETITION_POST_COMMIT_LEASE_MS = int(os.environ.get("COMPETITION_POST_COMMIT_LEASE_MS", "30000"))
COMPETITION_POST_COMMIT_RETRY_MAX_MS = int(os.environ.get("COMPETITION_POST_COMMIT_RETRY_MAX_MS", "60000"))
COMPETITION_CAPITAL_PLAN_MAX_AGE_MS = int(
    os.environ.get("COMPETITION_CAPITAL_PLAN_MAX_AGE_MS", "15000")
)
MODEL_COMPETITION_SCOPE = "model_competition"
MODEL_COMPETITION_SYMBOL = "*"
MODEL_COMPETITION_HORIZON_S = 0
_POST_COMMIT_OUTBOX_TABLE = "competition_post_commit_actions"

_POST_COMMIT_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS {_POST_COMMIT_OUTBOX_TABLE} (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action_name TEXT NOT NULL,
  args_json TEXT NOT NULL,
  kwargs_json TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  created_ts_ms INTEGER NOT NULL,
  updated_ts_ms INTEGER NOT NULL,
  available_ts_ms INTEGER NOT NULL,
  lease_expires_ts_ms INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  last_error_ts_ms INTEGER,
  completed_ts_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_competition_post_commit_status_available
ON {_POST_COMMIT_OUTBOX_TABLE}(status, available_ts_ms, lease_expires_ts_ms, id);
"""

_COMPETITION_LOCK = threading.RLock()
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()
_ASSIGNMENT_STATES = {"shadow", "challenger", "champion", "retired"}
_ALLOWED_ASSIGNMENT_TRANSITIONS = {
    ("shadow", "challenger"),
    ("challenger", "champion"),
    ("champion", "retired"),
    ("challenger", "shadow"),
}


class IllegalChampionTransition(RuntimeError):
    """Raised when a model attempts to bypass the shadow/challenger path."""


def _warn_nonfatal(code: str, error: Exception, *, once_key: str | None = None, **extra: Any) -> None:
    key = str(once_key or "")
    if key:
        if key in _WARNED_NONFATAL_KEYS:
            return
        _WARNED_NONFATAL_KEYS.add(key)
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.strategy.champion_manager",
        extra=extra or None,
        include_health=False,
        persist=False,
    )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(v: Any, default: float = 0.0) -> float:
    if v is None:
        return float(default)
    if isinstance(v, str) and not v.strip():
        return float(default)
    try:
        return float(v)
    except Exception as e:
        _warn_nonfatal("CHAMPION_MANAGER_SAFE_FLOAT_FAILED", e, once_key="safe_float", value=repr(v), default=float(default))
        return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    if v is None:
        return int(default)
    if isinstance(v, str) and not v.strip():
        return int(default)
    try:
        return int(v)
    except Exception as e:
        _warn_nonfatal("CHAMPION_MANAGER_SAFE_INT_FAILED", e, once_key="safe_int", value=repr(v), default=int(default))
        return int(default)


def _safe_json_dict(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return dict(v)
    if isinstance(v, str) and v.strip():
        try:
            obj = json.loads(v)
            return dict(obj) if isinstance(obj, dict) else {}
        except Exception as e:
            _warn_nonfatal("CHAMPION_MANAGER_JSON_PARSE_FAILED", e, once_key="safe_json_dict", value=repr(v)[:512])
            return {}
    return {}


def _json_dumps(v: Any) -> str:
    return json.dumps(v, separators=(",", ":"), sort_keys=True)


def _safe_json_value(v: Any, default: Any) -> Any:
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str) and v.strip():
        try:
            return json.loads(v)
        except Exception as e:
            _warn_nonfatal(
                "CHAMPION_MANAGER_JSON_VALUE_PARSE_FAILED",
                e,
                once_key="safe_json_value",
                value=repr(v)[:512],
            )
    return default


def _begin_owned_write(con) -> bool:
    if bool(getattr(con, "in_transaction", False)):
        return False
    begin = getattr(con, "begin_managed_write", None)
    if callable(begin):
        begin()
        return True
    raise RuntimeError("managed_write_begin_unavailable")


def _ensure_post_commit_schema(con) -> None:
    con.executescript(_POST_COMMIT_SCHEMA)


def _queue_post_commit_action(con, action_name: str, *args: Any, **kwargs: Any) -> int:
    _ensure_post_commit_schema(con)
    now_ms = _now_ms()
    row = con.execute(
        f"""
        INSERT INTO {_POST_COMMIT_OUTBOX_TABLE}(
          action_name, args_json, kwargs_json, status, attempt_count,
          created_ts_ms, updated_ts_ms, available_ts_ms, lease_expires_ts_ms
        )
        VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            str(action_name or "").strip(),
            _json_dumps(list(args or ())),
            _json_dumps(dict(kwargs or {})),
            "pending",
            0,
            int(now_ms),
            int(now_ms),
            int(now_ms),
            0,
        ),
    )
    return _safe_int(getattr(row, "lastrowid", 0), 0)


def _extract_feature_contract(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    feature_ids = candidate.get("feature_ids")
    if not isinstance(feature_ids, list):
        feature_ids = meta.get("feature_ids")
    feature_ids = [str(x) for x in (feature_ids or []) if str(x or "").strip()]

    feature_set_tag = str(
        candidate.get("feature_set_tag")
        or meta.get("feature_set_tag")
        or ""
    ).strip()

    feature_schema = candidate.get("feature_schema")
    if not isinstance(feature_schema, dict):
        feature_schema = meta.get("feature_schema")
    if not isinstance(feature_schema, dict):
        feature_schema = {}
    feature_schema = dict(feature_schema)

    if feature_ids and not isinstance(feature_schema.get("feature_ids"), list):
        feature_schema["feature_ids"] = list(feature_ids)
    if feature_set_tag and not str(feature_schema.get("feature_set_tag") or "").strip():
        feature_schema["feature_set_tag"] = str(feature_set_tag)

    out: Dict[str, Any] = {}
    if feature_ids:
        out["feature_ids"] = list(feature_ids)
    if feature_set_tag:
        out["feature_set_tag"] = str(feature_set_tag)
    if feature_schema:
        out["feature_schema"] = feature_schema
    return out


def _competition_key(symbol: str, horizon_s: int, regime: str) -> str:
    return "|".join(
        [
            str(symbol or "").upper().strip(),
            str(int(horizon_s or 0)),
            str(regime or "global").strip() or "global",
        ]
    )


def _candidate_block_key(model_name: str, symbol: str, horizon_s: int, regime: str) -> str:
    return "|".join(
        [
            str(model_name or "").strip(),
            str(symbol or "").upper().strip(),
            str(int(horizon_s or 0)),
            str(regime or "global").strip() or "global",
        ]
    )


def _candidate_is_deployable(row: Optional[Dict[str, Any]]) -> bool:
    meta = dict((row or {}).get("meta") or {})
    score_source = str(meta.get("score_source") or "").strip().lower()
    model_name = str((row or {}).get("model_name") or "").strip().lower()
    model_kind = str(meta.get("model_kind") or "").strip().lower()
    if score_source == "shadow_predictions":
        if model_kind == "shadow_regime_stats" or model_name.startswith("regime_stats_"):
            return True
        return False
    return True


def _score_source_is_realized_pnl(meta: Optional[Dict[str, Any]]) -> bool:
    source = str((meta or {}).get("score_source") or "").strip().lower()
    return source in {"pnl_attribution", "execution_fills", "broker_fills"}


def _score_source_is_competition_candidate(meta: Optional[Dict[str, Any]]) -> bool:
    source = str((meta or {}).get("score_source") or "").strip().lower()
    return bool(_score_source_is_realized_pnl(meta) or source == "shadow_predictions")


def _candidate_is_live_promotable(row: Optional[Dict[str, Any]]) -> bool:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    return bool(_score_source_is_realized_pnl(meta) and _candidate_is_deployable(candidate))


def _candidate_replay_row(
    row: Optional[Dict[str, Any]],
    replay_models: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    candidate = dict(row or {})
    models = dict(replay_models or {})
    if not candidate or not models:
        return {}
    model_name = str(candidate.get("model_name") or "").strip()
    symbol = str(candidate.get("symbol") or "").upper().strip()
    horizon_s = _safe_int(candidate.get("horizon_s"), 0)
    regime = str(candidate.get("regime") or "global").strip() or "global"
    specific = models.get("|".join([model_name, symbol, str(horizon_s), regime]))
    if isinstance(specific, dict) and specific:
        return dict(specific)
    aggregate = models.get("|".join([model_name, "*", str(horizon_s), regime]))
    if isinstance(aggregate, dict) and aggregate:
        return dict(aggregate)
    return {}


def _promotion_replay_provenance(
    row: Optional[Dict[str, Any]],
    replay_models: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    replay_row = _candidate_replay_row(row, replay_models)
    if not replay_row:
        return {}
    return {
        "model_name": str(replay_row.get("model_name") or ""),
        "symbol": str(replay_row.get("symbol") or ""),
        "horizon_s": _safe_int(replay_row.get("horizon_s"), 0),
        "regime": str(replay_row.get("regime") or "global"),
        "model_kind": str(replay_row.get("model_kind") or ""),
        "model_ts_ms": _safe_int(replay_row.get("model_ts_ms"), 0),
        "approved": bool(replay_row.get("approved")),
        "source": str(replay_row.get("source") or ""),
        "n": _safe_int(replay_row.get("n"), 0),
        "baseline_n": _safe_int(replay_row.get("baseline_n"), 0),
        "dir_acc": _safe_float(replay_row.get("dir_acc"), 0.0),
        "baseline_dir_acc": replay_row.get("baseline_dir_acc"),
        "dir_acc_delta": replay_row.get("dir_acc_delta"),
        "net_rmse": _safe_float(replay_row.get("net_rmse"), 0.0),
        "baseline_net_rmse": replay_row.get("baseline_net_rmse"),
        "net_rmse_delta": replay_row.get("net_rmse_delta"),
        "last_event_id": _safe_int(replay_row.get("last_event_id"), 0),
        "window_end_ms": _safe_int(replay_row.get("window_end_ms"), 0),
    }


def _sync_assignment_to_model_registry(row: Optional[Dict[str, Any]]) -> None:
    candidate = dict(row or {})
    if not candidate or not _candidate_is_deployable(candidate):
        return

    meta = dict(candidate.get("meta") or {})
    model_name = str(candidate.get("model_name") or "").strip()
    model_kind = str(meta.get("model_kind") or "").strip()
    model_ts_ms = _safe_int(meta.get("model_ts_ms"), 0)
    regime = str(candidate.get("regime") or "global").strip() or "global"
    if not model_name or not model_kind or model_ts_ms <= 0:
        return

    metrics = {
        "score": _safe_float(candidate.get("score"), 0.0),
        "trades": _safe_int(candidate.get("trades"), 0),
        "wins": _safe_int(candidate.get("wins"), 0),
        "losses": _safe_int(candidate.get("losses"), 0),
        "net_pnl": _safe_float(candidate.get("net_pnl"), 0.0),
        "source": "competition_cycle",
        "symbol": str(candidate.get("symbol") or "").upper().strip(),
        "horizon_s": _safe_int(candidate.get("horizon_s"), 0),
    }
    if str(meta.get("artifact_sha256") or "").strip():
        metrics["artifact_sha256"] = str(meta.get("artifact_sha256") or "").strip()
    if str(meta.get("artifact_alias") or meta.get("artifact_uri") or "").strip():
        metrics["artifact_alias"] = str(meta.get("artifact_alias") or meta.get("artifact_uri") or "").strip()
    metrics.update(_extract_feature_contract(candidate))

    try:
        existing = get_stage_latest(model_name, "challenger", regime=regime)
        if not existing or str(existing.get("model_kind") or "") != model_kind or _safe_int(existing.get("model_ts_ms"), 0) != model_ts_ms:
            register_model(
                model_name=model_name,
                model_kind=model_kind,
                model_ts_ms=model_ts_ms,
                stage="challenger",
                metrics=metrics,
                note="competition_cycle_sync",
                regime=regime,
            )
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_SYNC_ASSIGNMENT_TO_MODEL_REGISTRY_FAILED",
            e,
            model_name=model_name,
            model_kind=model_kind,
            model_ts_ms=model_ts_ms,
            regime=regime,
        )


def _copy_candidate_row(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidate = dict(row or {})
    candidate["meta"] = dict(candidate.get("meta") or {})
    return candidate


def _load_all_champions() -> List[Dict[str, Any]]:
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
            FROM champion_assignments
            WHERE state='champion'
            ORDER BY symbol ASC, horizon_s ASC, updated_ts_ms DESC
            """
        ).fetchall()
        out = []
        for row in rows or []:
            out.append(
                {
                    "scope": str(row[0] or ""),
                    "symbol": str(row[1] or ""),
                    "horizon_s": _safe_int(row[2], 0),
                    "model_name": str(row[3] or ""),
                    "challenger_name": str(row[4] or ""),
                    "regime": str(row[5] or "global"),
                    "state": str(row[6] or "champion"),
                    "assigned_ts_ms": _safe_int(row[7], 0),
                    "updated_ts_ms": _safe_int(row[8], 0),
                    "meta": _safe_json_dict(row[9]),
                }
            )
        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("CHAMPION_MANAGER_LOAD_ALL_CHAMPIONS_CLOSE_FAILED", e)


def _sync_marketplace_stages(con, candidates: List[Dict[str, Any]], champion_name: str) -> None:
    for row in candidates or []:
        con.execute(
            """
            UPDATE model_marketplace_scores
            SET stage=?, updated_ts_ms=?
            WHERE model_name=? AND symbol=? AND horizon_s=? AND regime=?
              AND model_id=?
            """,
            (
                "champion" if str(row.get("model_name") or "") == str(champion_name or "") else "challenger",
                int(_now_ms()),
                str(row.get("model_name") or ""),
                str(row.get("symbol") or "").upper().strip(),
                int(row.get("horizon_s") or 0),
                str(row.get("regime") or "global"),
                str(row.get("model_id") or "baseline"),
            ),
        )


def _clear_champion_assignment(*, scope: str, symbol: str, horizon_s: int, con) -> None:
    con.execute(
        """
        DELETE FROM champion_assignments
        WHERE scope=? AND symbol=? AND horizon_s=?
        """,
        (
            str(scope or ""),
            str(symbol or "").upper().strip(),
            int(horizon_s or 0),
        ),
    )


def get_champion_assignment(scope: str, symbol: str, horizon_s: int = 0) -> Dict[str, Any]:
    con = connect()
    try:
        row = con.execute(
            """
            SELECT scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
            FROM champion_assignments
            WHERE scope=? AND symbol=? AND horizon_s=?
            """,
            (str(scope), str(symbol).upper().strip(), int(horizon_s)),
        ).fetchone()

        if not row:
            return {}

        try:
            meta = json.loads(row[9]) if row[9] else {}
            if not isinstance(meta, dict):
                meta = {}
        except Exception:
            meta = {}

        return {
            "scope": str(row[0] or ""),
            "symbol": str(row[1] or ""),
            "horizon_s": int(row[2] or 0),
            "model_name": str(row[3] or ""),
            "challenger_name": str(row[4] or ""),
            "regime": str(row[5] or "global"),
            "state": str(row[6] or "champion"),
            "assigned_ts_ms": int(row[7] or 0),
            "updated_ts_ms": int(row[8] or 0),
            "meta": meta,
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("CHAMPION_MANAGER_GET_ASSIGNMENT_CLOSE_FAILED", e)


def _current_assignment_state(db, payload: Dict[str, Any]) -> Optional[str]:
    row = db.execute(
        """
        SELECT state
        FROM champion_assignments
        WHERE scope=?
          AND symbol=?
          AND horizon_s=?
          AND model_name=?
        LIMIT 1
        """,
        (
            str(payload["scope"]),
            str(payload["symbol"]),
            int(payload["horizon_s"]),
            str(payload["model_name"]),
        ),
    ).fetchone()
    if not row:
        return None
    return str(row[0] or "").strip().lower() or None


def _validate_assignment_transition(db, payload: Dict[str, Any]) -> None:
    new_state = str(payload.get("state") or "champion").strip().lower()
    if new_state not in _ASSIGNMENT_STATES:
        raise IllegalChampionTransition(f"unknown champion assignment state: {new_state}")
    current_state = _current_assignment_state(db, payload)
    if new_state == "champion" and current_state != "challenger":
        raise IllegalChampionTransition(
            f"cannot transition model {payload['model_name']} to champion from "
            f"{current_state or 'unassigned'}; current state must be challenger"
        )
    if current_state is None or current_state == new_state:
        return
    if (current_state, new_state) not in _ALLOWED_ASSIGNMENT_TRANSITIONS:
        raise IllegalChampionTransition(
            f"illegal champion assignment transition for model {payload['model_name']}: "
            f"{current_state} -> {new_state}"
        )


def set_champion_assignment(
    *,
    scope: str,
    symbol: str,
    model_name: str,
    horizon_s: int = 0,
    challenger_name: str = "",
    regime: str = "global",
    state: str = "champion",
    meta: Optional[Dict[str, Any]] = None,
    con=None,
) -> Dict[str, Any]:
    now = _now_ms()
    state_key = str(state or "champion").strip().lower()
    payload = {
        "scope": str(scope),
        "symbol": str(symbol).upper().strip(),
        "horizon_s": int(horizon_s),
        "model_name": str(model_name),
        "challenger_name": str(challenger_name or ""),
        "regime": str(regime or "global"),
        "state": str(state_key),
        "assigned_ts_ms": int(now),
        "updated_ts_ms": int(now),
        "meta": dict(meta or {}),
    }

    def _apply(db) -> None:
        _validate_assignment_transition(db, payload)
        db.execute(
            """
            INSERT INTO champion_assignments(
              scope, symbol, horizon_s, model_name, challenger_name, regime, state, assigned_ts_ms, updated_ts_ms, meta_json
            )
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(scope, symbol, horizon_s) DO UPDATE SET
              model_name=excluded.model_name,
              challenger_name=excluded.challenger_name,
              regime=excluded.regime,
              state=excluded.state,
              updated_ts_ms=excluded.updated_ts_ms,
              meta_json=excluded.meta_json
            """,
            (
                payload["scope"],
                payload["symbol"],
                payload["horizon_s"],
                payload["model_name"],
                payload["challenger_name"],
                payload["regime"],
                payload["state"],
                payload["assigned_ts_ms"],
                payload["updated_ts_ms"],
                _json_dumps(payload["meta"]),
            ),
        )

    if con is None:
        run_write_txn(
            _apply,
            table="champion_assignments",
            operation="set_champion_assignment",
            context={
                "scope": str(payload["scope"]),
                "symbol": str(payload["symbol"]),
                "horizon_s": int(payload["horizon_s"]),
                "model_name": str(payload["model_name"]),
            },
        )
        publish_runtime_meta = True
    else:
        owns_txn = False
        try:
            owns_txn = _begin_owned_write(con)
            _apply(con)
            if owns_txn:
                con.commit()
        except Exception:
            if owns_txn and bool(getattr(con, "in_transaction", False)):
                con.rollback()
            raise
        publish_runtime_meta = bool(owns_txn)

    # When the caller supplies an open write transaction, avoid nesting a
    # second write transaction via meta_set(). The runtime mirror is refreshed
    # after commit by the competition snapshot path.
    if publish_runtime_meta:
        meta_set("competition_champion", _json_dumps(payload))
    return payload


def _ranking_model_name(model_json_raw: Any, fallback_model_id: Any) -> str:
    model_json = _safe_json_dict(model_json_raw)
    for key in ("model_name", "strategy_name", "strategy"):
        value = model_json.get(key)
        if isinstance(value, str) and value.strip():
            return str(value).strip()
    nested = model_json.get("model")
    if isinstance(nested, dict):
        for key in ("model_name", "name", "id"):
            value = nested.get(key)
            if isinstance(value, str) and value.strip():
                return str(value).strip()
    fallback = str(model_json.get("model_id") or fallback_model_id or "").strip()
    return fallback or "baseline"


def _ranking_capital_base(signal_json_raw: Any, pnl_value: float) -> float:
    signal_json = _safe_json_dict(signal_json_raw)
    pnl_block = _safe_json_dict(signal_json.get("pnl_attribution"))
    pnl_extra = _safe_json_dict(pnl_block.get("extra"))
    candidates = [
        signal_json.get("capital_base"),
        signal_json.get("allocated_capital"),
        signal_json.get("deployed_capital"),
        signal_json.get("notional_traded"),
        pnl_extra.get("notional_traded"),
        pnl_block.get("notional_traded"),
    ]
    avg_price = _safe_float(
        signal_json.get("avg_price"),
        _safe_float(signal_json.get("entry_price"), _safe_float(pnl_block.get("avg_price"), 0.0)),
    )
    position_size = abs(
        _safe_float(signal_json.get("position_size"), _safe_float(pnl_block.get("position_size"), 0.0))
    )
    if avg_price > 0.0 and position_size > 0.0:
        candidates.append(avg_price * position_size)
    last_price = _safe_float(signal_json.get("last_price"), 0.0)
    if last_price > 0.0 and position_size > 0.0:
        candidates.append(last_price * position_size)
    for candidate in candidates:
        if candidate is None:
            continue
        base = abs(_safe_float(candidate, 0.0))
        if base > 0.0:
            return float(base)
    return 0.0


def _capital_adjusted_return_pct(pnl_value: float, capital_base: float) -> float:
    base = abs(_safe_float(capital_base, 0.0))
    if base <= 0.0:
        return 0.0
    return float(_safe_float(pnl_value, 0.0) / base) * 100.0


def _primary_comparison_metric(*, capital_base: float, return_pct: float, net_pnl: float) -> tuple[float, str]:
    if abs(_safe_float(capital_base, 0.0)) > 0.0:
        return _safe_float(return_pct, 0.0), "return_pct"
    return _safe_float(net_pnl, 0.0), "net_pnl"


def _promotion_delta(best_metrics: Optional[Dict[str, Any]], current_metrics: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    best = dict(best_metrics or {})
    current = dict(current_metrics or {})
    best_capital = abs(_safe_float(best.get("capital_base"), 0.0))
    current_capital = abs(_safe_float(current.get("capital_base"), 0.0))
    return_delta = _safe_float(best.get("return_pct"), 0.0) - _safe_float(current.get("return_pct"), 0.0)
    pnl_delta = _safe_float(best.get("rolling_total_pnl"), 0.0) - _safe_float(current.get("rolling_total_pnl"), 0.0)
    use_return_pct = bool(best_capital > 0.0 and current_capital > 0.0)
    return {
        "metric": ("return_pct" if use_return_pct else "net_pnl"),
        "delta": float(return_delta if use_return_pct else pnl_delta),
        "return_pct_delta": float(return_delta),
        "net_pnl_delta": float(pnl_delta),
        "threshold": float(PROMOTION_MIN_RETURN_PCT_DELTA if use_return_pct else PROMOTION_MIN_NET_PNL_DELTA),
    }


def _rank_models(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for row in rows or []:
        event_pnls = [float(v) for v in list(row.get("event_pnls") or [])]
        cumulative = 0.0
        peak = 0.0
        path_drawdown = 0.0
        for pnl_value in event_pnls:
            cumulative += float(pnl_value)
            peak = max(peak, cumulative)
            path_drawdown = max(path_drawdown, peak - cumulative)
        trade_count = max(_safe_int(row.get("trade_count"), 0), int(len(event_pnls)))
        wins = max(_safe_int(row.get("wins"), 0), int(sum(1 for pnl_value in event_pnls if pnl_value > 0.0)))
        losses = max(_safe_int(row.get("losses"), 0), int(sum(1 for pnl_value in event_pnls if pnl_value < 0.0)))
        max_drawdown = max(_safe_float(row.get("max_drawdown"), 0.0), float(path_drawdown))
        capital_base_sum = float(row.get("capital_base_sum") or 0.0)
        return_pct = _capital_adjusted_return_pct(_safe_float(row.get("net_pnl"), 0.0), capital_base_sum)
        win_rate = (float(wins) / float(trade_count)) if trade_count > 0 else None
        metrics = dict(row)
        metrics.update(
            {
                "score": _safe_float(row.get("score"), 0.0),
                "return_pct": float(return_pct),
                "max_drawdown": float(max_drawdown),
                "trade_count": trade_count,
                "wins": wins,
                "losses": losses,
                "win_rate": (float(win_rate) if win_rate is not None else None),
            }
        )
        ranked.append(metrics)

    ranked.sort(
        key=lambda row: (
            -_primary_comparison_metric(
                capital_base=_safe_float(row.get("capital_base_sum"), 0.0),
                return_pct=_safe_float(row.get("return_pct"), 0.0),
                net_pnl=_safe_float(row.get("net_pnl"), 0.0),
            )[0],
            _safe_float(row.get("max_drawdown"), 0.0),
            -_safe_float(row.get("win_rate"), -1.0),
            -_safe_int(row.get("trade_count"), 0),
            -_safe_float(row.get("net_pnl"), 0.0),
            -_safe_float(row.get("score"), 0.0),
            str(row.get("model_name") or ""),
        )
    )
    for idx, row in enumerate(ranked, start=1):
        row["rank"] = int(idx)
    return ranked


def _serialize_ranking_row(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    rec = dict(row or {})
    for key in ("symbols", "horizons", "regimes"):
        value = rec.get(key)
        if isinstance(value, set):
            rec[key] = sorted(value)
    return rec


def get_model_competition_rankings(limit: int = 25, ranking_scope: str = "global") -> List[Dict[str, Any]]:
    init_db()
    con = connect()
    try:
        rows = con.execute(
            """
            SELECT rank, model_name, net_pnl, return_pct, max_drawdown, win_rate, trade_count,
                   wins, losses, last_trade_ts_ms, source, updated_ts_ms, metrics_json
            FROM model_competition_rankings
            WHERE ranking_scope=?
            ORDER BY rank ASC, model_name ASC
            LIMIT ?
            """,
            (str(ranking_scope or "global"), int(max(1, min(500, int(limit or 25))))),
        ).fetchall() or []
        out = []
        for row in rows:
            metrics = _safe_json_dict(row[12])
            out.append(
                    {
                        "rank": _safe_int(row[0], 0),
                        "model_name": str(row[1] or ""),
                        "score": _safe_float(metrics.get("score"), 0.0),
                        "net_pnl": _safe_float(row[2], 0.0),
                        "return_pct": _safe_float(row[3], 0.0),
                        "max_drawdown": _safe_float(row[4], 0.0),
                    "win_rate": (None if row[5] is None else _safe_float(row[5], 0.0)),
                    "trade_count": _safe_int(row[6], 0),
                    "wins": _safe_int(row[7], 0),
                    "losses": _safe_int(row[8], 0),
                    "last_trade_ts_ms": _safe_int(row[9], 0),
                    "source": str(row[10] or ""),
                    "updated_ts_ms": _safe_int(row[11], 0),
                    "metrics": metrics,
                }
            )
        return out
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("CHAMPION_MANAGER_MODEL_COMPETITION_STATUS_CLOSE_FAILED", e)


def _clear_model_competition_champion(*, con) -> None:
    con.execute(
        """
        DELETE FROM champion_assignments
        WHERE scope=? AND symbol=? AND horizon_s=?
        """,
        (
            MODEL_COMPETITION_SCOPE,
            MODEL_COMPETITION_SYMBOL,
            MODEL_COMPETITION_HORIZON_S,
        ),
    )


def recompute_model_rankings(ranking_scope: str = "global") -> Dict[str, Any]:
    init_db()
    scope = str(ranking_scope or "global").strip() or "global"
    with _COMPETITION_LOCK:
        con = connect()
        try:
            ranking_rows: Dict[str, Dict[str, Any]] = {}
            score_rows = con.execute(
                """
                SELECT model_id, model_name, symbol, horizon_s, regime, trades, wins, losses, score, net_pnl, meta_json, updated_ts_ms
                FROM model_marketplace_scores
                ORDER BY updated_ts_ms ASC, model_name ASC
                """
            ).fetchall() or []

            for model_id, model_name, symbol, horizon_s, regime, trades, wins, losses, score, net_pnl, meta_json, updated_ts_ms in score_rows:
                mid = str(model_id or "baseline").strip() or "baseline"
                name = str(model_name or mid).strip() or mid
                meta = _safe_json_dict(meta_json)
                if not _score_source_is_realized_pnl(meta):
                    continue
                key = f"{mid}|{name}"
                rec = ranking_rows.setdefault(
                    key,
                    {
                        "model_id": str(mid),
                        "model_name": str(name),
                        "net_pnl": 0.0,
                        "realized_pnl": 0.0,
                        "unrealized_pnl": 0.0,
                        "total_pnl": 0.0,
                        "capital_base_sum": 0.0,
                        "last_trade_ts_ms": 0,
                        "source": "model_marketplace_scores",
                        "score_weighted_sum": 0.0,
                        "score_weight": 0.0,
                        "score": 0.0,
                        "event_pnls": [],
                        "evaluation_timestamps": [],
                        "regime_labels": [],
                        "challenger_predictions": [],
                        "realized_returns": [],
                        "symbols": set(),
                        "horizons": set(),
                        "regimes": set(),
                        "rolling_window_ms": int(meta.get("rolling_window_ms") or (MODEL_COMPETITION_WINDOW_S * 1000)),
                        "observation_ms": 0,
                        "recent_total_pnl": 0.0,
                        "prior_total_pnl": 0.0,
                    },
                )
                realized_pnl = _safe_float(meta.get("rolling_realized_pnl"), _safe_float(meta.get("realized_pnl"), 0.0))
                unrealized_pnl = _safe_float(meta.get("rolling_unrealized_pnl"), _safe_float(meta.get("unrealized_pnl"), 0.0))
                total_pnl = _safe_float(
                    meta.get("rolling_total_pnl"),
                    realized_pnl + unrealized_pnl - _safe_float(meta.get("transaction_cost"), 0.0),
                )
                rec["realized_pnl"] += float(realized_pnl)
                rec["unrealized_pnl"] += float(unrealized_pnl)
                rec["total_pnl"] += float(total_pnl)
                rec["net_pnl"] += float(total_pnl)
                rec["capital_base_sum"] += _ranking_capital_base(meta, total_pnl)
                rec["last_trade_ts_ms"] = max(
                    _safe_int(rec.get("last_trade_ts_ms"), 0),
                    _safe_int(meta.get("last_signal_ts_ms"), _safe_int(updated_ts_ms, 0)),
                )
                realized_trade_pnls = []
                raw_trade_pnls = meta.get("realized_trade_pnls")
                if isinstance(raw_trade_pnls, list):
                    for raw_trade_pnl in raw_trade_pnls:
                        try:
                            trade_pnl = float(raw_trade_pnl)
                        except Exception as e:
                            _warn_nonfatal(
                                "CHAMPION_MANAGER_RANKING_TRADE_PNL_PARSE_FAILED",
                                e,
                                once_key="ranking_trade_pnl_parse",
                                value=repr(raw_trade_pnl)[:128],
                            )
                            continue
                        if trade_pnl == trade_pnl and trade_pnl not in (float("inf"), float("-inf")):
                            realized_trade_pnls.append(float(trade_pnl))
                if realized_trade_pnls:
                    rec["event_pnls"].extend(realized_trade_pnls)
                else:
                    rec["event_pnls"].append(float(total_pnl))
                for key in ("evaluation_timestamps", "regime_labels", "challenger_predictions", "realized_returns"):
                    values = meta.get(key)
                    if isinstance(values, list) and values:
                        rec.setdefault(key, []).extend(list(values))
                rec["trade_count"] = _safe_int(rec.get("trade_count"), 0) + _safe_int(trades, 0)
                rec["wins"] = _safe_int(rec.get("wins"), 0) + _safe_int(wins, 0)
                rec["losses"] = _safe_int(rec.get("losses"), 0) + _safe_int(losses, 0)
                row_score = _safe_float(
                    meta.get("risk_adjusted_score"),
                    _safe_float(score, 0.0),
                )
                score_weight = float(max(1, _safe_int(trades, 0)))
                rec["score_weighted_sum"] = _safe_float(rec.get("score_weighted_sum"), 0.0) + (float(row_score) * score_weight)
                rec["score_weight"] = _safe_float(rec.get("score_weight"), 0.0) + float(score_weight)
                rec["max_drawdown"] = max(
                    _safe_float(rec.get("max_drawdown"), 0.0),
                    _safe_float(meta.get("max_drawdown"), 0.0),
                )
                rec["observation_ms"] = max(
                    _safe_int(rec.get("observation_ms"), 0),
                    _safe_int(meta.get("observation_duration_ms"), 0),
                )
                rec["recent_total_pnl"] += _safe_float(meta.get("recent_total_pnl"), 0.0)
                rec["prior_total_pnl"] += _safe_float(meta.get("prior_total_pnl"), 0.0)
                rec["symbols"].add(str(symbol or "").upper().strip())
                rec["horizons"].add(_safe_int(horizon_s, 0))
                rec["regimes"].add(str(regime or "global"))

            prepared_rows: List[Dict[str, Any]] = list(ranking_rows.values())
            for row in prepared_rows:
                trade_count = _safe_int(row.get("trade_count"), 0)
                wins = _safe_int(row.get("wins"), 0)
                row["win_rate"] = (float(wins) / float(trade_count)) if trade_count > 0 else None
                score_weight = _safe_float(row.get("score_weight"), 0.0)
                if score_weight > 0.0:
                    row["score"] = _safe_float(row.get("score_weighted_sum"), 0.0) / float(score_weight)
                else:
                    row["score"] = 0.0

            ranked_rows = [_serialize_ranking_row(row) for row in _rank_models(prepared_rows)]
            champion = dict(ranked_rows[0]) if ranked_rows else {}
            challengers = [dict(row) for row in ranked_rows[1:]]
            challenger_name = str((challengers[0] or {}).get("model_name") or "") if challengers else ""
            updated_ts_ms = _now_ms()

            _begin_owned_write(con)
            con.execute(
                "DELETE FROM model_competition_rankings WHERE ranking_scope=?",
                (scope,),
            )
            for row in ranked_rows:
                metrics = {
                    "score": _safe_float(row.get("score"), 0.0),
                    "capital_base_sum": float(row.get("capital_base_sum") or 0.0),
                    "model_id": str(row.get("model_id") or "baseline"),
                    "realized_pnl": float(row.get("realized_pnl") or 0.0),
                    "unrealized_pnl": float(row.get("unrealized_pnl") or 0.0),
                    "total_pnl": float(row.get("total_pnl") or 0.0),
                    "rolling_window_ms": int(row.get("rolling_window_ms") or (MODEL_COMPETITION_WINDOW_S * 1000)),
                    "observation_ms": int(row.get("observation_ms") or 0),
                    "recent_total_pnl": float(row.get("recent_total_pnl") or 0.0),
                    "prior_total_pnl": float(row.get("prior_total_pnl") or 0.0),
                    "symbols": sorted(str(x) for x in (row.get("symbols") or set()) if str(x).strip()),
                    "horizons": sorted(int(x) for x in (row.get("horizons") or set())),
                    "regimes": sorted(str(x) for x in (row.get("regimes") or set()) if str(x).strip()),
                    "evaluation_timestamps": list(row.get("evaluation_timestamps") or []),
                    "regime_labels": list(row.get("regime_labels") or []),
                    "challenger_predictions": list(row.get("challenger_predictions") or []),
                    "realized_returns": list(row.get("realized_returns") or []),
                    "event_pnls": list(row.get("event_pnls") or []),
                    "source": str(row.get("source") or ""),
                }
                con.execute(
                    """
                    INSERT INTO model_competition_rankings(
                      ranking_scope, model_name, rank, net_pnl, return_pct, max_drawdown, win_rate,
                      trade_count, wins, losses, last_trade_ts_ms, source, updated_ts_ms, metrics_json
                    )
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        scope,
                        str(row.get("model_name") or ""),
                        _safe_int(row.get("rank"), 0),
                        _safe_float(row.get("net_pnl"), 0.0),
                        _safe_float(row.get("return_pct"), 0.0),
                        _safe_float(row.get("max_drawdown"), 0.0),
                        row.get("win_rate"),
                        _safe_int(row.get("trade_count"), 0),
                        _safe_int(row.get("wins"), 0),
                        _safe_int(row.get("losses"), 0),
                        _safe_int(row.get("last_trade_ts_ms"), 0),
                        str(row.get("source") or ""),
                        int(updated_ts_ms),
                        _json_dumps(metrics),
                    ),
                )

            con.commit()
        except Exception:
            if bool(getattr(con, "in_transaction", False)):
                con.rollback()
            raise
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("CHAMPION_MANAGER_CURRENT_COMPETITION_SNAPSHOT_CLOSE_FAILED", e)

        snapshot = {
            "ok": True,
            "ranking_scope": scope,
            "updated_ts_ms": int(updated_ts_ms),
            "status": ("ranked" if ranked_rows else "no_realized_pnl"),
            "champion": champion,
            "challengers": challengers,
            "rows": ranked_rows,
        }
        meta_set("competition_rankings", _json_dumps(snapshot))
        champion_payload = {
            "scope": MODEL_COMPETITION_SCOPE,
            "symbol": MODEL_COMPETITION_SYMBOL,
            "horizon_s": MODEL_COMPETITION_HORIZON_S,
            "model_name": str(champion.get("model_name") or ""),
            "challenger_name": challenger_name,
            "regime": scope,
            "state": ("champion" if champion else "missing"),
            "assigned_ts_ms": int(updated_ts_ms),
            "updated_ts_ms": int(updated_ts_ms),
            "meta": {
                "rank": _safe_int(champion.get("rank"), 0),
                "score": _safe_float(champion.get("score"), 0.0),
                "net_pnl": _safe_float(champion.get("net_pnl"), 0.0),
                "return_pct": _safe_float(champion.get("return_pct"), 0.0),
                "max_drawdown": _safe_float(champion.get("max_drawdown"), 0.0),
                "win_rate": champion.get("win_rate"),
                "trade_count": _safe_int(champion.get("trade_count"), 0),
                "ranking_scope": scope,
                "status": ("ranked" if champion else "no_realized_pnl"),
            },
        }
        meta_set("competition_champion", _json_dumps(champion_payload))
        return snapshot


def current_competition_snapshot(active_symbols: Optional[List[str]] = None) -> Dict[str, Any]:
    # This is the operator-facing read model for competition state. It combines
    # durable champion assignments with fresher runtime artifacts such as
    # rankings, replay validation, self-critic status, and capital allocation.
    rankings_snapshot = _safe_json_dict(meta_get("competition_rankings", "") or "{}")
    ranking_rows = list(rankings_snapshot.get("rows") or [])
    ranking_champion = dict(rankings_snapshot.get("champion") or {})
    ranking_challengers = list(rankings_snapshot.get("challengers") or [])

    champions = _load_all_champions()
    champion = get_champion_assignment(
        MODEL_COMPETITION_SCOPE,
        MODEL_COMPETITION_SYMBOL,
        MODEL_COMPETITION_HORIZON_S,
    )
    if champion and ranking_champion:
        champion["meta"] = {**dict(champion.get("meta") or {}), **dict(ranking_champion)}

    capital_plan = _load_competition_capital_plan()
    replay_validation = _safe_json_dict(
        meta_get("competition_replay_validation", "") or "{}"
    )
    replay_status = _safe_json_dict(
        meta_get("competition_replay_validation_status", "") or "{}"
    )
    self_critic = _safe_json_dict(meta_get("competition_self_critic", "") or "{}")
    cycle_status = _safe_json_dict(meta_get("competition_cycle_status", "") or "{}")
    attribution_completeness = _safe_json_dict(meta_get("attribution_completeness", "") or "{}")
    post_commit_actions = get_competition_post_commit_status()

    snap = {
        "ok": True,
        "champion": champion,
        "champions": champions,
        "challengers": ranking_challengers,
        "model_rankings": ranking_rows,
        "capital_plan": capital_plan,
        "replay_validation": replay_validation,
        "replay_validation_status": replay_status,
        "self_critic": self_critic,
        "cycle_status": cycle_status,
        "post_commit_actions": post_commit_actions,
        "attribution_completeness": attribution_completeness,
        "active_symbols": list(active_symbols or []),
        "updated_ts_ms": _now_ms(),
    }
    meta_set("competition_runtime", _json_dumps(snap))
    meta_set("competition_champion", _json_dumps(champion))
    return snap


def _load_competition_capital_plan(*, force_refresh: bool = False) -> Dict[str, Any]:
    capital_plan = _safe_json_dict(meta_get("competition_capital_plan", "") or "{}")
    allocations = capital_plan.get("allocations") if isinstance(capital_plan, dict) else {}
    updated_ts_ms = _safe_int(capital_plan.get("updated_ts_ms"), 0) if isinstance(capital_plan, dict) else 0
    age_ms = max(0, _now_ms() - int(updated_ts_ms)) if updated_ts_ms > 0 else int(COMPETITION_CAPITAL_PLAN_MAX_AGE_MS) + 1
    if force_refresh or not isinstance(allocations, dict) or not allocations or age_ms > int(max(1000, COMPETITION_CAPITAL_PLAN_MAX_AGE_MS)):
        try:
            fresh = compute_capital_plan()
            if isinstance(fresh, dict) and fresh:
                capital_plan = dict(fresh)
        except Exception as e:
            _warn_nonfatal(
                "CHAMPION_MANAGER_CAPITAL_PLAN_REFRESH_FAILED",
                e,
                once_key="capital_plan_refresh",
                force_refresh=bool(force_refresh),
                age_ms=age_ms,
            )
    capital_plan = dict(capital_plan or {})
    capital_plan["updated_ts_ms"] = int(_safe_int(capital_plan.get("updated_ts_ms"), updated_ts_ms))
    capital_plan["age_ms"] = max(0, _now_ms() - int(capital_plan.get("updated_ts_ms") or 0)) if int(capital_plan.get("updated_ts_ms") or 0) > 0 else int(COMPETITION_CAPITAL_PLAN_MAX_AGE_MS) + 1
    capital_plan["max_age_ms"] = int(COMPETITION_CAPITAL_PLAN_MAX_AGE_MS)
    capital_plan["fresh"] = bool(int(capital_plan.get("updated_ts_ms") or 0) > 0 and int(capital_plan.get("age_ms") or 0) <= int(COMPETITION_CAPITAL_PLAN_MAX_AGE_MS))
    return capital_plan


def get_live_competition_champion_name(symbol: Optional[str] = None, horizon_s: int = 0) -> str:
    champion = get_champion_assignment(
        MODEL_COMPETITION_SCOPE,
        MODEL_COMPETITION_SYMBOL,
        MODEL_COMPETITION_HORIZON_S,
    )
    name = str((champion or {}).get("model_name") or "").strip()
    if name:
        return name
    if symbol:
        scoped = get_champion_assignment("global", str(symbol), int(horizon_s or 0))
        name = str((scoped or {}).get("model_name") or "").strip()
        if name:
            return name
    return ""

def get_competition_policy_for_intent(
    *,
    symbol: str,
    horizon_s: int,
    model_name: Optional[str] = None,
    regime: str = "global",
) -> Dict[str, Any]:
    symbol_u = str(symbol or "").upper().strip()
    reg = str(regime or "global").strip() or "global"
    capital_plan = _load_competition_capital_plan()
    allocations = capital_plan.get("allocations") if isinstance(capital_plan, dict) else {}
    if not isinstance(allocations, dict):
        allocations = {}

    group_key = _competition_key(symbol_u, int(horizon_s), reg)
    group = allocations.get(group_key) or (
        allocations.get(_competition_key(symbol_u, 0, reg)) if int(horizon_s) != 0 else None
    )
    group = dict(group or {})
    ranked_models = list(group.get("models") or []) if isinstance(group, dict) else []
    candidate_name = str(model_name or "").strip()
    champion_model_name = str(group.get("champion_model_name") or "").strip()
    allocation_strategy = str(
        group.get("allocation_strategy")
        or capital_plan.get("allocation_strategy")
        or "proportional"
    ).strip() or "proportional"
    allocation_fraction = 0.0
    effective_allocation_fraction = 0.0
    model_risk_limit_multiplier = _safe_float(group.get("risk_limit_multiplier"), 1.0)
    for row in ranked_models:
        if str((row or {}).get("model_name") or "").strip() == candidate_name:
            allocation_fraction = _safe_float((row or {}).get("allocation_fraction"), 0.0)
            effective_allocation_fraction = _safe_float(
                (row or {}).get("effective_allocation_fraction"),
                allocation_fraction,
            )
            model_risk_limit_multiplier = _safe_float(
                (row or {}).get("model_risk_limit_multiplier"),
                model_risk_limit_multiplier,
            )
            break

    model_budget_fraction = 0.0
    for alloc in allocations.values():
        if not isinstance(alloc, dict):
            continue
        models = list(alloc.get("models") or [])
        for row in models:
            if str((row or {}).get("model_name") or "").strip() == candidate_name:
                model_budget_fraction += _safe_float(
                    (row or {}).get("effective_allocation_fraction"),
                    _safe_float((row or {}).get("allocation_fraction"), 0.0),
                )
                break

    group_budget_fraction = _safe_float(group.get("group_budget_fraction"), 0.0)
    if group_budget_fraction <= 0.0 and ranked_models:
        group_budget_fraction = sum(
            max(
                0.0,
                _safe_float(
                    (row or {}).get("effective_allocation_fraction"),
                    _safe_float((row or {}).get("allocation_fraction"), 0.0),
                ),
            )
            for row in ranked_models
        )
    group_budget_fraction = min(1.0, max(0.0, group_budget_fraction))

    policy = {
        "allowed": True,
        "blocked": False,
        "reason": "",
        "champion_model_name": champion_model_name,
        "allocation_strategy": str(allocation_strategy),
        "model_weight": float(max(0.0, allocation_fraction)),
        "capital_multiplier": float(max(0.0, effective_allocation_fraction)),
        "risk_limit_multiplier": float(
            min(
                _safe_float(group.get("risk_limit_multiplier"), 1.0),
                max(0.0, model_risk_limit_multiplier),
            )
        ),
        "group_risk_limit_multiplier": _safe_float(group.get("risk_limit_multiplier"), 1.0),
        "model_risk_limit_multiplier": float(max(0.0, model_risk_limit_multiplier)),
        "horizon_s": int((group or {}).get("horizon_s") or horizon_s or 0),
        "regime": str((group or {}).get("regime") or reg),
        "allocation_fraction": float(max(0.0, allocation_fraction)),
        "effective_allocation_fraction": float(max(0.0, effective_allocation_fraction)),
        "model_budget_fraction": float(max(0.0, model_budget_fraction)),
        "group_budget_fraction": float(max(0.0, group_budget_fraction)),
        "group_key": str(group_key),
        "capital_plan_updated_ts_ms": int(_safe_int(capital_plan.get("updated_ts_ms"), 0)),
        "capital_plan_age_ms": int(_safe_int(capital_plan.get("age_ms"), 0)),
        "capital_plan_max_age_ms": int(_safe_int(capital_plan.get("max_age_ms"), COMPETITION_CAPITAL_PLAN_MAX_AGE_MS)),
        "capital_plan_fresh": bool(capital_plan.get("fresh", False)),
        "models": [
            {
                "model_name": str((row or {}).get("model_name") or ""),
                "allocation_fraction": float(max(0.0, _safe_float((row or {}).get("allocation_fraction"), 0.0))),
                "effective_allocation_fraction": float(
                    max(
                        0.0,
                        _safe_float(
                            (row or {}).get("effective_allocation_fraction"),
                            _safe_float((row or {}).get("allocation_fraction"), 0.0),
                        ),
                    )
                ),
                "score": _safe_float((row or {}).get("score"), 0.0),
                "performance_score": _safe_float((row or {}).get("performance_score"), 0.0),
                "stability_score": _safe_float((row or {}).get("effective_stability_score"), 0.0),
                "model_risk_limit_multiplier": _safe_float((row or {}).get("model_risk_limit_multiplier"), 1.0),
            }
            for row in ranked_models
        ],
    }

    if not ranked_models:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "no_group_allocation"
        policy["capital_multiplier"] = 0.0
    elif not candidate_name:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "model_identity_missing"
        policy["capital_multiplier"] = 0.0
    elif allocation_fraction <= 0.0:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "model_not_allocated"
        policy["capital_multiplier"] = 0.0
    elif effective_allocation_fraction <= 0.0:
        policy["allowed"] = False
        policy["blocked"] = True
        policy["reason"] = "model_effective_capital_zero"
        policy["capital_multiplier"] = 0.0

    return policy


def _candidate_runtime_metrics(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    trades = _safe_int(candidate.get("trades"), 0)
    wins = _safe_int(candidate.get("wins"), 0)
    win_rate = (float(wins) / float(trades)) if trades > 0 else None
    first_ts = _safe_int(meta.get("first_signal_ts_ms"), 0)
    last_ts = _safe_int(meta.get("last_signal_ts_ms"), 0)
    observation_ms = max(0, last_ts - first_ts) if first_ts > 0 and last_ts > 0 else 0
    rolling_total_pnl = _safe_float(meta.get("rolling_total_pnl"), _safe_float(candidate.get("net_pnl"), 0.0))
    capital_base = _ranking_capital_base(meta, rolling_total_pnl)
    return {
        "rolling_score": _safe_float(candidate.get("score"), _safe_float(meta.get("risk_adjusted_score"), 0.0)),
        "rolling_realized_pnl": _safe_float(meta.get("rolling_realized_pnl"), _safe_float(meta.get("realized_pnl"), 0.0)),
        "rolling_unrealized_pnl": _safe_float(meta.get("rolling_unrealized_pnl"), _safe_float(meta.get("unrealized_pnl"), 0.0)),
        "rolling_total_pnl": rolling_total_pnl,
        "rolling_window_ms": _safe_int(meta.get("rolling_window_ms"), MODEL_COMPETITION_WINDOW_S * 1000),
        "max_drawdown": _safe_float(meta.get("max_drawdown"), 0.0),
        "recent_total_pnl": _safe_float(meta.get("recent_total_pnl"), 0.0),
        "prior_total_pnl": _safe_float(meta.get("prior_total_pnl"), 0.0),
        "capital_base": float(capital_base),
        "return_pct": _capital_adjusted_return_pct(rolling_total_pnl, capital_base),
        "trade_count": trades,
        "win_rate": win_rate,
        "observation_ms": observation_ms,
        "first_signal_ts_ms": first_ts,
        "last_signal_ts_ms": last_ts,
    }


def _candidate_is_eligible(row: Optional[Dict[str, Any]]) -> bool:
    metrics = _candidate_runtime_metrics(row)
    if _safe_int(metrics.get("trade_count"), 0) < int(PROMOTION_MIN_TRADES):
        return False
    if _safe_int(metrics.get("observation_ms"), 0) < int(PROMOTION_MIN_OBSERVATION_S * 1000):
        return False
    win_rate = metrics.get("win_rate")
    if win_rate is not None and float(win_rate) < float(STABILITY_MIN_WIN_RATE):
        return False
    if _safe_float(metrics.get("rolling_total_pnl"), 0.0) <= 0.0:
        return False
    return True


def _promotion_return_series(row: Optional[Dict[str, Any]]) -> List[float]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    raw_values = None
    for source in (
        meta.get("realized_trade_pnls"),
        candidate.get("event_pnls"),
        metrics.get("event_pnls"),
        meta.get("event_pnls"),
        metrics.get("realized_trade_pnls"),
    ):
        if isinstance(source, list) and source:
            raw_values = list(source)
            break
    out: List[float] = []
    for value in list(raw_values or []):
        try:
            number = float(value)
        except Exception as e:
            _warn_nonfatal(
                "CHAMPION_MANAGER_PROMOTION_RETURN_PARSE_FAILED",
                e,
                once_key="promotion_return_parse",
                value=repr(value)[:128],
            )
            continue
        if number == number and number not in (float("inf"), float("-inf")):
            out.append(float(number))
    if out:
        return out
    fallback = _safe_float(
        meta.get("rolling_total_pnl"),
        _safe_float(metrics.get("total_pnl"), _safe_float(candidate.get("net_pnl"), 0.0)),
    )
    if fallback == 0.0 and not candidate:
        return []
    return [float(fallback)]


def _promotion_models_returns(rows: Optional[List[Dict[str, Any]]]) -> Dict[str, List[float]]:
    models_returns: Dict[str, List[float]] = {}
    for row in list(rows or []):
        candidate = dict(row or {})
        model_name = str(candidate.get("model_name") or "").strip()
        if not model_name:
            continue
        if "stage" in candidate and not _candidate_is_live_promotable(candidate):
            continue
        returns = _promotion_return_series(candidate)
        if returns:
            models_returns[model_name] = list(returns)
    return models_returns


def _promotion_candidate_version(row: Optional[Dict[str, Any]]) -> str:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    for value in (
        meta.get("model_version"),
        metrics.get("model_version"),
        candidate.get("model_id"),
        metrics.get("model_id"),
        meta.get("model_ts_ms"),
        metrics.get("model_ts_ms"),
        candidate.get("updated_ts_ms"),
    ):
        text = str(value or "").strip()
        if text:
            return text
    return str(candidate.get("model_name") or "").strip()


def _promotion_stat_gate_cache_key(row: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    candidate = dict(row or {})
    metrics = dict(candidate.get("metrics") or {})
    model_id = str(candidate.get("model_id") or metrics.get("model_id") or "").strip()
    return str(model_id), str(_promotion_candidate_version(candidate))


def _promotion_feature_ids(row: Optional[Dict[str, Any]]) -> List[str]:
    contract = _extract_feature_contract(row)
    ids = contract.get("feature_ids")
    if isinstance(ids, list):
        return [str(fid) for fid in ids if str(fid or "").strip()]
    schema = contract.get("feature_schema")
    if isinstance(schema, dict) and isinstance(schema.get("feature_ids"), list):
        return [str(fid) for fid in schema.get("feature_ids") if str(fid or "").strip()]
    return []


def _promotion_feature_gate_payload(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    out: Dict[str, Any] = {}
    for key in (
        "candidate_features",
        "new_features",
        "feature_returns",
        "feature_p_values",
        "feature_t_stats",
        "neutralization_features",
        "feature_snapshots",
        "feature_values",
        "features_matrix",
    ):
        value = meta.get(key, metrics.get(key))
        if value is not None:
            out[key] = value
    return out


def _promotion_era_gate_payload(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    out: Dict[str, Any] = {}
    aliases = {
        "evaluation_timestamps": (
            "evaluation_timestamps",
            "event_timestamps",
            "event_ts_ms",
            "signal_ts_ms",
            "ts_ms",
        ),
        "era_labels": ("era_labels", "eras", "calendar_eras"),
        "regime_labels": ("regime_labels", "regimes", "regime_series"),
        "challenger_predictions": (
            "challenger_predictions",
            "predictions",
            "predicted_z",
            "net_predictions",
            "net_pred_z",
        ),
        "realized_returns": ("realized_returns", "realized", "realized_z", "labels", "target"),
    }
    for output_key, keys in aliases.items():
        for key in keys:
            value = meta.get(key, metrics.get(key))
            if isinstance(value, list) and value:
                out[output_key] = value
                break
    return out


def _promotion_trial_count(rows: List[Dict[str, Any]]) -> int:
    trial_count = sum(1 for row in (rows or []) if _candidate_is_live_promotable(row))
    return max(1, int(trial_count))


def _promotion_gate_overrides(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    candidate = dict(row or {})
    model_name = str(candidate.get("model_name") or "").strip()
    candidate_version = _promotion_candidate_version(candidate)
    if not model_name or not candidate_version:
        return {}
    try:
        from engine.strategy.model_lifecycle import get_model_version
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_PROMOTION_REQUIREMENTS_IMPORT_FAILED",
            e,
            model_name=model_name,
            candidate_version=str(candidate_version),
        )
        return {}

    try:
        version_row = dict(get_model_version(model_name, candidate_version) or {})
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_PROMOTION_REQUIREMENTS_LOOKUP_FAILED",
            e,
            model_name=model_name,
            candidate_version=str(candidate_version),
        )
        return {}

    if not version_row:
        return {}

    train_scope = dict(version_row.get("train_scope") or {})
    meta = dict(version_row.get("meta") or {})
    trigger_meta = dict(meta.get("trigger") or {}) if isinstance(meta.get("trigger"), dict) else {}
    requirements = dict(train_scope.get("promotion_requirements") or {})
    if not requirements:
        requirements = dict(trigger_meta.get("promotion_requirements") or {})
    if not requirements:
        return {}

    overrides = dict(requirements.get("config") or {})
    if bool(requirements.get("require_stat_gate")):
        overrides["enabled"] = True
    if bool(requirements.get("require_cpcv")):
        cpcv_overrides = dict(overrides.get("cpcv") or {})
        cpcv_overrides["enabled"] = True
        overrides["cpcv"] = cpcv_overrides
    if overrides:
        overrides["required_by_candidate"] = True
        overrides["requirement_source"] = str(requirements.get("source") or "")
    return overrides


def _evaluate_promotion_stat_gate(
    row: Optional[Dict[str, Any]],
    n_competing_trials: int,
    models_returns: Optional[Dict[str, List[float]]] = None,
    champion_row: Optional[Dict[str, Any]] = None,
    con=None,
) -> Tuple[bool, Dict[str, Any]]:
    candidate = dict(row or {})
    model_name = str(candidate.get("model_name") or "").strip()
    if not model_name:
        return False, {"enabled": False, "status": "missing_model_name", "passed": False}
    env_gate_enabled = str(os.environ.get("CHAMPION_PROMOTION_USE_STAT_GATE", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    metrics = dict(candidate.get("metrics") or {})
    gate_config = _promotion_gate_overrides(candidate)
    config_gate_enabled = bool(
        gate_config.get("enabled")
        or gate_config.get("use_gate")
        or gate_config.get("use_stat_gate")
        or gate_config.get("required_by_candidate")
    )
    cpcv_gate_enabled = str(os.environ.get("CPCV_ENABLED", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    legacy_gate_enabled = bool(env_gate_enabled or config_gate_enabled or cpcv_gate_enabled)
    evidence_model_id = str(candidate.get("model_id") or metrics.get("model_id") or "").strip()
    if not evidence_model_id:
        if config_gate_enabled:
            legacy_config = dict(gate_config or {})
            nested_cpcv = dict(legacy_config.get("cpcv") or {}) if isinstance(legacy_config.get("cpcv"), dict) else {}
            cpcv_requested = bool(cpcv_gate_enabled or nested_cpcv.get("enabled"))
            if not cpcv_requested:
                nested_cpcv["enabled"] = False
                legacy_config["cpcv"] = nested_cpcv
            configured_passed, configured_diagnostics = evaluate_statistical_promotion_gate(
                model_name=model_name,
                candidate_version=_promotion_candidate_version(candidate),
                returns=_promotion_return_series(candidate),
                n_competing_trials=int(n_competing_trials),
                models_returns=models_returns,
                config=legacy_config,
                persist=False,
            )
            statistical_gate = dict((configured_diagnostics or {}).get("statistical_gate") or {})
            payload = dict(configured_diagnostics or {})
            payload["enabled"] = True
            payload["applied"] = True
            payload["model_name"] = str(model_name)
            payload["candidate_version"] = _promotion_candidate_version(candidate)
            payload["legacy_stat_gate"] = dict(configured_diagnostics or {})
            payload["configured_gate"] = dict(configured_diagnostics or {})
            payload["record_legacy_hypothesis"] = bool(statistical_gate.get("enabled"))
            payload["required_by_candidate"] = True
            payload["validation_enabled"] = True
            payload["passed"] = bool(configured_passed)
            payload["status"] = (
                str((configured_diagnostics or {}).get("status") or "configured_gate_failed")
                if not bool(configured_passed)
                else str((configured_diagnostics or {}).get("status") or "evaluated")
            )
            payload["missing_model_id_advisory"] = True
            return bool(configured_passed), payload
        return False, {
            "enabled": True,
            "applied": True,
            "status": "missing_model_id",
            "passed": False,
            "model_name": str(model_name),
            "candidate_version": _promotion_candidate_version(candidate),
        }
    feature_payload = _promotion_feature_gate_payload(candidate)
    era_payload = _promotion_era_gate_payload(candidate)
    challenger_returns = _promotion_return_series(candidate)
    champion_returns = _promotion_return_series(champion_row)
    aligned_observations = (
        min(len(challenger_returns), len(champion_returns))
        if champion_returns
        else len(challenger_returns)
    )
    min_assessment_observations = max(
        2,
        _safe_int(os.environ.get("CHAMPION_PROMOTION_MIN_OBSERVATIONS"), 50),
    )
    if not legacy_gate_enabled and int(aligned_observations) < int(min_assessment_observations):
        return True, {
            "enabled": True,
            "applied": False,
            "status": "insufficient_observations_advisory",
            "passed": True,
            "model_id": str(evidence_model_id),
            "model_name": str(model_name),
            "candidate_version": _promotion_candidate_version(candidate),
            "n_observations": int(aligned_observations),
            "min_observations": int(min_assessment_observations),
            "legacy_stat_gate": {
                "enabled": False,
                "applied": False,
                "status": "disabled",
                "passed": True,
            },
            "record_legacy_hypothesis": False,
            "validation_enabled": False,
        }
    passed, diagnostics = assess_challenger(
        model_id=str(evidence_model_id),
        model_name=model_name,
        candidate_version=_promotion_candidate_version(candidate),
        challenger_returns=challenger_returns,
        champion_returns=champion_returns,
        models_returns=models_returns,
        evaluation_timestamps=era_payload.get("evaluation_timestamps"),
        era_labels=era_payload.get("era_labels"),
        regime_labels=era_payload.get("regime_labels"),
        challenger_predictions=era_payload.get("challenger_predictions"),
        realized_returns=era_payload.get("realized_returns"),
        neutralization_features=(
            feature_payload.get("neutralization_features")
            or feature_payload.get("feature_snapshots")
            or feature_payload.get("feature_values")
            or feature_payload.get("features_matrix")
        ),
        current_feature_ids=_promotion_feature_ids(champion_row),
        challenger_feature_ids=_promotion_feature_ids(candidate),
        candidate_features=feature_payload.get("candidate_features"),
        new_features=feature_payload.get("new_features"),
        feature_returns=feature_payload.get("feature_returns"),
        feature_p_values=feature_payload.get("feature_p_values"),
        feature_t_stats=feature_payload.get("feature_t_stats"),
        con=con,
    )
    payload = dict(diagnostics or {})
    if legacy_gate_enabled:
        legacy_config = dict(gate_config or {})
        nested_cpcv = dict(legacy_config.get("cpcv") or {}) if isinstance(legacy_config.get("cpcv"), dict) else {}
        cpcv_requested = bool(cpcv_gate_enabled or nested_cpcv.get("enabled"))
        if not cpcv_requested:
            nested_cpcv["enabled"] = False
            legacy_config["cpcv"] = nested_cpcv
        configured_passed, configured_diagnostics = evaluate_statistical_promotion_gate(
            model_name=model_name,
            candidate_version=_promotion_candidate_version(candidate),
            returns=_promotion_return_series(candidate),
            n_competing_trials=int(n_competing_trials),
            models_returns=models_returns,
            config=legacy_config,
            persist=False,
        )
        statistical_gate = dict((configured_diagnostics or {}).get("statistical_gate") or {})
        payload["legacy_stat_gate"] = dict(configured_diagnostics or {})
        payload["configured_gate"] = dict(configured_diagnostics or {})
        payload["record_legacy_hypothesis"] = bool(statistical_gate.get("enabled"))
        payload["passed"] = bool(payload.get("passed")) and bool(configured_passed)
        payload["status"] = (
            str((configured_diagnostics or {}).get("status") or "configured_gate_failed")
            if not bool(configured_passed)
            else str(payload.get("status") or "evaluated")
        )
        payload["requested_gate_config"] = dict(legacy_config)
        payload["required_by_candidate"] = bool(config_gate_enabled)
        payload["validation_enabled"] = True
        return bool(passed and configured_passed), payload
    payload["legacy_stat_gate"] = {"enabled": False, "applied": False, "status": "disabled", "passed": True}
    payload["record_legacy_hypothesis"] = False
    payload["validation_enabled"] = True
    return bool(passed), payload


def _sync_registry_runtime(row: Optional[Dict[str, Any]], *, status: str, last_promotion_ts_ms: Optional[int] = None) -> None:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    model_name = str(candidate.get("model_name") or "").strip()
    if not model_name:
        return
    metrics = {
        **_candidate_runtime_metrics(candidate),
        "symbol": str(candidate.get("symbol") or "").upper().strip(),
        "horizon_s": _safe_int(candidate.get("horizon_s"), 0),
        "regime": str(candidate.get("regime") or "global"),
        "score": _safe_float(candidate.get("score"), 0.0),
    }
    try:
        update_model_runtime(
            model_name,
            regime=str(candidate.get("regime") or "global"),
            model_kind=(str(meta.get("model_kind") or "").strip() or None),
            model_ts_ms=(_safe_int(meta.get("model_ts_ms"), 0) or None),
            status=str(status),
            performance_metrics=metrics,
            last_promotion_ts_ms=last_promotion_ts_ms,
        )
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_SYNC_REGISTRY_RUNTIME_FAILED",
            e,
            model_name=model_name,
            status=str(status),
            regime=str(candidate.get("regime") or "global"),
        )


def _ranking_runtime_metrics(row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ranking = dict(row or {})
    metrics = dict(ranking.get("metrics") or {})
    trade_count = _safe_int(ranking.get("trade_count"), 0)
    win_rate = ranking.get("win_rate")
    if win_rate is None and trade_count > 0:
        win_rate = float(_safe_int(ranking.get("wins"), 0)) / float(trade_count)
    rolling_total_pnl = _safe_float(metrics.get("total_pnl"), _safe_float(ranking.get("net_pnl"), 0.0))
    capital_base = _safe_float(metrics.get("capital_base_sum"), _safe_float(ranking.get("capital_base_sum"), 0.0))
    return {
        "rolling_score": _safe_float(metrics.get("score"), _safe_float(ranking.get("score"), 0.0)),
        "rolling_total_pnl": rolling_total_pnl,
        "rolling_window_ms": _safe_int(metrics.get("rolling_window_ms"), MODEL_COMPETITION_WINDOW_S * 1000),
        "max_drawdown": _safe_float(ranking.get("max_drawdown"), 0.0),
        "recent_total_pnl": _safe_float(metrics.get("recent_total_pnl"), 0.0),
        "prior_total_pnl": _safe_float(metrics.get("prior_total_pnl"), 0.0),
        "capital_base": float(capital_base),
        "return_pct": _capital_adjusted_return_pct(rolling_total_pnl, capital_base),
        "trade_count": trade_count,
        "win_rate": (None if win_rate is None else float(win_rate)),
        "observation_ms": _safe_int(metrics.get("observation_ms"), 0),
    }


def _ranking_is_eligible(row: Optional[Dict[str, Any]]) -> bool:
    metrics = _ranking_runtime_metrics(row)
    if _safe_int(metrics.get("trade_count"), 0) < int(PROMOTION_MIN_TRADES):
        return False
    if _safe_int(metrics.get("observation_ms"), 0) < int(PROMOTION_MIN_OBSERVATION_S * 1000):
        return False
    win_rate = metrics.get("win_rate")
    if win_rate is not None and float(win_rate) < float(STABILITY_MIN_WIN_RATE):
        return False
    if _safe_float(metrics.get("rolling_total_pnl"), 0.0) <= 0.0:
        return False
    return True


def _load_post_commit_action_row(row: Any) -> Dict[str, Any]:
    return {
        "id": _safe_int(row[0], 0),
        "action_name": str(row[1] or ""),
        "args": list(_safe_json_value(row[2], []) or []),
        "kwargs": dict(_safe_json_value(row[3], {}) or {}),
        "attempt_count": _safe_int(row[4], 0),
        "status": str(row[5] or "pending"),
        "available_ts_ms": _safe_int(row[6], 0),
        "lease_expires_ts_ms": _safe_int(row[7], 0),
        "last_error": (None if row[8] in (None, "") else str(row[8])),
    }


def _execute_post_commit_action(action_name: str, args: Optional[List[Any]] = None, kwargs: Optional[Dict[str, Any]] = None) -> None:
    fn_name = str(action_name or "")
    call_args = list(args or [])
    call_kwargs = dict(kwargs or {})
    if fn_name == "sync_assignment_to_model_registry":
        _sync_assignment_to_model_registry(*call_args, **call_kwargs)
    elif fn_name == "sync_registry_runtime":
        _sync_registry_runtime(*call_args, **call_kwargs)
    elif fn_name == "audit":
        audit(**call_kwargs)
    elif fn_name == "record_hypothesis_result":
        record_hypothesis_result(**call_kwargs)
    else:
        raise ValueError(f"unknown_post_commit_action:{fn_name}")


def get_competition_post_commit_status(*, limit_failed: int = 10) -> Dict[str, Any]:
    init_db()
    con = connect()
    try:
        _ensure_post_commit_schema(con)
        rows = con.execute(
            f"""
            SELECT status, COUNT(*)
            FROM {_POST_COMMIT_OUTBOX_TABLE}
            GROUP BY status
            """
        ).fetchall() or []
        counts = {str(status or ""): _safe_int(count, 0) for status, count in rows}
        failed_rows = con.execute(
            f"""
            SELECT id, action_name, attempt_count, status, available_ts_ms, lease_expires_ts_ms, last_error
            FROM {_POST_COMMIT_OUTBOX_TABLE}
            WHERE status IN ('pending','failed','running')
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(max(1, limit_failed)),),
        ).fetchall() or []
        return {
            "ok": True,
            "pending_count": int(counts.get("pending", 0)),
            "running_count": int(counts.get("running", 0)),
            "failed_count": int(counts.get("failed", 0)),
            "completed_count": int(counts.get("completed", 0)),
            "failed_actions": [
                {
                    "id": _safe_int(row[0], 0),
                    "action_name": str(row[1] or ""),
                    "attempt_count": _safe_int(row[2], 0),
                    "status": str(row[3] or ""),
                    "available_ts_ms": _safe_int(row[4], 0),
                    "lease_expires_ts_ms": _safe_int(row[5], 0),
                    "last_error": (None if row[6] in (None, "") else str(row[6])),
                }
                for row in failed_rows
            ],
            "degraded": bool(int(counts.get("pending", 0)) > 0 or int(counts.get("failed", 0)) > 0),
        }
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("CHAMPION_MANAGER_POST_COMMIT_STATUS_CLOSE_FAILED", e)


def drain_competition_post_commit_actions(*, max_actions: Optional[int] = None) -> Dict[str, Any]:
    init_db()
    processed = 0
    completed = 0
    failed = 0
    limit = int(max(1, int(max_actions or COMPETITION_POST_COMMIT_MAX_ACTIONS)))
    with _COMPETITION_LOCK:
        seen_action_ids: set[int] = set()
        while processed < limit:
            con = connect()
            action: Dict[str, Any] = {}
            try:
                _ensure_post_commit_schema(con)
                now_ms = _now_ms()
                row = con.execute(
                    f"""
                    SELECT id, action_name, args_json, kwargs_json, attempt_count, status, available_ts_ms, lease_expires_ts_ms, last_error
                    FROM {_POST_COMMIT_OUTBOX_TABLE}
                    WHERE (
                      status IN ('pending','failed') AND available_ts_ms <= ?
                    ) OR (
                      status='running' AND lease_expires_ts_ms > 0 AND lease_expires_ts_ms <= ?
                    )
                    ORDER BY id ASC
                    LIMIT 1
                    """,
                    (int(now_ms), int(now_ms)),
                ).fetchone()
                if not row:
                    break
                action = _load_post_commit_action_row(row)
                action_id = _safe_int(action.get("id"), 0)
                if action_id in seen_action_ids:
                    break
                seen_action_ids.add(action_id)
                owns_txn = _begin_owned_write(con)
                try:
                    con.execute(
                        f"""
                        UPDATE {_POST_COMMIT_OUTBOX_TABLE}
                        SET status='running',
                            attempt_count=?,
                            updated_ts_ms=?,
                            lease_expires_ts_ms=?
                        WHERE id=?
                        """,
                        (
                            int(_safe_int(action.get("attempt_count"), 0) + 1),
                            int(now_ms),
                            int(now_ms + max(1, COMPETITION_POST_COMMIT_LEASE_MS)),
                            int(action.get("id") or 0),
                        ),
                    )
                    con.commit()
                except Exception:
                    if owns_txn and bool(getattr(con, "in_transaction", False)):
                        con.rollback()
                    raise
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn_nonfatal("CHAMPION_MANAGER_POST_COMMIT_LOOP_CLOSE_FAILED", e)

            if not action:
                break

            processed += 1
            action_id = _safe_int(action.get("id"), 0)
            action_name = str(action.get("action_name") or "")
            action_args = list(action.get("args") or [])
            action_kwargs = dict(action.get("kwargs") or {})
            try:
                _execute_post_commit_action(action_name, action_args, action_kwargs)
            except Exception as e:
                failed += 1
                retry_count = max(1, _safe_int(action.get("attempt_count"), 0) + 1)
                delay_ms = min(int(COMPETITION_POST_COMMIT_RETRY_MAX_MS), int(1000 * (2 ** min(retry_count - 1, 6))))
                con = connect()
                try:
                    _ensure_post_commit_schema(con)
                    owns_txn = _begin_owned_write(con)
                    try:
                        err_ts_ms = _now_ms()
                        con.execute(
                            f"""
                            UPDATE {_POST_COMMIT_OUTBOX_TABLE}
                            SET status='failed',
                                updated_ts_ms=?,
                                available_ts_ms=?,
                                lease_expires_ts_ms=0,
                                last_error=?,
                                last_error_ts_ms=?
                            WHERE id=?
                            """,
                            (
                                int(err_ts_ms),
                                int(err_ts_ms + delay_ms),
                                f"{type(e).__name__}: {e}",
                                int(err_ts_ms),
                                int(action_id),
                            ),
                        )
                        con.commit()
                    except Exception:
                        if owns_txn and bool(getattr(con, "in_transaction", False)):
                            con.rollback()
                        raise
                finally:
                    try:
                        con.close()
                    except Exception as close_err:
                        _warn_nonfatal("CHAMPION_MANAGER_POST_COMMIT_ACTION_CLOSE_FAILED", close_err)
                _warn_nonfatal(
                    "CHAMPION_MANAGER_POST_COMMIT_ACTION_FAILED",
                    e,
                    fn_name=action_name,
                    args_count=len(action_args),
                    kwargs_keys=sorted(str(k) for k in action_kwargs.keys()),
                    action_id=int(action_id),
                    retry_delay_ms=int(delay_ms),
                )
                continue

            completed += 1
            con = connect()
            try:
                _ensure_post_commit_schema(con)
                owns_txn = _begin_owned_write(con)
                try:
                    done_ts_ms = _now_ms()
                    con.execute(
                        f"""
                        UPDATE {_POST_COMMIT_OUTBOX_TABLE}
                        SET status='completed',
                            updated_ts_ms=?,
                            completed_ts_ms=?,
                            lease_expires_ts_ms=0,
                            last_error=NULL,
                            last_error_ts_ms=NULL
                        WHERE id=?
                        """,
                        (
                            int(done_ts_ms),
                            int(done_ts_ms),
                            int(action_id),
                        ),
                    )
                    con.commit()
                except Exception:
                    if owns_txn and bool(getattr(con, "in_transaction", False)):
                        con.rollback()
                    raise
            finally:
                try:
                    con.close()
                except Exception as e:
                    _warn_nonfatal("CHAMPION_MANAGER_POST_COMMIT_FINAL_CLOSE_FAILED", e)

    status = get_competition_post_commit_status()
    status.update(
        {
            "processed": int(processed),
            "completed_now": int(completed),
            "failed_now": int(failed),
        }
    )
    return status


def evaluate_competition_cycle() -> Dict[str, Any]:
    init_db()
    pre_post_commit = drain_competition_post_commit_actions(max_actions=COMPETITION_POST_COMMIT_MAX_ACTIONS)
    rankings = recompute_model_rankings()
    replay_cache = get_cached_replay_validation_snapshot(max_age_ms=int(REPLAY_FRESH_MAX_AGE_MS))
    replay_validation = dict(replay_cache.get("snapshot") or {})
    replay_models = dict(replay_validation.get("models") or {})
    replay_fresh = bool(replay_cache.get("fresh"))
    self_critic = run_self_critic(replay_snapshot=replay_validation)
    blocked_keys = set(str(x) for x in (self_critic.get("blocked_keys") or []))

    with _COMPETITION_LOCK:
        con = connect()
        try:
            _ensure_post_commit_schema(con)
            stat_gate_cache: Dict[Tuple[str, str], Tuple[bool, Dict[str, Any]]] = {}

            def _enqueue_post_commit(fn_name: str, *args, **kwargs) -> None:
                _queue_post_commit_action(con, str(fn_name), *args, **kwargs)

            def _cached_promotion_stat_gate(
                target_row: Optional[Dict[str, Any]],
                trial_count: int,
                *,
                candidate_returns: Optional[Dict[str, List[float]]],
                incumbent_row: Optional[Dict[str, Any]],
            ) -> Tuple[bool, Dict[str, Any]]:
                cache_key = _promotion_stat_gate_cache_key(target_row)
                if cache_key[0] and cache_key in stat_gate_cache:
                    cached_ok, cached_payload = stat_gate_cache[cache_key]
                    payload = dict(cached_payload or {})
                    payload["cache_hit"] = True
                    return bool(cached_ok), payload
                ok, payload = _evaluate_promotion_stat_gate(
                    target_row,
                    int(trial_count),
                    models_returns=candidate_returns,
                    champion_row=incumbent_row,
                    con=con,
                )
                if cache_key[0]:
                    stat_gate_cache[cache_key] = (bool(ok), dict(payload or {}))
                return bool(ok), dict(payload or {})

            rows = con.execute(
                """
                SELECT model_id, model_name, symbol, horizon_s, regime, stage, score, trades, wins, losses, net_pnl, meta_json
                FROM model_marketplace_scores
                ORDER BY symbol ASC, horizon_s ASC, regime ASC, score DESC, updated_ts_ms DESC
                """
            ).fetchall()

            grouped: Dict[str, List[Dict[str, Any]]] = {}
            for row in rows or []:
                meta = _safe_json_dict(row[11])
                if not _score_source_is_competition_candidate(meta):
                    continue
                rec = {
                    "model_id": str(row[0] or "baseline"),
                    "model_name": str(row[1] or ""),
                    "symbol": str(row[2] or "").upper().strip(),
                    "horizon_s": _safe_int(row[3], 0),
                    "regime": str(row[4] or "global"),
                    "stage": str(row[5] or "challenger"),
                    "score": _safe_float(row[6], 0.0),
                    "trades": _safe_int(row[7], 0),
                    "wins": _safe_int(row[8], 0),
                    "losses": _safe_int(row[9], 0),
                    "net_pnl": _safe_float(row[10], 0.0),
                    "meta": meta,
                }
                grouped.setdefault(
                    _competition_key(rec["symbol"], rec["horizon_s"], rec["regime"]),
                    [],
                ).append(rec)

            changes: List[Dict[str, Any]] = []
            if not replay_fresh:
                capital_plan = compute_capital_plan()
                out = {
                    "ok": True,
                    "changes": [],
                    "replay_validation": replay_validation,
                    "replay_cache": replay_cache,
                    "self_critic": self_critic,
                    "post_commit_actions": {
                        "pre_drain": pre_post_commit,
                        "post_drain": get_competition_post_commit_status(),
                    },
                    "capital_plan": capital_plan,
                    "status": ("post_commit_degraded" if bool(get_competition_post_commit_status().get("degraded")) else "replay_stale"),
                }
                try:
                    meta_set("competition_cycle_status", json.dumps(out, separators=(",", ":"), sort_keys=True))
                except Exception as e:
                    _warn_nonfatal(
                        "CHAMPION_MANAGER_REPLAY_STALE_STATUS_PERSIST_FAILED",
                        e,
                        once_key="replay_stale_status_persist",
                    )
                out["snapshot"] = current_competition_snapshot()
                return out

            _begin_owned_write(con)

            # Each symbol/horizon/regime group runs the same promotion logic:
            # rank candidates, verify replay approval, respect self-critic
            # blocks and cooldowns, then decide whether to keep, promote, or
            # demote the current champion.
            for _, candidates in grouped.items():
                if not candidates:
                    continue
                ranked_candidates = sorted(
                    candidates,
                    key=lambda x: (
                        -_primary_comparison_metric(
                            capital_base=_safe_float(_candidate_runtime_metrics(x).get("capital_base"), 0.0),
                            return_pct=_safe_float(_candidate_runtime_metrics(x).get("return_pct"), 0.0),
                            net_pnl=_safe_float(_candidate_runtime_metrics(x).get("rolling_total_pnl"), 0.0),
                        )[0],
                        _safe_float(_candidate_runtime_metrics(x).get("max_drawdown"), 0.0),
                        -_safe_float(_candidate_runtime_metrics(x).get("win_rate"), -1.0),
                        -_safe_int(x.get("trades"), 0),
                        -_safe_float(_candidate_runtime_metrics(x).get("rolling_total_pnl"), 0.0),
                        -_safe_float(x.get("score"), 0.0),
                    ),
                )
                candidates = ranked_candidates
                best = next((row for row in ranked_candidates if _candidate_is_live_promotable(row)), ranked_candidates[0])
                best_metrics = _candidate_runtime_metrics(best)
                current = get_champion_assignment(
                    "global",
                    str(best.get("symbol") or ""),
                    int(best.get("horizon_s") or 0),
                )
                current_name = str((current or {}).get("model_name") or "")
                current_row = next(
                    (row for row in candidates if str(row.get("model_name") or "") == current_name),
                    None,
                )
                current_metrics = _candidate_runtime_metrics(current_row)

                best_blocked = _candidate_block_key(
                    str(best.get("model_name") or ""),
                    str(best.get("symbol") or ""),
                    _safe_int(best.get("horizon_s"), 0),
                    str(best.get("regime") or "global"),
                ) in blocked_keys
                best_replay = _candidate_replay_row(best, replay_models)
                best_replay_approved = bool(best_replay.get("approved")) if best_replay else False
                current_blocked = False
                current_replay = _candidate_replay_row(current_row, replay_models) if current_row else {}
                current_replay_approved = bool(current_replay.get("approved")) if current_replay else False
                if current_row:
                    current_blocked = _candidate_block_key(
                        str(current_row.get("model_name") or ""),
                        str(current_row.get("symbol") or ""),
                        _safe_int(current_row.get("horizon_s"), 0),
                        str(current_row.get("regime") or "global"),
                    ) in blocked_keys
                current_meta = dict((current or {}).get("meta") or {})
                last_promotion_ts_ms = _safe_int(
                    current_meta.get("last_promotion_ts_ms")
                    or current_meta.get("promotion_ts_ms")
                    or current_meta.get("updated_ts_ms"),
                    0,
                )
                cooldown_active = (
                    last_promotion_ts_ms > 0
                    and (_now_ms() - last_promotion_ts_ms) < int(PROMOTION_COOLDOWN_S * 1000)
                )
                challenger_score_delta = (
                    _safe_float(best_metrics.get("rolling_score"), 0.0)
                    - _safe_float(current_metrics.get("rolling_score"), 0.0)
                )
                challenger_pnl_delta = (
                    _safe_float(best_metrics.get("rolling_total_pnl"), 0.0)
                    - _safe_float(current_metrics.get("rolling_total_pnl"), 0.0)
                )
                challenger_return_delta = (
                    _safe_float(best_metrics.get("return_pct"), 0.0)
                    - _safe_float(current_metrics.get("return_pct"), 0.0)
                )
                promotion_delta = _promotion_delta(best_metrics, current_metrics)
                champion_decay = (
                    _safe_float(current_metrics.get("recent_total_pnl"), 0.0)
                    - _safe_float(current_metrics.get("prior_total_pnl"), 0.0)
                )
                champion_drawdown = _safe_float(current_metrics.get("max_drawdown"), 0.0)
                hard_demote = bool(
                    current_row
                    and (
                        champion_drawdown >= float(DEMOTION_MAX_DRAWDOWN)
                        or champion_decay <= float(DEMOTION_DECAY_THRESHOLD)
                        or _safe_float(current_row.get("net_pnl"), 0.0) <= float(DEMOTION_MIN_NET_PNL)
                    )
                )

                target = current_row or {}
                reason = "keep_current" if current_row else "no_bootstrap"
                if not current_row:
                    if (
                        _candidate_is_live_promotable(best)
                        and
                        not best_blocked
                        and best_replay_approved
                        and _candidate_is_eligible(best)
                    ):
                        target = best
                        reason = "bootstrap_best"
                elif current_blocked or hard_demote:
                    fallback = next(
                        (
                            row
                            for row in candidates
                            if str(row.get("model_name") or "") != str(current_name)
                            if _candidate_is_live_promotable(row)
                            and bool(_candidate_replay_row(row, replay_models).get("approved"))
                            and _candidate_is_eligible(row)
                            and _candidate_block_key(
                                str(row.get("model_name") or ""),
                                str(row.get("symbol") or ""),
                                _safe_int(row.get("horizon_s"), 0),
                                str(row.get("regime") or "global"),
                            )
                            not in blocked_keys
                        ),
                        {},
                    )
                    target = fallback
                    if champion_drawdown >= float(DEMOTION_MAX_DRAWDOWN):
                        reason = "demotion_drawdown"
                    elif champion_decay <= float(DEMOTION_DECAY_THRESHOLD):
                        reason = "demotion_decay"
                    else:
                        reason = "demotion_fallback"
                elif (
                    str(best.get("model_name") or "") != str(current_name)
                    and _candidate_is_live_promotable(best)
                    and not best_blocked
                    and best_replay_approved
                    and _candidate_is_eligible(best)
                    and float(promotion_delta.get("delta") or 0.0) >= float(promotion_delta.get("threshold") or 0.0)
                    and (not cooldown_active)
                ):
                    target = best
                    reason = "challenger_outperformance"
                elif best_blocked and current_row:
                    target = current_row
                    reason = "best_blocked_self_critic"
                elif current_row and (
                    _candidate_is_live_promotable(best)
                    and best_replay_approved
                    and _candidate_is_eligible(best)
                    and float(promotion_delta.get("delta") or 0.0) >= float(
                        max(float(promotion_delta.get("threshold") or 0.0), 0.0)
                    )
                ) and not best_blocked:
                    target = best
                    reason = "demotion_replace"
                elif not _candidate_is_live_promotable(best):
                    target = current_row or target
                    reason = "shadow_candidate_only"
                elif _candidate_is_live_promotable(best) and not best_replay_approved:
                    target = current_row or {}
                    reason = "replay_gate_blocked"

                stat_gate: Dict[str, Any] = {}
                target_name = str((target or {}).get("model_name") or "")
                if target_name and target_name != str(current_name):
                    stat_gate_ok, stat_gate = _cached_promotion_stat_gate(
                        target,
                        _promotion_trial_count(candidates),
                        candidate_returns=_promotion_models_returns(candidates),
                        incumbent_row=current_row,
                    )
                    if bool((stat_gate or {}).get("record_legacy_hypothesis")):
                        _enqueue_post_commit(
                            "record_hypothesis_result",
                            model_name=str((target or {}).get("model_name") or ""),
                            candidate_version=_promotion_candidate_version(target),
                            n_observations=_safe_int((stat_gate or {}).get("n_observations"), 0),
                            t_statistic=_safe_float((stat_gate or {}).get("t_statistic"), 0.0),
                            deflated_sharpe=_safe_float((stat_gate or {}).get("deflated_sharpe"), 0.0),
                            threshold_t=_safe_float((stat_gate or {}).get("threshold_t"), 0.0),
                            n_competing_trials=_safe_int((stat_gate or {}).get("n_competing_trials"), 0),
                            passed=bool((stat_gate or {}).get("passed")),
                            diagnostics=dict(stat_gate or {}),
                        )
                    if not stat_gate_ok:
                        reason = f"{reason}_stat_gate_blocked"
                        if current_row and not (current_blocked or hard_demote):
                            target = current_row
                        else:
                            target = {}

                previous_meta = dict((current or {}).get("meta") or {})
                previous_name = str((current or {}).get("model_name") or "")
                target_name = str((target or {}).get("model_name") or "")
                stat_gate_meta = dict(stat_gate or {}) if bool((stat_gate or {}).get("validation_enabled", (stat_gate or {}).get("enabled"))) else {}
                change = {
                    "symbol": str(best.get("symbol") or ""),
                    "horizon_s": _safe_int(best.get("horizon_s"), 0),
                    "regime": str(best.get("regime") or "global"),
                    "from_model_name": previous_name,
                    "to_model_name": target_name,
                    "reason": reason,
                    "blocked_current": bool(current_blocked),
                    "blocked_best": bool(best_blocked),
                    "current_replay_approved": bool(current_replay_approved),
                    "best_replay_approved": bool(best_replay_approved),
                    "rolling_window_ms": _safe_int(best_metrics.get("rolling_window_ms"), MODEL_COMPETITION_WINDOW_S * 1000),
                    "comparison_metric": str(promotion_delta.get("metric") or "net_pnl"),
                    "challenger_delta": float(promotion_delta.get("delta") or 0.0),
                    "challenger_score_delta": float(challenger_score_delta),
                    "challenger_return_delta": float(challenger_return_delta),
                    "challenger_pnl_delta": float(challenger_pnl_delta),
                    "champion_drawdown": float(champion_drawdown),
                    "champion_decay": float(champion_decay),
                    "cooldown_active": bool(cooldown_active),
                    "stat_gate": stat_gate_meta,
                }

                if not target_name:
                    _sync_marketplace_stages(con, candidates, "")
                    if current_row:
                        _clear_champion_assignment(
                            con=con,
                            scope="global",
                            symbol=str(best.get("symbol") or ""),
                            horizon_s=_safe_int(best.get("horizon_s"), 0),
                        )
                        _enqueue_post_commit(
                            "sync_registry_runtime",
                            _copy_candidate_row(current_row),
                            status="challenger",
                            last_promotion_ts_ms=int(last_promotion_ts_ms or 0),
                        )
                        changes.append(change)
                    for row in candidates:
                        _enqueue_post_commit(
                            "sync_registry_runtime",
                            _copy_candidate_row(row),
                            status="challenger",
                            last_promotion_ts_ms=None,
                        )
                    continue

                if (not current_row) or previous_name != target_name:
                    promotion_ts_ms = _now_ms()
                    meta = {
                        **previous_meta,
                        "reason": str(reason),
                        "previous_model_name": previous_name,
                        "previous_updated_ts_ms": _safe_int((current or {}).get("updated_ts_ms"), 0),
                        "last_promotion_ts_ms": int(promotion_ts_ms),
                        "promotion_ts_ms": int(promotion_ts_ms),
                        "rolling_metrics": _candidate_runtime_metrics(target),
                    }
                    if stat_gate_meta:
                        meta["stat_gate"] = dict(stat_gate_meta)
                    set_champion_assignment(
                        con=con,
                        scope="global",
                        symbol=str(best.get("symbol") or ""),
                        model_name=target_name,
                        horizon_s=_safe_int(best.get("horizon_s"), 0),
                        challenger_name=previous_name,
                        regime=str(best.get("regime") or "global"),
                        state="challenger",
                        meta=meta,
                    )
                    set_champion_assignment(
                        con=con,
                        scope="global",
                        symbol=str(best.get("symbol") or ""),
                        model_name=target_name,
                        horizon_s=_safe_int(best.get("horizon_s"), 0),
                        challenger_name=previous_name,
                        regime=str(best.get("regime") or "global"),
                        state="champion",
                        meta=meta,
                    )
                    _enqueue_post_commit("sync_assignment_to_model_registry", _copy_candidate_row(target))
                    _enqueue_post_commit(
                        "sync_registry_runtime",
                        _copy_candidate_row(target),
                        status="champion",
                        last_promotion_ts_ms=int(promotion_ts_ms),
                    )
                    if current_row and previous_name and previous_name != target_name:
                        _enqueue_post_commit(
                            "sync_registry_runtime",
                            _copy_candidate_row(current_row),
                            status="challenger",
                            last_promotion_ts_ms=int(last_promotion_ts_ms or 0),
                        )
                    target_meta = dict((target or {}).get("meta") or {})
                    previous_model_kind = str(previous_meta.get("model_kind") or "").strip() or None
                    previous_model_ts_ms = _safe_int(previous_meta.get("model_ts_ms"), 0) or None
                    target_model_kind = str(target_meta.get("model_kind") or "").strip() or None
                    target_model_ts_ms = _safe_int(target_meta.get("model_ts_ms"), 0) or None
                    _enqueue_post_commit(
                        "audit",
                        actor="competition_cycle",
                        action="promote_competition_champion",
                        model_name=target_name,
                        from_kind=previous_model_kind,
                        from_ts_ms=previous_model_ts_ms,
                        to_kind=target_model_kind,
                        to_ts_ms=target_model_ts_ms,
                        from_artifact_sha256=(
                            str(previous_meta.get("artifact_sha256") or "").strip() or None
                        ),
                        to_artifact_sha256=(
                            str(target_meta.get("artifact_sha256") or "").strip() or None
                        ),
                        regime=str(best.get("regime") or "global"),
                        reason={
                            **change,
                            "target_score": _safe_float((target or {}).get("score"), 0.0),
                            "target_trades": _safe_int((target or {}).get("trades"), 0),
                            "current_score": _safe_float((current_row or {}).get("score"), 0.0),
                            "current_trades": _safe_int((current_row or {}).get("trades"), 0),
                            "target_replay": _promotion_replay_provenance(target, replay_models),
                            "current_replay": _promotion_replay_provenance(current_row, replay_models),
                            "stat_gate": stat_gate_meta,
                        },
                    )
                    changes.append(change)
                else:
                    _enqueue_post_commit(
                        "sync_registry_runtime",
                        _copy_candidate_row(target),
                        status="champion",
                        last_promotion_ts_ms=int(last_promotion_ts_ms or 0),
                    )

                _sync_marketplace_stages(con, candidates, target_name)
                for row in candidates:
                    if str(row.get("model_name") or "") != str(target_name):
                        _enqueue_post_commit(
                            "sync_registry_runtime",
                            _copy_candidate_row(row),
                            status="challenger",
                            last_promotion_ts_ms=None,
                        )

            global_rows = list(rankings.get("rows") or [])
            global_best = dict(global_rows[0]) if global_rows else {}
            global_current = get_champion_assignment(
                MODEL_COMPETITION_SCOPE,
                MODEL_COMPETITION_SYMBOL,
                MODEL_COMPETITION_HORIZON_S,
            )
            global_current_name = str((global_current or {}).get("model_name") or "").strip()
            global_current_row = next(
                (row for row in global_rows if str((row or {}).get("model_name") or "").strip() == global_current_name),
                None,
            )
            global_best_name = str((global_best or {}).get("model_name") or "").strip()
            global_last_promotion_ts_ms = _safe_int(
                dict((global_current or {}).get("meta") or {}).get("last_promotion_ts_ms")
                or (global_current or {}).get("updated_ts_ms"),
                0,
            )
            global_cooldown_active = bool(
                global_last_promotion_ts_ms > 0
                and (_now_ms() - global_last_promotion_ts_ms) < int(PROMOTION_COOLDOWN_S * 1000)
            )
            global_best_metrics = _ranking_runtime_metrics(global_best)
            global_current_metrics = _ranking_runtime_metrics(global_current_row)
            global_score_delta = (
                _safe_float(global_best_metrics.get("rolling_score"), 0.0)
                - _safe_float(global_current_metrics.get("rolling_score"), 0.0)
            )
            global_pnl_delta = (
                _safe_float(global_best_metrics.get("rolling_total_pnl"), 0.0)
                - _safe_float(global_current_metrics.get("rolling_total_pnl"), 0.0)
            )
            global_return_delta = (
                _safe_float(global_best_metrics.get("return_pct"), 0.0)
                - _safe_float(global_current_metrics.get("return_pct"), 0.0)
            )
            global_promotion_delta = _promotion_delta(global_best_metrics, global_current_metrics)
            global_drawdown = _safe_float(global_current_metrics.get("max_drawdown"), 0.0)
            global_decay = (
                _safe_float(global_current_metrics.get("recent_total_pnl"), 0.0)
                - _safe_float(global_current_metrics.get("prior_total_pnl"), 0.0)
            )
            global_current_invalid = bool(
                global_current_row
                and (
                    global_drawdown >= float(DEMOTION_MAX_DRAWDOWN)
                    or global_decay <= float(DEMOTION_DECAY_THRESHOLD)
                    or _safe_float(global_current_row.get("net_pnl"), 0.0) <= float(DEMOTION_MIN_NET_PNL)
                )
            )
            global_target = global_current_row or {}
            global_reason = "keep_current" if global_current_row else "no_bootstrap"
            if not global_current_row:
                if global_best_name and _ranking_is_eligible(global_best):
                    global_target = global_best
                    global_reason = "bootstrap_best"
            elif global_current_invalid:
                fallback = next(
                    (
                        row for row in global_rows
                        if str((row or {}).get("model_name") or "").strip() != str(global_current_name)
                        and _ranking_is_eligible(row)
                    ),
                    {},
                )
                global_target = fallback
                if global_drawdown >= float(DEMOTION_MAX_DRAWDOWN):
                    global_reason = "demotion_drawdown"
                elif global_decay <= float(DEMOTION_DECAY_THRESHOLD):
                    global_reason = "demotion_decay"
                else:
                    global_reason = "demotion_fallback"
            elif (
                global_best_name
                and global_best_name != global_current_name
                and _ranking_is_eligible(global_best)
                and float(global_promotion_delta.get("delta") or 0.0) >= float(global_promotion_delta.get("threshold") or 0.0)
                and not global_cooldown_active
            ):
                global_target = global_best
                global_reason = "challenger_outperformance"

            global_stat_gate: Dict[str, Any] = {}
            global_target_name = str((global_target or {}).get("model_name") or "").strip()
            if global_target_name and global_target_name != str(global_current_name):
                global_stat_gate_ok, global_stat_gate = _cached_promotion_stat_gate(
                    global_target,
                    max(1, len(global_rows)),
                    candidate_returns=_promotion_models_returns(global_rows),
                    incumbent_row=global_current_row,
                )
                if bool((global_stat_gate or {}).get("record_legacy_hypothesis")):
                    _enqueue_post_commit(
                        "record_hypothesis_result",
                        model_name=str((global_target or {}).get("model_name") or ""),
                        candidate_version=_promotion_candidate_version(global_target),
                        n_observations=_safe_int((global_stat_gate or {}).get("n_observations"), 0),
                        t_statistic=_safe_float((global_stat_gate or {}).get("t_statistic"), 0.0),
                        deflated_sharpe=_safe_float((global_stat_gate or {}).get("deflated_sharpe"), 0.0),
                        threshold_t=_safe_float((global_stat_gate or {}).get("threshold_t"), 0.0),
                        n_competing_trials=_safe_int((global_stat_gate or {}).get("n_competing_trials"), 0),
                        passed=bool((global_stat_gate or {}).get("passed")),
                        diagnostics=dict(global_stat_gate or {}),
                    )
                if not global_stat_gate_ok:
                    global_reason = f"{global_reason}_stat_gate_blocked"
                    if global_current_row and not global_current_invalid:
                        global_target = global_current_row
                    else:
                        global_target = {}
                    global_target_name = str((global_target or {}).get("model_name") or "").strip()

            if global_target_name:
                global_stat_gate_meta = (
                    dict(global_stat_gate or {})
                    if bool((global_stat_gate or {}).get("validation_enabled", (global_stat_gate or {}).get("enabled")))
                    else {}
                )
                global_meta = {
                    "reason": str(global_reason),
                    "rank": _safe_int((global_target or {}).get("rank"), 0),
                    "net_pnl": _safe_float((global_target or {}).get("net_pnl"), 0.0),
                    "return_pct": _safe_float((global_target or {}).get("return_pct"), 0.0),
                    "max_drawdown": _safe_float((global_target or {}).get("max_drawdown"), 0.0),
                    "win_rate": (global_target or {}).get("win_rate"),
                    "trade_count": _safe_int((global_target or {}).get("trade_count"), 0),
                    "rolling_metrics": _ranking_runtime_metrics(global_target),
                    "last_promotion_ts_ms": int(_now_ms() if global_target_name != global_current_name else global_last_promotion_ts_ms),
                }
                if global_stat_gate_meta:
                    global_meta["stat_gate"] = dict(global_stat_gate_meta)
                if global_target_name != global_current_name:
                    set_champion_assignment(
                        con=con,
                        scope=MODEL_COMPETITION_SCOPE,
                        symbol=MODEL_COMPETITION_SYMBOL,
                        model_name=str(global_target_name),
                        horizon_s=MODEL_COMPETITION_HORIZON_S,
                        challenger_name=str(global_current_name),
                        regime="global",
                        state="challenger",
                        meta=global_meta,
                    )
                    set_champion_assignment(
                        con=con,
                        scope=MODEL_COMPETITION_SCOPE,
                        symbol=MODEL_COMPETITION_SYMBOL,
                        model_name=str(global_target_name),
                        horizon_s=MODEL_COMPETITION_HORIZON_S,
                        challenger_name=str(global_current_name),
                        regime="global",
                        state="champion",
                        meta=global_meta,
                    )
                    changes.append(
                        {
                            "scope": MODEL_COMPETITION_SCOPE,
                            "symbol": MODEL_COMPETITION_SYMBOL,
                            "horizon_s": MODEL_COMPETITION_HORIZON_S,
                            "from_model_name": str(global_current_name),
                            "to_model_name": str(global_target_name),
                            "reason": str(global_reason),
                            "comparison_metric": str(global_promotion_delta.get("metric") or "net_pnl"),
                            "challenger_delta": float(global_promotion_delta.get("delta") or 0.0),
                            "challenger_score_delta": float(global_score_delta),
                            "challenger_return_delta": float(global_return_delta),
                            "challenger_pnl_delta": float(global_pnl_delta),
                            "cooldown_active": bool(global_cooldown_active),
                            "stat_gate": global_stat_gate_meta,
                        }
                    )
            else:
                _clear_model_competition_champion(con=con)
                if global_current_row:
                    global_stat_gate_meta = (
                        dict(global_stat_gate or {})
                        if bool((global_stat_gate or {}).get("validation_enabled", (global_stat_gate or {}).get("enabled")))
                        else {}
                    )
                    changes.append(
                        {
                            "scope": MODEL_COMPETITION_SCOPE,
                            "symbol": MODEL_COMPETITION_SYMBOL,
                            "horizon_s": MODEL_COMPETITION_HORIZON_S,
                            "from_model_name": str(global_current_name),
                            "to_model_name": "",
                            "reason": str(global_reason),
                            "comparison_metric": str(global_promotion_delta.get("metric") or "net_pnl"),
                            "challenger_delta": float(global_promotion_delta.get("delta") or 0.0),
                            "challenger_score_delta": float(global_score_delta),
                            "challenger_return_delta": float(global_return_delta),
                            "challenger_pnl_delta": float(global_pnl_delta),
                            "cooldown_active": bool(global_cooldown_active),
                            "stat_gate": global_stat_gate_meta,
                        }
                    )

            con.commit()
        except Exception:
            if bool(getattr(con, "in_transaction", False)):
                con.rollback()
            raise
        finally:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal("CHAMPION_MANAGER_EVALUATE_COMPETITION_CLOSE_FAILED", e)

    post_commit = drain_competition_post_commit_actions(max_actions=COMPETITION_POST_COMMIT_MAX_ACTIONS)
    capital_plan = compute_capital_plan()
    out = {
        "ok": True,
        "rankings": rankings,
        "changes": changes,
        "replay_validation": replay_validation,
        "replay_cache": replay_cache,
        "self_critic": self_critic,
        "post_commit_actions": {
            "pre_drain": pre_post_commit,
            "post_drain": post_commit,
        },
        "capital_plan": capital_plan,
        "status": ("post_commit_degraded" if bool(post_commit.get("degraded")) else "ready"),
    }
    try:
        meta_set("competition_cycle_status", json.dumps(out, separators=(",", ":"), sort_keys=True))
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_STATUS_PERSIST_FAILED",
            e,
            once_key="competition_cycle_status_persist",
        )
    out["snapshot"] = current_competition_snapshot()
    return out


def run_model_competition_job() -> Dict[str, Any]:
    marketplace = recompute_marketplace_scores()
    rankings = recompute_model_rankings()
    cycle = evaluate_competition_cycle()
    return {
        "ok": bool(marketplace.get("ok")) and bool(rankings.get("ok")) and bool(cycle.get("ok")),
        "marketplace": marketplace,
        "rankings": rankings,
        "competition": cycle,
        "snapshot": current_competition_snapshot(),
        "ts_ms": _now_ms(),
    }


def auto_promote_best():
    result = evaluate_competition_cycle()
    changes = list(result.get("changes") or [])
    if changes:
        return changes[0]
    return None
