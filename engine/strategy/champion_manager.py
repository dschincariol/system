"""Champion/challenger assignment logic for live model competition.

This module evaluates scored challengers, replay validation, self-critic
results, cooldowns, and realized PnL before deciding whether the current
champion should stay live, be replaced by a challenger, or be demoted because
its live behavior has degraded.
"""
import json
import math
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
from engine.strategy.promotion_guard import assess_challenger, evaluate_statistical_promotion_gate, promotion_allowed
from engine.strategy.experiment_ledger import evaluate_experiment_ledger_promotion_gate
from engine.strategy.model_marketplace import (
    compute_capital_plan,
    get_cached_replay_validation_snapshot,
    recompute_marketplace_scores,
    run_self_critic,
)
from engine.strategy.model_competition import (
    CompetitionRepository,
    IllegalChampionTransition,  # noqa: F401 - compatibility re-export
    PromotionStatGateEvaluator,
)
from engine.strategy.ope_gate import evaluate_policy_ope_gate
from engine.strategy.learned_alpha_decay import champion_gate_for_candidate

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
    return json.dumps(_json_sanitize(v), separators=(",", ":"), sort_keys=True, allow_nan=False)


def _table_columns_or_none(con, table_name: str) -> Optional[set[str]]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
    except Exception:
        return None
    return {str(row[1] or "").strip() for row in (rows or []) if row and len(row) > 1}


def _alter_add_column_if_missing(con, table_name: str, column_name: str, ddl: str) -> None:
    columns = _table_columns_or_none(con, table_name)
    if columns is None or str(column_name) in columns:
        return
    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl}")


def _ensure_model_competition_rankings_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS model_competition_rankings (
          ranking_scope TEXT NOT NULL DEFAULT 'global',
          model_name TEXT NOT NULL,
          rank INTEGER NOT NULL,
          net_pnl REAL NOT NULL DEFAULT 0,
          return_pct REAL NOT NULL DEFAULT 0,
          max_drawdown REAL NOT NULL DEFAULT 0,
          win_rate REAL,
          trade_count INTEGER NOT NULL DEFAULT 0,
          wins INTEGER NOT NULL DEFAULT 0,
          losses INTEGER NOT NULL DEFAULT 0,
          last_trade_ts_ms INTEGER,
          source TEXT NOT NULL DEFAULT 'trade_attribution_ledger',
          updated_ts_ms INTEGER NOT NULL DEFAULT 0,
          metrics_json TEXT,
          PRIMARY KEY (ranking_scope, model_name)
        )
        """
    )
    for column_name, ddl in (
        ("ranking_scope", "TEXT NOT NULL DEFAULT 'global'"),
        ("model_name", "TEXT NOT NULL DEFAULT ''"),
        ("rank", "INTEGER NOT NULL DEFAULT 0"),
        ("net_pnl", "REAL NOT NULL DEFAULT 0"),
        ("return_pct", "REAL NOT NULL DEFAULT 0"),
        ("max_drawdown", "REAL NOT NULL DEFAULT 0"),
        ("win_rate", "REAL"),
        ("trade_count", "INTEGER NOT NULL DEFAULT 0"),
        ("wins", "INTEGER NOT NULL DEFAULT 0"),
        ("losses", "INTEGER NOT NULL DEFAULT 0"),
        ("last_trade_ts_ms", "INTEGER"),
        ("source", "TEXT NOT NULL DEFAULT 'trade_attribution_ledger'"),
        ("updated_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("metrics_json", "TEXT"),
    ):
        _alter_add_column_if_missing(con, "model_competition_rankings", column_name, ddl)
    try:
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_model_competition_rankings_scope_rank
              ON model_competition_rankings(ranking_scope, rank ASC, model_name ASC)
            """
        )
    except Exception as e:
        _warn_nonfatal("CHAMPION_MANAGER_RANKINGS_INDEX_CREATE_FAILED", e)


def _commit_schema_normalization(con, *, context: str) -> None:
    if not bool(getattr(con, "in_transaction", False)):
        return
    if bool(getattr(con, "_managed_write_active", False)):
        return
    try:
        con.commit()
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_SCHEMA_NORMALIZATION_COMMIT_FAILED",
            e,
            context=str(context),
        )
        raise


def _ensure_competition_read_schema(
    con,
    *,
    champion_assignments: bool = True,
    rankings: bool = True,
    context: str = "competition_read",
) -> None:
    if champion_assignments:
        CompetitionRepository(con).ensure_champion_assignments_schema()
    if rankings:
        _ensure_model_competition_rankings_schema(con)
    _commit_schema_normalization(con, context=context)


def _json_sanitize(v: Any) -> Any:
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, dict):
        return {str(key): _json_sanitize(value) for key, value in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [_json_sanitize(value) for value in v]
    return v


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


def _empty_post_commit_status() -> Dict[str, Any]:
    return {
        "ok": True,
        "pending_count": 0,
        "running_count": 0,
        "failed_count": 0,
        "completed_count": 0,
        "failed_actions": [],
        "degraded": False,
    }


def _is_missing_relation_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}".lower()
    return (
        "undefinedtable" in text
        or "no such table" in text
        or "does not exist" in text and _POST_COMMIT_OUTBOX_TABLE in text
    )


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


def _candidate_model_id(row: Optional[Dict[str, Any]]) -> str:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    value = candidate.get("model_id") or metrics.get("model_id") or meta.get("model_id")
    text = str(value or "").strip()
    return text or "baseline"


def _as_nonempty_strings(value: Any) -> List[str]:
    if isinstance(value, (list, tuple, set)):
        values = list(value)
    elif value is None:
        values = []
    else:
        values = [value]
    out: List[str] = []
    for item in values:
        text = str(item or "").strip()
        if text:
            out.append(text)
    return out


def _as_positive_ints(value: Any) -> List[int]:
    values = list(value) if isinstance(value, (list, tuple, set)) else ([] if value is None else [value])
    out: List[int] = []
    for item in values:
        number = _safe_int(item, 0)
        if number > 0:
            out.append(int(number))
    return out


def _candidate_contexts(row: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    candidate = dict(row or {})
    if not candidate:
        return []
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    model_name = str(candidate.get("model_name") or metrics.get("model_name") or meta.get("model_name") or "").strip()
    model_id = _candidate_model_id(candidate)
    contexts: List[Dict[str, Any]] = []
    raw_contexts = candidate.get("contexts") or metrics.get("contexts") or meta.get("contexts")
    if isinstance(raw_contexts, list):
        for raw in raw_contexts:
            if not isinstance(raw, dict):
                continue
            symbol = str(raw.get("symbol") or "").upper().strip()
            horizon_s = _safe_int(raw.get("horizon_s"), 0)
            regime = str(raw.get("regime") or "global").strip() or "global"
            if model_name and symbol and horizon_s > 0:
                contexts.append(
                    {
                        "model_name": model_name,
                        "model_id": model_id,
                        "symbol": symbol,
                        "horizon_s": int(horizon_s),
                        "regime": regime,
                    }
                )
    if not contexts:
        symbols = _as_nonempty_strings(
            candidate.get("symbols") or metrics.get("symbols") or meta.get("symbols")
        )
        horizons = _as_positive_ints(
            candidate.get("horizons") or metrics.get("horizons") or meta.get("horizons")
        )
        regimes = _as_nonempty_strings(
            candidate.get("regimes") or metrics.get("regimes") or meta.get("regimes")
        ) or ["global"]
        if model_name and symbols and horizons:
            for symbol in symbols:
                for horizon_s in horizons:
                    for regime in regimes:
                        contexts.append(
                            {
                                "model_name": model_name,
                                "model_id": model_id,
                                "symbol": str(symbol).upper().strip(),
                                "horizon_s": int(horizon_s),
                                "regime": str(regime or "global").strip() or "global",
                            }
                        )
    if not contexts:
        symbol = str(candidate.get("symbol") or metrics.get("symbol") or meta.get("symbol") or "").upper().strip()
        horizon_s = _safe_int(candidate.get("horizon_s") or metrics.get("horizon_s") or meta.get("horizon_s"), 0)
        regime = str(candidate.get("regime") or metrics.get("regime") or meta.get("regime") or "global").strip() or "global"
        if model_name and symbol and horizon_s > 0:
            contexts.append(
                {
                    "model_name": model_name,
                    "model_id": model_id,
                    "symbol": symbol,
                    "horizon_s": int(horizon_s),
                    "regime": regime,
                }
            )

    deduped: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, int, str]] = set()
    for context in contexts:
        key = (
            str(context.get("model_name") or ""),
            str(context.get("symbol") or "").upper().strip(),
            _safe_int(context.get("horizon_s"), 0),
            str(context.get("regime") or "global"),
        )
        if not key[0] or not key[1] or key[2] <= 0 or key in seen:
            continue
        seen.add(key)
        deduped.append(context)
    return deduped


def _candidate_replay_lookup_keys(context: Dict[str, Any]) -> List[str]:
    model_name = str(context.get("model_name") or "").strip()
    model_id = str(context.get("model_id") or "baseline").strip() or "baseline"
    symbol = str(context.get("symbol") or "").upper().strip()
    horizon_s = _safe_int(context.get("horizon_s"), 0)
    regime = str(context.get("regime") or "global").strip() or "global"
    raw_keys = [
        "|".join([model_name, model_id, symbol, str(horizon_s), regime]),
        "|".join([model_name, model_id, "*", str(horizon_s), regime]),
        "|".join([model_name, "baseline", symbol, str(horizon_s), regime]),
        "|".join([model_name, "baseline", "*", str(horizon_s), regime]),
        "|".join([model_name, symbol, str(horizon_s), regime]),
        "|".join([model_name, "*", str(horizon_s), regime]),
        "|".join([model_id, symbol, str(horizon_s), regime]),
        "|".join([model_id, "*", str(horizon_s), regime]),
    ]
    out: List[str] = []
    for key in raw_keys:
        if key and key not in out:
            out.append(key)
    return out


def _candidate_self_critic_lookup_keys(context: Dict[str, Any]) -> List[str]:
    model_name = str(context.get("model_name") or "").strip()
    model_id = str(context.get("model_id") or "baseline").strip() or "baseline"
    symbol = str(context.get("symbol") or "").upper().strip()
    horizon_s = _safe_int(context.get("horizon_s"), 0)
    regime = str(context.get("regime") or "global").strip() or "global"
    raw_keys = [
        _candidate_block_key(model_name, symbol, horizon_s, regime),
        "|".join([model_name, model_id, symbol, str(horizon_s), regime]),
        "|".join([model_name, "baseline", symbol, str(horizon_s), regime]),
        "|".join([model_id, symbol, str(horizon_s), regime]),
    ]
    out: List[str] = []
    for key in raw_keys:
        if key and key not in out:
            out.append(key)
    return out


def _candidate_replay_checks(
    row: Optional[Dict[str, Any]],
    replay_models: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    models = dict(replay_models or {})
    checks: List[Dict[str, Any]] = []
    for context in _candidate_contexts(row):
        lookup_keys = _candidate_replay_lookup_keys(context)
        replay_row: Dict[str, Any] = {}
        matched_key = ""
        for lookup_key in lookup_keys:
            value = models.get(lookup_key)
            if isinstance(value, dict) and value:
                replay_row = dict(value)
                matched_key = str(lookup_key)
                break
        checks.append(
            {
                "context": dict(context),
                "lookup_keys": list(lookup_keys),
                "matched_key": matched_key,
                "replay": replay_row,
                "approved": bool(replay_row.get("approved")) if replay_row else False,
                "missing": not bool(replay_row),
            }
        )
    return checks


def _promotion_replay_check_provenance(checks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for check in checks:
        replay_row = dict(check.get("replay") or {})
        context = dict(check.get("context") or {})
        payload = {
            "context": {
                "model_name": str(context.get("model_name") or ""),
                "model_id": str(context.get("model_id") or ""),
                "symbol": str(context.get("symbol") or ""),
                "horizon_s": _safe_int(context.get("horizon_s"), 0),
                "regime": str(context.get("regime") or "global"),
            },
            "matched_key": str(check.get("matched_key") or ""),
            "approved": bool(check.get("approved")),
            "missing": bool(check.get("missing")),
        }
        if replay_row:
            payload.update(
                {
                    "model_name": str(replay_row.get("model_name") or ""),
                    "symbol": str(replay_row.get("symbol") or ""),
                    "horizon_s": _safe_int(replay_row.get("horizon_s"), 0),
                    "regime": str(replay_row.get("regime") or "global"),
                    "model_kind": str(replay_row.get("model_kind") or ""),
                    "model_ts_ms": _safe_int(replay_row.get("model_ts_ms"), 0),
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
            )
        out.append(payload)
    return out


def _candidate_promotion_eligibility(
    row: Optional[Dict[str, Any]],
    *,
    replay_models: Optional[Dict[str, Any]],
    replay_fresh: bool,
    blocked_keys: set[str],
    runtime_eligible_fn,
    live_promotable_fn=None,
    learned_alpha_gate: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    candidate = dict(row or {})
    block_reasons: List[str] = []
    if not candidate:
        block_reasons.append("missing_candidate")

    live_promotable = True
    if candidate and live_promotable_fn is not None:
        live_promotable = bool(live_promotable_fn(candidate))
        if not live_promotable:
            if not _candidate_has_net_cost_evidence(candidate):
                block_reasons.append("net_cost_evidence_missing")
            else:
                block_reasons.append("not_live_promotable")

    runtime_eligible = bool(runtime_eligible_fn(candidate)) if candidate else False
    if candidate and not runtime_eligible:
        if not _candidate_has_net_cost_evidence(candidate):
            block_reasons.append("net_cost_evidence_missing")
        else:
            block_reasons.append("runtime_eligibility_failed")

    replay_checks = _candidate_replay_checks(candidate, replay_models)
    replay_missing = bool(not replay_checks or any(bool(check.get("missing")) for check in replay_checks))
    replay_approved = bool(replay_checks and all(bool(check.get("approved")) for check in replay_checks))
    if not bool(replay_fresh):
        block_reasons.append("replay_stale")
    elif replay_missing:
        block_reasons.append("replay_missing")
    elif not replay_approved:
        block_reasons.append("replay_not_approved")

    critic_keys: List[str] = []
    for context in _candidate_contexts(candidate):
        for lookup_key in _candidate_self_critic_lookup_keys(context):
            if str(lookup_key) in blocked_keys and str(lookup_key) not in critic_keys:
                critic_keys.append(str(lookup_key))
    self_critic_blocked = bool(critic_keys)
    if self_critic_blocked:
        block_reasons.append("self_critic_blocked")

    learned_alpha = dict(learned_alpha_gate or {})
    learned_alpha_allowed = bool(learned_alpha.get("allowed", True))
    if learned_alpha and not learned_alpha_allowed:
        block_reasons.append("learned_alpha_blocked")

    deduped_reasons: List[str] = []
    for reason in block_reasons:
        if reason not in deduped_reasons:
            deduped_reasons.append(reason)

    eligible = bool(
        candidate
        and live_promotable
        and runtime_eligible
        and bool(replay_fresh)
        and replay_approved
        and not self_critic_blocked
        and learned_alpha_allowed
    )
    return {
        "eligible": bool(eligible),
        "status": "eligible" if eligible else (deduped_reasons[0] if deduped_reasons else "blocked"),
        "block_reasons": deduped_reasons,
        "replay_fresh": bool(replay_fresh),
        "replay_approved": bool(replay_approved),
        "replay_missing": bool(replay_missing),
        "replay_checks": _promotion_replay_check_provenance(replay_checks),
        "self_critic_blocked": bool(self_critic_blocked),
        "self_critic_blocked_keys": list(critic_keys),
        "runtime_eligible": bool(runtime_eligible),
        "live_promotable": bool(live_promotable),
        "learned_alpha": learned_alpha,
    }


def _promotion_assignment_block_reason(prefix: str, eligibility: Optional[Dict[str, Any]]) -> str:
    reasons = list((eligibility or {}).get("block_reasons") or [])
    if "learned_alpha_blocked" in reasons:
        return "best_blocked_learned_alpha"
    if "self_critic_blocked" in reasons:
        return "best_blocked_self_critic"
    if "replay_stale" in reasons:
        return "replay_stale"
    if "replay_missing" in reasons or "replay_not_approved" in reasons:
        return "replay_gate_blocked"
    if "net_cost_evidence_missing" in reasons:
        return "net_cost_evidence_missing"
    if "not_live_promotable" in reasons:
        return "shadow_candidate_only"
    if "runtime_eligibility_failed" in reasons:
        return f"{prefix}_eligibility_blocked"
    return str(prefix)


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
    return bool(_score_source_is_realized_pnl(meta) or source in {"shadow_predictions", "model_oos_predictions"})


def _candidate_is_live_promotable(row: Optional[Dict[str, Any]]) -> bool:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    return bool(
        _score_source_is_realized_pnl(meta)
        and _candidate_is_deployable(candidate)
        and _candidate_has_net_cost_evidence(candidate)
    )


def _candidate_has_net_cost_evidence(row: Optional[Dict[str, Any]]) -> bool:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    evidence = meta.get("net_cost_evidence")
    if isinstance(evidence, dict) and bool(evidence.get("available")) and _safe_int(evidence.get("n"), 0) > 0:
        return True
    return bool(
        bool(meta.get("net_cost_evidence_available"))
        and _safe_int(meta.get("net_cost_label_count"), 0) > 0
    )


def _candidate_replay_row(
    row: Optional[Dict[str, Any]],
    replay_models: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    for check in _candidate_replay_checks(row, replay_models):
        replay_row = dict(check.get("replay") or {})
        if replay_row:
            return replay_row
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
        _ensure_competition_read_schema(
            con,
            champion_assignments=True,
            rankings=False,
            context="load_all_champions",
        )
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
    CompetitionRepository(con).set_marketplace_stage_for_score_keys(
        candidates,
        champion_name=str(champion_name or ""),
        updated_ts_ms=int(_now_ms()),
    )


def _clear_champion_assignment(*, scope: str, symbol: str, horizon_s: int, con) -> None:
    CompetitionRepository(con).clear_champion_assignment(
        scope=str(scope or ""),
        symbol=str(symbol or ""),
        horizon_s=int(horizon_s or 0),
    )


def get_champion_assignment(scope: str, symbol: str, horizon_s: int = 0) -> Dict[str, Any]:
    con = connect()
    try:
        _ensure_competition_read_schema(
            con,
            champion_assignments=True,
            rankings=False,
            context="get_champion_assignment",
        )
        return CompetitionRepository(con).get_champion_assignment(
            scope=str(scope),
            symbol=str(symbol),
            horizon_s=int(horizon_s),
        )
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal("CHAMPION_MANAGER_GET_ASSIGNMENT_CLOSE_FAILED", e)


def _current_assignment_state(db, payload: Dict[str, Any]) -> Optional[str]:
    return CompetitionRepository(db).current_assignment_state(payload)


def _validate_assignment_transition(db, payload: Dict[str, Any]) -> None:
    CompetitionRepository(db).validate_assignment_transition(payload)


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
        CompetitionRepository(db).set_champion_assignment(
            scope=str(payload["scope"]),
            symbol=str(payload["symbol"]),
            model_name=str(payload["model_name"]),
            horizon_s=int(payload["horizon_s"]),
            challenger_name=str(payload["challenger_name"]),
            regime=str(payload["regime"]),
            state=str(payload["state"]),
            meta=dict(payload["meta"]),
            assigned_ts_ms=int(payload["assigned_ts_ms"]),
            updated_ts_ms=int(payload["updated_ts_ms"]),
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
    contexts = rec.get("contexts")
    if isinstance(contexts, set):
        rec["contexts"] = [
            {"symbol": str(symbol), "horizon_s": int(horizon_s), "regime": str(regime)}
            for symbol, horizon_s, regime in sorted(contexts)
        ]
    for key in ("symbols", "horizons", "regimes"):
        value = rec.get(key)
        if isinstance(value, set):
            rec[key] = sorted(value)
    return rec


def get_model_competition_rankings(limit: int = 25, ranking_scope: str = "global") -> List[Dict[str, Any]]:
    init_db()
    con = connect()
    try:
        _ensure_competition_read_schema(
            con,
            champion_assignments=False,
            rankings=True,
            context="get_model_competition_rankings",
        )
        columns = _table_columns_or_none(con, "model_competition_rankings") or set()
        legacy_score_expr = "score" if "score" in columns else "NULL"
        rows = con.execute(
            f"""
            SELECT rank, model_name, net_pnl, return_pct, max_drawdown, win_rate, trade_count,
                   wins, losses, last_trade_ts_ms, source, updated_ts_ms, metrics_json, {legacy_score_expr}
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
            score = _safe_float(metrics.get("score"), _safe_float(row[13], 0.0))
            out.append(
                {
                    "rank": _safe_int(row[0], 0),
                    "model_name": str(row[1] or ""),
                    "score": float(score),
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
    CompetitionRepository(con).clear_champion_assignment(
        scope=MODEL_COMPETITION_SCOPE,
        symbol=MODEL_COMPETITION_SYMBOL,
        horizon_s=MODEL_COMPETITION_HORIZON_S,
    )


def recompute_model_rankings(ranking_scope: str = "global") -> Dict[str, Any]:
    init_db()
    scope = str(ranking_scope or "global").strip() or "global"
    with _COMPETITION_LOCK:
        con = connect()
        try:
            _ensure_competition_read_schema(
                con,
                champion_assignments=False,
                rankings=True,
                context="recompute_model_rankings",
            )
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
                if not _candidate_has_net_cost_evidence({"meta": meta}):
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
                        "contexts": set(),
                        "rolling_window_ms": int(meta.get("rolling_window_ms") or (MODEL_COMPETITION_WINDOW_S * 1000)),
                        "observation_ms": 0,
                        "recent_total_pnl": 0.0,
                        "prior_total_pnl": 0.0,
                        "net_cost_label_count": 0,
                    },
                )
                for meta_key in (
                    "feature_ids",
                    "feature_schema",
                    "feature_set_tag",
                    "graph_relational",
                    "graph_relational_v1",
                    "graph_metadata",
                    "model_family",
                    "model_kind",
                    "model_ts_ms",
                    "model_version",
                ):
                    value = meta.get(meta_key)
                    if value is not None and value != "":
                        rec[meta_key] = value
                evidence = meta.get("net_cost_evidence") if isinstance(meta.get("net_cost_evidence"), dict) else {}
                rec["net_cost_label_count"] = _safe_int(rec.get("net_cost_label_count"), 0) + _safe_int(
                    meta.get("net_cost_label_count") or evidence.get("n"),
                    0,
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
                rec["contexts"].add(
                    (
                        str(symbol or "").upper().strip(),
                        _safe_int(horizon_s, 0),
                        str(regime or "global"),
                    )
                )

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
                    "contexts": list(row.get("contexts") or []),
                    "evaluation_timestamps": list(row.get("evaluation_timestamps") or []),
                    "regime_labels": list(row.get("regime_labels") or []),
                    "challenger_predictions": list(row.get("challenger_predictions") or []),
                    "realized_returns": list(row.get("realized_returns") or []),
                    "event_pnls": list(row.get("event_pnls") or []),
                    "net_cost_label_count": int(row.get("net_cost_label_count") or 0),
                    "net_cost_evidence_available": bool(_safe_int(row.get("net_cost_label_count"), 0) > 0),
                    "source": str(row.get("source") or ""),
                }
                for meta_key in (
                    "feature_ids",
                    "feature_schema",
                    "feature_set_tag",
                    "graph_relational",
                    "graph_relational_v1",
                    "graph_metadata",
                    "model_family",
                    "model_kind",
                    "model_ts_ms",
                    "model_version",
                ):
                    value = row.get(meta_key)
                    if value is not None and value != "":
                        metrics[meta_key] = value
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
    ranking_scope = str(rankings_snapshot.get("ranking_scope") or "global").strip() or "global"
    ranking_rows = list(rankings_snapshot.get("rows") or [])
    ranking_champion = dict(rankings_snapshot.get("champion") or {})
    ranking_challengers = list(rankings_snapshot.get("challengers") or [])
    if not ranking_rows:
        ranking_rows = get_model_competition_rankings(limit=100, ranking_scope=ranking_scope)
        if ranking_rows:
            ranking_champion = dict(ranking_champion or ranking_rows[0])
            ranking_challengers = list(ranking_challengers or ranking_rows[1:])

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
    has_competition_data = bool(champion or champions or ranking_rows)
    reason = "" if has_competition_data else "no_competition_data"
    status = "ready" if has_competition_data else "empty"

    snap = {
        "ok": True,
        "status": status,
        "reason": reason,
        "degraded": bool(not has_competition_data),
        "champion": champion,
        "champions": champions,
        "ranking_champion": ranking_champion,
        "challengers": ranking_challengers,
        "rankings": ranking_rows,
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
    if not _candidate_has_net_cost_evidence(row):
        return False
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


def _promotion_mode_name() -> str:
    return str(os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"


def _promotion_strict_runtime(mode_name: str) -> bool:
    env = str(os.environ.get("ENV") or os.environ.get("NODE_ENV") or "dev").strip().lower()
    supervised = str(os.environ.get("ENGINE_SUPERVISED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }
    return bool(supervised or env in {"prod", "production"} or str(mode_name).strip().lower() in {"live", "paper"})


def _promotion_insufficient_observation_payload(
    *,
    model_id: str,
    model_name: str,
    candidate_version: str,
    challenger_observations: int,
    champion_observations: int,
    aligned_observations: int,
    min_observations: int,
    legacy_gate_enabled: bool,
) -> Dict[str, Any]:
    mode_name = _promotion_mode_name()
    fail_closed = bool(_promotion_strict_runtime(mode_name) or legacy_gate_enabled)
    status = "insufficient_observations" if fail_closed else "insufficient_observations_advisory"
    payload: Dict[str, Any] = {
        "enabled": True,
        "applied": bool(fail_closed),
        "status": status,
        "passed": not fail_closed,
        "model_id": str(model_id),
        "model_name": str(model_name),
        "candidate_version": str(candidate_version),
        "n_observations": int(aligned_observations),
        "current_observations": int(aligned_observations),
        "min_observations": int(min_observations),
        "required_observations": int(min_observations),
        "challenger_observations": int(challenger_observations),
        "champion_observations": int(champion_observations),
        "promotion_mode": str(mode_name),
        "strict_runtime": bool(_promotion_strict_runtime(mode_name)),
        "fail_closed": bool(fail_closed),
        "advisory": not fail_closed,
        "legacy_gate_enabled": bool(legacy_gate_enabled),
        "insufficient_observations": True,
        "non_bypassable_observation_gate": {
            "enabled": True,
            "applied": bool(fail_closed),
            "status": status,
            "passed": not fail_closed,
            "current_observations": int(aligned_observations),
            "required_observations": int(min_observations),
            "min_observations": int(min_observations),
        },
        "legacy_stat_gate": {
            "enabled": bool(legacy_gate_enabled),
            "applied": False,
            "status": "not_evaluated_insufficient_observations" if legacy_gate_enabled else "disabled",
            "passed": not legacy_gate_enabled,
        },
        "record_legacy_hypothesis": False,
        "validation_enabled": True,
    }
    if fail_closed:
        payload["blockers"] = ["insufficient_observations"]
    return payload


def _learned_alpha_candidate_gate(con, row: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    try:
        return dict(champion_gate_for_candidate(con, dict(row or {})) or {})
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_LEARNED_ALPHA_GATE_FAILED",
            e,
            once_key=f"learned_alpha_gate:{str((row or {}).get('model_name') or '')}",
            model_name=str((row or {}).get("model_name") or ""),
        )
        return {"allowed": True, "available": False, "reason": f"gate_failed:{type(e).__name__}"}


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


def _promotion_ope_gate_cache_key(row: Optional[Dict[str, Any]]) -> Tuple[str, str, str, int, str]:
    candidate = dict(row or {})
    metrics = dict(candidate.get("metrics") or {})
    model_id = str(candidate.get("model_id") or metrics.get("model_id") or "").strip()
    return (
        str(model_id or candidate.get("model_name") or ""),
        str(_promotion_candidate_version(candidate)),
        str(candidate.get("symbol") or "").upper().strip(),
        _safe_int(candidate.get("horizon_s"), 0),
        str(candidate.get("regime") or "global"),
    )


def _promotion_feature_ids(row: Optional[Dict[str, Any]]) -> List[str]:
    contract = _extract_feature_contract(row)
    ids = contract.get("feature_ids")
    if isinstance(ids, list):
        return [str(fid) for fid in ids if str(fid or "").strip()]
    schema = contract.get("feature_schema")
    if isinstance(schema, dict) and isinstance(schema.get("feature_ids"), list):
        return [str(fid) for fid in schema.get("feature_ids") if str(fid or "").strip()]
    return []


def _first_non_none_value(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


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
        "deconfounded_validation",
        "confounders",
        "control_rows",
        "beta",
        "sector",
        "size",
        "volatility",
        "liquidity",
        "existing_model_exposure",
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


def _candidate_has_generated_lineage(row: Optional[Dict[str, Any]]) -> bool:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    for source in (candidate, meta, metrics):
        generation_method = str(source.get("generation_method") or "").strip()
        mutation_kind = str(source.get("mutation_kind") or "").strip()
        if generation_method or mutation_kind in {"alpha_discovery", "symbolic_alpha_discovery", "llm_alpha_discovery"}:
            return True
        if source.get("alpha_candidate_id") is not None:
            return True
        symbolic_candidate = source.get("symbolic_candidate")
        if isinstance(symbolic_candidate, dict) and symbolic_candidate:
            return True
    return False


def _evaluate_candidate_experiment_ledger(
    row: Optional[Dict[str, Any]],
    *,
    con=None,
) -> tuple[bool, Dict[str, Any]]:
    candidate = dict(row or {})
    model_name = str(candidate.get("model_name") or "").strip()
    if not model_name:
        return False, {"enabled": True, "required": False, "status": "missing_model_name", "passed": False}
    version = _promotion_candidate_version(candidate)
    return evaluate_experiment_ledger_promotion_gate(
        model_name=str(model_name),
        candidate_version=str(version),
        generated_hint=_candidate_has_generated_lineage(candidate),
        con=con,
    )


def _evaluate_candidate_ope_gate(
    row: Optional[Dict[str, Any]],
    *,
    con=None,
) -> tuple[bool, Dict[str, Any]]:
    candidate = dict(row or {})
    meta = dict(candidate.get("meta") or {})
    metrics = dict(candidate.get("metrics") or {})
    model_name = str(candidate.get("model_name") or "").strip()
    if not model_name:
        return False, {"enabled": True, "applied": True, "status": "missing_model_name", "passed": False}
    return evaluate_policy_ope_gate(
        model_id=str(candidate.get("model_id") or metrics.get("model_id") or meta.get("model_id") or ""),
        model_name=str(model_name),
        candidate_type=(
            str(meta.get("policy_type") or meta.get("candidate_type") or metrics.get("policy_type") or "")
            or None
        ),
        model_kind=(str(meta.get("model_kind") or metrics.get("model_kind") or "").strip() or None),
        candidate_version=_promotion_candidate_version(candidate),
        symbol=str(candidate.get("symbol") or metrics.get("symbol") or "").upper().strip(),
        horizon_s=_safe_int(candidate.get("horizon_s") or metrics.get("horizon_s"), 0),
        regime=str(candidate.get("regime") or metrics.get("regime") or "global"),
        metadata={**metrics, **meta},
        con=con,
    )


def _evaluate_candidate_graph_gate(row: Optional[Dict[str, Any]]) -> tuple[bool, Dict[str, Any]]:
    try:
        from engine.strategy.graph_relational import evaluate_graph_promotion_gate

        passed, diagnostics = evaluate_graph_promotion_gate(dict(row or {}))
        return bool(passed), dict(diagnostics or {})
    except Exception as e:
        _warn_nonfatal(
            "CHAMPION_MANAGER_GRAPH_RELATIONAL_GATE_FAILED",
            e,
            once_key=f"graph_relational_gate:{str((row or {}).get('model_name') or '')}",
            model_name=str((row or {}).get("model_name") or ""),
        )
        return False, {
            "enabled": True,
            "applied": True,
            "status": f"gate_failed:{type(e).__name__}",
            "passed": False,
        }


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
            ledger_passed, ledger_diagnostics = _evaluate_candidate_experiment_ledger(candidate, con=con)
            payload["experiment_ledger"] = dict(ledger_diagnostics or {})
            payload["passed"] = bool(configured_passed and ledger_passed)
            payload["status"] = (
                str((configured_diagnostics or {}).get("status") or "configured_gate_failed")
                if not bool(configured_passed)
                else "experiment_ledger_failed"
                if not bool(ledger_passed)
                else str((configured_diagnostics or {}).get("status") or "evaluated")
            )
            payload["missing_model_id_advisory"] = True
            return bool(configured_passed and ledger_passed), payload
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
    challenger_observations = len(challenger_returns)
    champion_observations = len(champion_returns)
    aligned_observations = (
        min(challenger_observations, champion_observations)
        if champion_returns
        else challenger_observations
    )
    min_assessment_observations = max(
        2,
        _safe_int(os.environ.get("CHAMPION_PROMOTION_MIN_OBSERVATIONS"), 50),
    )
    if int(aligned_observations) < int(min_assessment_observations):
        observation_payload = _promotion_insufficient_observation_payload(
            model_id=str(evidence_model_id),
            model_name=str(model_name),
            candidate_version=_promotion_candidate_version(candidate),
            challenger_observations=int(challenger_observations),
            champion_observations=int(champion_observations),
            aligned_observations=int(aligned_observations),
            min_observations=int(min_assessment_observations),
            legacy_gate_enabled=bool(legacy_gate_enabled),
        )
        return bool(observation_payload.get("passed")), observation_payload
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
        neutralization_features=_first_non_none_value(
            feature_payload.get("neutralization_features"),
            feature_payload.get("feature_snapshots"),
            feature_payload.get("feature_values"),
            feature_payload.get("features_matrix"),
        ),
        current_feature_ids=_promotion_feature_ids(champion_row),
        challenger_feature_ids=_promotion_feature_ids(candidate),
        candidate_symbols=[str(candidate.get("symbol") or metrics.get("symbol") or "").upper().strip()],
        candidate_features=feature_payload.get("candidate_features"),
        deconfounded_validation=(
            feature_payload.get("deconfounded_validation")
            or {
                key: value
                for key, value in {
                    "controls": _first_non_none_value(
                        feature_payload.get("confounders"),
                        feature_payload.get("control_rows"),
                        feature_payload.get("neutralization_features"),
                        feature_payload.get("feature_snapshots"),
                        feature_payload.get("feature_values"),
                        feature_payload.get("features_matrix"),
                    ),
                    "beta": feature_payload.get("beta"),
                    "sector": feature_payload.get("sector"),
                    "size": feature_payload.get("size"),
                    "volatility": feature_payload.get("volatility"),
                    "liquidity": feature_payload.get("liquidity"),
                    "existing_model_exposure": feature_payload.get("existing_model_exposure"),
                    "candidate_signal": era_payload.get("challenger_predictions"),
                    "outcome": era_payload.get("realized_returns"),
                    "regime": era_payload.get("regime_labels"),
                    "stability_labels": _first_non_none_value(era_payload.get("era_labels"), era_payload.get("regime_labels")),
                }.items()
                if value is not None
            }
        ),
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
        ledger_passed, ledger_diagnostics = _evaluate_candidate_experiment_ledger(candidate, con=con)
        payload["experiment_ledger"] = dict(ledger_diagnostics or {})
        if not bool(ledger_passed):
            payload["passed"] = False
            payload["status"] = "experiment_ledger_failed"
        return bool(passed and configured_passed and ledger_passed), payload
    payload["legacy_stat_gate"] = {"enabled": False, "applied": False, "status": "disabled", "passed": True}
    payload["record_legacy_hypothesis"] = False
    payload["validation_enabled"] = True
    ledger_passed, ledger_diagnostics = _evaluate_candidate_experiment_ledger(candidate, con=con)
    payload["experiment_ledger"] = dict(ledger_diagnostics or {})
    if not bool(ledger_passed):
        payload["passed"] = False
        payload["status"] = "experiment_ledger_failed"
    return bool(passed and ledger_passed), payload


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
        "rolling_window_ms": _safe_int(
            metrics.get("rolling_window_ms"),
            _safe_int(ranking.get("rolling_window_ms"), MODEL_COMPETITION_WINDOW_S * 1000),
        ),
        "max_drawdown": _safe_float(ranking.get("max_drawdown"), 0.0),
        "recent_total_pnl": _safe_float(metrics.get("recent_total_pnl"), _safe_float(ranking.get("recent_total_pnl"), 0.0)),
        "prior_total_pnl": _safe_float(metrics.get("prior_total_pnl"), _safe_float(ranking.get("prior_total_pnl"), 0.0)),
        "capital_base": float(capital_base),
        "return_pct": _capital_adjusted_return_pct(rolling_total_pnl, capital_base),
        "trade_count": trade_count,
        "win_rate": (None if win_rate is None else float(win_rate)),
        "observation_ms": _safe_int(metrics.get("observation_ms"), _safe_int(ranking.get("observation_ms"), 0)),
        "net_cost_label_count": _safe_int(metrics.get("net_cost_label_count"), _safe_int(ranking.get("net_cost_label_count"), 0)),
        "net_cost_evidence_available": bool(
            metrics.get("net_cost_evidence_available")
            or ranking.get("net_cost_evidence_available")
            or _safe_int(metrics.get("net_cost_label_count"), _safe_int(ranking.get("net_cost_label_count"), 0)) > 0
        ),
    }


def _ranking_is_eligible(row: Optional[Dict[str, Any]]) -> bool:
    metrics = _ranking_runtime_metrics(row)
    if not bool(metrics.get("net_cost_evidence_available")) or _safe_int(metrics.get("net_cost_label_count"), 0) <= 0:
        return False
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
    con = connect()
    try:
        try:
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
        except Exception as e:
            if _is_missing_relation_error(e):
                return _empty_post_commit_status()
            raise
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
            ope_gate_cache: Dict[Tuple[str, str, str, int, str], Tuple[bool, Dict[str, Any]]] = {}
            graph_gate_cache: Dict[Tuple[str, str], Tuple[bool, Dict[str, Any]]] = {}

            def _enqueue_post_commit(fn_name: str, *args, **kwargs) -> None:
                _queue_post_commit_action(con, str(fn_name), *args, **kwargs)

            def _cached_ope_gate(target_row: Optional[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
                cache_key = _promotion_ope_gate_cache_key(target_row)
                if cache_key[0] and cache_key in ope_gate_cache:
                    cached_ok, cached_payload = ope_gate_cache[cache_key]
                    payload = dict(cached_payload or {})
                    payload["cache_hit"] = True
                    return bool(cached_ok), payload
                ok, payload = _evaluate_candidate_ope_gate(target_row, con=con)
                if cache_key[0]:
                    ope_gate_cache[cache_key] = (bool(ok), dict(payload or {}))
                return bool(ok), dict(payload or {})

            def _cached_graph_gate(target_row: Optional[Dict[str, Any]]) -> Tuple[bool, Dict[str, Any]]:
                cache_key = _promotion_stat_gate_cache_key(target_row)
                if cache_key[0] and cache_key in graph_gate_cache:
                    cached_ok, cached_payload = graph_gate_cache[cache_key]
                    payload = dict(cached_payload or {})
                    payload["cache_hit"] = True
                    return bool(cached_ok), payload
                ok, payload = _evaluate_candidate_graph_gate(target_row)
                if cache_key[0]:
                    graph_gate_cache[cache_key] = (bool(ok), dict(payload or {}))
                return bool(ok), dict(payload or {})

            stat_gate_evaluator = PromotionStatGateEvaluator(
                evaluate_gate=_evaluate_promotion_stat_gate,
                cache_key=_promotion_stat_gate_cache_key,
                candidate_version=_promotion_candidate_version,
                enqueue_legacy_hypothesis=_enqueue_post_commit,
                safe_int=_safe_int,
                safe_float=_safe_float,
                con=con,
            )

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
                global_best_metrics = _ranking_runtime_metrics(global_best)
                global_current_metrics = _ranking_runtime_metrics(global_current_row)
                global_promotion_delta = _promotion_delta(global_best_metrics, global_current_metrics)
                global_best_eligibility = _candidate_promotion_eligibility(
                    global_best,
                    replay_models=replay_models,
                    replay_fresh=False,
                    blocked_keys=blocked_keys,
                    runtime_eligible_fn=_ranking_is_eligible,
                )
                global_current_eligibility = (
                    _candidate_promotion_eligibility(
                        global_current_row,
                        replay_models=replay_models,
                        replay_fresh=False,
                        blocked_keys=blocked_keys,
                        runtime_eligible_fn=_ranking_is_eligible,
                    )
                    if global_current_row
                    else {}
                )
                if global_best and global_best_eligibility.get("block_reasons"):
                    changes.append(
                        {
                            "scope": MODEL_COMPETITION_SCOPE,
                            "symbol": MODEL_COMPETITION_SYMBOL,
                            "horizon_s": MODEL_COMPETITION_HORIZON_S,
                            "from_model_name": str(global_current_name),
                            "to_model_name": str(global_current_name if global_current_row else ""),
                            "reason": _promotion_assignment_block_reason("replay_stale", global_best_eligibility),
                            "comparison_metric": str(global_promotion_delta.get("metric") or "net_pnl"),
                            "challenger_delta": float(global_promotion_delta.get("delta") or 0.0),
                            "cooldown_active": False,
                            "promotion_eligibility": dict(global_current_eligibility or {}),
                            "best_promotion_eligibility": dict(global_best_eligibility or {}),
                            "current_promotion_eligibility": dict(global_current_eligibility or {}),
                            "system_guard": {"allowed": False, "status": "not_evaluated_replay_stale"},
                        }
                    )
                capital_plan = compute_capital_plan()
                out = {
                    "ok": True,
                    "changes": changes,
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

            promotion_system_allowed, promotion_system_guard = promotion_allowed()
            promotion_system_guard = dict(promotion_system_guard or {})
            promotion_system_guard["allowed"] = bool(promotion_system_allowed)

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

                current_replay = _candidate_replay_row(current_row, replay_models) if current_row else {}
                current_replay_approved = bool(current_replay.get("approved")) if current_replay else False
                best_learned_alpha_gate = _learned_alpha_candidate_gate(con, best)
                current_learned_alpha_gate = _learned_alpha_candidate_gate(con, current_row) if current_row else {
                    "allowed": True,
                    "available": False,
                    "reason": "no_current_row",
                }
                best_eligibility = _candidate_promotion_eligibility(
                    best,
                    replay_models=replay_models,
                    replay_fresh=bool(replay_fresh),
                    blocked_keys=blocked_keys,
                    runtime_eligible_fn=_candidate_is_eligible,
                    live_promotable_fn=_candidate_is_live_promotable,
                    learned_alpha_gate=best_learned_alpha_gate,
                )
                current_eligibility = (
                    _candidate_promotion_eligibility(
                        current_row,
                        replay_models=replay_models,
                        replay_fresh=bool(replay_fresh),
                        blocked_keys=blocked_keys,
                        runtime_eligible_fn=_candidate_is_eligible,
                        live_promotable_fn=_candidate_is_live_promotable,
                        learned_alpha_gate=current_learned_alpha_gate,
                    )
                    if current_row
                    else {}
                )
                best_blocked = bool(
                    best_eligibility.get("self_critic_blocked")
                    or "learned_alpha_blocked" in list(best_eligibility.get("block_reasons") or [])
                )
                best_replay_approved = bool(best_eligibility.get("replay_approved"))
                current_blocked = bool(
                    (current_eligibility or {}).get("self_critic_blocked")
                    or "learned_alpha_blocked" in list((current_eligibility or {}).get("block_reasons") or [])
                )
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
                    if bool(best_eligibility.get("eligible")):
                        target = best
                        reason = "bootstrap_best"
                    elif best_eligibility.get("block_reasons"):
                        reason = _promotion_assignment_block_reason("no_bootstrap", best_eligibility)
                elif current_blocked or hard_demote:
                    fallback = next(
                        (
                            row
                            for row in candidates
                            if str(row.get("model_name") or "") != str(current_name)
                            if bool(
                                _candidate_promotion_eligibility(
                                    row,
                                    replay_models=replay_models,
                                    replay_fresh=bool(replay_fresh),
                                    blocked_keys=blocked_keys,
                                    runtime_eligible_fn=_candidate_is_eligible,
                                    live_promotable_fn=_candidate_is_live_promotable,
                                ).get("eligible")
                            )
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
                    and bool(best_eligibility.get("eligible"))
                    and float(promotion_delta.get("delta") or 0.0) >= float(promotion_delta.get("threshold") or 0.0)
                    and (not cooldown_active)
                ):
                    target = best
                    reason = "challenger_outperformance"
                elif best_blocked and current_row:
                    target = current_row
                    reason = _promotion_assignment_block_reason("keep_current", best_eligibility)
                elif current_row and (
                    bool(best_eligibility.get("eligible"))
                    and float(promotion_delta.get("delta") or 0.0) >= float(
                        max(float(promotion_delta.get("threshold") or 0.0), 0.0)
                    )
                ):
                    target = best
                    reason = "demotion_replace"
                elif best_eligibility.get("block_reasons"):
                    target = current_row or {}
                    reason = _promotion_assignment_block_reason("keep_current", best_eligibility)

                stat_gate: Dict[str, Any] = {}
                ope_gate: Dict[str, Any] = {}
                graph_gate: Dict[str, Any] = {}
                target_name = str((target or {}).get("model_name") or "")
                if target_name and target_name != str(current_name):
                    if not bool(promotion_system_allowed):
                        reason = f"{reason}_system_guard_blocked"
                        if current_row and not (current_blocked or hard_demote):
                            target = current_row
                        else:
                            target = {}
                        target_name = str((target or {}).get("model_name") or "")

                if target_name and target_name != str(current_name):
                    graph_gate_ok, graph_gate = _cached_graph_gate(target)
                    if not bool(graph_gate_ok):
                        reason = f"{reason}_graph_gate_blocked"
                        if current_row and not (current_blocked or hard_demote):
                            target = current_row
                        else:
                            target = {}
                        target_name = str((target or {}).get("model_name") or "")

                if target_name and target_name != str(current_name):
                    ope_gate_ok, ope_gate = _cached_ope_gate(target)
                    if not bool(ope_gate_ok):
                        reason = f"{reason}_ope_gate_blocked"
                        if current_row and not (current_blocked or hard_demote):
                            target = current_row
                        else:
                            target = {}
                        target_name = str((target or {}).get("model_name") or "")

                if target_name and target_name != str(current_name):
                    stat_gate_ok, stat_gate = stat_gate_evaluator.evaluate(
                        target,
                        _promotion_trial_count(candidates),
                        candidate_returns=_promotion_models_returns(candidates),
                        incumbent_row=current_row,
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
                ope_gate_meta = dict(ope_gate or {}) if bool((ope_gate or {}).get("applied")) else {}
                stat_gate_meta = dict(stat_gate or {}) if bool((stat_gate or {}).get("validation_enabled", (stat_gate or {}).get("enabled"))) else {}
                graph_gate_meta = dict(graph_gate or {}) if bool((graph_gate or {}).get("applied")) else {}
                target_eligibility = (
                    best_eligibility
                    if target_name and target_name == str(best.get("model_name") or "")
                    else current_eligibility
                    if target_name and target_name == str(current_name)
                    else {}
                )
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
                    "ope_gate": ope_gate_meta,
                    "stat_gate": stat_gate_meta,
                    "graph_gate": graph_gate_meta,
                    "promotion_eligibility": dict(target_eligibility or {}),
                    "best_promotion_eligibility": dict(best_eligibility or {}),
                    "current_promotion_eligibility": dict(current_eligibility or {}),
                    "system_guard": dict(promotion_system_guard or {}),
                    "learned_alpha": {
                        "best": dict(best_learned_alpha_gate),
                        "current": dict(current_learned_alpha_gate),
                    },
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
                    elif stat_gate_meta or best_eligibility.get("block_reasons"):
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
                        "promotion_eligibility": dict(target_eligibility or {}),
                    }
                    if stat_gate_meta:
                        meta["stat_gate"] = dict(stat_gate_meta)
                    if ope_gate_meta:
                        meta["ope_gate"] = dict(ope_gate_meta)
                    if graph_gate_meta:
                        meta["graph_gate"] = dict(graph_gate_meta)
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
                            "ope_gate": ope_gate_meta,
                            "stat_gate": stat_gate_meta,
                            "graph_gate": graph_gate_meta,
                        },
                    )
                    changes.append(change)
                else:
                    if stat_gate_meta and str(reason).endswith("_stat_gate_blocked"):
                        changes.append(change)
                    elif best_eligibility.get("block_reasons"):
                        changes.append(change)
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
            global_best_eligibility = _candidate_promotion_eligibility(
                global_best,
                replay_models=replay_models,
                replay_fresh=bool(replay_fresh),
                blocked_keys=blocked_keys,
                runtime_eligible_fn=_ranking_is_eligible,
            )
            global_current_eligibility = (
                _candidate_promotion_eligibility(
                    global_current_row,
                    replay_models=replay_models,
                    replay_fresh=bool(replay_fresh),
                    blocked_keys=blocked_keys,
                    runtime_eligible_fn=_ranking_is_eligible,
                )
                if global_current_row
                else {}
            )
            global_current_blocked = bool((global_current_eligibility or {}).get("self_critic_blocked"))
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
                    global_current_blocked
                    or
                    global_drawdown >= float(DEMOTION_MAX_DRAWDOWN)
                    or global_decay <= float(DEMOTION_DECAY_THRESHOLD)
                    or _safe_float(global_current_row.get("net_pnl"), 0.0) <= float(DEMOTION_MIN_NET_PNL)
                )
            )
            global_target = global_current_row or {}
            global_reason = "keep_current" if global_current_row else "no_bootstrap"
            if not global_current_row:
                if global_best_name and bool(global_best_eligibility.get("eligible")):
                    global_target = global_best
                    global_reason = "bootstrap_best"
                elif global_best_name and global_best_eligibility.get("block_reasons"):
                    global_reason = _promotion_assignment_block_reason("no_bootstrap", global_best_eligibility)
            elif global_current_invalid:
                fallback = next(
                    (
                        row for row in global_rows
                        if str((row or {}).get("model_name") or "").strip() != str(global_current_name)
                        and bool(
                            _candidate_promotion_eligibility(
                                row,
                                replay_models=replay_models,
                                replay_fresh=bool(replay_fresh),
                                blocked_keys=blocked_keys,
                                runtime_eligible_fn=_ranking_is_eligible,
                            ).get("eligible")
                        )
                    ),
                    {},
                )
                global_target = fallback
                if global_current_blocked:
                    global_reason = "demotion_self_critic"
                elif global_drawdown >= float(DEMOTION_MAX_DRAWDOWN):
                    global_reason = "demotion_drawdown"
                elif global_decay <= float(DEMOTION_DECAY_THRESHOLD):
                    global_reason = "demotion_decay"
                else:
                    global_reason = "demotion_fallback"
            elif (
                global_best_name
                and global_best_name != global_current_name
                and bool(global_best_eligibility.get("eligible"))
                and float(global_promotion_delta.get("delta") or 0.0) >= float(global_promotion_delta.get("threshold") or 0.0)
                and not global_cooldown_active
            ):
                global_target = global_best
                global_reason = "challenger_outperformance"
            elif global_best_name and global_best_name != global_current_name and global_best_eligibility.get("block_reasons"):
                global_target = global_current_row or {}
                global_reason = _promotion_assignment_block_reason("keep_current", global_best_eligibility)

            global_stat_gate: Dict[str, Any] = {}
            global_ope_gate: Dict[str, Any] = {}
            global_graph_gate: Dict[str, Any] = {}
            global_target_name = str((global_target or {}).get("model_name") or "").strip()
            if global_target_name and global_target_name != str(global_current_name):
                if not bool(promotion_system_allowed):
                    global_reason = f"{global_reason}_system_guard_blocked"
                    if global_current_row and not global_current_invalid:
                        global_target = global_current_row
                    else:
                        global_target = {}
                    global_target_name = str((global_target or {}).get("model_name") or "").strip()

            if global_target_name and global_target_name != str(global_current_name):
                global_graph_gate_ok, global_graph_gate = _cached_graph_gate(global_target)
                if not bool(global_graph_gate_ok):
                    global_reason = f"{global_reason}_graph_gate_blocked"
                    if global_current_row and not global_current_invalid:
                        global_target = global_current_row
                    else:
                        global_target = {}
                    global_target_name = str((global_target or {}).get("model_name") or "").strip()

            if global_target_name and global_target_name != str(global_current_name):
                global_ope_gate_ok, global_ope_gate = _cached_ope_gate(global_target)
                if not bool(global_ope_gate_ok):
                    global_reason = f"{global_reason}_ope_gate_blocked"
                    if global_current_row and not global_current_invalid:
                        global_target = global_current_row
                    else:
                        global_target = {}
                    global_target_name = str((global_target or {}).get("model_name") or "").strip()

            if global_target_name and global_target_name != str(global_current_name):
                global_stat_gate_ok, global_stat_gate = stat_gate_evaluator.evaluate(
                    global_target,
                    max(1, len(global_rows)),
                    candidate_returns=_promotion_models_returns(global_rows),
                    incumbent_row=global_current_row,
                )
                if not global_stat_gate_ok:
                    global_reason = f"{global_reason}_stat_gate_blocked"
                    if global_current_row and not global_current_invalid:
                        global_target = global_current_row
                    else:
                        global_target = {}
                    global_target_name = str((global_target or {}).get("model_name") or "").strip()

            if global_target_name:
                global_target_eligibility = (
                    global_best_eligibility
                    if global_target_name == str(global_best_name)
                    else global_current_eligibility
                    if global_target_name == str(global_current_name)
                    else {}
                )
                global_ope_gate_meta = dict(global_ope_gate or {}) if bool((global_ope_gate or {}).get("applied")) else {}
                global_graph_gate_meta = (
                    dict(global_graph_gate or {}) if bool((global_graph_gate or {}).get("applied")) else {}
                )
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
                    "promotion_eligibility": dict(global_target_eligibility or {}),
                }
                if global_ope_gate_meta:
                    global_meta["ope_gate"] = dict(global_ope_gate_meta)
                if global_graph_gate_meta:
                    global_meta["graph_gate"] = dict(global_graph_gate_meta)
                if global_stat_gate_meta:
                    global_meta["stat_gate"] = dict(global_stat_gate_meta)
                global_change = {
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
                    "ope_gate": global_ope_gate_meta,
                    "stat_gate": global_stat_gate_meta,
                    "graph_gate": global_graph_gate_meta,
                    "promotion_eligibility": dict(global_target_eligibility or {}),
                    "best_promotion_eligibility": dict(global_best_eligibility or {}),
                    "current_promotion_eligibility": dict(global_current_eligibility or {}),
                    "system_guard": dict(promotion_system_guard or {}),
                }
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
                    changes.append(global_change)
                elif global_stat_gate_meta and str(global_reason).endswith("_stat_gate_blocked"):
                    changes.append(global_change)
                elif global_best_eligibility.get("block_reasons"):
                    changes.append(global_change)
            else:
                _clear_model_competition_champion(con=con)
                if global_current_row or global_best_eligibility.get("block_reasons"):
                    global_ope_gate_meta = dict(global_ope_gate or {}) if bool((global_ope_gate or {}).get("applied")) else {}
                    global_graph_gate_meta = (
                        dict(global_graph_gate or {}) if bool((global_graph_gate or {}).get("applied")) else {}
                    )
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
                            "ope_gate": global_ope_gate_meta,
                            "stat_gate": global_stat_gate_meta,
                            "graph_gate": global_graph_gate_meta,
                            "promotion_eligibility": {},
                            "best_promotion_eligibility": dict(global_best_eligibility or {}),
                            "current_promotion_eligibility": dict(global_current_eligibility or {}),
                            "system_guard": dict(promotion_system_guard or {}),
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
