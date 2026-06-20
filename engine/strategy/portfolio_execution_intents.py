"""
FILE: portfolio_execution_intents.py

Loads the latest portfolio orders and adapts them into execution intents with
extra metadata such as signal TTL, lifecycle fields, and execution stress
annotations. This is a translation layer between portfolio output and execution
input.
"""

import json
import time
import os
import math
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from engine.execution.deployable_capital import compute_deployable_equity
from engine.execution.options_readiness import force_options_shadow_intent
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.metrics import emit_timing
from engine.runtime.state_cache import cache_get_or_load
from engine.strategy.champion_manager import get_competition_policy_for_intent
try:
    from engine.decision_engine import DEFAULT_ENGINE as DEFAULT_DECISION_ENGINE
except Exception:
    DEFAULT_DECISION_ENGINE = None  # type: ignore[assignment]


DEFAULT_BATCH_WINDOW_MS = 2500  # group "latest run" orders by recent ts_ms window
DEFAULT_SIGNAL_TTL_MS = int(os.environ.get("DEFAULT_SIGNAL_TTL_MS", "1800000"))  # 30m
SHADOW_EXECUTION_INTENT_LOOKBACK_MS = int(
    os.environ.get("SHADOW_EXECUTION_INTENT_LOOKBACK_MS", str(60 * 60 * 1000))
)

# Execution stress adjustments are read-only overlays applied at handoff time.
_EXEC_SKEW_Z_THRESH = float(os.environ.get("EXEC_SKEW_Z_THRESH", "1.5"))
_EXEC_FLOW_Z_THRESH = float(os.environ.get("EXEC_FLOW_Z_THRESH", "2.0"))
_EXEC_STRESS_SIZE_MAX_REDUCTION = float(os.environ.get("EXEC_STRESS_SIZE_MAX_REDUCTION", "0.35"))

_EARNINGS_HALF_LIFE_DAYS = float(os.environ.get("EARNINGS_HALF_LIFE_DAYS", "5.0"))
_EXEC_EARNINGS_SIZE_MAX_REDUCTION = float(os.environ.get("EXEC_EARNINGS_SIZE_MAX_REDUCTION", "0.55"))
_COMPETITION_TOTAL_CAPITAL_FRACTION = float(
    os.environ.get("COMPETITION_TOTAL_CAPITAL_FRACTION", "1.0")
)
_QUERY_SLOW_MS = float(os.environ.get("EXECUTION_INTENTS_QUERY_SLOW_MS", "50.0"))
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: Optional[str] = None, **extra: Any) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_safe_int_failed",
            "PORTFOLIO_EXECUTION_INTENTS_SAFE_INT_FAILED",
            e,
            warn_key="safe_int",
            value=repr(value)[:120],
        )
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_safe_float_failed",
            "PORTFOLIO_EXECUTION_INTENTS_SAFE_FLOAT_FAILED",
            e,
            warn_key="safe_float",
            value=repr(value)[:120],
        )
        return float(default)


def _emit_query_latency(query_name: str, started_at: float, *, row_count: Optional[int] = None) -> None:
    duration_ms = (time.perf_counter() - float(started_at)) * 1000.0
    if duration_ms < float(_QUERY_SLOW_MS):
        return
    emit_timing(
        "execution_intents_query_latency_ms",
        duration_ms,
        component=__name__,
        job="load_latest_execution_intents",
        extra_tags={
            "query": str(query_name),
            "row_count": (int(row_count) if row_count is not None else None),
            "slow": "1",
        },
    )


def _safe_json_loads(s: Optional[str]) -> Any:
    if not s:
        return None
    try:
        return json.loads(s)
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_json_load_failed",
            "PORTFOLIO_EXECUTION_INTENTS_JSON_LOAD_FAILED",
            e,
            warn_key="portfolio_execution_intents_json_load_failed",
        )
        return None


def _terminal_signed_qty(explain: Any) -> Optional[float]:
    if not isinstance(explain, dict):
        return None
    terminal = explain.get("terminal_order")
    if not isinstance(terminal, dict):
        return None
    if str(terminal.get("sizing") or "").strip().lower() != "quantity":
        return None
    try:
        signed_qty = float(terminal.get("signed_qty"))
    except Exception:
        try:
            qty = abs(float(terminal.get("qty") or 0.0))
            side = str(terminal.get("side") or "").strip().upper()
            signed_qty = qty if side == "BUY" else -qty
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_terminal_qty_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_TERMINAL_QTY_PARSE_FAILED",
                e,
                warn_key=f"portfolio_execution_intents_terminal_qty_parse_failed:{repr(terminal)[:96]}",
                terminal_order=repr(terminal)[:240],
            )
            return None
    if not math.isfinite(float(signed_qty)) or abs(float(signed_qty)) <= 0.0:
        return None
    return float(signed_qty)


def _normalize_model_id(model_id: Any) -> str:
    mid = str(model_id or "").strip()
    return mid or "baseline"


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        v = float(x)
    except Exception:
        v = float(lo)
    return float(max(float(lo), min(float(hi), v)))


def _table_exists(con, table_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
            (str(table_name),),
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_table_exists_query_failed",
            "PORTFOLIO_EXECUTION_INTENTS_TABLE_EXISTS_QUERY_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_table_exists_query_failed:{table_name}",
            table=str(table_name),
        )
        return False
    return bool(row)


def _scale_intent_to_weight(intent: Dict[str, Any], scale: float) -> None:
    sc = max(0.0, float(scale))
    try:
        from_w = float(intent.get("from_weight") or 0.0)
    except Exception:
        from_w = 0.0
    try:
        to_w = float(intent.get("to_weight") or 0.0)
    except Exception:
        to_w = 0.0
    intent["to_weight"] = float(to_w * sc)
    intent["delta_weight"] = float(intent["to_weight"]) - float(from_w)


def _get_factor_feature_asof(con, feature_id: str, ts_ms: int) -> float:
    if not _table_exists(con, "factor_features"):
        return 0.0
    try:
        row = con.execute(
            """
            SELECT value
            FROM factor_features
            WHERE feature_id=?
              AND asof_ts <= ?
              AND effective_ts <= ?
            ORDER BY asof_ts DESC, effective_ts DESC
            LIMIT 1
            """,
            (str(feature_id), int(ts_ms), int(ts_ms)),
        ).fetchone()
        if not row:
            return 0.0
        return float(row[0]) if row[0] is not None else 0.0
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_factor_feature_lookup_failed",
            "PORTFOLIO_EXECUTION_INTENTS_FACTOR_FEATURE_LOOKUP_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_factor_feature_lookup_failed:{feature_id}",
            feature_id=str(feature_id),
            ts_ms=int(ts_ms),
        )
        return 0.0


def _ymd_from_ts_ms(ts_ms: int) -> str:
    try:
        dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_ts_to_ymd_failed",
            "PORTFOLIO_EXECUTION_INTENTS_TS_TO_YMD_FAILED",
            e,
            warn_key="portfolio_execution_intents_ts_to_ymd_failed",
            ts_ms=ts_ms,
        )
        ts_value = _now_ms()
        try:
            ts_value = int(ts_ms)
        except Exception as nested_error:
            _warn_nonfatal(
                "portfolio_execution_intents_ts_to_ymd_fallback_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_TS_TO_YMD_FALLBACK_PARSE_FAILED",
                nested_error,
                warn_key="portfolio_execution_intents_ts_to_ymd_fallback_parse_failed",
                ts_ms=ts_ms,
            )
        return time.strftime("%Y-%m-%d", time.gmtime(int(ts_value) / 1000.0))


def _earnings_proximity_decay(con, symbol: str, ts_ms: int) -> float:
    """
    Returns [0,1]. 1.0 = very near earnings date, 0.0 = far.
    Uses nearest earnings_calendar row by date distance.
    """
    sym = str(symbol or "").strip().upper()
    if not sym:
        return 0.0
    if not _table_exists(con, "earnings_calendar"):
        return 0.0

    try:
        today = _ymd_from_ts_ms(int(ts_ms))
        rows = con.execute(
            """
            SELECT earnings_date
            FROM earnings_calendar
            WHERE symbol=?
            """,
            (sym,),
        ).fetchall()
        if not rows:
            return 0.0

        today_date = datetime.strptime(str(today), "%Y-%m-%d").date()
        nearest_days: float | None = None
        for row in rows:
            ed = str(row[0] or "").strip()
            if not ed:
                continue
            try:
                delta_days = float((datetime.strptime(ed, "%Y-%m-%d").date() - today_date).days)
            except Exception as e:
                _warn_nonfatal(
                    "portfolio_execution_intents_earnings_date_parse_failed",
                    "PORTFOLIO_EXECUTION_INTENTS_EARNINGS_DATE_PARSE_FAILED",
                    e,
                    warn_key=f"portfolio_execution_intents_earnings_date_parse_failed:{sym}:{ed}",
                    symbol=str(sym),
                    earnings_date=ed,
                )
                continue
            if nearest_days is None or abs(delta_days) < abs(nearest_days):
                nearest_days = delta_days

        if nearest_days is None:
            return 0.0

        hl = max(0.5, float(_EARNINGS_HALF_LIFE_DAYS))
        return float(_clamp(math.exp(-abs(nearest_days) / hl), 0.0, 1.0))
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_earnings_decay_failed",
            "PORTFOLIO_EXECUTION_INTENTS_EARNINGS_DECAY_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_earnings_decay_failed:{sym}",
            symbol=str(sym),
            ts_ms=int(ts_ms),
        )
        return 0.0


def _portfolio_orders_latest_anchor(con) -> Optional[Tuple[int, int]]:
    try:
        row = con.execute(
            """
            SELECT id, ts_ms
            FROM portfolio_orders
            ORDER BY id DESC
            LIMIT 1
            """
        ).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_latest_anchor_query_failed",
            "PORTFOLIO_EXECUTION_INTENTS_LATEST_ANCHOR_QUERY_FAILED",
            e,
            warn_key="portfolio_execution_intents_latest_anchor_query_failed",
        )
        return None

    if not row:
        return None
    try:
        return int(row[0]), int(row[1] or 0)
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_latest_anchor_parse_failed",
            "PORTFOLIO_EXECUTION_INTENTS_LATEST_ANCHOR_PARSE_FAILED",
            e,
            warn_key="portfolio_execution_intents_latest_anchor_parse_failed",
            row_repr=repr(row),
        )
        return None


def _connection_db_identity(con) -> str:
    try:
        rows = con.execute("PRAGMA database_list").fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_db_identity_failed",
            "PORTFOLIO_EXECUTION_INTENTS_DB_IDENTITY_FAILED",
            e,
            warn_key="portfolio_execution_intents_db_identity_failed",
        )
        rows = []

    for row in rows:
        try:
            if len(row) > 2 and str(row[2] or "").strip():
                return str(row[2]).strip()
        except Exception as nested_error:
            _warn_nonfatal(
                "portfolio_execution_intents_db_identity_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_DB_IDENTITY_PARSE_FAILED",
                nested_error,
                warn_key="portfolio_execution_intents_db_identity_parse_failed",
                row_repr=repr(row),
            )
    db_path = str(os.environ.get("DB_PATH") or "").strip()
    return db_path or f"conn:{id(con)}"


def _table_change_signature(con, table_name: str, preferred_cols: Iterable[str]) -> str:
    table = str(table_name or "").strip()
    if not table or not _table_exists(con, table):
        return f"{table}:missing"

    try:
        cols = {
            str(r[1]).strip().lower()
            for r in (con.execute(f"PRAGMA table_info({table})").fetchall() or [])
            if r and len(r) > 1 and r[1]
        }
    except Exception:
        cols = set()

    preferred = [str(col or "").strip().lower() for col in (preferred_cols or []) if str(col or "").strip()]
    change_col = next((col for col in preferred if col in cols), "")
    if change_col:
        sql = f"SELECT COALESCE(MAX({change_col}), 0), COUNT(*) FROM {table}"
    else:
        sql = f"SELECT COALESCE(MAX(rowid), 0), COUNT(*) FROM {table}"
    try:
        row = con.execute(sql).fetchone()
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_table_signature_query_failed",
            "PORTFOLIO_EXECUTION_INTENTS_TABLE_SIGNATURE_QUERY_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_table_signature_query_failed:{table}",
            table=str(table),
        )
        return f"{table}:error"
    try:
        return f"{table}:{int((row or [0, 0])[0] or 0)}:{int((row or [0, 0])[1] or 0)}"
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_table_signature_parse_failed",
            "PORTFOLIO_EXECUTION_INTENTS_TABLE_SIGNATURE_PARSE_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_table_signature_parse_failed:{table}",
            table=str(table),
            row_repr=repr(row),
        )
        return f"{table}:parse_error"


def _execution_intents_cache_key(con, *, window_ms: int, max_rows: int) -> str:
    db_path = _connection_db_identity(con)
    signatures = [
        _table_change_signature(con, "portfolio_orders", ("id", "ts_ms")),
        _table_change_signature(con, "portfolio_state", ("updated_ts_ms", "opened_ts_ms")),
        _table_change_signature(con, "alerts", ("id", "ts_ms")),
        _table_change_signature(con, "runtime_meta", ("updated_ts_ms",)),
        _table_change_signature(con, "strategy_allocations", ("ts_ms",)),
        _table_change_signature(con, "broker_positions", ("updated_ts_ms", "ts_ms")),
        _table_change_signature(con, "broker_account", ("updated_ts_ms", "ts_ms", "id")),
        _table_change_signature(con, "prices", ("ts_ms",)),
    ]
    if DEFAULT_DECISION_ENGINE is not None:
        try:
            signatures.append(str(DEFAULT_DECISION_ENGINE.cache_token()))
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_decision_cache_token_failed",
                "PORTFOLIO_EXECUTION_INTENTS_DECISION_CACHE_TOKEN_FAILED",
                e,
                warn_key="portfolio_execution_intents_decision_cache_token_failed",
            )
            signatures.append("decision_engine:cache_token_error")
    return "|".join(
        [
            str(db_path),
            str(int(window_ms)),
            str(int(max_rows)),
            *signatures,
        ]
    )


def _portfolio_orders_batch_rows(
    con,
    window_ms: int = DEFAULT_BATCH_WINDOW_MS,
    max_rows: int = 5000,
) -> Tuple[Optional[int], Optional[int], List[tuple], Dict[str, bool]]:
    """
    Returns (batch_id, batch_ts_ms, rows, cols_present)
    batch_id is the max id within the batch window.
    """
    anchor = _portfolio_orders_latest_anchor(con)
    if not anchor:
        return None, None, [], {}

    anchor_id, anchor_ts = anchor
    if anchor_ts <= 0:
        anchor_ts = _now_ms()

    lo = int(anchor_ts) - int(max(250, int(window_ms)))

    try:
        cols = {
            str(r[1]).strip().lower()
            for r in (con.execute("PRAGMA table_info(portfolio_orders)").fetchall() or [])
            if r and len(r) > 1 and r[1]
        }
    except Exception:
        cols = set()

    has_reason = "reason" in cols
    has_model_id = "model_id" in cols
    has_source_rule_id = "source_rule_id" in cols

    select_cols = [
        "id",
        "ts_ms",
        ("model_id" if has_model_id else "'baseline' AS model_id"),
        "symbol",
        "action",
        "from_side",
        "to_side",
        "from_weight",
        "to_weight",
        "delta_weight",
        ("reason" if has_reason else "NULL AS reason"),
        "source_alert_id",
        ("source_rule_id" if has_source_rule_id else "NULL AS source_rule_id"),
        "explain_json",
    ]

    try:
        rows = con.execute(
            f"""
            SELECT
              {", ".join(select_cols)}
            FROM portfolio_orders
            WHERE ts_ms >= ?
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(lo), int(max_rows)),
        ).fetchall()
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_batch_rows_query_failed",
            "PORTFOLIO_EXECUTION_INTENTS_BATCH_ROWS_QUERY_FAILED",
            e,
            warn_key="portfolio_execution_intents_batch_rows_query_failed",
            lo=int(lo),
            max_rows=int(max_rows),
        )
        return None, None, [], {"reason": has_reason, "model_id": has_model_id, "source_rule_id": has_source_rule_id}

    batch_id = None
    batch_ts = None
    if rows:
        try:
            batch_id = int(max(int(r[0]) for r in rows))
        except Exception:
            batch_id = int(anchor_id)
        try:
            batch_ts = int(max(int(r[1] or 0) for r in rows))
        except Exception:
            batch_ts = int(anchor_ts)

    return batch_id, batch_ts, rows or [], {"reason": has_reason, "model_id": has_model_id, "source_rule_id": has_source_rule_id}


def _shadow_side_to_to_side(side: Any) -> str:
    s = str(side or "").strip().lower()
    if s in ("buy", "long"):
        return "LONG"
    if s in ("sell", "short"):
        return "SHORT"
    return "FLAT"


def _signed_shadow_qty(side: Any, qty: Any) -> float:
    try:
        q = abs(float(qty or 0.0))
    except Exception:
        q = 0.0
    if q <= 0.0:
        return 0.0
    to_side = _shadow_side_to_to_side(side)
    if to_side == "SHORT":
        return -float(q)
    if to_side == "LONG":
        return float(q)
    return 0.0


def _shadow_target_weight(meta: Dict[str, Any], to_side: str) -> Optional[float]:
    model_intent = meta.get("model_intent")
    if not isinstance(model_intent, dict):
        return None
    try:
        raw = model_intent.get("target_weight")
        if raw in (None, ""):
            return None
        tw = abs(float(raw))
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_shadow_target_weight_parse_failed",
            "PORTFOLIO_EXECUTION_INTENTS_SHADOW_TARGET_WEIGHT_PARSE_FAILED",
            e,
            warn_key="portfolio_execution_intents_shadow_target_weight_parse_failed",
            to_side=str(to_side),
            target_weight=model_intent.get("target_weight"),
        )
        return None
    if not math.isfinite(tw) or tw <= 0.0 or to_side not in ("LONG", "SHORT"):
        return None
    return float(tw)


def _load_pending_shadow_execution_intents(
    con,
    *,
    ts_ref_ms: int,
    max_rows: int,
) -> List[Dict[str, Any]]:
    try:
        rows = con.execute(
            """
            SELECT
              id,
              ts_ms,
              model_name,
              symbol,
              horizon_s,
              side,
              qty,
              ref_price,
              confidence,
              regime,
              meta_json
            FROM challenger_shadow_orders
            WHERE status='shadow'
            ORDER BY ts_ms ASC, id ASC
            LIMIT ?
            """,
            (int(max_rows),),
        ).fetchall() or []
    except Exception:
        rows = []

    intents: List[Dict[str, Any]] = []
    for (
        row_id,
        ts_ms,
        model_name,
        symbol,
        horizon_s,
        side,
        qty,
        ref_price,
        confidence,
        regime,
        meta_json,
    ) in rows:
        sym = str(symbol or "").strip().upper()
        name = str(model_name or "").strip()
        if not sym or not name:
            continue

        meta = _safe_json_loads(meta_json)
        if not isinstance(meta, dict):
            meta = {}
        explain = dict(meta.get("explain") or {}) if isinstance(meta.get("explain"), dict) else {}
        to_side = _shadow_side_to_to_side(side)
        if to_side == "FLAT":
            continue

        model_id = _normalize_model_id(meta.get("model_id") or _extract_model_id_from_explain(explain) or name)
        model_kind = str(meta.get("model_kind") or _extract_model_kind_from_explain(explain) or "").strip() or None
        model_version = str(meta.get("model_version") or _extract_model_version_from_explain(explain) or "").strip() or None
        model_ts_ms = meta.get("model_ts_ms")
        try:
            model_ts_ms = int(model_ts_ms) if model_ts_ms not in (None, "") else _extract_model_ts_ms_from_explain(explain)
        except Exception:
            model_ts_ms = _extract_model_ts_ms_from_explain(explain)

        hs = int(horizon_s or meta.get("horizon_s") or explain.get("horizon_s") or 0)
        signal_ts_ms = int(meta.get("signal_ts_ms") or ts_ms or _now_ms())
        ttl_ms = int(meta.get("alpha_ttl_ms") or 0)
        if ttl_ms <= 0:
            ttl_ms = _derive_alpha_ttl_ms(hs)
        half_life_ms = int(meta.get("alpha_half_life_ms") or 0)
        if half_life_ms <= 0:
            half_life_ms = _derive_half_life_ms(ttl_ms)

        conf = 0.0
        try:
            conf = float(confidence if confidence is not None else meta.get("confidence") or explain.get("confidence") or 0.0)
        except Exception:
            conf = 0.0

        explain.setdefault("model_name", name)
        explain["model_id"] = str(model_id)
        if model_kind:
            explain["model_kind"] = str(model_kind)
        if model_ts_ms is not None:
            explain["model_ts_ms"] = int(model_ts_ms)
        if model_version:
            explain["model_version"] = str(model_version)
        explain["regime"] = str(regime or meta.get("regime") or explain.get("regime") or "global")
        explain["competition_role"] = "challenger"
        explain["execution_target"] = "shadow"
        explain["shadow_order_row_id"] = int(row_id)
        if meta.get("predicted_z") is not None and explain.get("predicted_z") in (None, ""):
            try:
                explain["predicted_z"] = _safe_float(meta.get("predicted_z"))
            except Exception as e:
                _warn_nonfatal(
                    "portfolio_execution_intents_shadow_predicted_z_parse_failed",
                    "PORTFOLIO_EXECUTION_INTENTS_SHADOW_PREDICTED_Z_PARSE_FAILED",
                    e,
                    warn_key=f"portfolio_execution_intents_shadow_predicted_z_parse_failed:{row_id}",
                    shadow_order_row_id=int(row_id),
                    symbol=str(sym),
                )
        if meta.get("model_intent") is not None and explain.get("model_intent") is None:
            if isinstance(meta.get("model_intent"), dict):
                explain["model_intent"] = dict(meta.get("model_intent") or {})

        synthetic_alert_id = -int(row_id)
        intent: Dict[str, Any] = {
            "source_order_id": -int(row_id),
            "shadow_order_row_id": int(row_id),
            "ts_ms": int(ts_ms or signal_ts_ms),
            "signal_ts_ms": int(signal_ts_ms),
            "model_id": str(model_id),
            "model_name": str(name),
            "model_kind": model_kind,
            "model_ts_ms": (int(model_ts_ms) if model_ts_ms is not None else None),
            "model_version": model_version,
            "symbol": sym,
            "action": "OPEN",
            "from_side": "FLAT",
            "to_side": str(to_side),
            "from_weight": 0.0,
            "to_weight": 0.0,
            "delta_weight": 0.0,
            "reason": "challenger_shadow",
            "source_alert_id": _safe_int(meta.get("source_alert_id"), synthetic_alert_id),
            "source_rule_id": None,
            "explain": explain,
            "confidence": float(conf),
            "horizon_s": int(hs),
            "alpha_ttl_ms": int(ttl_ms),
            "alpha_half_life_ms": int(half_life_ms),
            "regime": str(explain.get("regime") or "global"),
            "execution_target": "shadow",
            "competition": {
                "allowed": False,
                "blocked": True,
                "reason": "challenger_shadow",
            },
        }
        if ref_price is not None:
            try:
                intent["ref_px"] = float(ref_price)
            except Exception as e:
                _warn_nonfatal(
                    "portfolio_execution_intents_shadow_ref_price_parse_failed",
                    "PORTFOLIO_EXECUTION_INTENTS_SHADOW_REF_PRICE_PARSE_FAILED",
                    e,
                    warn_key=f"portfolio_execution_intents_shadow_ref_price_parse_failed:{row_id}",
                    shadow_order_row_id=int(row_id),
                    symbol=str(sym),
                    ref_price=ref_price,
                )

        target_weight = _shadow_target_weight(meta, str(to_side))
        if target_weight is not None:
            intent["to_weight"] = float(target_weight)
            intent["delta_weight"] = float(target_weight)
        else:
            signed_qty = _signed_shadow_qty(side, qty)
            if abs(signed_qty) <= 1e-12:
                continue
            intent["qty"] = float(signed_qty)

        intent = force_options_shadow_intent(intent)
        intents.append(intent)

    return intents


def _parse_alert_meta(alert_id: int, ts_ms: Any, horizon_s: Any, explain_json: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "signal_ts_ms": _safe_int(ts_ms),
        "horizon_s": _safe_int(horizon_s),
    }
    ex = _safe_json_loads(explain_json) if explain_json else None
    if not isinstance(ex, dict):
        return out

    # common keys seen in explain_json variants
    for k in ("volatility", "vol", "sigma", "realized_vol"):
        if k not in ex or ex.get(k) is None:
            continue
        try:
            out["volatility"] = _safe_float(ex.get(k))
            break
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_alert_meta_volatility_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_ALERT_META_VOLATILITY_PARSE_FAILED",
                e,
                warn_key="portfolio_execution_intents_alert_meta_volatility_parse_failed",
                alert_id=int(alert_id),
                key=str(k),
                value=ex.get(k),
            )

    for k in ("alpha_ttl_ms", "ttl_ms"):
        if k not in ex or ex.get(k) is None:
            continue
        try:
            out["alpha_ttl_ms"] = _safe_int(ex.get(k))
            break
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_alert_meta_ttl_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_ALERT_META_TTL_PARSE_FAILED",
                e,
                warn_key="portfolio_execution_intents_alert_meta_ttl_parse_failed",
                alert_id=int(alert_id),
                key=str(k),
                value=ex.get(k),
            )

    for k in ("alpha_half_life_ms", "half_life_ms"):
        if k not in ex or ex.get(k) is None:
            continue
        try:
            out["alpha_half_life_ms"] = _safe_int(ex.get(k))
            break
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_alert_meta_half_life_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_ALERT_META_HALF_LIFE_PARSE_FAILED",
                e,
                warn_key="portfolio_execution_intents_alert_meta_half_life_parse_failed",
                alert_id=int(alert_id),
                key=str(k),
                value=ex.get(k),
            )

    if ex.get("market_regime") is not None:
        out["market_regime"] = str(ex.get("market_regime") or "mean_reversion")
    elif ex.get("market_regime_label") is not None:
        out["market_regime"] = str(ex.get("market_regime_label") or "mean_reversion")

    mr = ex.get("market_regime_snapshot")
    if isinstance(mr, dict):
        out["market_regime_snapshot"] = {
            "label": str(mr.get("label") or out.get("market_regime") or "mean_reversion"),
            "volatility": float(mr.get("volatility", 0.0) or 0.0),
            "volatility_baseline": float(mr.get("volatility_baseline", 0.0) or 0.0),
            "trend": float(mr.get("trend", 0.0) or 0.0),
            "trend_strength": float(mr.get("trend_strength", 0.0) or 0.0),
        }

    pipeline_timing = ex.get("pipeline_timing")
    if isinstance(pipeline_timing, dict):
        for key in (
            "source_event_ts_ms",
            "db_observed_ts_ms",
            "ingestion_to_db_latency_ms",
            "db_to_prediction_latency_ms",
            "prediction_ts_ms",
            "prediction_to_decision_latency_ms",
            "decision_ts_ms",
        ):
            if pipeline_timing.get(key) is None:
                continue
            out[key] = _safe_int(pipeline_timing.get(key))

    return out


def _load_alert_meta_map(con, alert_ids: Iterable[int]) -> Dict[int, Dict[str, Any]]:
    ids = sorted({int(alert_id) for alert_id in (alert_ids or []) if alert_id is not None})
    if not ids:
        return {}

    out: Dict[int, Dict[str, Any]] = {}
    chunk_size = 250
    started = time.perf_counter()

    try:
        for idx in range(0, len(ids), chunk_size):
            chunk = ids[idx : idx + chunk_size]
            placeholders = ",".join("?" for _ in chunk)
            rows = con.execute(
                f"""
                SELECT id, ts_ms, horizon_s, explain_json
                FROM alerts
                WHERE id IN ({placeholders})
                """,
                tuple(chunk),
            ).fetchall() or []
            for alert_id, ts_ms, horizon_s, explain_json in rows:
                out[int(alert_id)] = _parse_alert_meta(int(alert_id), ts_ms, horizon_s, explain_json)
        _emit_query_latency("alerts_meta_lookup", started, row_count=len(out))
        return out
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_alert_meta_failed",
            "PORTFOLIO_EXECUTION_INTENTS_ALERT_META_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_alert_meta_failed:{len(ids)}",
            alert_count=int(len(ids)),
        )
        return {}


def _alert_meta(con, alert_id: int) -> Dict[str, Any]:
    return _load_alert_meta_map(con, [int(alert_id)]).get(int(alert_id), {})


def _derive_alpha_ttl_ms(horizon_s: int) -> int:
    # Practical defaults: min 20s, horizon-driven if available, bounded by configured default TTL
    try:
        hs = int(horizon_s or 0)
    except Exception:
        hs = 0

    ttl = hs * 1000 if hs > 0 else int(DEFAULT_SIGNAL_TTL_MS)
    ttl_cap = max(int(DEFAULT_SIGNAL_TTL_MS), 20_000)
    ttl = max(20_000, min(int(ttl_cap), int(ttl)))
    return int(ttl)


def _derive_half_life_ms(ttl_ms: int) -> int:
    ttl_ms = int(ttl_ms or 0)
    if ttl_ms <= 0:
        return 90_000
    # 1/3 ttl, clamped
    hl = int(ttl_ms // 3)
    hl = max(10_000, min(10 * 60 * 1000, hl))
    return int(hl)


def _extract_model_name_from_explain(explain: Optional[Dict[str, Any]]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_name", "strategy_name", "strategy", "model"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    strategy_obj = ex.get("strategy")
    if isinstance(strategy_obj, dict):
        val = strategy_obj.get("name")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_name", "name", "id"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()

    return ""


def _extract_model_id_from_explain(explain: Optional[Dict[str, Any]]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_id", "agent_id"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return _normalize_model_id(val)

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_id", "id", "agent_id"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                return _normalize_model_id(val)

    return "baseline"


def _extract_model_version_from_explain(explain: Optional[Dict[str, Any]]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    val = ex.get("model_version")
    if isinstance(val, str) and val.strip():
        return str(val).strip()

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        val = model_obj.get("model_version") or model_obj.get("version")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_meta_obj = ex.get("model_meta")
    if isinstance(model_meta_obj, dict):
        val = model_meta_obj.get("model_version") or model_meta_obj.get("version")
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    return ""


def _extract_model_kind_from_explain(explain: Optional[Dict[str, Any]]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_kind", "kind", "type"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_kind", "kind", "type"):
            val = model_obj.get(key)
            if isinstance(val, str) and val.strip():
                return str(val).strip()

    return ""


def _extract_model_ts_ms_from_explain(explain: Optional[Dict[str, Any]]) -> Optional[int]:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
        val = ex.get(key)
        if val is not None:
            try:
                return int(val)
            except Exception as e:
                _warn_nonfatal(
                    "portfolio_execution_intents_model_ts_parse_failed",
                    "PORTFOLIO_EXECUTION_INTENTS_MODEL_TS_PARSE_FAILED",
                    e,
                    warn_key="portfolio_execution_intents_model_ts_parse_failed",
                    key=str(key),
                    value=val,
                )

    model_obj = ex.get("model")
    if isinstance(model_obj, dict):
        for key in ("model_ts_ms", "ts_ms", "trained_ts_ms"):
            val = model_obj.get(key)
            if val is not None:
                try:
                    return int(val)
                except Exception as e:
                    _warn_nonfatal(
                        "portfolio_execution_intents_nested_model_ts_parse_failed",
                        "PORTFOLIO_EXECUTION_INTENTS_NESTED_MODEL_TS_PARSE_FAILED",
                        e,
                        warn_key="portfolio_execution_intents_nested_model_ts_parse_failed",
                        key=str(key),
                        value=val,
                    )

    return None


def _extract_regime_from_explain(explain: Optional[Dict[str, Any]]) -> str:
    ex = explain if isinstance(explain, dict) else {}
    for key in ("regime", "current_regime", "regime_label"):
        val = ex.get(key)
        if isinstance(val, str) and val.strip():
            return str(val).strip()
    return "global"


def _extract_horizon_s_from_explain(explain: Optional[Dict[str, Any]]) -> int:
    ex = explain if isinstance(explain, dict) else {}
    candidates = [
        ex.get("horizon_s"),
        ex.get("horizon"),
        ((ex.get("model_intent") or {}).get("horizon_s") if isinstance(ex.get("model_intent"), dict) else None),
        ((ex.get("signal") or {}).get("horizon_s") if isinstance(ex.get("signal"), dict) else None),
    ]
    for raw in candidates:
        if raw in (None, ""):
            continue
        try:
            return int(raw)
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_horizon_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_HORIZON_PARSE_FAILED",
                e,
                warn_key="portfolio_execution_intents_horizon_parse_failed",
                value=repr(raw)[:120],
            )
            continue
    return 0


def _competition_group_key(symbol: str, horizon_s: int, regime: str) -> str:
    return "|".join(
        [
            str(symbol or "").upper().strip(),
            str(int(horizon_s or 0)),
            str(regime or "global").strip() or "global",
        ]
    )


def _load_latest_prices(con) -> Dict[str, float]:
    out: Dict[str, float] = {}
    try:
        cols = {
            str(r[1]).strip().lower()
            for r in (con.execute("PRAGMA table_info(prices)").fetchall() or [])
            if r and len(r) > 1 and r[1]
        }
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_latest_prices_query_failed",
            "PORTFOLIO_EXECUTION_INTENTS_LATEST_PRICES_QUERY_FAILED",
            e,
            warn_key="portfolio_execution_intents_latest_prices_query_failed:pragma",
            query="PRAGMA table_info(prices)",
        )
        cols = set()

    price_col = "price" if "price" in cols else ("px" if "px" in cols else None)
    if not price_col:
        return out

    sql = f"""
        SELECT p.symbol, p.{price_col}
        FROM prices p
        INNER JOIN (
            SELECT symbol, MAX(ts_ms) AS max_ts_ms
            FROM prices
            GROUP BY symbol
        ) latest
          ON latest.symbol = p.symbol
         AND latest.max_ts_ms = p.ts_ms
    """
    started = time.perf_counter()
    try:
        rows = con.execute(sql).fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_latest_prices_query_failed",
            "PORTFOLIO_EXECUTION_INTENTS_LATEST_PRICES_QUERY_FAILED",
            e,
            warn_key=f"portfolio_execution_intents_latest_prices_query_failed:{price_col}",
            query=sql.strip(),
        )
        rows = []
    _emit_query_latency("latest_prices_lookup", started, row_count=len(rows))

    for sym, px in rows:
        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue
        try:
            out[sym_u] = float(px or 0.0)
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_latest_price_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_LATEST_PRICE_PARSE_FAILED",
                e,
                warn_key=f"portfolio_execution_intents_latest_price_parse_failed:{sym_u}",
                symbol=str(sym_u),
                price=px,
            )
    return out


def _deployable_equity(con) -> float:
    row = None
    try:
        account_cols = {
            str(r[1]).strip().lower()
            for r in (con.execute("PRAGMA table_info(broker_account)").fetchall() or [])
            if r and len(r) > 1 and r[1]
        }
    except Exception as e:
        _warn_nonfatal(
            "portfolio_execution_intents_broker_account_columns_failed",
            "PORTFOLIO_EXECUTION_INTENTS_BROKER_ACCOUNT_COLUMNS_FAILED",
            e,
            warn_key="portfolio_execution_intents_broker_account_columns_failed",
        )
        account_cols = set()

    cash_expr = "cash" if "cash" in account_cols else "NULL AS cash"
    equity_expr = "equity" if "equity" in account_cols else "NULL AS equity"
    buying_power_expr = "buying_power" if "buying_power" in account_cols else "NULL AS buying_power"
    select_expr = f"{cash_expr}, {equity_expr}, {buying_power_expr}"

    queries: List[str] = []
    if account_cols:
        if "id" in account_cols:
            queries.append(f"SELECT {select_expr} FROM broker_account WHERE id=1 LIMIT 1")

        order_cols = [
            col
            for col in ("updated_ts_ms", "ts_ms", "id")
            if col in account_cols
        ]
        if order_cols:
            order_expr = ", ".join(f"{col} DESC" for col in order_cols)
            queries.append(f"SELECT {select_expr} FROM broker_account ORDER BY {order_expr} LIMIT 1")
        else:
            queries.append(f"SELECT {select_expr} FROM broker_account LIMIT 1")

    for sql in queries:
        try:
            row = con.execute(sql).fetchone()
        except Exception:
            row = None
        if row:
            break

    cash = 0.0
    equity = 0.0
    buying_power = 0.0
    if row:
        try:
            cash = float(row[0] or 0.0)
        except Exception:
            cash = 0.0
        try:
            equity = float(row[1] or 0.0)
        except Exception:
            equity = 0.0
        try:
            buying_power = float(row[2] or 0.0)
        except Exception:
            buying_power = 0.0
    if equity <= 0.0:
        try:
            if _table_exists(con, "equity_history"):
                row = con.execute("SELECT equity FROM equity_history ORDER BY ts_ms DESC LIMIT 1").fetchone()
                equity = float((row or [0.0])[0] or 0.0)
        except Exception:
            equity = 0.0
    return float(
        compute_deployable_equity(
            {"equity": float(equity), "cash": float(cash), "buying_power": float(buying_power or equity)},
            default_equity=float(equity),
        )
        or 0.0
    )


def _build_exposure_context(con, prices: Dict[str, float]) -> Dict[str, Any]:
    started = time.perf_counter()
    try:
        state_rows = con.execute(
            """
            SELECT model_id, symbol, weight, explain_json
            FROM portfolio_state
            WHERE ABS(COALESCE(weight, 0.0)) > 0.0
            """
        ).fetchall() or []
    except Exception:
        state_rows = []

    state_by_model_id: Dict[Tuple[str, str], float] = {}
    state_by_model_name: Dict[Tuple[str, str], float] = {}
    state_by_group: Dict[str, float] = {}
    open_positions = 0
    open_positions_by_symbol: Dict[str, int] = {}
    for state_model_id, symbol, weight, explain_json in state_rows:
        state_model_id_n = _normalize_model_id(state_model_id)
        explain = _safe_json_loads(explain_json)
        if not isinstance(explain, dict):
            explain = {}
        state_model_name = str(_extract_model_name_from_explain(explain) or "").strip()
        state_regime = str(_extract_regime_from_explain(explain) or "global").strip() or "global"
        state_horizon_s = int(_extract_horizon_s_from_explain(explain) or 0)
        try:
            weight_abs = abs(float(weight or 0.0))
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_state_exposure_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_STATE_EXPOSURE_PARSE_FAILED",
                e,
                warn_key=f"portfolio_execution_intents_state_exposure_parse_failed:{state_model_id_n}:{symbol}",
                model_id=str(state_model_id_n),
                symbol=str(symbol or ""),
                weight=weight,
            )
            continue
        open_positions += 1
        sym_u = str(symbol or "").upper().strip()
        if sym_u:
            open_positions_by_symbol[sym_u] = int(open_positions_by_symbol.get(sym_u, 0)) + 1
        state_by_model_id[(str(state_model_id_n), str(state_regime))] = (
            state_by_model_id.get((str(state_model_id_n), str(state_regime)), 0.0) + weight_abs
        )
        if state_model_name:
            state_by_model_name[(str(state_model_name), str(state_regime))] = (
                state_by_model_name.get((str(state_model_name), str(state_regime)), 0.0) + weight_abs
            )
        state_group_key = _competition_group_key(str(symbol or "").upper().strip(), int(state_horizon_s), str(state_regime))
        state_by_group[state_group_key] = state_by_group.get(state_group_key, 0.0) + weight_abs

    try:
        pos_rows = con.execute("SELECT symbol, qty FROM broker_positions").fetchall() or []
    except Exception:
        pos_rows = []

    try:
        champ_rows = con.execute(
            """
            SELECT symbol, horizon_s, model_name, regime
            FROM champion_assignments
            WHERE state='champion'
            """
        ).fetchall() or []
    except Exception:
        champ_rows = []

    symbol_to_models: Dict[str, List[Tuple[int, str, str]]] = {}
    for sym, hs, mn, rg in champ_rows:
        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue
        symbol_to_models.setdefault(sym_u, []).append(
            (
                int(hs or 0),
                str(mn or "").strip(),
                str(rg or "global").strip() or "global",
            )
        )

    position_by_model_name: Dict[Tuple[str, str], float] = {}
    position_by_group: Dict[str, float] = {}
    for sym, qty in pos_rows:
        sym_u = str(sym or "").upper().strip()
        if not sym_u:
            continue
        assigned_rows = list(symbol_to_models.get(sym_u) or [])
        if not assigned_rows:
            continue
        px = float(prices.get(sym_u) or 0.0)
        if px <= 0.0:
            continue
        try:
            exposure = abs(float(qty or 0.0)) * px
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_position_exposure_parse_failed",
                "PORTFOLIO_EXECUTION_INTENTS_POSITION_EXPOSURE_PARSE_FAILED",
                e,
                warn_key=f"portfolio_execution_intents_position_exposure_parse_failed:{sym_u}",
                symbol=str(sym_u),
                qty=qty,
                price=float(px),
            )
            continue
        assigned = assigned_rows[0]
        key = (str(assigned[1]), str(assigned[2]))
        position_by_model_name[key] = position_by_model_name.get(key, 0.0) + float(exposure)
        if len(assigned_rows) == 1:
            group_key = _competition_group_key(sym_u, int(assigned[0]), str(assigned[2]))
            position_by_group[group_key] = position_by_group.get(group_key, 0.0) + float(exposure)

    _emit_query_latency(
        "exposure_context_build",
        started,
        row_count=len(state_rows) + len(pos_rows) + len(champ_rows),
    )
    return {
        "state_by_model_id": state_by_model_id,
        "state_by_model_name": state_by_model_name,
        "state_by_group": state_by_group,
        "position_by_model_name": position_by_model_name,
        "position_by_group": position_by_group,
        "open_positions": int(open_positions),
        "open_positions_by_symbol": {str(sym): int(count) for sym, count in open_positions_by_symbol.items()},
    }


def _current_model_exposure_fraction_from_context(
    exposure_context: Dict[str, Any],
    *,
    model_id: Optional[str] = None,
    model_name: str,
    regime: str,
    equity: float,
) -> float:
    model_id_raw = str(model_id or "").strip()
    model_id_n = _normalize_model_id(model_id_raw) if model_id_raw else ""
    name = str(model_name or "").strip()
    reg = str(regime or "global").strip() or "global"
    if not name and not model_id_n:
        return 0.0

    state_by_model_id = exposure_context.get("state_by_model_id") or {}
    state_by_model_name = exposure_context.get("state_by_model_name") or {}
    position_by_model_name = exposure_context.get("position_by_model_name") or {}

    if model_id_n:
        state_exposure = float(state_by_model_id.get((str(model_id_n), str(reg)), 0.0) or 0.0)
        if state_exposure > 0.0:
            return float(state_exposure)

    if name:
        state_exposure = float(state_by_model_name.get((str(name), str(reg)), 0.0) or 0.0)
        if state_exposure > 0.0:
            return float(state_exposure)

    if not name:
        return 0.0
    if equity <= 1e-9:
        return 0.0
    exposure = float(position_by_model_name.get((str(name), str(reg)), 0.0) or 0.0)
    return float(exposure) / float(max(1e-9, equity))


def _current_group_exposure_fraction_from_context(
    exposure_context: Dict[str, Any],
    *,
    symbol: str,
    horizon_s: int,
    regime: str,
    equity: float,
) -> float:
    group_key = _competition_group_key(str(symbol or "").upper().strip(), int(horizon_s or 0), str(regime or "global"))
    state_by_group = exposure_context.get("state_by_group") or {}
    position_by_group = exposure_context.get("position_by_group") or {}
    state_exposure = float(state_by_group.get(group_key, 0.0) or 0.0)
    if state_exposure > 0.0:
        return float(state_exposure)
    if equity <= 1e-9:
        return 0.0
    return float(position_by_group.get(group_key, 0.0) or 0.0) / float(max(1e-9, equity))


def _extract_first_numeric(*values: Any) -> Optional[float]:
    for value in values:
        if value in (None, ""):
            continue
        try:
            out = float(value)
        except Exception as e:
            _warn_nonfatal(
                "portfolio_execution_intents_extract_first_numeric_failed",
                "PORTFOLIO_EXECUTION_INTENTS_EXTRACT_FIRST_NUMERIC_FAILED",
                e,
                warn_key="extract_first_numeric",
                value=repr(value)[:120],
            )
            continue
        if math.isfinite(out):
            return float(out)
    return None


def _decision_prediction(intent: Dict[str, Any]) -> Optional[float]:
    explain = dict(intent.get("explain") or {})
    signal = dict(explain.get("signal") or {}) if isinstance(explain.get("signal"), dict) else {}
    model_intent = dict(explain.get("model_intent") or {}) if isinstance(explain.get("model_intent"), dict) else {}
    return _extract_first_numeric(
        intent.get("prediction"),
        intent.get("predicted_z"),
        intent.get("expected_z"),
        model_intent.get("expected_z"),
        signal.get("expected_z"),
        signal.get("predicted_z"),
        explain.get("expected_z"),
        explain.get("predicted_z"),
    )


def _decision_confidence(intent: Dict[str, Any]) -> Optional[float]:
    explain = dict(intent.get("explain") or {})
    signal = dict(explain.get("signal") or {}) if isinstance(explain.get("signal"), dict) else {}
    model_intent = dict(explain.get("model_intent") or {}) if isinstance(explain.get("model_intent"), dict) else {}
    return _extract_first_numeric(
        intent.get("confidence"),
        model_intent.get("confidence"),
        signal.get("confidence"),
        explain.get("confidence"),
    )


def _decision_risk_context(
    intent: Dict[str, Any],
    *,
    open_positions: int,
    symbol_open_positions: int,
    now_ms: int,
) -> Dict[str, Any]:
    explain = dict(intent.get("explain") or {})
    signal = dict(explain.get("signal") or {}) if isinstance(explain.get("signal"), dict) else {}
    model_intent = dict(explain.get("model_intent") or {}) if isinstance(explain.get("model_intent"), dict) else {}
    tradability = (
        dict(signal.get("tradability") or {})
        if isinstance(signal.get("tradability"), dict)
        else dict(explain.get("tradability") or {})
        if isinstance(explain.get("tradability"), dict)
        else {}
    )
    market_stress_block = (
        dict(signal.get("market_stress") or {})
        if isinstance(signal.get("market_stress"), dict)
        else dict(explain.get("market_stress") or {})
        if isinstance(explain.get("market_stress"), dict)
        else {}
    )
    competition = dict(intent.get("competition") or {})
    signal_ts_ms = _safe_int(intent.get("signal_ts_ms"), _safe_int(intent.get("ts_ms"), now_ms))
    signal_age_s = 0.0
    if signal_ts_ms > 0:
        signal_age_s = max(0.0, float(now_ms - int(signal_ts_ms)) / 1000.0)
    return {
        "action": str(intent.get("action") or ""),
        "from_side": str(intent.get("from_side") or ""),
        "to_side": str(intent.get("to_side") or ""),
        "execution_target": str(intent.get("execution_target") or "real"),
        "current_weight": _safe_float(intent.get("from_weight"), 0.0),
        "target_weight": _safe_float(intent.get("to_weight"), 0.0),
        "signal_age_s": float(signal_age_s),
        "risk_blocked": bool(competition.get("blocked")),
        "risk_score": _extract_first_numeric(
            signal.get("risk_score"),
            explain.get("risk_score"),
            model_intent.get("risk_score"),
        )
        or 0.0,
        "expected_drawdown": _extract_first_numeric(
            model_intent.get("expected_dd"),
            tradability.get("expected_dd"),
            signal.get("expected_dd"),
            explain.get("expected_dd"),
        )
        or 0.0,
        "market_stress": _extract_first_numeric(
            market_stress_block.get("score"),
            explain.get("market_stress_score"),
            signal.get("market_stress_score"),
        )
        or 0.0,
        "open_positions": int(max(0, int(open_positions))),
        "symbol_open_positions": int(max(0, int(symbol_open_positions))),
        "is_new_position": (
            str(intent.get("from_side") or "").strip().upper() == "FLAT"
            and str(intent.get("to_side") or "").strip().upper() in {"LONG", "SHORT"}
            and abs(_safe_float(intent.get("to_weight"), 0.0)) > 1e-12
        ),
    }


def _apply_position_projection(
    intent: Dict[str, Any],
    *,
    open_positions: int,
    open_positions_by_symbol: Dict[str, int],
) -> int:
    sym = str(intent.get("symbol") or "").upper().strip()
    from_side = str(intent.get("from_side") or "").strip().upper()
    to_side = str(intent.get("to_side") or "").strip().upper()
    target_weight = abs(_safe_float(intent.get("to_weight"), 0.0))

    if from_side == "FLAT" and to_side in {"LONG", "SHORT"} and target_weight > 1e-12:
        open_positions += 1
        if sym:
            open_positions_by_symbol[sym] = int(open_positions_by_symbol.get(sym, 0)) + 1
        return int(open_positions)

    if from_side in {"LONG", "SHORT"} and (to_side == "FLAT" or target_weight <= 1e-12):
        open_positions = max(0, int(open_positions) - 1)
        if sym:
            open_positions_by_symbol[sym] = max(0, int(open_positions_by_symbol.get(sym, 0)) - 1)
        return int(open_positions)

    return int(open_positions)


def _apply_decision_gate(
    intents: List[Dict[str, Any]],
    *,
    exposure_context: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    live_decision_state: Dict[str, Any] = {}
    try:
        from engine.runtime.live_ai_safety import live_decision_gate_snapshot

        live_decision_state = live_decision_gate_snapshot(
            execution_mode=os.environ.get("EXECUTION_MODE", ""),
            broker=os.environ.get("BROKER") or os.environ.get("BROKER_NAME") or os.environ.get("LIVE_BROKER") or "",
            decision_engine=DEFAULT_DECISION_ENGINE,
        )
        if bool(live_decision_state.get("required")) and not bool(live_decision_state.get("ok")):
            blockers = ",".join(str(item) for item in list(live_decision_state.get("blockers") or []))
            raise RuntimeError(f"live_decision_gate_failed:{blockers}")
    except RuntimeError:
        raise
    except Exception as e:
        live_context = str(os.environ.get("EXECUTION_MODE") or os.environ.get("ENGINE_MODE") or "").strip().lower() == "live"
        if live_context:
            raise RuntimeError(f"live_decision_gate_check_failed:{type(e).__name__}:{e}") from e
        live_decision_state = {"required": False, "ok": True, "reason": "check_unavailable_non_live"}

    if DEFAULT_DECISION_ENGINE is None:
        return list(intents or []), [], {"enabled": False, "reason": "decision_engine_unavailable"}
    if not bool(getattr(DEFAULT_DECISION_ENGINE, "enabled", False)):
        return list(intents or []), [], {"enabled": False, "reason": "feature_flag_disabled"}

    projected_open_positions = int(max(0, int((exposure_context.get("open_positions") or 0))))
    projected_by_symbol = {
        str(sym).upper().strip(): int(max(0, int(count)))
        for sym, count in dict(exposure_context.get("open_positions_by_symbol") or {}).items()
        if str(sym).strip()
    }
    now_ms = _now_ms()
    decisioned: List[Dict[str, Any]] = []
    shadowed: List[Dict[str, Any]] = []
    evaluated = 0

    for raw_intent in list(intents or []):
        intent = dict(raw_intent or {})
        if str(intent.get("execution_target") or "real").strip().lower() != "real":
            decisioned.append(intent)
            continue

        sym = str(intent.get("symbol") or "").upper().strip()
        risk = _decision_risk_context(
            intent,
            open_positions=int(projected_open_positions),
            symbol_open_positions=int(projected_by_symbol.get(sym, 0)),
            now_ms=int(now_ms),
        )
        prediction = _decision_prediction(intent)
        confidence = _decision_confidence(intent)

        try:
            decision = dict(
                DEFAULT_DECISION_ENGINE.evaluate(
                    prediction=prediction,
                    confidence=confidence,
                    risk=risk,
                )
                or {}
            )
        except Exception as e:
            if bool(live_decision_state.get("required")):
                raise RuntimeError(
                    f"live_decision_gate_evaluation_failed:{type(e).__name__}:{e}"
                ) from e
            _warn_nonfatal(
                "portfolio_execution_intents_decision_gate_failed",
                "PORTFOLIO_EXECUTION_INTENTS_DECISION_GATE_FAILED",
                e,
                warn_key=f"portfolio_execution_intents_decision_gate_failed:{intent.get('source_order_id')}",
                source_order_id=int(intent.get("source_order_id") or 0),
                symbol=str(sym),
            )
            decision = {
                "enabled": True,
                "execute": True,
                "reason": f"fail_open:{type(e).__name__}",
                "reasons": [],
                "risk": dict(risk),
            }

        evaluated += 1
        intent["decision"] = dict(decision)
        if bool(decision.get("execute", True)):
            projected_open_positions = _apply_position_projection(
                intent,
                open_positions=int(projected_open_positions),
                open_positions_by_symbol=projected_by_symbol,
            )
            decisioned.append(intent)
            continue

        intent["execution_target"] = "shadow"
        intent["decision"]["downgraded_execution_target"] = "shadow"
        shadowed.append(dict(intent))
        decisioned.append(intent)

    return decisioned, shadowed, {
        "enabled": True,
        "evaluated": int(evaluated),
        "shadowed": int(len(shadowed)),
        "allowed": int(max(0, evaluated - len(shadowed))),
        "open_positions_start": int(max(0, int((exposure_context.get("open_positions") or 0)))),
        "open_positions_end": int(max(0, int(projected_open_positions))),
        "live_decision_gate": dict(live_decision_state or {}),
    }


def load_latest_execution_intents(
    con,
    window_ms: int = DEFAULT_BATCH_WINDOW_MS,
    max_rows: int = 5000,
) -> Dict[str, Any]:
    """
    Returns:
      {
        ok: bool,
        batch_id: int|None,
        batch_ts_ms: int|None,
        intents: [dict...]
      }

    Each intent includes:
      symbol, to_side, to_weight, source_alert_id, source_rule_id, explain_json (parsed best-effort),
      plus: signal_ts_ms, alpha_ttl_ms, alpha_half_life_ms, volatility (best-effort)
    """
    cache_key = _execution_intents_cache_key(con, window_ms=int(window_ms), max_rows=int(max_rows))

    def _load() -> Dict[str, Any]:
        batch_rows_started = time.perf_counter()
        batch_id, batch_ts_ms, rows, cols_present = _portfolio_orders_batch_rows(con, window_ms=window_ms, max_rows=max_rows)
        _emit_query_latency("portfolio_orders_batch_rows", batch_rows_started, row_count=len(rows))
        prices = _load_latest_prices(con)
        deployable_equity = _deployable_equity(con)
        exposure_context = _build_exposure_context(con, prices)
        alert_meta_by_id = _load_alert_meta_map(
            con,
            [
                int(r[11])
                for r in (rows or [])
                if r and len(r) > 11 and r[11] is not None
            ],
        )
        factor_cache: Dict[Tuple[str, int], float] = {}
        earnings_cache: Dict[Tuple[str, str], float] = {}
        competition_cache: Dict[Tuple[str, int, str, str], Dict[str, Any]] = {}
        projected_model_exposure: Dict[Tuple[str, str], float] = {}
        projected_group_exposure: Dict[str, float] = {}

        def _factor_value(feature_id: str, ts_ref: int) -> float:
            key = (str(feature_id), int(ts_ref))
            if key not in factor_cache:
                factor_cache[key] = float(_get_factor_feature_asof(con, feature_id, int(ts_ref)))
            return float(factor_cache[key])

        def _earnings_decay(symbol: str, ts_ref: int) -> float:
            day_key = _ymd_from_ts_ms(int(ts_ref))
            key = (str(symbol), str(day_key))
            if key not in earnings_cache:
                earnings_cache[key] = float(_earnings_proximity_decay(con, symbol, int(ts_ref)))
            return float(earnings_cache[key])

        def _competition_policy(symbol: str, horizon_s: int, model_name: str, regime: str) -> Dict[str, Any]:
            key = (
                str(symbol or "").upper().strip(),
                int(horizon_s or 0),
                str(model_name or "").strip(),
                str(regime or "global").strip() or "global",
            )
            if key not in competition_cache:
                competition_cache[key] = dict(
                    get_competition_policy_for_intent(
                        symbol=key[0],
                        horizon_s=int(key[1]),
                        model_name=str(key[2]),
                        regime=str(key[3]),
                    )
                    or {}
                )
            return dict(competition_cache[key])

        intents: List[Dict[str, Any]] = []
        for r in rows or []:
            try:
                (
                    rid, ts_ms, row_model_id, symbol, action,
                    from_side, to_side,
                    from_w, to_w, delta_w,
                    reason, source_alert_id, source_rule_id, explain_json,
                ) = r
            except Exception as e:
                _warn_nonfatal(
                    "portfolio_execution_intents_row_unpack_failed",
                    "PORTFOLIO_EXECUTION_INTENTS_ROW_UNPACK_FAILED",
                    e,
                    warn_key=f"portfolio_execution_intents_row_unpack_failed:{repr(r)[:96]}",
                    row_repr=repr(r),
                )
                continue

            sym = str(symbol or "").strip().upper()
            if not sym:
                continue

            ex_obj = _safe_json_loads(explain_json) if explain_json else None

            intent: Dict[str, Any] = {
                "source_order_id": int(rid),
                "ts_ms": int(ts_ms or 0),
                "model_id": _normalize_model_id(row_model_id),
                "symbol": sym,
                "action": str(action or ""),
                "from_side": str(from_side or ""),
                "to_side": str(to_side or ""),
                "from_weight": float(from_w or 0.0),
                "to_weight": float(to_w or 0.0),
                "delta_weight": float(delta_w or 0.0),
                "reason": (str(reason or "") if cols_present.get("reason") else ""),
                "source_alert_id": (int(source_alert_id) if source_alert_id is not None else None),
                "source_rule_id": (int(source_rule_id) if (cols_present.get("source_rule_id") and source_rule_id is not None) else None),
                "explain": (ex_obj if isinstance(ex_obj, dict) else None),
            }
            if not cols_present.get("model_id"):
                intent["model_id"] = _extract_model_id_from_explain(intent.get("explain"))
            intent["model_name"] = _extract_model_name_from_explain(intent.get("explain"))
            intent["model_kind"] = _extract_model_kind_from_explain(intent.get("explain"))
            intent["model_ts_ms"] = _extract_model_ts_ms_from_explain(intent.get("explain"))
            intent["model_version"] = _extract_model_version_from_explain(intent.get("explain"))
            intent["regime"] = _extract_regime_from_explain(intent.get("explain"))
            terminal_qty = _terminal_signed_qty(intent.get("explain"))
            if terminal_qty is not None:
                intent["qty"] = float(terminal_qty)
                intent["order_sizing"] = "quantity"
                intent["terminal_order"] = True

            # attach alpha meta from alert when possible
            a_id = intent.get("source_alert_id")
            if a_id is not None:
                meta = dict(alert_meta_by_id.get(int(a_id)) or {})
                sig_ts = int(meta.get("signal_ts_ms") or 0)
                horizon_s = int(meta.get("horizon_s") or 0)

                ttl_ms = int(meta.get("alpha_ttl_ms") or 0)
                if ttl_ms <= 0:
                    ttl_ms = _derive_alpha_ttl_ms(horizon_s)

                hl_ms = int(meta.get("alpha_half_life_ms") or 0)
                if hl_ms <= 0:
                    hl_ms = _derive_half_life_ms(ttl_ms)

                intent["signal_ts_ms"] = sig_ts if sig_ts > 0 else int(intent.get("ts_ms") or 0)
                intent["alpha_ttl_ms"] = int(ttl_ms)
                intent["alpha_half_life_ms"] = int(hl_ms)
                intent["horizon_s"] = int(horizon_s)

                if "volatility" in meta:
                    try:
                        intent["volatility"] = float(meta["volatility"])
                    except Exception as e:
                        _warn_nonfatal(
                            "portfolio_execution_intents_intent_volatility_parse_failed",
                            "PORTFOLIO_EXECUTION_INTENTS_INTENT_VOLATILITY_PARSE_FAILED",
                            e,
                            warn_key=f"portfolio_execution_intents_intent_volatility_parse_failed:{intent.get('source_order_id')}",
                            source_order_id=int(intent.get("source_order_id") or 0),
                            source_alert_id=int(a_id),
                            symbol=str(sym),
                        )
                if meta.get("market_regime") is not None:
                    intent["market_regime"] = str(meta.get("market_regime") or "mean_reversion")
                if isinstance(meta.get("market_regime_snapshot"), dict):
                    intent["market_regime_snapshot"] = dict(meta.get("market_regime_snapshot") or {})
                for timing_key in (
                    "source_event_ts_ms",
                    "db_observed_ts_ms",
                    "ingestion_to_db_latency_ms",
                    "db_to_prediction_latency_ms",
                    "prediction_ts_ms",
                    "prediction_to_decision_latency_ms",
                    "decision_ts_ms",
                ):
                    if meta.get(timing_key) is not None:
                        intent[timing_key] = _safe_int(meta.get(timing_key))
            else:
                intent["signal_ts_ms"] = int(intent.get("ts_ms") or 0)
                intent["horizon_s"] = int(intent.get("horizon_s") or 0)

            # ------------------------------------------------------------
            # Model-aware sizing: apply execution stress regime upstream
            # (broker_sim remains microstructure realism; sizing shifts here)
            # ------------------------------------------------------------
            ts_ref = int(intent.get("ts_ms") or batch_ts_ms or 0)
            if ts_ref <= 0:
                ts_ref = _now_ms()

            # base weight (signed as stored in portfolio_orders; preserve sign)
            try:
                base_to_w = float(intent.get("to_weight") or 0.0)
            except Exception:
                base_to_w = 0.0

            competition_policy = _competition_policy(
                symbol=sym,
                horizon_s=int(intent.get("horizon_s") or 0),
                model_name=str(intent.get("model_name") or ""),
                regime=str(intent.get("regime") or "global"),
            )
            explain_signal = dict(((intent.get("explain") or {}).get("signal") or {}))
            explain_competition = dict(explain_signal.get("competition") or {})
            explain_competition_policy = dict(explain_competition.get("policy") or {})
            if explain_competition_policy and (
                not isinstance(competition_policy, dict)
                or not competition_policy
                or str((competition_policy or {}).get("reason") or "") in {"model_identity_missing", "model_not_allocated", "no_group_allocation"}
            ):
                competition_policy = dict(explain_competition_policy)
            intent["competition"] = dict(competition_policy or {})
            requested_execution_target = str(intent.get("execution_target") or "").strip().lower()
            execution_target = "shadow" if requested_execution_target == "shadow" else "real"
            if execution_target != "shadow" and bool((competition_policy or {}).get("blocked")):
                execution_target = "shadow"
            intent["execution_target"] = str(execution_target)
            intent = force_options_shadow_intent(intent)
            execution_target = str(intent.get("execution_target") or "real").strip().lower()

            skew_z = _factor_value("options.skew_25d_z", int(ts_ref))
            flow_z = _factor_value("flows.index_constituent_imbalance_z", int(ts_ref))

            stress_mag = max(
                0.0,
                max(
                    abs(float(skew_z)) - float(_EXEC_SKEW_Z_THRESH),
                    abs(float(flow_z)) - float(_EXEC_FLOW_Z_THRESH),
                ),
            )

            stress_mult = 1.0
            if stress_mag > 0.0:
                stress_mult = float(
                    _clamp(
                        1.0 - (float(stress_mag) * float(_EXEC_STRESS_SIZE_MAX_REDUCTION)),
                        0.20,
                        1.0,
                    )
                )

            earnings_decay = _earnings_decay(sym, int(ts_ref))
            earnings_mult = 1.0
            if float(earnings_decay) > 0.0:
                earnings_mult = float(
                    _clamp(
                        1.0 - (float(earnings_decay) * float(_EXEC_EARNINGS_SIZE_MAX_REDUCTION)),
                        0.20,
                        1.0,
                    )
                )

            final_mult = float(stress_mult) * float(earnings_mult)
            capital_applied_upstream = bool(explain_competition.get("capital_applied_upstream"))
            intent["competition"]["capital_applied_upstream"] = bool(capital_applied_upstream)
            if execution_target == "real" and not capital_applied_upstream:
                final_mult *= float(
                    (competition_policy or {}).get("capital_multiplier")
                    or (competition_policy or {}).get("effective_allocation_fraction")
                    or (competition_policy or {}).get("model_weight")
                    or 1.0
                )
            try:
                from_w0 = float(intent.get("from_weight") or 0.0)
            except Exception:
                from_w0 = 0.0

            learned_alpha_adjustment: Dict[str, Any] = {
                "available": False,
                "blocked": False,
                "size_multiplier": 1.0,
                "reason": "not_evaluated",
            }
            if execution_target == "real":
                try:
                    from engine.strategy.learned_alpha_decay import portfolio_adjustment_for_intent

                    signal_age_ms = max(0, int(ts_ref) - int(intent.get("signal_ts_ms") or ts_ref))
                    learned_alpha_adjustment = portfolio_adjustment_for_intent(
                        con,
                        intent,
                        signal_age_ms=int(signal_age_ms),
                        now_ms=int(ts_ref),
                    )
                    learned_size_raw = learned_alpha_adjustment.get("size_multiplier")
                    learned_size_mult = max(
                        0.0,
                        min(1.0, float(1.0 if learned_size_raw is None else learned_size_raw)),
                    )
                    risk_increasing_intent = abs(float(base_to_w)) > abs(float(from_w0)) + 1e-12
                    if bool(learned_alpha_adjustment.get("blocked")) and bool(risk_increasing_intent):
                        final_mult = 0.0
                        intent["learned_alpha_block_reason"] = str(
                            learned_alpha_adjustment.get("reason") or "learned_alpha_blocked"
                        )
                        intent["competition"]["blocked"] = True
                        intent["competition"]["allowed"] = False
                        intent["competition"]["reason"] = str(intent["learned_alpha_block_reason"])
                    elif bool(risk_increasing_intent):
                        final_mult *= float(learned_size_mult)
                    intent["learned_alpha_decay"] = dict(learned_alpha_adjustment)
                except Exception as e:
                    _warn_nonfatal(
                        "portfolio_execution_intents_learned_alpha_failed",
                        "PORTFOLIO_EXECUTION_INTENTS_LEARNED_ALPHA_FAILED",
                        e,
                        warn_key=f"portfolio_execution_intents_learned_alpha_failed:{intent.get('source_order_id')}",
                        source_order_id=int(intent.get("source_order_id") or 0),
                        symbol=str(sym),
                    )
            final_to_w = float(base_to_w) * float(final_mult)

            if execution_target == "real" and bool((competition_policy or {}).get("blocked")):
                final_to_w = 0.0
                intent["competition_block_reason"] = str(
                    (competition_policy or {}).get("reason") or "competition_blocked"
                )

            if execution_target == "real":
                risk_limit_mult = float((competition_policy or {}).get("risk_limit_multiplier") or 1.0)
                max_abs_weight = max(0.01, abs(float(base_to_w)) * max(0.25, risk_limit_mult))
                final_to_w = _clamp(final_to_w, -max_abs_weight, max_abs_weight)

            # ------------------------------------------------------------
            # Hard capital enforcement: clamp/zero intents when the current
            # model or group budget is already exhausted.
            # ------------------------------------------------------------
            if execution_target == "real":
                group_budget_fraction = max(0.0, float((competition_policy or {}).get("group_budget_fraction") or 0.0))
                model_budget_fraction = max(0.0, float((competition_policy or {}).get("model_budget_fraction") or 0.0))
                group_key = str(
                    (competition_policy or {}).get("group_key")
                    or _competition_group_key(
                        sym,
                        int(intent.get("horizon_s") or 0),
                        str((competition_policy or {}).get("regime") or intent.get("regime") or "global"),
                    )
                )
                current_model_exposure = _current_model_exposure_fraction_from_context(
                    exposure_context,
                    model_id=str(intent.get("model_id") or ""),
                    model_name=str(intent.get("model_name") or ""),
                    regime=str((competition_policy or {}).get("regime") or intent.get("regime") or "global"),
                    equity=float(deployable_equity),
                )
                model_tracker_key = (
                    (f"id:{_normalize_model_id(intent.get('model_id'))}" if str(intent.get("model_id") or "").strip() else f"name:{str(intent.get('model_name') or '').strip()}"),
                    str((competition_policy or {}).get("regime") or intent.get("regime") or "global"),
                )
                if model_tracker_key not in projected_model_exposure:
                    projected_model_exposure[model_tracker_key] = float(current_model_exposure)
                current_model_exposure = float(projected_model_exposure.get(model_tracker_key, current_model_exposure) or 0.0)
                current_group_exposure = _current_group_exposure_fraction_from_context(
                    exposure_context,
                    symbol=str(sym),
                    horizon_s=int(intent.get("horizon_s") or 0),
                    regime=str((competition_policy or {}).get("regime") or intent.get("regime") or "global"),
                    equity=float(deployable_equity),
                )
                if group_key not in projected_group_exposure:
                    projected_group_exposure[group_key] = float(current_group_exposure)
                current_group_exposure = float(projected_group_exposure.get(group_key, current_group_exposure) or 0.0)
                intent["competition"]["current_model_exposure_fraction"] = float(current_model_exposure)
                intent["competition"]["current_group_exposure_fraction"] = float(current_group_exposure)
                intent["competition"]["deployable_equity"] = float(deployable_equity)
                intent["competition"]["group_key"] = str(group_key)

                requested_abs_weight = abs(float(final_to_w))
                from_abs_weight = abs(float(from_w0))
                remaining_group_budget = None
                remaining_model_budget = None
                allowed_abs_weight = float(requested_abs_weight)
                clamp_reasons: List[str] = []

                if group_budget_fraction > 0.0:
                    remaining_group_budget = max(
                        0.0,
                        float(group_budget_fraction) - max(0.0, float(current_group_exposure) - float(from_abs_weight)),
                    )
                    allowed_abs_weight = min(float(allowed_abs_weight), float(remaining_group_budget))
                    intent["competition"]["group_budget_fraction"] = float(group_budget_fraction)
                    intent["competition"]["remaining_group_budget_fraction"] = float(remaining_group_budget)

                if model_budget_fraction > 0.0:
                    remaining_model_budget = max(
                        0.0,
                        float(model_budget_fraction) - max(0.0, float(current_model_exposure) - float(from_abs_weight)),
                    )
                    allowed_abs_weight = min(float(allowed_abs_weight), float(remaining_model_budget))
                    intent["competition"]["remaining_budget_fraction"] = float(remaining_model_budget)
                    intent["competition"]["model_budget_fraction"] = float(model_budget_fraction)

                allowed_abs_weight = max(0.0, float(allowed_abs_weight))
                if requested_abs_weight > allowed_abs_weight + 1e-12:
                    final_sign = -1.0 if float(final_to_w) < 0.0 else 1.0
                    final_to_w = float(final_sign) * float(allowed_abs_weight)
                    if group_budget_fraction > 0.0 and remaining_group_budget is not None and allowed_abs_weight <= float(remaining_group_budget) + 1e-12:
                        clamp_reasons.append("group_budget_remaining")
                    if model_budget_fraction > 0.0 and remaining_model_budget is not None and allowed_abs_weight <= float(remaining_model_budget) + 1e-12:
                        clamp_reasons.append("model_budget_remaining")

                if allowed_abs_weight <= 1e-12 and requested_abs_weight > 1e-12:
                    intent["competition_capital_block_reason"] = "|".join(clamp_reasons or ["competition_budget_exhausted"])
                    intent["competition"]["blocked"] = True
                    intent["competition"]["allowed"] = False
                    intent["competition"]["reason"] = str(intent.get("competition_capital_block_reason") or "competition_budget_exhausted")
                elif clamp_reasons:
                    intent["competition"]["resize_reason"] = "|".join(clamp_reasons)

                projected_model_exposure[model_tracker_key] = max(
                    0.0,
                    float(current_model_exposure) - float(from_abs_weight) + abs(float(final_to_w)),
                )
                projected_group_exposure[group_key] = max(
                    0.0,
                    float(current_group_exposure) - float(from_abs_weight) + abs(float(final_to_w)),
                )

            # ------------------------------------------------------------
            # Regime-aware alpha boost
            # Calm regime => longer alpha persistence
            # Stress regime => shorter alpha persistence
            # ------------------------------------------------------------
            alpha_boost_mult = 1.0

            if stress_mag <= 0.0:
                # reward calm regime (extend TTL modestly)
                alpha_boost_mult = 1.15
            else:
                # penalize stressed regime
                alpha_boost_mult = float(
                    _clamp(
                        1.0 - (float(stress_mag) * 0.25),
                        0.60,
                        1.0,
                    )
                )

            # Adjust alpha TTL and half-life if present
            try:
                ttl0 = int(intent.get("alpha_ttl_ms") or 0)
                hl0 = int(intent.get("alpha_half_life_ms") or 0)

                if ttl0 > 0:
                    intent["alpha_ttl_ms"] = int(float(ttl0) * float(alpha_boost_mult))

                if hl0 > 0:
                    intent["alpha_half_life_ms"] = int(float(hl0) * float(alpha_boost_mult))
            except Exception as e:
                _warn_nonfatal(
                    "portfolio_execution_intents_alpha_boost_apply_failed",
                    "PORTFOLIO_EXECUTION_INTENTS_ALPHA_BOOST_APPLY_FAILED",
                    e,
                    warn_key=f"portfolio_execution_intents_alpha_boost_apply_failed:{intent.get('source_order_id')}",
                    source_order_id=int(intent.get("source_order_id") or 0),
                    symbol=str(sym),
                )

            # update intent weights (and keep delta consistent). Manual
            # terminal orders carry explicit quantity, so keep the weight fields
            # neutral and let broker adapters use `qty` as the source of truth.
            if str(intent.get("order_sizing") or "").strip().lower() == "quantity" and intent.get("qty") is not None:
                intent["to_weight"] = 0.0
                intent["delta_weight"] = 0.0
            else:
                intent["to_weight"] = float(final_to_w)
                intent["delta_weight"] = float(final_to_w) - float(from_w0)

            # attach regime context for auditing + downstream attribution
            intent["exec_regime"] = {
                "ts_ref": int(ts_ref),
                "skew_z": float(skew_z),
                "flow_z": float(flow_z),
                "stress_mag": float(stress_mag),
                "stress_mult": float(stress_mult),
                "earnings_decay": float(earnings_decay),
                "earnings_mult": float(earnings_mult),
                "final_mult": float(final_mult),
                "learned_alpha": dict(learned_alpha_adjustment),
            }

            intents.append(intent)

        shadow_intents = _load_pending_shadow_execution_intents(
            con,
            ts_ref_ms=int(batch_ts_ms or _now_ms()),
            max_rows=max_rows,
        )
        if shadow_intents:
            intents.extend(list(shadow_intents))
            shadow_ts_ms = max(int(o.get("ts_ms") or 0) for o in shadow_intents)
            if batch_ts_ms is None or int(shadow_ts_ms) > int(batch_ts_ms or 0):
                batch_ts_ms = int(shadow_ts_ms)

        if not intents:
            return {"ok": True, "batch_id": None, "batch_ts_ms": None, "intents": []}

        group_realized_gross: Dict[str, float] = {}
        group_caps: Dict[str, float] = {}
        for intent in intents:
            if str(intent.get("execution_target") or "real") != "real":
                continue
            competition = dict(intent.get("competition") or {})
            group_key = str(
                competition.get("group_key")
                or "|".join(
                    [
                        str(intent.get("symbol") or "").upper().strip(),
                        str(int(intent.get("horizon_s") or 0)),
                        str(intent.get("regime") or "global"),
                    ]
                )
            )
            group_realized_gross[group_key] = group_realized_gross.get(group_key, 0.0) + abs(
                float(intent.get("to_weight") or 0.0)
            )
            group_caps[group_key] = max(
                0.0,
                float(competition.get("group_budget_fraction") or 0.0),
            )

        for intent in intents:
            if str(intent.get("execution_target") or "real") != "real":
                continue
            competition = dict(intent.get("competition") or {})
            group_key = str(
                competition.get("group_key")
                or "|".join(
                    [
                        str(intent.get("symbol") or "").upper().strip(),
                        str(int(intent.get("horizon_s") or 0)),
                        str(intent.get("regime") or "global"),
                    ]
                )
            )
            gross = float(group_realized_gross.get(group_key) or 0.0)
            cap = float(group_caps.get(group_key) or 0.0)
            if cap <= 0.0 or gross <= cap or gross <= 1e-12:
                continue
            scale = float(cap) / float(gross)
            _scale_intent_to_weight(intent, scale)
            intent["competition"]["group_scale"] = float(scale)
            intent["competition"]["group_realized_gross"] = float(gross)
            intent["competition"]["group_cap_enforced"] = float(cap)

        total_real_gross = sum(
            abs(float(intent.get("to_weight") or 0.0))
            for intent in intents
            if str(intent.get("execution_target") or "real") == "real"
        )
        total_cap = max(0.0, min(1.0, float(_COMPETITION_TOTAL_CAPITAL_FRACTION)))
        if total_cap > 0.0 and total_real_gross > total_cap and total_real_gross > 1e-12:
            total_scale = float(total_cap) / float(total_real_gross)
            for intent in intents:
                if str(intent.get("execution_target") or "real") != "real":
                    continue
                _scale_intent_to_weight(intent, total_scale)
                intent["competition"]["portfolio_scale"] = float(total_scale)
                intent["competition"]["portfolio_realized_gross"] = float(total_real_gross)
                intent["competition"]["portfolio_cap_enforced"] = float(total_cap)

        intents, shadowed_intents, decision_summary = _apply_decision_gate(
            intents,
            exposure_context=exposure_context,
        )

        return {
            "ok": True,
            "batch_id": batch_id,
            "batch_ts_ms": batch_ts_ms,
            "intents": intents,
            "shadowed_intents": shadowed_intents,
            "decision_summary": decision_summary,
        }

    return cache_get_or_load("portfolio_orders", cache_key, _load, ttl_s=0.75)
