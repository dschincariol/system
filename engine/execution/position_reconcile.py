# FILE: dev_core/position_reconcile.py
# NEW FILE (CREATE)

"""
Pre-Live Position Reconciliation Gate

Purpose:
- Before sending LIVE orders, reconcile broker positions vs a stored baseline.
- If mismatch exceeds tolerances, trip kill-switch (global) and block execution.

Env:
  EXECUTION_PRELIVE_RECONCILE=1              (default 1)
  EXECUTION_RECONCILE_REQUIRE_BASELINE=1     (default 1)  -> if no baseline, block unless allow bootstrap
  EXECUTION_RECONCILE_ALLOW_BOOTSTRAP=0      (default 0)  -> if 1 and no baseline, create confirmed baseline
  TS_RECONCILE_BOOTSTRAP_TOKEN               expected operator bootstrap token
  TS_RECONCILE_BOOTSTRAP_CONFIRM             operator confirmation token for this bootstrap
  POSITION_RECONCILE_FETCH_FAILURE_HALT_THRESHOLD=3
  POSITION_RECONCILE_FETCH_FAILURE_COOLDOWN_MS=300000

Tolerances:
  POSITION_RECONCILE_QTY_TOL=0.01            (absolute qty tolerance per symbol)
  POSITION_RECONCILE_IGNORE_QTY_LT=0.001     (ignore tiny positions)
  POSITION_RECONCILE_MAX_MISMATCHED=0        (max mismatched symbols allowed)
"""

import hashlib
import hmac
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from engine.audit.chain import append_chain_row
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.execution.kill_switch import set_kill_switch

LOG = get_logger("engine.execution.position_reconcile")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.position_reconcile",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_f(x, d: float = 0.0) -> float:
    try:
        v = float(x)
        if v == v:
            return float(v)
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_FLOAT_PARSE_FAILED",
            e,
            once_key="safe_float",
            value_repr=repr(x),
        )
    return float(d)


def _safe_int(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_INT_PARSE_FAILED",
            e,
            once_key=f"safe_int:{repr(x)[:80]}",
            value_repr=repr(x),
        )
    return int(d)


def _safe_int_env(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(str(name))
    try:
        value = int(str(raw if raw is not None else default).strip())
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_INT_ENV_PARSE_FAILED",
            e,
            once_key=f"int_env:{name}",
            env_name=str(name),
            value_repr=repr(raw),
            default=int(default),
        )
        value = int(default)
    return max(int(minimum), int(value))


def _safe_float_env(name: str, default: float, *, minimum: float = 0.0) -> float:
    raw = os.environ.get(str(name))
    try:
        value = float(str(raw if raw is not None else default).strip())
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_FLOAT_ENV_PARSE_FAILED",
            e,
            once_key=f"float_env:{name}",
            env_name=str(name),
            value_repr=repr(raw),
            default=float(default),
        )
        value = float(default)
    return max(float(minimum), float(value))


def _fetch_failure_halt_config() -> Tuple[int, int]:
    threshold = _safe_int_env("POSITION_RECONCILE_FETCH_FAILURE_HALT_THRESHOLD", 3, minimum=1)
    raw_ms = os.environ.get("POSITION_RECONCILE_FETCH_FAILURE_COOLDOWN_MS")
    if raw_ms is not None:
        cooldown_ms = _safe_int_env("POSITION_RECONCILE_FETCH_FAILURE_COOLDOWN_MS", 300_000, minimum=0)
    else:
        cooldown_s = _safe_int_env("POSITION_RECONCILE_FETCH_FAILURE_COOLDOWN_S", 300, minimum=0)
        cooldown_ms = int(cooldown_s) * 1000
    return int(threshold), int(cooldown_ms)


def _norm_positions(positions: List[Dict[str, Any]], ignore_lt: float) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for p in positions or []:
        try:
            sym = str(p.get("symbol") or "").strip().upper()
            if not sym:
                continue
            qty = _safe_f(p.get("qty"), 0.0)
            if abs(qty) < float(ignore_lt):
                continue
            out[sym] = float(qty)
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_POSITION_PARSE_FAILED",
                e,
                once_key=f"norm_positions:{repr(p)}",
                position=repr(p),
            )
            continue
    return out


def _normalize_mode(value: Any = None) -> str:
    return str(value if value is not None else os.environ.get("ENGINE_MODE", "safe") or "safe").strip().lower() or "safe"


def configured_position_reconcile_broker(*, engine_mode: Any = None, broker: Optional[str] = None) -> str:
    if broker is not None and str(broker).strip():
        raw = str(broker).strip()
    else:
        mode = _normalize_mode(engine_mode)
        raw = ""
        try:
            from engine.execution.broker_failover_policy import configured_failover_chain

            chain = [str(item or "").strip() for item in list(configured_failover_chain() or []) if str(item or "").strip()]
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_BROKER_CHAIN_FAILED",
                e,
                once_key="broker_chain",
            )
            chain = []
        if mode == "live":
            raw = (
                str(os.environ.get("LIVE_BROKER", "") or "").strip()
                or str(os.environ.get("BROKER", "") or "").strip()
                or str(os.environ.get("BROKER_NAME", "") or "").strip()
                or next((item for item in chain if item not in {"sim", "paper", "sandbox"}), "")
            )
        elif mode == "paper":
            raw = (
                str(os.environ.get("BROKER", "") or "").strip()
                or str(os.environ.get("BROKER_NAME", "") or "").strip()
                or (chain[0] if chain else "")
                or "paper"
            )
        else:
            raw = (
                str(os.environ.get("BROKER", "") or "").strip()
                or str(os.environ.get("BROKER_NAME", "") or "").strip()
                or (chain[0] if chain else "")
            )
    try:
        from engine.execution.broker_failover_policy import canonical_broker_name

        return canonical_broker_name(raw)
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_BROKER_CANONICALIZE_FAILED",
            e,
            once_key=f"broker_canonical:{raw}",
            broker=str(raw),
        )
        return str(raw or "").strip().lower()


def _ensure_schema(con) -> None:
    # Baseline and audit are persisted so pre-live reconciliation remains
    # deterministic across restarts and operator investigations.
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS position_reconcile_baseline (
            broker TEXT PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            positions_json TEXT NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS position_reconcile_state (
            broker TEXT PRIMARY KEY,
            re_reconcile_pending INTEGER NOT NULL DEFAULT 0,
            pending_since_ts_ms INTEGER,
            updated_ts_ms INTEGER NOT NULL,
            fetch_failure_count INTEGER NOT NULL DEFAULT 0,
            fetch_failure_first_ts_ms INTEGER,
            fetch_failure_last_ts_ms INTEGER,
            fetch_failure_halt_tripped INTEGER NOT NULL DEFAULT 0,
            detail_json TEXT
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS position_reconcile_bootstrap_audit (
            id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            broker TEXT NOT NULL,
            actor TEXT NOT NULL,
            status TEXT NOT NULL,
            token_hash TEXT,
            positions_json TEXT NOT NULL,
            detail_json TEXT,
            prev_hash BLOB,
            row_hash BLOB NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_position_reconcile_bootstrap_audit_broker_ts
        ON position_reconcile_bootstrap_audit(broker, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS position_reconcile_audit (
            ts_ms INTEGER PRIMARY KEY,
            broker TEXT NOT NULL,
            ok INTEGER NOT NULL,
            status TEXT NOT NULL,
            mismatched_n INTEGER NOT NULL DEFAULT 0,
            max_abs_qty_diff REAL NOT NULL DEFAULT 0,
            total_abs_qty_diff REAL NOT NULL DEFAULT 0,
            detail_json TEXT,
            prev_hash BLOB,
            row_hash BLOB NOT NULL
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_position_reconcile_audit_broker_ts
        ON position_reconcile_audit(broker, ts_ms DESC)
        """
    )
    for column_name, ddl in (("prev_hash", "BLOB"), ("row_hash", "BLOB")):
        if not _has_column(con, "position_reconcile_audit", column_name):
            try:
                con.execute(f"ALTER TABLE position_reconcile_audit ADD COLUMN {column_name} {ddl}")
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_AUDIT_MIGRATION_FAILED",
                    e,
                    once_key=f"audit_column:{column_name}",
                    column=str(column_name),
                )
        if not _has_column(con, "position_reconcile_bootstrap_audit", column_name):
            try:
                con.execute(f"ALTER TABLE position_reconcile_bootstrap_audit ADD COLUMN {column_name} {ddl}")
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_BOOTSTRAP_AUDIT_MIGRATION_FAILED",
                    e,
                    once_key=f"bootstrap_audit_column:{column_name}",
                    column=str(column_name),
                )
    for column_name, ddl in (
        ("fetch_failure_count", "INTEGER NOT NULL DEFAULT 0"),
        ("fetch_failure_first_ts_ms", "INTEGER"),
        ("fetch_failure_last_ts_ms", "INTEGER"),
        ("fetch_failure_halt_tripped", "INTEGER NOT NULL DEFAULT 0"),
    ):
        if not _has_column(con, "position_reconcile_state", column_name):
            try:
                con.execute(f"ALTER TABLE position_reconcile_state ADD COLUMN {column_name} {ddl}")
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_STATE_MIGRATION_FAILED",
                    e,
                    once_key=f"state_column:{column_name}",
                    column=str(column_name),
                )


def _has_column(con, table_name: str, column_name: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_COLUMN_LOOKUP_FAILED",
            e,
            once_key=f"column_lookup:{table_name}:{column_name}",
            table=str(table_name),
            column=str(column_name),
        )
        return False
    target = str(column_name or "").strip().lower()
    return any(str(row[1] or "").strip().lower() == target for row in rows if row and len(row) > 1)


def _json_dict_or_empty(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_JSON_DICT_PARSE_FAILED",
            e,
            once_key=f"json_dict:{text[:80]}",
            raw_preview=text[:120],
        )
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _json_list_or_empty(raw: Any) -> List[Any]:
    if isinstance(raw, list):
        return list(raw)
    text = str(raw or "").strip()
    if not text:
        return []
    try:
        payload = json.loads(text)
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_JSON_LIST_PARSE_FAILED",
            e,
            once_key=f"json_list:{text[:80]}",
            raw_preview=text[:120],
        )
        return []
    return list(payload) if isinstance(payload, list) else []


def _dedupe_strs(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for value in values or []:
        item = str(value or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _append_reconcile_audit(
    con,
    *,
    ts_ms: int,
    broker: str,
    ok: bool,
    status: str,
    mismatched_n: int,
    max_abs_qty_diff: float,
    total_abs_qty_diff: float,
    detail: Dict[str, Any],
) -> None:
    append_chain_row(
        "position_reconcile_audit",
        {
            "ts_ms": int(ts_ms),
            "broker": str(broker),
            "ok": 1 if ok else 0,
            "status": str(status),
            "mismatched_n": int(mismatched_n),
            "max_abs_qty_diff": float(max_abs_qty_diff),
            "total_abs_qty_diff": float(total_abs_qty_diff),
            "detail_json": dict(detail or {}),
        },
        con,
    )


def _bootstrap_actor() -> str:
    return str(
        os.environ.get("TS_RECONCILE_BOOTSTRAP_ACTOR")
        or os.environ.get("USER")
        or os.environ.get("USERNAME")
        or "unknown"
    ).strip() or "unknown"


def _hash_token(token: str) -> str:
    return hashlib.sha256(str(token or "").encode("utf-8")).hexdigest()


def _bootstrap_confirmation() -> Tuple[bool, str, str, Optional[str]]:
    expected = str(os.environ.get("TS_RECONCILE_BOOTSTRAP_TOKEN") or "").strip()
    provided = str(
        os.environ.get("TS_RECONCILE_BOOTSTRAP_CONFIRM")
        or os.environ.get("EXECUTION_RECONCILE_BOOTSTRAP_TOKEN")
        or ""
    ).strip()
    actor = _bootstrap_actor()
    if not expected:
        return False, "bootstrap_token_missing", actor, None
    if not provided:
        return False, "bootstrap_confirmation_missing", actor, _hash_token(expected)
    if not hmac.compare_digest(expected, provided):
        return False, "bootstrap_confirmation_mismatch", actor, _hash_token(expected)
    return True, "confirmed", actor, _hash_token(expected)


def _append_bootstrap_audit(
    con,
    *,
    ts_ms: int,
    broker: str,
    actor: str,
    status: str,
    token_hash: Optional[str],
    positions: Dict[str, float],
    detail: Dict[str, Any],
) -> None:
    append_chain_row(
        "position_reconcile_bootstrap_audit",
        {
            "ts_ms": int(ts_ms),
            "broker": str(broker),
            "actor": str(actor or "unknown"),
            "status": str(status),
            "token_hash": str(token_hash) if token_hash else None,
            "positions_json": dict(positions or {}),
            "detail_json": dict(detail or {}),
        },
        con,
    )


def _load_re_reconcile_pending(con, broker: str) -> bool:
    row = con.execute(
        """
        SELECT re_reconcile_pending
        FROM position_reconcile_state
        WHERE broker=?
        LIMIT 1
        """,
        (str(broker),),
    ).fetchone()
    return bool(row and int(row[0] or 0) == 1)


def _set_re_reconcile_pending(
    con,
    *,
    broker: str,
    pending: bool,
    ts_ms: int,
    detail: Dict[str, Any],
) -> None:
    con.execute(
        """
        INSERT INTO position_reconcile_state(
            broker, re_reconcile_pending, pending_since_ts_ms, updated_ts_ms, detail_json
        )
        VALUES(?,?,?,?,?)
        ON CONFLICT(broker) DO UPDATE SET
          re_reconcile_pending=excluded.re_reconcile_pending,
          pending_since_ts_ms=excluded.pending_since_ts_ms,
          updated_ts_ms=excluded.updated_ts_ms,
          detail_json=excluded.detail_json
        """,
        (
            str(broker),
            1 if pending else 0,
            int(ts_ms) if pending else None,
            int(ts_ms),
            json.dumps(detail or {}, separators=(",", ":"), sort_keys=True),
        ),
    )


def _load_fetch_failure_state(con, broker: str) -> Dict[str, int]:
    row = con.execute(
        """
        SELECT fetch_failure_count, fetch_failure_first_ts_ms,
               fetch_failure_last_ts_ms, fetch_failure_halt_tripped
        FROM position_reconcile_state
        WHERE broker=?
        LIMIT 1
        """,
        (str(broker),),
    ).fetchone()
    if not row:
        return {
            "count": 0,
            "first_ts_ms": 0,
            "last_ts_ms": 0,
            "halt_tripped": 0,
        }
    return {
        "count": int(row[0] or 0),
        "first_ts_ms": int(row[1] or 0),
        "last_ts_ms": int(row[2] or 0),
        "halt_tripped": int(row[3] or 0),
    }


def _record_fetch_failure(
    con,
    *,
    broker: str,
    ts_ms: int,
    error: str,
    threshold: int,
    cooldown_ms: int,
) -> Dict[str, Any]:
    prior = _load_fetch_failure_state(con, str(broker))
    failure_count = int(prior.get("count") or 0) + 1
    first_ts_ms = int(prior.get("first_ts_ms") or 0) or int(ts_ms)
    elapsed_ms = max(0, int(ts_ms) - int(first_ts_ms))
    threshold_reached = failure_count >= int(threshold)
    cooldown_exceeded = bool(int(cooldown_ms) > 0 and elapsed_ms >= int(cooldown_ms))
    persistent_halt = bool(threshold_reached or cooldown_exceeded)
    detail = {
        "error": str(error),
        "fetch_failure_count": int(failure_count),
        "fetch_failure_first_ts_ms": int(first_ts_ms),
        "fetch_failure_last_ts_ms": int(ts_ms),
        "fetch_failure_elapsed_ms": int(elapsed_ms),
        "fetch_failure_halt_threshold": int(threshold),
        "fetch_failure_cooldown_ms": int(cooldown_ms),
        "fetch_failure_threshold_reached": bool(threshold_reached),
        "fetch_failure_cooldown_exceeded": bool(cooldown_exceeded),
        "persistent_halt": bool(persistent_halt),
    }
    con.execute(
        """
        INSERT INTO position_reconcile_state(
            broker, re_reconcile_pending, pending_since_ts_ms, updated_ts_ms,
            fetch_failure_count, fetch_failure_first_ts_ms,
            fetch_failure_last_ts_ms, fetch_failure_halt_tripped, detail_json
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(broker) DO UPDATE SET
          updated_ts_ms=excluded.updated_ts_ms,
          fetch_failure_count=excluded.fetch_failure_count,
          fetch_failure_first_ts_ms=excluded.fetch_failure_first_ts_ms,
          fetch_failure_last_ts_ms=excluded.fetch_failure_last_ts_ms,
          fetch_failure_halt_tripped=excluded.fetch_failure_halt_tripped,
          detail_json=excluded.detail_json
        """,
        (
            str(broker),
            0,
            None,
            int(ts_ms),
            int(failure_count),
            int(first_ts_ms),
            int(ts_ms),
            1 if persistent_halt else int(prior.get("halt_tripped") or 0),
            json.dumps(detail, separators=(",", ":"), sort_keys=True),
        ),
    )
    return detail


def _reset_fetch_failure_state(con, *, broker: str, ts_ms: int) -> None:
    con.execute(
        """
        INSERT INTO position_reconcile_state(
            broker, re_reconcile_pending, pending_since_ts_ms, updated_ts_ms,
            fetch_failure_count, fetch_failure_first_ts_ms,
            fetch_failure_last_ts_ms, fetch_failure_halt_tripped, detail_json
        )
        VALUES(?,?,?,?,?,?,?,?,?)
        ON CONFLICT(broker) DO UPDATE SET
          updated_ts_ms=excluded.updated_ts_ms,
          fetch_failure_count=0,
          fetch_failure_first_ts_ms=NULL,
          fetch_failure_last_ts_ms=NULL,
          fetch_failure_halt_tripped=0,
          detail_json=excluded.detail_json
        """,
        (
            str(broker),
            0,
            None,
            int(ts_ms),
            0,
            None,
            None,
            0,
            json.dumps({"status": "positions_fetch_succeeded"}, separators=(",", ":"), sort_keys=True),
        ),
    )


def _compare_position_maps(
    broker_map: Dict[str, float],
    baseline_map: Dict[str, float],
    *,
    qty_tol: float,
) -> Tuple[List[Dict[str, Any]], float, float]:
    keys = set((broker_map or {}).keys()) | set((baseline_map or {}).keys())
    mismatched: List[Dict[str, float]] = []
    total_abs = 0.0
    max_abs = 0.0
    for sym in sorted(keys):
        broker_has = sym in (broker_map or {})
        expected_has = sym in (baseline_map or {})
        bq = _safe_f((broker_map or {}).get(sym), 0.0)
        eq = _safe_f((baseline_map or {}).get(sym), 0.0)
        d = float(bq - eq)
        ad = abs(d)
        if ad <= float(qty_tol):
            continue
        if broker_has and not expected_has:
            mismatch_type = "broker_orphan"
        elif expected_has and not broker_has:
            mismatch_type = "expected_orphan"
        else:
            mismatch_type = "quantity_mismatch"
        mismatched.append(
            {
                "symbol": str(sym),
                "broker_qty": bq,
                "expected_qty": eq,
                "diff_qty": d,
                "mismatch_type": mismatch_type,
            }
        )
        total_abs += float(ad)
        if ad > max_abs:
            max_abs = float(ad)
    return mismatched, float(total_abs), float(max_abs)


def _mismatch_summary(mismatched: List[Dict[str, Any]]) -> Dict[str, int]:
    broker_orphan_n = 0
    expected_orphan_n = 0
    quantity_mismatch_n = 0
    for item in list(mismatched or []):
        row = dict(item or {})
        kind = str(row.get("mismatch_type") or "").strip().lower()
        if not kind:
            broker_qty = _safe_f(row.get("broker_qty"), 0.0)
            expected_qty = _safe_f(row.get("expected_qty"), 0.0)
            if abs(broker_qty) > 0 and abs(expected_qty) <= 0:
                kind = "broker_orphan"
            elif abs(expected_qty) > 0 and abs(broker_qty) <= 0:
                kind = "expected_orphan"
            else:
                kind = "quantity_mismatch"
        if kind == "broker_orphan":
            broker_orphan_n += 1
        elif kind == "expected_orphan":
            expected_orphan_n += 1
        else:
            quantity_mismatch_n += 1
    return {
        "broker_orphan_n": int(broker_orphan_n),
        "expected_orphan_n": int(expected_orphan_n),
        "quantity_mismatch_n": int(quantity_mismatch_n),
        "orphan_position_n": int(broker_orphan_n + expected_orphan_n),
    }


def _reconcile_required_for_mode(mode: Any) -> bool:
    return _normalize_mode(mode) in {"paper", "live"}


def _position_reconcile_max_age_s() -> float:
    return _safe_float_env("POSITION_RECONCILE_EVIDENCE_MAX_AGE_S", 900.0, minimum=0.0)


def position_reconcile_evidence_snapshot(
    *,
    engine_mode: Any = None,
    broker: Optional[str] = None,
    con=None,
    now_ms: Optional[int] = None,
    max_age_s: Optional[float] = None,
) -> Dict[str, Any]:
    """Return the latest persisted broker-position reconcile evidence.

    This function intentionally does not call broker APIs or write rows. It is
    the read-only contract used by startup/readiness/preflight gates.
    """

    mode = _normalize_mode(engine_mode)
    required = _reconcile_required_for_mode(mode)
    resolved_broker = (
        configured_position_reconcile_broker(engine_mode=mode, broker=broker)
        if (required or (broker is not None and str(broker).strip()))
        else ""
    )
    ts_ms = int(now_ms if now_ms is not None else _now_ms())
    max_age = float(max_age_s if max_age_s is not None else _position_reconcile_max_age_s())
    owns = False
    if con is None:
        try:
            con = connect(readonly=True)
        except TypeError:
            con = connect()
        owns = True

    out: Dict[str, Any] = {
        "ok": not required,
        "required": bool(required),
        "mode": mode,
        "broker": resolved_broker,
        "available": False,
        "exercised": False,
        "fresh": False,
        "stale": False,
        "fatal_reconcile": False,
        "status": "unavailable",
        "reason": "ok" if not required else "position_reconcile_not_exercised",
        "blockers": ([] if not required else ["position_reconcile_not_exercised"]),
        "updated_ts_ms": None,
        "age_s": None,
        "max_age_s": float(max_age),
        "mismatched_n": 0,
        "max_abs_qty_diff": 0.0,
        "total_abs_qty_diff": 0.0,
        "broker_orphan_n": 0,
        "expected_orphan_n": 0,
        "orphan_position_n": 0,
        "quantity_mismatch_n": 0,
        "detail": "not_required" if not required else "position_reconcile_audit_missing",
        "detail_json": {},
    }

    if required and not resolved_broker:
        out["reason"] = "position_reconcile_broker_unknown"
        out["blockers"] = ["position_reconcile_broker_unknown"]
        out["detail"] = "position_reconcile_broker_unknown"
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_EVIDENCE_CLOSE_FAILED",
                    e,
                    once_key="evidence_close_unknown_broker",
                )
        return out

    query_failed = False
    try:
        params: Tuple[Any, ...]
        where = ""
        if resolved_broker:
            where = "WHERE broker=?"
            params = (str(resolved_broker),)
        else:
            params = ()
        row = con.execute(
            f"""
            SELECT ts_ms, broker, ok, status, mismatched_n, max_abs_qty_diff, total_abs_qty_diff, detail_json
            FROM position_reconcile_audit
            {where}
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
    except Exception as e:
        detail = f"position_reconcile_query_failed:{type(e).__name__}:{e}"
        out.update(
            {
                "ok": not required,
                "available": False,
                "exercised": False,
                "fresh": False,
                "stale": False,
                "fatal_reconcile": bool(required),
                "status": "query_failed",
                "reason": "ok" if not required else "position_reconcile_not_exercised",
                "blockers": ([] if not required else ["position_reconcile_not_exercised"]),
                "detail": detail,
            }
        )
        query_failed = True
    finally:
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_EVIDENCE_CLOSE_FAILED",
                    e,
                    once_key="evidence_close",
                )

    if query_failed:
        return out

    out["available"] = True
    if not row:
        out.update(
            {
                "ok": not required,
                "exercised": False,
                "fresh": False,
                "stale": False,
                "fatal_reconcile": bool(required),
                "status": "empty",
                "reason": "ok" if not required else "position_reconcile_not_exercised",
                "blockers": ([] if not required else ["position_reconcile_not_exercised"]),
                "detail": "position_reconcile_audit_empty",
            }
        )
        return out

    updated_ts_ms = _safe_int(row[0])
    row_broker = str(row[1] or "").strip()
    row_ok = bool(row[2])
    status = str(row[3] or "").strip() or ("ok" if row_ok else "unknown")
    detail_json = _json_dict_or_empty(row[7])
    mismatched_raw = detail_json.get("mismatched")
    mismatched = [dict(item or {}) for item in _json_list_or_empty(mismatched_raw) if isinstance(item, dict)]
    if not mismatched and isinstance(mismatched_raw, list):
        mismatched = [dict(item or {}) for item in mismatched_raw if isinstance(item, dict)]
    summary = _mismatch_summary(mismatched)
    mismatched_n = _safe_int(row[4])
    age_s = round(max(0, ts_ms - int(updated_ts_ms)) / 1000.0, 1) if updated_ts_ms > 0 else None
    stale = bool(required and (updated_ts_ms <= 0 or (age_s is not None and float(age_s) > float(max_age))))
    pending = bool(
        detail_json.get("re_reconcile_pending")
        or status.endswith("_re_reconcile_pending")
        or status in {"baseline_bootstrapped_re_reconcile_pending", "baseline_created_re_reconcile_pending"}
    )
    fatal = bool((not row_ok) or detail_json.get("fatal_reconcile"))
    blockers: List[str] = []
    if required:
        if stale:
            blockers.append("position_reconcile_stale")
        if pending:
            blockers.append("position_reconcile_recheck_pending")
        if fatal:
            if int(summary.get("orphan_position_n") or 0) > 0:
                blockers.append("position_reconcile_orphan_positions")
            if mismatched_n > 0:
                blockers.append("position_reconcile_mismatched_positions")
            blockers.append("position_reconcile_unhealthy")

    blockers = _dedupe_strs(blockers)
    out.update(
        {
            "ok": not blockers,
            "available": True,
            "exercised": True,
            "fresh": bool(not stale),
            "stale": bool(stale),
            "fatal_reconcile": bool(fatal),
            "status": status,
            "reason": "ok" if not blockers else blockers[0],
            "blockers": blockers,
            "broker": row_broker or resolved_broker,
            "updated_ts_ms": (int(updated_ts_ms) if updated_ts_ms > 0 else None),
            "age_s": age_s,
            "mismatched_n": int(mismatched_n),
            "max_abs_qty_diff": _safe_f(row[5], 0.0),
            "total_abs_qty_diff": _safe_f(row[6], 0.0),
            "broker_orphan_n": int(summary.get("broker_orphan_n") or 0),
            "expected_orphan_n": int(summary.get("expected_orphan_n") or 0),
            "orphan_position_n": int(summary.get("orphan_position_n") or 0),
            "quantity_mismatch_n": int(summary.get("quantity_mismatch_n") or 0),
            "detail": "ok" if not blockers else blockers[0],
            "detail_json": detail_json,
        }
    )
    return out


def _load_baseline(con, broker: str) -> Optional[Dict[str, float]]:
    if not bool(getattr(con, "in_transaction", False)):
        try:
            from engine.cache.wrappers.position_baseline import read_positions

            cached = read_positions(str(broker))
            if cached is not None:
                return dict(cached)
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_BASELINE_CACHE_READ_FAILED",
                e,
                once_key=f"baseline_cache_read:{broker}",
                broker=str(broker),
            )
    r = con.execute(
        "SELECT positions_json FROM position_reconcile_baseline WHERE broker=?",
        (str(broker),),
    ).fetchone()
    if not r:
        return None
    try:
        raw = json.loads(r[0] or "{}")
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        return None
    out: Dict[str, float] = {}
    for k, v in raw.items():
        try:
            sym = str(k).strip().upper()
            qty = _safe_f(v, 0.0)
            out[sym] = float(qty)
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_BASELINE_PARSE_FAILED",
                e,
                once_key=f"baseline_parse:{k!r}",
                symbol=str(k),
            )
            continue
    return out


def _save_baseline(con, broker: str, ts_ms: int, pos_map: Dict[str, float]) -> None:
    try:
        from engine.cache.wrappers.position_baseline import set_position_baseline

        set_position_baseline(str(broker), dict(pos_map or {}), ts_ms=int(ts_ms), con=con)
        return
    except Exception as e:
        _warn_nonfatal(
            "POSITION_RECONCILE_BASELINE_CACHE_WRITE_FAILED",
            e,
            once_key=f"baseline_cache_write:{broker}",
            broker=str(broker),
        )
    con.execute(
        """
        INSERT INTO position_reconcile_baseline(broker, ts_ms, positions_json)
        VALUES(?,?,?)
        ON CONFLICT(broker) DO UPDATE SET
          ts_ms=excluded.ts_ms,
          positions_json=excluded.positions_json
        """,
        (
            str(broker),
            int(ts_ms),
            json.dumps(pos_map or {}, separators=(",", ":"), sort_keys=True),
        ),
    )


def _begin_owned_write(con) -> bool:
    if bool(getattr(con, "in_transaction", False)):
        return False
    con.begin_managed_write()
    return True


def _broker_positions(broker: str) -> Tuple[bool, str, List[Dict[str, Any]]]:
    b = str(broker or "").lower().strip()

    if b in ("alpaca", "alpaca_rest"):
        try:
            from engine.execution.broker_alpaca_rest import get_positions
            res = get_positions() or []
            out = [{"symbol": str(x.get("symbol") or "").upper(), "qty": float(x.get("qty") or x.get("quantity") or x.get("qty_available") or x.get("qty_long") or x.get("qty_short") or x.get("qty", 0) or 0.0)} for x in []]  # never used
            # Normalize Alpaca format
            norm = []
            for x in (res or []):
                sym = str(x.get("symbol") or "").upper().strip()
                if not sym:
                    continue
                try:
                    q = float(x.get("qty") or 0.0)
                except Exception:
                    q = 0.0
                norm.append({"symbol": sym, "qty": q})
            return True, "ok", norm
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_ALPACA_POSITIONS_FAILED",
                e,
                once_key="alpaca_positions",
            )
            return False, f"alpaca_positions_error:{e}", []

    if b in ("ibkr", "interactivebrokers", "interactive_brokers", "ib_gateway", "ibgateway", "tws"):
        try:
            from engine.execution.broker_ibkr_gateway import get_positions_live
            res = get_positions_live() or []
            return True, "ok", list(res or [])
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_IBKR_POSITIONS_FAILED",
                e,
                once_key="ibkr_positions",
            )
            return False, f"ibkr_positions_error:{e}", []

    if b in ("sim", "paper", "sandbox"):
        # Optional: best-effort from broker_sim tables
        try:
            con = connect()
            try:
                rows = con.execute(
                    "SELECT symbol, qty FROM broker_positions"
                ).fetchall() or []
                out = [{"symbol": str(r[0]).upper().strip(), "qty": _safe_f(r[1], 0.0)} for r in rows if r and r[0]]
                return True, "ok", out
            finally:
                con.close()
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_SIM_POSITIONS_FAILED",
                e,
                once_key="sim_positions",
            )
            return False, f"sim_positions_error:{e}", []

    return False, "unknown_broker_for_positions", []


def pre_live_position_reconcile(
    broker: str,
    *,
    con=None,
) -> Dict[str, Any]:
    """
    Returns dict:
      { ok, status, broker, mismatched_n, max_abs_qty_diff, total_abs_qty_diff, detail, fatal_reconcile }
    """
    # This gate is specifically for live execution safety. If disabled, the
    # caller gets an explicit skipped result rather than implicit success.
    enabled = os.environ.get("EXECUTION_PRELIVE_RECONCILE", "1") == "1"
    if not enabled:
        return {"ok": True, "status": "skipped_disabled", "broker": str(broker), "fatal_reconcile": False}

    require_baseline = os.environ.get("EXECUTION_RECONCILE_REQUIRE_BASELINE", "1") == "1"
    allow_bootstrap = os.environ.get("EXECUTION_RECONCILE_ALLOW_BOOTSTRAP", "0") == "1"

    qty_tol = float(os.environ.get("POSITION_RECONCILE_QTY_TOL", "0.01"))
    ignore_lt = float(os.environ.get("POSITION_RECONCILE_IGNORE_QTY_LT", "0.001"))
    max_mismatched = int(os.environ.get("POSITION_RECONCILE_MAX_MISMATCHED", "0"))

    owns = False
    if con is None:
        init_db()
        con = connect()
        owns = True

    ts_ms = _now_ms()
    owns_txn = False

    try:
        _ensure_schema(con)

        fetch_failure_threshold, fetch_failure_cooldown_ms = _fetch_failure_halt_config()
        ok_b, bstatus, broker_pos = _broker_positions(str(broker))
        if not ok_b:
            if not owns_txn:
                owns_txn = _begin_owned_write(con)
            detail = _record_fetch_failure(
                con,
                broker=str(broker),
                ts_ms=int(ts_ms),
                error=str(bstatus),
                threshold=int(fetch_failure_threshold),
                cooldown_ms=int(fetch_failure_cooldown_ms),
            )
            persistent_halt = bool(detail.get("persistent_halt"))
            status = "positions_fetch_persistent_halt" if persistent_halt else "positions_fetch_failed"
            if persistent_halt:
                reason = "prelive_position_fetch_failures"
                detail["kill_switch_reason"] = reason
                try:
                    set_kill_switch(
                        scope="global",
                        key="global",
                        enabled=1,
                        reason=reason,
                        actor="position_reconcile",
                        meta={
                            "broker": str(broker),
                            "status": str(status),
                            "error": str(bstatus),
                            "fetch_failure_count": int(detail.get("fetch_failure_count") or 0),
                            "fetch_failure_first_ts_ms": int(detail.get("fetch_failure_first_ts_ms") or 0),
                            "fetch_failure_last_ts_ms": int(detail.get("fetch_failure_last_ts_ms") or 0),
                            "fetch_failure_elapsed_ms": int(detail.get("fetch_failure_elapsed_ms") or 0),
                            "fetch_failure_halt_threshold": int(fetch_failure_threshold),
                            "fetch_failure_cooldown_ms": int(fetch_failure_cooldown_ms),
                            "operator_recovery_required": True,
                        },
                        action="TRIP",
                        con=con,
                    )
                except Exception as e:
                    detail["kill_switch_error"] = f"{type(e).__name__}:{e}"
                    _warn_nonfatal(
                        "POSITION_RECONCILE_FETCH_FAILURE_KILL_SWITCH_FAILED",
                        e,
                        once_key="fetch_failure_kill_switch_trip",
                        broker=str(broker),
                        failure_count=int(detail.get("fetch_failure_count") or 0),
                    )
            _append_reconcile_audit(
                con,
                ts_ms=int(ts_ms),
                broker=str(broker),
                ok=False,
                status=str(status),
                mismatched_n=0,
                max_abs_qty_diff=0.0,
                total_abs_qty_diff=0.0,
                detail=detail,
            )
            if owns_txn:
                con.commit()
            return {
                "ok": False,
                "status": str(status),
                "broker": str(broker),
                "detail": detail,
                "fatal_reconcile": True,
                "persistent_halt": bool(persistent_halt),
            }

        bmap = _norm_positions(broker_pos, ignore_lt=ignore_lt)

        baseline = _load_baseline(con, str(broker))
        if baseline is None:
            if allow_bootstrap:
                confirmed, token_status, actor, token_hash = _bootstrap_confirmation()
                if not owns_txn:
                    owns_txn = _begin_owned_write(con)
                _reset_fetch_failure_state(con, broker=str(broker), ts_ms=int(ts_ms))
                if not confirmed:
                    detail = {
                        "error": "bootstrap_confirmation_required",
                        "token_status": str(token_status),
                        "actor": str(actor),
                        "n": int(len(bmap)),
                    }
                    _append_bootstrap_audit(
                        con,
                        ts_ms=int(ts_ms),
                        broker=str(broker),
                        actor=str(actor),
                        status="bootstrap_denied",
                        token_hash=token_hash,
                        positions=bmap,
                        detail=detail,
                    )
                    _append_reconcile_audit(
                        con,
                        ts_ms=int(ts_ms),
                        broker=str(broker),
                        ok=False,
                        status="bootstrap_confirmation_required",
                        mismatched_n=0,
                        max_abs_qty_diff=0.0,
                        total_abs_qty_diff=0.0,
                        detail=detail,
                    )
                    if owns_txn:
                        con.commit()
                    return {
                        "ok": False,
                        "status": "bootstrap_confirmation_required",
                        "broker": str(broker),
                        "detail": detail,
                        "fatal_reconcile": True,
                    }

                _save_baseline(con, str(broker), ts_ms, bmap)
                detail = {
                    "status": "baseline_bootstrapped",
                    "n": int(len(bmap)),
                    "actor": str(actor),
                    "re_reconcile_pending": True,
                }
                _set_re_reconcile_pending(
                    con,
                    broker=str(broker),
                    pending=True,
                    ts_ms=int(ts_ms),
                    detail=detail,
                )
                _append_bootstrap_audit(
                    con,
                    ts_ms=int(ts_ms),
                    broker=str(broker),
                    actor=str(actor),
                    status="baseline_bootstrapped",
                    token_hash=token_hash,
                    positions=bmap,
                    detail=detail,
                )
                _append_reconcile_audit(
                    con,
                    ts_ms=int(ts_ms),
                    broker=str(broker),
                    ok=False,
                    status="baseline_bootstrapped_re_reconcile_pending",
                    mismatched_n=0,
                    max_abs_qty_diff=0.0,
                    total_abs_qty_diff=0.0,
                    detail=detail,
                )
                if owns_txn:
                    con.commit()
                return {
                    "ok": False,
                    "status": "baseline_bootstrapped_re_reconcile_pending",
                    "bootstrap_status": "baseline_bootstrapped",
                    "broker": str(broker),
                    "mismatched_n": 0,
                    "max_abs_qty_diff": 0.0,
                    "total_abs_qty_diff": 0.0,
                    "detail": detail,
                    "fatal_reconcile": True,
                    "re_reconcile_pending": True,
                }

            if require_baseline:
                detail = {"error": "baseline_missing", "require_baseline": True, "allow_bootstrap": False}
                if not owns_txn:
                    owns_txn = _begin_owned_write(con)
                _reset_fetch_failure_state(con, broker=str(broker), ts_ms=int(ts_ms))
                _append_reconcile_audit(
                    con,
                    ts_ms=int(ts_ms),
                    broker=str(broker),
                    ok=False,
                    status="baseline_missing",
                    mismatched_n=0,
                    max_abs_qty_diff=0.0,
                    total_abs_qty_diff=0.0,
                    detail=detail,
                )
                if owns_txn:
                    con.commit()
                return {
                    "ok": False,
                    "status": "baseline_missing",
                    "broker": str(broker),
                    "detail": detail,
                    "fatal_reconcile": True,
                }

            # Explicit non-required mode still creates a baseline; require the
            # same operator confirmation and force the next trade to re-check.
            confirmed, token_status, actor, token_hash = _bootstrap_confirmation()
            if not owns_txn:
                owns_txn = _begin_owned_write(con)
            _reset_fetch_failure_state(con, broker=str(broker), ts_ms=int(ts_ms))
            if not confirmed:
                detail = {
                    "error": "bootstrap_confirmation_required",
                    "token_status": str(token_status),
                    "actor": str(actor),
                    "require_baseline": False,
                    "n": int(len(bmap)),
                }
                _append_bootstrap_audit(
                    con,
                    ts_ms=int(ts_ms),
                    broker=str(broker),
                    actor=str(actor),
                    status="bootstrap_denied",
                    token_hash=token_hash,
                    positions=bmap,
                    detail=detail,
                )
                _append_reconcile_audit(
                    con,
                    ts_ms=int(ts_ms),
                    broker=str(broker),
                    ok=False,
                    status="bootstrap_confirmation_required",
                    mismatched_n=0,
                    max_abs_qty_diff=0.0,
                    total_abs_qty_diff=0.0,
                    detail=detail,
                )
                if owns_txn:
                    con.commit()
                return {
                    "ok": False,
                    "status": "bootstrap_confirmation_required",
                    "broker": str(broker),
                    "detail": detail,
                    "fatal_reconcile": True,
                }
            _save_baseline(con, str(broker), ts_ms, bmap)
            detail = {
                "status": "baseline_created",
                "n": int(len(bmap)),
                "actor": str(actor),
                "require_baseline": False,
                "re_reconcile_pending": True,
            }
            _set_re_reconcile_pending(
                con,
                broker=str(broker),
                pending=True,
                ts_ms=int(ts_ms),
                detail=detail,
            )
            _append_bootstrap_audit(
                con,
                ts_ms=int(ts_ms),
                broker=str(broker),
                actor=str(actor),
                status="baseline_created",
                token_hash=token_hash,
                positions=bmap,
                detail=detail,
            )
            _append_reconcile_audit(
                con,
                ts_ms=int(ts_ms),
                broker=str(broker),
                ok=False,
                status="baseline_created_re_reconcile_pending",
                mismatched_n=0,
                max_abs_qty_diff=0.0,
                total_abs_qty_diff=0.0,
                detail=detail,
            )
            if owns_txn:
                con.commit()
            return {
                "ok": False,
                "status": "baseline_created_re_reconcile_pending",
                "bootstrap_status": "baseline_created",
                "broker": str(broker),
                "detail": detail,
                "fatal_reconcile": True,
                "re_reconcile_pending": True,
            }

        if _load_re_reconcile_pending(con, str(broker)):
            mismatched, total_abs, max_abs = _compare_position_maps(bmap, baseline, qty_tol=qty_tol)
            updated_baseline = bool(mismatched)
            if not owns_txn:
                owns_txn = _begin_owned_write(con)
            _reset_fetch_failure_state(con, broker=str(broker), ts_ms=int(ts_ms))
            if updated_baseline:
                _save_baseline(con, str(broker), ts_ms, bmap)
            detail = {
                "status": "bootstrap_re_reconciled",
                "updated_baseline": bool(updated_baseline),
                "mismatched": mismatched[:50],
                "mismatched_n": int(len(mismatched)),
                "qty_tol": float(qty_tol),
                "ignore_lt": float(ignore_lt),
                **_mismatch_summary(mismatched),
            }
            _set_re_reconcile_pending(
                con,
                broker=str(broker),
                pending=False,
                ts_ms=int(ts_ms),
                detail=detail,
            )
            _append_bootstrap_audit(
                con,
                ts_ms=int(ts_ms),
                broker=str(broker),
                actor="position_reconcile",
                status="re_reconcile_completed",
                token_hash=None,
                positions=bmap,
                detail=detail,
            )
            _append_reconcile_audit(
                con,
                ts_ms=int(ts_ms),
                broker=str(broker),
                ok=True,
                status="bootstrap_re_reconciled",
                mismatched_n=int(len(mismatched)),
                max_abs_qty_diff=float(max_abs),
                total_abs_qty_diff=float(total_abs),
                detail=detail,
            )
            if owns_txn:
                con.commit()
            return {
                "ok": True,
                "status": "bootstrap_re_reconciled",
                "broker": str(broker),
                "mismatched_n": int(len(mismatched)),
                "max_abs_qty_diff": float(max_abs),
                "total_abs_qty_diff": float(total_abs),
                "detail": detail,
                "fatal_reconcile": False,
                "re_reconcile_pending": False,
            }

        # Compare
        mismatched, total_abs, max_abs = _compare_position_maps(bmap, baseline, qty_tol=qty_tol)

        mismatched_n = int(len(mismatched))
        ok = (mismatched_n <= int(max_mismatched))

        status = "ok" if ok else "mismatch"
        detail = {
            "mismatched": mismatched[:50],  # cap
            "mismatched_n": mismatched_n,
            "qty_tol": float(qty_tol),
            "ignore_lt": float(ignore_lt),
            "max_mismatched": int(max_mismatched),
            **_mismatch_summary(mismatched),
        }

        if not owns_txn:
            owns_txn = _begin_owned_write(con)
        _reset_fetch_failure_state(con, broker=str(broker), ts_ms=int(ts_ms))
        _append_reconcile_audit(
            con,
            ts_ms=int(ts_ms),
            broker=str(broker),
            ok=bool(ok),
            status=str(status),
            mismatched_n=int(mismatched_n),
            max_abs_qty_diff=float(max_abs),
            total_abs_qty_diff=float(total_abs),
            detail=detail,
        )

        if ok:
            if owns_txn:
                con.commit()
            return {
                "ok": True,
                "status": "ok",
                "broker": str(broker),
                "mismatched_n": mismatched_n,
                "max_abs_qty_diff": float(max_abs),
                "total_abs_qty_diff": float(total_abs),
                "detail": detail,
                "fatal_reconcile": False,
            }

        # Mismatch → trip kill switch (global) and block.
        try:
            set_kill_switch(
                scope="global",
                key="global",
                enabled=1,
                reason="prelive_position_mismatch",
                actor="position_reconcile",
                meta={
                    "broker": str(broker),
                    "mismatched_n": mismatched_n,
                    "max_abs_qty_diff": float(max_abs),
                    "total_abs_qty_diff": float(total_abs),
                    "qty_tol": float(qty_tol),
                },
                action="TRIP",
                con=con,
            )
        except Exception as e:
            _warn_nonfatal(
                "POSITION_RECONCILE_KILL_SWITCH_FAILED",
                e,
                once_key="kill_switch_trip",
                broker=str(broker),
                mismatched_n=int(mismatched_n),
            )
        if owns_txn:
            con.commit()

        return {
            "ok": False,
            "status": "mismatch",
            "broker": str(broker),
            "mismatched_n": mismatched_n,
            "max_abs_qty_diff": float(max_abs),
            "total_abs_qty_diff": float(total_abs),
            "detail": detail,
            "fatal_reconcile": True,
        }

    finally:
        if owns_txn and bool(getattr(con, "in_transaction", False)):
            try:
                con.rollback()
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_ROLLBACK_FAILED",
                    e,
                    once_key="rollback",
                    broker=str(broker),
                )
        if owns:
            try:
                con.close()
            except Exception as e:
                _warn_nonfatal(
                    "POSITION_RECONCILE_CLOSE_FAILED",
                    e,
                    once_key="close",
                    broker=str(broker),
                )
