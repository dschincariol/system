"""
FILE: train_size_policy.py

Execution subsystem module for `train_size_policy`.
"""

# train_size_policy.py
"""
Learn a confidence -> size factor policy from realized net returns.

- Join predictions (confidence) with labels_exec (net_ret)
- Bucket by confidence
- For each bucket: mean(net_ret), std(net_ret)
- Convert to factor using (mean/std) vs a normalization constant
- Enforce monotone non-decreasing factors w.r.t confidence
- Store in size_policy + size_policy_points
"""

import os
import json
import time
import math
import logging
from typing import Any, List, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db, run_write_txn

_WARNED_NONFATAL_KEYS: set[str] = set()
LOG = get_logger("execution.train_size_policy")
_SIZE_POLICY_SCHEMA_MARKER_KEY = "size_policy_schema_version"
_SIZE_POLICY_SCHEMA_MARKER_VALUE = "1"
_SIZE_POLICY_SCHEMA_INDEXES = (
    "idx_size_policy_ts",
    "idx_size_policy_points_policy",
)
_SIZE_POLICY_SCHEMA_LOCAL_READY = False


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event="execution_train_size_policy_nonfatal",
        code=code,
        message=code,
        error=error,
        level=logging.WARNING,
        component="engine.execution.train_size_policy",
        extra=dict(extra or {}) or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)

# ------            -- ------------------------------------------------------
# Schema (owned by this module)
# ------            -- ------------------------------------------------------
SIZE_POLICY_SCHEMA = """
CREATE TABLE IF NOT EXISTS size_policy (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  ts_ms INTEGER NOT NULL,
  lookback_days INTEGER NOT NULL,
  buckets INTEGER NOT NULL,
  method TEXT NOT NULL,
  params_json TEXT,
  metrics_json TEXT
);

CREATE TABLE IF NOT EXISTS size_policy_points (
  policy_id INTEGER NOT NULL,
  bucket_idx INTEGER NOT NULL,
  conf_lo REAL NOT NULL,
  conf_hi REAL NOT NULL,
  n INTEGER NOT NULL,
  mean_net_ret REAL NOT NULL,
  std_net_ret REAL NOT NULL,
  factor REAL NOT NULL,
  PRIMARY KEY (policy_id, bucket_idx)
);

CREATE INDEX IF NOT EXISTS idx_size_policy_ts
  ON size_policy(ts_ms);

CREATE INDEX IF NOT EXISTS idx_size_policy_points_policy
  ON size_policy_points(policy_id, bucket_idx);
"""


def _ensure_size_policy_schema(con):
    """
    Backward-compatible schema fix for size_policy.
    Adds missing columns if the table already exists.
    """
    cols = {
        "lookback_days": "INTEGER",
        "buckets": "INTEGER",
    }

    existing = {
        r[1] for r in con.execute("PRAGMA table_info(size_policy)").fetchall()
    }

    for name, typ in cols.items():
        if name not in existing:
            con.execute(f"ALTER TABLE size_policy ADD COLUMN {name} {typ}")


def _index_exists(con, index_name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=?",
            (str(index_name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "TRAIN_SIZE_POLICY_INDEX_EXISTS_FAILED",
            e,
            once_key=f"index_exists:{index_name}",
            index_name=str(index_name),
        )
        return False


def _table_columns(con, table_name: str) -> set[str]:
    try:
        return {
            str(row[1] or "").strip()
            for row in (con.execute(f"PRAGMA table_info({table_name})").fetchall() or [])
            if row and len(row) > 1
        }
    except Exception as e:
        _warn_nonfatal(
            "TRAIN_SIZE_POLICY_TABLE_COLUMNS_FAILED",
            e,
            once_key=f"table_columns:{table_name}",
            table_name=str(table_name),
        )
        return set()


def _size_policy_schema_marker_ready() -> bool:
    global _SIZE_POLICY_SCHEMA_LOCAL_READY

    if _SIZE_POLICY_SCHEMA_LOCAL_READY:
        return True

    con = connect(readonly=True)
    try:
        if not _table_exists(con, "size_policy"):
            return False
        if not _table_exists(con, "size_policy_points"):
            return False

        for index_name in _SIZE_POLICY_SCHEMA_INDEXES:
            if not _index_exists(con, index_name):
                return False
        if _table_exists(con, "runtime_meta"):
            row = con.execute(
                "SELECT value FROM runtime_meta WHERE key=?",
                (_SIZE_POLICY_SCHEMA_MARKER_KEY,),
            ).fetchone()
            marker = str((row or [None])[0] or "").strip()
            if marker not in ("", _SIZE_POLICY_SCHEMA_MARKER_VALUE):
                return False
    finally:
        try:
            con.close()
        except Exception as e:
            _warn_nonfatal(
                "TRAIN_SIZE_POLICY_MARKER_VERIFY_CLOSE_FAILED",
                e,
                once_key="size_policy_marker_close",
            )

    _SIZE_POLICY_SCHEMA_LOCAL_READY = True
    return True


def _mark_size_policy_schema_ready(con) -> None:
    con.execute(
        """
        INSERT INTO runtime_meta(key, value, updated_ts_ms)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value=excluded.value,
          updated_ts_ms=excluded.updated_ts_ms
        """,
        (
            _SIZE_POLICY_SCHEMA_MARKER_KEY,
            _SIZE_POLICY_SCHEMA_MARKER_VALUE,
            int(time.time() * 1000),
        ),
    )


def _init_size_policy_schema(con) -> None:
    con.executescript(SIZE_POLICY_SCHEMA)
    _ensure_size_policy_schema(con)
    _mark_size_policy_schema_ready(con)


def init_size_policy_schema() -> None:
    global _SIZE_POLICY_SCHEMA_LOCAL_READY

    if _size_policy_schema_marker_ready():
        return
    run_write_txn(
        _init_size_policy_schema,
        table="size_policy",
        operation="init_size_policy_schema",
        direct=True,
    )
    _SIZE_POLICY_SCHEMA_LOCAL_READY = True

LOOKBACK_DAYS = int(os.environ.get("SIZE_POLICY_LOOKBACK_DAYS", "90"))
BUCKETS = int(os.environ.get("SIZE_POLICY_BUCKETS", "10"))
MIN_SAMPLES = int(os.environ.get("SIZE_POLICY_MIN_SAMPLES", "200"))

# Convert bucket Sharpe proxy -> factor
SHARPE_NORM = float(os.environ.get("SIZE_POLICY_SHARPE_NORM", "1.0"))
MAX_FACTOR = float(os.environ.get("SIZE_POLICY_MAX_FACTOR", "1.0"))
MIN_FACTOR = float(os.environ.get("SIZE_POLICY_MIN_FACTOR", "0.0"))

# Prefer broker-fills if present
PREFER_REALIZED = os.environ.get("SIZE_POLICY_PREFER_REALIZED", "1") == "1"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _preflight_smoke_enabled() -> bool:
    raw = str(os.environ.get("PREFLIGHT_SMOKE", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _std(vals: List[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = sum(vals) / len(vals)
    var = sum((x - m) ** 2 for x in vals) / max(1, (len(vals) - 1))
    return math.sqrt(var)


def _table_exists(con, name: str) -> bool:
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "TRAIN_SIZE_POLICY_TABLE_EXISTS_FAILED",
            e,
            once_key=f"table_exists:{name}",
            table_name=str(name),
        )
        return False


def _column_exists(con, table_name: str, column_name: str) -> bool:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall()
        return any(str(r[1] or "") == str(column_name) for r in rows or [])
    except Exception as e:
        _warn_nonfatal(
            "TRAIN_SIZE_POLICY_COLUMN_EXISTS_FAILED",
            e,
            once_key=f"column_exists:{table_name}:{column_name}",
            table_name=str(table_name),
            column_name=str(column_name),
        )
        return False


def _store_size_policy(
    con,
    *,
    ts_ms: int,
    params: dict[str, Any],
    metrics: dict[str, Any],
    points: List[dict[str, Any]],
) -> int:
    size_policy_columns = _table_columns(con, "size_policy")
    params_json = json.dumps(params, separators=(",", ":"), sort_keys=True)
    metrics_json = json.dumps(metrics, separators=(",", ":"), sort_keys=True)

    if "lookback_days" in size_policy_columns and "buckets" in size_policy_columns:
        con.execute(
            """
            INSERT INTO size_policy(ts_ms, lookback_days, buckets, method, params_json, metrics_json)
            VALUES (?,?,?,?,?,?)
            """,
            (
                int(ts_ms),
                int(LOOKBACK_DAYS),
                int(BUCKETS),
                "bucket_sharpe_monotone",
                params_json,
                metrics_json,
            ),
        )
    else:
        con.execute(
            """
            INSERT INTO size_policy(ts_ms, method, params_json, metrics_json)
            VALUES (?,?,?,?)
            """,
            (
                int(ts_ms),
                "bucket_sharpe_monotone",
                params_json,
                metrics_json,
            ),
        )
    row = con.execute("SELECT last_insert_rowid()").fetchone()
    policy_id = int((row or [0])[0] or 0)

    for point in points:
        con.execute(
            """
            INSERT INTO size_policy_points(policy_id, bucket_idx, conf_lo, conf_hi, n, mean_net_ret, std_net_ret, factor)
            VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                int(policy_id),
                int(point["bucket_idx"]),
                float(point["conf_lo"]),
                float(point["conf_hi"]),
                int(point["n"]),
                float(point["mean_net_ret"]),
                float(point["std_net_ret"]),
                float(point["factor"]),
            ),
        )

    return int(policy_id)


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("train_size_policy must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    init_size_policy_schema()
    con = connect(readonly=True)

    try:
        if not _table_exists(con, "predictions"):
            print("[size_policy] skip: predictions table missing")
            return

        if not _table_exists(con, "labels_exec"):
            print("[size_policy] skip: labels_exec table missing")
            return

        min_ts = _now_ms() - LOOKBACK_DAYS * 86400 * 1000

        # Learn a monotone size curve from realized net returns, preferring
        # realized labels when available but degrading gracefully if not.
        # Join confidence -> net_ret
        # Prefer realized rows if enabled
        rows = []
        realized_col_ok = _column_exists(con, "labels_exec", "realized")

        if PREFER_REALIZED and realized_col_ok:
            q = """
            SELECT p.confidence, le.net_ret
            FROM predictions p
            JOIN labels_exec le
              ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
            WHERE p.ts_ms >= ?
              AND le.net_ret IS NOT NULL
              AND le.realized=1
              AND COALESCE(le.source, '') <> 'broker_sim_placeholder'
            """
            rows = con.execute(q, (int(min_ts),)).fetchall()

            # If insufficient realized samples, fall back to any labels_exec
            if len(rows or []) < MIN_SAMPLES:
                q2 = """
                SELECT p.confidence, le.net_ret
                FROM predictions p
                JOIN labels_exec le
                  ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
                WHERE p.ts_ms >= ?
                  AND le.net_ret IS NOT NULL
                  AND COALESCE(le.source, '') <> 'broker_sim_placeholder'
                """
                rows = con.execute(q2, (int(min_ts),)).fetchall()
        else:
            q2 = """
            SELECT p.confidence, le.net_ret
            FROM predictions p
            JOIN labels_exec le
              ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
            WHERE p.ts_ms >= ?
              AND le.net_ret IS NOT NULL
              AND COALESCE(le.source, '') <> 'broker_sim_placeholder'
            """
            rows = con.execute(q2, (int(min_ts),)).fetchall()

        data: List[Tuple[float, float]] = []
        for c, r in rows or []:
            try:
                cf = float(c)
                rr = float(r)
            except Exception as e:
                _warn_nonfatal(
                    "TRAIN_SIZE_POLICY_SAMPLE_PARSE_FAILED",
                    e,
                    once_key="sample_parse",
                    confidence=repr(c)[:120],
                    realized_ret=repr(r)[:120],
                )
                continue
            if not (0.0 <= cf <= 1.0):
                continue
            data.append((cf, rr))

        if len(data) < MIN_SAMPLES:
            message = f"not enough samples: {len(data)} < {MIN_SAMPLES}"
            if _preflight_smoke_enabled():
                print(f"[size_policy] skip: {message}")
                return
            raise SystemExit(f"[size_policy] {message}")

        # Build buckets
        buckets = [[] for _ in range(BUCKETS)]
        for cf, rr in data:
            idx = min(BUCKETS - 1, max(0, int(cf * BUCKETS)))
            buckets[idx].append(rr)

        points = []
        for i in range(BUCKETS):
            clo = i / BUCKETS
            chi = (i + 1) / BUCKETS if i < BUCKETS - 1 else 1.0
            vals = buckets[i]
            n = len(vals)
            mean = sum(vals) / n if n > 0 else 0.0
            sd = _std(vals)

            sharpe = 0.0
            if sd > 1e-12:
                sharpe = mean / sd  # simple proxy

            # Convert to [0..1] factor
            f = sharpe / max(1e-9, SHARPE_NORM)
            if f < 0.0:
                f = 0.0
            f = max(MIN_FACTOR, min(MAX_FACTOR, f))

            points.append({
                "bucket_idx": i,
                "conf_lo": float(clo),
                "conf_hi": float(chi),
                "n": int(n),
                "mean_net_ret": float(mean),
                "std_net_ret": float(sd),
                "factor": float(f),
            })

        # Enforce monotone non-decreasing factor with confidence so downstream
        # sizing never penalizes higher-confidence buckets.
        # Enforce monotone non-decreasing factor with confidence
        # (higher confidence should never get smaller size)
        last = 0.0
        for p in points:
            if p["factor"] < last:
                p["factor"] = last
            else:
                last = p["factor"]

        # Store policy
        ts = _now_ms()
        params = {
            "lookback_days": LOOKBACK_DAYS,
            "buckets": BUCKETS,
            "min_samples": MIN_SAMPLES,
            "sharpe_norm": SHARPE_NORM,
            "prefer_realized": bool(PREFER_REALIZED),
        }
        metrics = {
            "n_samples": len(data),
            "method": "bucket_sharpe_monotone",
        }
        pid = int(
            run_write_txn(
                lambda write_con: _store_size_policy(
                    write_con,
                    ts_ms=int(ts),
                    params=params,
                    metrics=metrics,
                    points=points,
                ),
                table="size_policy",
                operation="train_size_policy_store",
                context={"n_samples": int(len(data)), "buckets": int(BUCKETS)},
            )
        )
        print(
            f"[size_policy] stored policy_id={pid} "
            f"n_samples={len(data)} points={len(points)}"
        )

    finally:
        con.close()


if __name__ == "__main__":
    main()
