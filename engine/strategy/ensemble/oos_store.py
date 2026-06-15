"""Out-of-sample prediction store for stacked ensemble training."""

from __future__ import annotations
import logging

import time
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from engine.runtime.storage import connect


OOS_TABLE = "model_oos_predictions"
DEFAULT_RUN_ID = "default"
LEGACY_RUN_ID = "legacy"


def _own_connection(con):
    if con is not None:
        return con, False
    return connect(), True


def _commit_if_possible(con) -> None:
    commit = getattr(con, "commit", None)
    if callable(commit):
        commit()


def _create_oos_table(con, table_name: str = OOS_TABLE) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            symbol TEXT NOT NULL,
            horizon INTEGER NOT NULL,
            family TEXT NOT NULL,
            ts INTEGER NOT NULL,
            run_id TEXT NOT NULL,
            prediction REAL NOT NULL,
            target REAL NULL,
            PRIMARY KEY(symbol, horizon, family, ts, run_id)
        )
        """
    )


def _sqlite_table_info(con) -> list[tuple[Any, ...]]:
    try:
        return [tuple(row) for row in con.execute(f"PRAGMA table_info({OOS_TABLE})").fetchall()]
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)
        return []


def _sqlite_pk_columns(table_info: Sequence[Sequence[Any]]) -> list[str]:
    keyed = [
        (int(row[5] or 0), str(row[1] or ""))
        for row in table_info
        if len(row) > 5 and int(row[5] or 0) > 0
    ]
    return [name for _position, name in sorted(keyed)]


def _migrate_sqlite_run_id_schema(con) -> None:
    table_info = _sqlite_table_info(con)
    if not table_info:
        return
    columns = {str(row[1] or "") for row in table_info if len(row) > 1}
    desired_pk = ["symbol", "horizon", "family", "ts", "run_id"]
    if "run_id" in columns and _sqlite_pk_columns(table_info) == desired_pk:
        return

    backup_table = f"{OOS_TABLE}__legacy_{int(time.time() * 1000)}"
    con.execute(f"ALTER TABLE {OOS_TABLE} RENAME TO {backup_table}")
    _create_oos_table(con, OOS_TABLE)
    run_id_expr = "COALESCE(run_id, 'legacy')" if "run_id" in columns else "'legacy'"
    con.execute(
        f"""
        INSERT INTO {OOS_TABLE}(symbol, horizon, family, ts, run_id, prediction, target)
        SELECT symbol, horizon, family, ts, {run_id_expr}, prediction, target
          FROM {backup_table}
        """
    )
    con.execute(f"DROP TABLE {backup_table}")


def _migrate_postgres_run_id_schema(con) -> None:
    try:
        con.execute(f"ALTER TABLE {OOS_TABLE} ADD COLUMN IF NOT EXISTS run_id TEXT")
        con.execute(f"UPDATE {OOS_TABLE} SET run_id = %s WHERE run_id IS NULL", (LEGACY_RUN_ID,))
        con.execute(f"ALTER TABLE {OOS_TABLE} ALTER COLUMN run_id SET NOT NULL")
        con.execute(
            """
            DO $$
            DECLARE pk_name text;
            BEGIN
              SELECT conname INTO pk_name
                FROM pg_constraint
               WHERE conrelid = 'model_oos_predictions'::regclass
                 AND contype = 'p'
               LIMIT 1;
              IF pk_name IS NOT NULL THEN
                EXECUTE format('ALTER TABLE model_oos_predictions DROP CONSTRAINT %I', pk_name);
              END IF;
              IF NOT EXISTS (
                SELECT 1
                  FROM pg_constraint
                 WHERE conrelid = 'model_oos_predictions'::regclass
                   AND conname = 'model_oos_predictions_pkey'
              ) THEN
                ALTER TABLE model_oos_predictions
                  ADD CONSTRAINT model_oos_predictions_pkey
                  PRIMARY KEY(symbol, horizon, family, ts, run_id);
              END IF;
            END $$;
            """
        )
    except Exception:
        logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def ensure_schema(con=None) -> None:
    """Create the OOS prediction table when migrations have not run yet."""

    con, own = _own_connection(con)
    try:
        _create_oos_table(con)
        if "sqlite" in type(con).__module__.lower():
            _migrate_sqlite_run_id_schema(con)
        else:
            _migrate_postgres_run_id_schema(con)
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_model_oos_predictions_lookup
              ON model_oos_predictions(symbol, horizon, ts)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_model_oos_predictions_family_ts
              ON model_oos_predictions(family, ts)
            """
        )
        if own:
            _commit_if_possible(con)
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_prediction_row(row: Mapping[str, Any] | Sequence[Any]) -> tuple[str, int, str, int, str, float, float | None]:
    if isinstance(row, Mapping):
        symbol = row.get("symbol")
        horizon = row.get("horizon", row.get("horizon_s"))
        family = row.get("family")
        ts = row.get("ts", row.get("ts_ms"))
        run_id = row.get("run_id", DEFAULT_RUN_ID)
        prediction = row.get("prediction", row.get("pred"))
        target = row.get("target")
    else:
        if len(row) < 5:
            raise ValueError("OOS prediction rows require symbol, horizon, family, ts, and prediction")
        symbol, horizon, family, ts, prediction = row[:5]
        if len(row) > 6:
            run_id = row[5]
            target = row[6]
        else:
            run_id = DEFAULT_RUN_ID
            target = row[5] if len(row) > 5 else None
    family_key = str(family or "").strip()
    symbol_key = str(symbol or "").strip()
    run_id_key = str(run_id or "").strip() or DEFAULT_RUN_ID
    if not symbol_key:
        raise ValueError("OOS prediction row is missing symbol")
    if not family_key:
        raise ValueError("OOS prediction row is missing family")
    return (
        symbol_key,
        int(horizon),
        family_key,
        int(ts),
        run_id_key,
        float(prediction),
        _float_or_none(target),
    )


def upsert_oos_prediction(
    *,
    symbol: str,
    horizon: int,
    family: str,
    ts: int,
    prediction: float,
    run_id: str = DEFAULT_RUN_ID,
    target: float | None = None,
    con=None,
    ensure: bool = True,
) -> None:
    upsert_oos_predictions(
        [
            {
                "symbol": symbol,
                "horizon": horizon,
                "family": family,
                "ts": ts,
                "run_id": run_id,
                "prediction": prediction,
                "target": target,
            }
        ],
        con=con,
        ensure=ensure,
    )


def upsert_oos_predictions(
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    con=None,
    ensure: bool = True,
) -> int:
    normalized = [_normalize_prediction_row(row) for row in rows]
    if not normalized:
        return 0
    con, own = _own_connection(con)
    try:
        if ensure:
            ensure_schema(con)
        sql = """
            INSERT INTO model_oos_predictions(symbol, horizon, family, ts, run_id, prediction, target)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, horizon, family, ts, run_id) DO UPDATE SET
              prediction = excluded.prediction,
              target = COALESCE(excluded.target, model_oos_predictions.target)
        """
        if hasattr(con, "executemany"):
            con.executemany(sql, normalized)
        else:
            for row in normalized:
                con.execute(sql, row)
        if own:
            _commit_if_possible(con)
        return len(normalized)
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def update_targets(
    rows: Iterable[Mapping[str, Any] | Sequence[Any]],
    *,
    con=None,
    ensure: bool = True,
) -> int:
    normalized = [_normalize_prediction_row(row) for row in rows]
    if not normalized:
        return 0
    con, own = _own_connection(con)
    try:
        if ensure:
            ensure_schema(con)
        count = 0
        for symbol, horizon, family, ts, run_id, _prediction, target in normalized:
            if target is None:
                continue
            con.execute(
                """
                UPDATE model_oos_predictions
                   SET target = ?
                 WHERE symbol = ?
                   AND horizon = ?
                   AND family = ?
                   AND ts = ?
                   AND run_id = ?
                """,
                (float(target), str(symbol), int(horizon), str(family), int(ts), str(run_id)),
            )
            count += 1
        if own:
            _commit_if_possible(con)
        return count
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [desc[0] for desc in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def read_oos_predictions(
    *,
    symbol: str | None = None,
    horizon: int | None = None,
    family: str | None = None,
    start_ts: int | None = None,
    end_ts: int | None = None,
    require_target: bool = False,
    latest_per_key: bool = True,
    con=None,
    ensure: bool = True,
) -> list[dict[str, Any]]:
    con, own = _own_connection(con)
    try:
        if ensure:
            ensure_schema(con)
        prefix = "o." if latest_per_key else ""
        where: list[str] = []
        params: list[Any] = []
        if symbol is not None:
            where.append(f"{prefix}symbol = ?")
            params.append(str(symbol))
        if horizon is not None:
            where.append(f"{prefix}horizon = ?")
            params.append(int(horizon))
        if family is not None:
            where.append(f"{prefix}family = ?")
            params.append(str(family))
        if start_ts is not None:
            where.append(f"{prefix}ts >= ?")
            params.append(int(start_ts))
        if end_ts is not None:
            where.append(f"{prefix}ts <= ?")
            params.append(int(end_ts))
        if require_target:
            where.append(f"{prefix}target IS NOT NULL")
        if latest_per_key:
            sql = """
                SELECT o.symbol, o.horizon, o.family, o.ts, o.run_id, o.prediction, o.target
                  FROM model_oos_predictions o
                 WHERE o.rowid = (
                       SELECT MAX(o2.rowid)
                         FROM model_oos_predictions o2
                        WHERE o2.symbol = o.symbol
                          AND o2.horizon = o.horizon
                          AND o2.family = o.family
                          AND o2.ts = o.ts
                   )
            """
            if where:
                sql += " AND " + " AND ".join(where)
        else:
            sql = """
                SELECT symbol, horizon, family, ts, run_id, prediction, target
                  FROM model_oos_predictions
            """
            if where:
                sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY {prefix}symbol, {prefix}horizon, {prefix}ts, {prefix}family, {prefix}run_id"
        return _rows_to_dicts(con.execute(sql, tuple(params)))
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def trailing_start_ts(days: int, *, now_ms: int | None = None) -> int:
    now = int(now_ms if now_ms is not None else time.time() * 1000)
    return int(now - max(0, int(days)) * 24 * 60 * 60 * 1000)
