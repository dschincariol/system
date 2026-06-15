"""Backfill realized targets onto stacked-ensemble OOS prediction rows."""

from __future__ import annotations
import logging

import os
import time
from typing import Any

from engine.runtime.storage import connect
from engine.strategy.ensemble.oos_store import ensure_schema, update_targets


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return int(default)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _rows_to_dicts(cursor) -> list[dict[str, Any]]:
    rows = cursor.fetchall()
    columns = [desc[0] for desc in (cursor.description or [])]
    return [dict(zip(columns, row)) for row in rows]


def _candidate_target_rows(con, *, cutoff_ts: int, limit: int) -> list[dict[str, Any]]:
    target_exprs = [
        "CASE WHEN le.realized=1 THEN le.net_z ELSE COALESCE(le.net_z, l.realized_z, l.impact_z) END",
        "COALESCE(le.net_z, l.impact_z)",
        "COALESCE(l.realized_z, l.impact_z)",
        "l.impact_z",
    ]
    joins = [
        """
        FROM model_oos_predictions o
        JOIN labels l
          ON l.symbol = o.symbol
         AND l.horizon_s = o.horizon
         AND l.ts_ms = o.ts
        LEFT JOIN labels_exec le
          ON le.event_id = l.event_id
         AND le.symbol = l.symbol
         AND le.horizon_s = l.horizon_s
        """,
        """
        FROM model_oos_predictions o
        JOIN labels l
          ON l.symbol = o.symbol
         AND l.horizon_s = o.horizon
         AND l.ts_ms = o.ts
        """,
    ]
    for join_sql in joins:
        for target_expr in target_exprs:
            if "le." in target_expr and "labels_exec" not in join_sql:
                continue
            sql = f"""
                SELECT
                  o.symbol,
                  o.horizon,
                  o.family,
                  o.ts,
                  o.run_id,
                  o.prediction,
                  {target_expr} AS target
                {join_sql}
                WHERE o.target IS NULL
                  AND o.ts <= ?
                  AND ({target_expr}) IS NOT NULL
                ORDER BY o.ts ASC
                LIMIT ?
            """
            try:
                return _rows_to_dicts(con.execute(sql, (int(cutoff_ts), int(limit))))
            except Exception:
                continue
    return []


def fill_targets_from_labels(
    *,
    con=None,
    now_ms: int | None = None,
    delay_ms: int | None = None,
    limit: int | None = None,
) -> dict[str, Any]:
    own = con is None
    con = connect() if con is None else con
    try:
        ensure_schema(con)
        now_value = int(now_ms if now_ms is not None else time.time() * 1000)
        delay_value = int(delay_ms if delay_ms is not None else _env_int("ENSEMBLE_OOS_TARGET_DELAY_MS", 0))
        limit_value = max(1, int(limit if limit is not None else _env_int("ENSEMBLE_OOS_TARGET_FILL_LIMIT", 10000)))
        cutoff_ts = int(now_value - max(0, delay_value))
        rows = _candidate_target_rows(con, cutoff_ts=cutoff_ts, limit=limit_value)
        updated = update_targets(rows, con=con, ensure=False)
        hedge_refresh: dict[str, Any] | None = None
        if int(updated) > 0 and _env_bool("ENSEMBLE_HEDGE_REFRESH_ON_TARGET_FILL", True):
            try:
                from engine.strategy.ensemble.hedge import refresh_hedge_weights

                hedge_refresh = refresh_hedge_weights(con=con, now_ms=now_value)
            except Exception as exc:
                hedge_refresh = {
                    "ok": False,
                    "error": f"{type(exc).__name__}:{exc}",
                }
        con.commit()
        return {
            "ok": True,
            "updated_count": int(updated),
            "candidate_count": int(len(rows)),
            "cutoff_ts": int(cutoff_ts),
            "delay_ms": int(delay_value),
            "limit": int(limit_value),
            "hedge_refresh": hedge_refresh,
        }
    finally:
        if own:
            try:
                con.close()
            except Exception:
                logging.getLogger(__name__).debug("Ignored recoverable exception.", exc_info=True)


def run(*, con=None) -> dict[str, Any]:
    return fill_targets_from_labels(con=con)


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
