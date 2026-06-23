"""
FILE: compute_exec_labels.py

Execution job entrypoint for `compute_exec_labels`.
"""

# compute_exec_labels.py
"""
Phase 5: Compute execution-aware labels (labels_exec).

Assumptions:
- Entry at alert/prediction time (ts_ms from predictions/alerts)
- Exit at ts_ms + horizon_s
- Use last/mid price from prices table (best-effort)
- side is inferred from predicted_z sign
"""

import json
import os
import time
from typing import Optional, Dict, Any, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger
from engine.runtime.storage import connect, init_db
from engine.execution.execution_costs import estimate_cost_bps, apply_cost_to_return
from engine.strategy.net_after_cost_labels import (
    build_net_after_cost_label,
    load_execution_trace,
    load_prediction_label_context,
    upsert_net_after_cost_label,
)

LOG = get_logger("engine.execution.jobs.compute_exec_labels")
_WARNED_NONFATAL_KEYS: set[str] = set()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.jobs.compute_exec_labels",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _get_px_at_or_before(con, symbol: str, ts_ms: int) -> Optional[Tuple[float, int]]:
    r = con.execute(
        """
        SELECT ts_ms, price
        FROM prices
        WHERE symbol = ?
        AND ts_ms <= ?
        ORDER BY ts_ms DESC
        LIMIT 1
        """,
        (symbol, ts_ms),

    ).fetchone()

    if not r:
        return None
    try:
        # r = (ts_ms, price)
        return (float(r[1]), int(r[0]))
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_PRICE_ROW_PARSE_FAILED", e, once_key="price_row_parse", symbol=str(symbol), ts_ms=int(ts_ms))
        return None

def _get_quote_meta_at_or_before(con, symbol: str, ts_ms: int) -> Dict[str, Any]:
    """
    Best-effort bid/ask/spread metadata from the canonical quote table.

    Older deployments optionally stored this data in prices.extra_json; only
    probe that legacy column after checking that it exists so Postgres does not
    leave the surrounding transaction aborted on a missing-column error.
    """
    if _table_exists(con, "price_quotes"):
        try:
            r = con.execute(
                """
                SELECT bid, ask, spread
                FROM price_quotes
                WHERE symbol=? AND ts_ms <= ?
                ORDER BY ts_ms DESC
                LIMIT 1
                """,
                (symbol, int(ts_ms)),
            ).fetchone()
            if r:
                meta = {
                    "bid": r[0],
                    "ask": r[1],
                    "spread": r[2],
                }
                return {key: value for key, value in meta.items() if value is not None}
        except Exception as e:
            _warn_nonfatal("COMPUTE_EXEC_LABELS_QUOTE_TABLE_PARSE_FAILED", e, once_key="quote_table_parse", symbol=str(symbol), ts_ms=int(ts_ms))

    if not _column_exists(con, "prices", "extra_json"):
        return {}

    try:
        r = con.execute(
            """
            SELECT extra_json
            FROM prices
            WHERE symbol=? AND ts_ms <= ?
            ORDER BY ts_ms DESC
            LIMIT 1
            """,
            (symbol, int(ts_ms)),
        ).fetchone()
        if not r or not r[0]:
            return {}
        return json.loads(r[0])
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_QUOTE_META_PARSE_FAILED", e, once_key="quote_meta_parse", symbol=str(symbol), ts_ms=int(ts_ms))
        return {}


def _table_exists(con, name: str) -> bool:
    try:
        from engine.runtime.storage import table_exists as _storage_table_exists

        if bool(_storage_table_exists(con, str(name))):
            return True
    except Exception as e:
        _warn_nonfatal(
            "COMPUTE_EXEC_LABELS_STORAGE_TABLE_EXISTS_FAILED",
            e,
            once_key=f"storage_table_exists:{name}",
            table_name=str(name),
        )
    if _looks_like_postgres(con):
        return False
    try:
        row = con.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (str(name),),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal("COMPUTE_EXEC_LABELS_TABLE_EXISTS_FAILED", e, once_key=f"table_exists:{name}", table_name=str(name))
        return False


def _looks_like_sqlite(con) -> bool:
    module_name = str(getattr(con, "__class__", type(con)).__module__ or "").lower()
    class_name = str(getattr(con, "__class__", type(con)).__name__ or "").lower()
    return "sqlite" in module_name or "sqlite" in class_name


def _looks_like_postgres(con) -> bool:
    module_name = str(getattr(con, "__class__", type(con)).__module__ or "").lower()
    class_name = str(getattr(con, "__class__", type(con)).__name__ or "").lower()
    raw = getattr(con, "raw", None)
    raw_module = str(getattr(getattr(raw, "__class__", type(raw)), "__module__", "") or "").lower()
    raw_class = str(getattr(getattr(raw, "__class__", type(raw)), "__name__", "") or "").lower()
    haystack = " ".join((module_name, class_name, raw_module, raw_class))
    return "psycopg" in haystack or "storage_pg" in haystack


def _column_exists(con, table: str, column: str) -> bool:
    if _looks_like_sqlite(con):
        try:
            rows = con.execute(f"PRAGMA table_info({str(table)})").fetchall()
            return str(column) in {str(row[1]) for row in rows or []}
        except Exception as e:
            _warn_nonfatal(
                "COMPUTE_EXEC_LABELS_COLUMN_EXISTS_FAILED",
                e,
                once_key=f"column_exists:{table}:{column}",
                table_name=str(table),
                column_name=str(column),
            )
            return False
    try:
        row = con.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = ANY (current_schemas(false))
              AND table_name=?
              AND column_name=?
            LIMIT 1
            """,
            (str(table), str(column)),
        ).fetchone()
        return bool(row)
    except Exception as e:
        _warn_nonfatal(
            "COMPUTE_EXEC_LABELS_COLUMN_EXISTS_FAILED",
            e,
            once_key=f"column_exists:{table}:{column}",
            table_name=str(table),
            column_name=str(column),
        )
        return False


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("compute_exec_labels must be launched by supervisor")
        raise SystemExit(1)

    init_db()
    con = connect()
    try:
        if not _table_exists(con, "predictions"):
            print("[labels_exec] skip: predictions table missing")
            return

        if not _table_exists(con, "labels_exec"):
            print("[labels_exec] skip: labels_exec table missing")
            return

        if not _table_exists(con, "prices"):
            print("[labels_exec] skip: prices table missing")
            return

        # Pull latest predictions that do not yet have exec labels
        rows = con.execute(
            """
            SELECT p.event_id, p.symbol, p.horizon_s, p.ts_ms, p.predicted_z
            FROM predictions p
            LEFT JOIN labels_exec le
              ON le.event_id=p.event_id AND le.symbol=p.symbol AND le.horizon_s=p.horizon_s
            WHERE le.event_id IS NULL
            ORDER BY p.ts_ms ASC
            LIMIT 20000
            """
        ).fetchall()

        n_ok = 0
        n_skip = 0

        for eid, sym, horizon_s, ts_ms, predicted_z in rows:
            eid = int(eid)
            sym = str(sym)
            horizon_s = int(horizon_s)
            ts_ms = int(ts_ms)
            try:
                pred = float(predicted_z)
            except Exception:
                pred = 0.0

            side = 1 if pred >= 0 else -1
            exit_target_ts_ms = int(ts_ms) + int(horizon_s) * 1000
            if _now_ms() < int(exit_target_ts_ms):
                n_skip += 1
                continue

            entry = _get_px_at_or_before(con, sym, ts_ms)
            exit_ = _get_px_at_or_before(con, sym, exit_target_ts_ms)

            if not entry or not exit_:
                n_skip += 1
                continue

            px_in, ts_in = entry
            px_out, ts_out = exit_

            if px_in <= 0 or px_out <= 0:
                n_skip += 1
                continue

            # This is a synthetic execution label based on market data, not a real
            # fill-derived attribution. The fill override job can replace it later.
            # gross return in trade direction
            gross_ret = (px_out / px_in - 1.0) * float(side)

            meta = _get_quote_meta_at_or_before(con, sym, ts_ms)
            bid = meta.get("bid")
            ask = meta.get("ask")
            spr = meta.get("spread")

            costs = estimate_cost_bps(px=px_in, bid=bid, ask=ask, side=side)
            net_ret = apply_cost_to_return(gross_ret, costs["total_cost_bps"], side=side)

            extra = {
                "entry_ts_px": ts_in,
                "exit_ts_px": ts_out,
                "exit_target_ts_ms": int(exit_target_ts_ms),
                "timestamp_safe": bool(int(ts_out) <= int(exit_target_ts_ms) <= _now_ms()),
                "quote_meta": meta,
            }

            con.execute(
                """
                INSERT OR REPLACE INTO labels_exec(
                  event_id, symbol, horizon_s, ts_ms,
                  side, gross_ret, net_ret,
                  gross_z, net_z,
                  mid_in, mid_out, spread_in,
                  fees_bps, slippage_bps, spread_bps, total_cost_bps,
                  extra_json
                )
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    eid, sym, horizon_s, ts_ms,
                    int(side), float(gross_ret), float(net_ret),
                    None, None,  # z computed in Phase 5D
                    float(px_in), float(px_out),
                    (float(spr) if spr is not None else None),
                    float(costs["fees_bps"]),
                    float(costs["slippage_bps"]),
                    float(costs["spread_bps"]),
                    float(costs["total_cost_bps"]),
                    json.dumps(extra),
                ),
            )
            ctx = load_prediction_label_context(
                con,
                event_id=int(eid),
                symbol=str(sym),
                horizon_s=int(horizon_s),
            )
            trace = load_execution_trace(
                con,
                event_id=int(eid),
                symbol=str(sym),
                horizon_s=int(horizon_s),
                label_ts_ms=int(ts_ms),
                exit_ts_ms=int(exit_target_ts_ms),
                prediction_id=ctx.get("prediction_id"),
                source_alert_id=ctx.get("source_alert_id"),
            )
            raw_forward_ret = (float(px_out) / float(px_in)) - 1.0
            artifact = build_net_after_cost_label(
                event_id=int(eid),
                symbol=str(sym),
                horizon_s=int(horizon_s),
                label_ts_ms=int(ts_ms),
                side=int(side),
                gross_return=float(gross_ret),
                net_return=float(net_ret),
                realized_forward_return=float(raw_forward_ret),
                source="synthetic_market_data",
                realized=0,
                entry_ts_ms=int(ts_in),
                exit_ts_ms=int(ts_out),
                costs=costs,
                context=ctx,
                execution_trace=trace,
                metadata={
                    "labels_exec_source": "heuristic",
                    "entry_price": float(px_in),
                    "exit_price": float(px_out),
                    "exit_target_ts_ms": int(exit_target_ts_ms),
                    "quote_meta": meta,
                },
            )
            upsert_net_after_cost_label(con, artifact)
            n_ok += 1

        con.commit()
        print(f"[labels_exec] wrote={n_ok} skipped={n_skip} total={len(rows)}")

    finally:
        con.close()


if __name__ == "__main__":
    main()
