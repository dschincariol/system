"""
FILE: prod_selftest.py

Runtime subsystem module for `prod_selftest`.
"""

# prod_selftest.py
"""
Hard startup self-test (fail-fast).

Runs:
- DB init
- core module imports
- inserts a synthetic event
- runs process_events once to embed + predict
- verifies event_embeddings + predictions rows exist

Usage:
  python prod_selftest.py
"""

import json
import time
import traceback

from engine.runtime.storage import connect, init_db, run_write_txn
from engine.strategy.validation import init_validation_db


def _now_ms() -> int:
    return int(time.time() * 1000)


def _require(cond: bool, msg: str):
    if not cond:
        raise RuntimeError(msg)


def main() -> int:
    # The self-test follows deployment dependency order: schema, imports,
    # synthetic write, pipeline execution, then verification.
    # 1) DB schema
    init_db()
    init_validation_db()

    # 2) Import core modules (crash here => broken deployment)
    import engine.strategy.predictor  # noqa: F401
    import engine.strategy.model_v2   # noqa: F401
    import engine.strategy.learning   # noqa: F401
    import engine.runtime.alerts     # noqa: F401
    import engine.execution.kill_switch  # noqa: F401
    import engine.strategy.capital_guard  # noqa: F401

    # 3) Insert synthetic event (unique key)
    observed_ts_ms = _now_ms()
    # Prioritize the synthetic event ahead of normal backlog so one bounded
    # process_events pass can still reach it in staging.
    ts_ms = 1
    event_key = f"__selftest__{observed_ts_ms}"
    title = f"SELFTEST EVENT {observed_ts_ms}"
    body = "selftest body"
    meta_json = json.dumps(
        {
            "pipeline_timing": {
                "db_observed_ts_ms": int(observed_ts_ms),
                "db_write_ts_ms": int(observed_ts_ms),
                "ingestion_to_db_latency_ms": 0,
            },
            "selftest": {
                "observed_ts_ms": int(observed_ts_ms),
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )

    row = run_write_txn(
        lambda con: (
            con.execute(
                """
                INSERT OR IGNORE INTO events(ts_ms, source, title, body, url, event_key, meta_json)
                VALUES (?,?,?,?,?,?,?)
                """,
                (int(ts_ms), "selftest", title, body, "", event_key, meta_json),
            ),
            con.execute("SELECT id FROM events WHERE event_key=?", (event_key,)).fetchone(),
        )[-1],
        table="events",
        operation="prod_selftest_insert_event",
        context={"event_key": str(event_key)},
    )

    _require(row is not None and row[0] is not None, "selftest: failed to insert event")
    eid = int(row[0])

    # 4) Run event processing once (embedding + predictions write)
    import engine.data.jobs.process_events as process_events
    process_events.main()

    # 5) Verify embedding + prediction rows exist
    con = connect()
    try:
        emb = con.execute("SELECT dim, length(vec) FROM event_embeddings WHERE event_id=?", (eid,)).fetchone()
        _require(emb is not None, "selftest: missing event_embeddings row")
        _require(int(emb[0] or 0) > 0, "selftest: embedding dim invalid")

        pred = con.execute("SELECT COUNT(*) FROM predictions WHERE event_id=?", (eid,)).fetchone()
        n_pred = int(pred[0] or 0) if pred else 0
        _require(n_pred > 0, "selftest: missing predictions rows")
    finally:
        try:
            con.close()
        except Exception as e:
            try:
                from engine.runtime.failure_diagnostics import log_failure
                from engine.runtime.logging import get_logger

                log_failure(
                    get_logger("engine.runtime.prod_selftest"),
                    event="prod_selftest_connection_close_failed",
                    code="PROD_SELFTEST_CONNECTION_CLOSE_FAILED",
                    message="prod_selftest_connection_close_failed",
                    error=e,
                    level=30,
                    component="engine.runtime.prod_selftest",
                    persist=False,
                )
            except Exception:
                raise

    out = {
        "status": "ok",
        "event_id": eid,
        "observed_ts_ms": int(observed_ts_ms),
        "ts_ms": int(ts_ms),
    }
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as e:
        err = {
            "status": "error",
            "error": str(e),
            "trace": traceback.format_exc(),
        }
        print(json.dumps(err, indent=2, sort_keys=True))
        raise SystemExit(2)
