"""
FILE: ingest_options.py

Job entrypoint or scheduled task for `ingest_options`.

README:
- Source: Polygon options snapshots normalized into ``options_chain_v2`` and
  mirrored legacy option-chain rows where available.
- Cadence: registered in ``engine/runtime/job_registry.py`` with an explicit
  300 second cadence; individual symbols degrade independently on provider
  errors.
- Availability lag: raw contract snapshots and derived symbol features use the
  provider snapshot timestamp as availability. Feature consumers must require
  ``snapshot_ts_ms <= asof_ts_ms``.
- Caveats: GEX is a naive dealer-positioning convention used for volatility
  regime conditioning, not direction. Flow imbalance is a snapshot proxy built
  from volume and OI changes, not trade-level signed order flow.
"""

import os
import json
import time
import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.storage import (
    connect,
    init_db,
    acquire_job_lock,
    release_job_lock,
    touch_job_lock,
    put_job_heartbeat,
)
from engine.runtime.ingestion_status import record_pipeline_status

from engine.data.options.options_polygon import fetch_options_chain_snapshot
from engine.data.options_features import emit_options_feature_events, materialize_options_features
from engine.strategy.options_surface_intelligence import compute_options_surface_intelligence


JOB_NAME = "ingest_options"
OWNER = os.environ.get(
    "JOB_OWNER",
    os.environ.get("COMPUTERNAME", os.environ.get("HOSTNAME", "unknown")),
)
PID = os.getpid()

LOCK_STALE_AFTER_S = int(os.environ.get("JOB_LOCK_STALE_AFTER_S", "180"))
HEARTBEAT_EVERY_S = float(os.environ.get("HEARTBEAT_EVERY_S", "10.0"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Options snapshot controls
OPT_LIMIT = int(os.environ.get("OPTIONS_SNAPSHOT_LIMIT", "250"))
OPT_MAX_PAGES = int(os.environ.get("OPTIONS_SNAPSHOT_MAX_PAGES", "4"))
OPT_TIMEOUT_S = int(os.environ.get("OPTIONS_SNAPSHOT_TIMEOUT_S", "8"))
OPT_SYMBOL_LIMIT = int(os.environ.get("OPTIONS_UNDERLYING_LIMIT", "20"))

# Optional filters
OPT_CONTRACT_TYPE = os.environ.get("OPTIONS_CONTRACT_TYPE", "").strip().lower() or None  # "call" | "put"
OPT_EXPIRATION_DATE = os.environ.get("OPTIONS_EXPIRATION_DATE", "").strip() or None      # "YYYY-MM-DD"


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [ingest_options] %(message)s",
)
LOGGER = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOGGER,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _get_underlyings(con, limit: int):
    rows = con.execute(
        """
        SELECT symbol
        FROM symbols
        WHERE status IN ('ACTIVE','WATCH')
        ORDER BY score DESC, updated_ts_ms DESC
        LIMIT ?
        """,
        (int(limit),),
    ).fetchall() or []
    return [str(r[0]) for r in rows if r and r[0]]


def _legacy_call_put(contract_type):
    c = str(contract_type or "").strip().lower()
    if c == "call":
        return "C"
    if c == "put":
        return "P"
    return None


def _put_options_rows(con, rows):
    # `options_chain_v2` is the richer canonical schema. We also mirror into the
    # legacy table so older analytics and dashboards keep working during migration.
    con.executemany(
        """
        INSERT INTO options_chain_v2(
          ts_ms,
          underlying, contract, expiration, contract_type, strike,
          iv, open_interest, volume,
          bid, ask,
          delta, gamma, theta, vega,
          source
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(contract, ts_ms) DO UPDATE SET
          underlying=excluded.underlying,
          expiration=excluded.expiration,
          contract_type=excluded.contract_type,
          strike=excluded.strike,
          iv=excluded.iv,
          open_interest=excluded.open_interest,
          volume=excluded.volume,
          bid=excluded.bid,
          ask=excluded.ask,
          delta=excluded.delta,
          gamma=excluded.gamma,
          theta=excluded.theta,
          vega=excluded.vega,
          source=excluded.source
        """,
        [
            (
                int(r["ts_ms"]),
                str(r["underlying"]),
                str(r["contract"]),
                (str(r["expiration"]) if r.get("expiration") is not None else None),
                (str(r["contract_type"]) if r.get("contract_type") is not None else None),
                (float(r["strike"]) if r.get("strike") is not None else None),
                (float(r["iv"]) if r.get("iv") is not None else None),
                (float(r["open_interest"]) if r.get("open_interest") is not None else None),
                (float(r["volume"]) if r.get("volume") is not None else None),
                (float(r["bid"]) if r.get("bid") is not None else None),
                (float(r["ask"]) if r.get("ask") is not None else None),
                (float(r["delta"]) if r.get("delta") is not None else None),
                (float(r["gamma"]) if r.get("gamma") is not None else None),
                (float(r["theta"]) if r.get("theta") is not None else None),
                (float(r["vega"]) if r.get("vega") is not None else None),
                str(r.get("source") or "polygon"),
            )
            for r in (rows or [])
            if r.get("contract")
        ],
    )

    legacy_rows = []
    for r in (rows or []):
        expiry = r.get("expiration")
        strike = r.get("strike")
        call_put = _legacy_call_put(r.get("contract_type"))
        if expiry is None or strike is None or not call_put:
            continue
        legacy_rows.append(
            (
                int(r["ts_ms"]),
                str(r["underlying"]),
                str(expiry),
                float(strike),
                str(call_put),
                (float(r["iv"]) if r.get("iv") is not None else None),
                (int(float(r["open_interest"])) if r.get("open_interest") is not None else None),
                (int(float(r["volume"])) if r.get("volume") is not None else None),
                str(r.get("source") or "polygon"),
            )
        )

    if legacy_rows:
        con.executemany(
            """
            INSERT INTO options_chain(
              ts_ms, symbol, expiry, strike, call_put,
              iv, open_interest, volume, source
            )
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, expiry, strike, call_put, ts_ms) DO UPDATE SET
              iv=excluded.iv,
              open_interest=excluded.open_interest,
              volume=excluded.volume,
              source=excluded.source
            """,
            legacy_rows,
        )


def main():
    if os.environ.get("ENGINE_SUPERVISED") != "1":
        print("options_poll must be launched by supervisor")
        raise SystemExit(1)

    init_db()

    if not acquire_job_lock(JOB_NAME, OWNER, PID, ttl_s=LOCK_STALE_AFTER_S):
        logging.error("another instance is holding the job lock; exiting")
        raise SystemExit(2)

    started_ms = int(time.time() * 1000)
    last_hb_s = 0.0
    total_rows = 0
    total_errs = 0
    total_surface = 0
    total_feature_rows = 0
    total_event_rows = 0
    last_ingested_ts_ms = started_ms

    try:
        try:
            now_s = time.time()
            if (now_s - last_hb_s) >= HEARTBEAT_EVERY_S:
                touch_job_lock(JOB_NAME, OWNER, PID)
                put_job_heartbeat(
                    JOB_NAME,
                    OWNER,
                    PID,
                    extra_json=json.dumps(
                        {
                            "limit": OPT_LIMIT,
                            "max_pages": OPT_MAX_PAGES,
                            "timeout_s": OPT_TIMEOUT_S,
                            "underlying_limit": OPT_SYMBOL_LIMIT,
                            "contract_type": OPT_CONTRACT_TYPE,
                            "expiration_date": OPT_EXPIRATION_DATE,
                        },
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                )
                last_hb_s = now_s

            con = connect()
            try:
                underlyings = _get_underlyings(con, OPT_SYMBOL_LIMIT)
            finally:
                con.close()

            feature_rows = []
            conw = connect()
            try:
                for u in underlyings:
                    # This job is a snapshot enrich step, not the primary live options
                    # feed. It tolerates per-underlying failures and keeps going.
                    rows, err = fetch_options_chain_snapshot(
                        underlying=u,
                        contract_type=OPT_CONTRACT_TYPE,
                        expiration_date=OPT_EXPIRATION_DATE,
                        limit=OPT_LIMIT,
                        max_pages=OPT_MAX_PAGES,
                        timeout_s=OPT_TIMEOUT_S,
                    )
                    if err:
                        total_errs += 1
                        _warn_nonfatal(
                            "INGEST_OPTIONS_UNDERLYING_FETCH_FAILED",
                            RuntimeError(str(err)),
                            once_key=f"underlying_fetch_failed:{u}",
                            underlying=str(u),
                            error_text=str(err),
                        )

                    if rows:
                        _put_options_rows(conw, rows)
                        total_rows += len(rows)

                # Surface intelligence is derived immediately after raw chain writes
                # so downstream strategy code can consume a coherent same-run snapshot.
                surface_stats = compute_options_surface_intelligence(conw, underlyings=underlyings)
                total_surface = int(surface_stats.get("updated", 0))

                feature_stats = materialize_options_features(conw, underlyings=underlyings)
                feature_rows = list(feature_stats.get("rows") or [])
                total_feature_rows = int(feature_stats.get("snapshots", 0))
                last_ingested_ts_ms = int(
                    feature_stats.get("ts_ms")
                    or surface_stats.get("ts_ms")
                    or started_ms
                )
                conw.commit()
            finally:
                try:
                    conw.close()
                except Exception as e:
                    _warn_nonfatal(
                        "INGEST_OPTIONS_WRITE_CONN_CLOSE_FAILED",
                        e,
                        once_key="ingest_options_write_conn_close",
                    )

            event_stats = emit_options_feature_events(feature_rows)
            total_event_rows = int(event_stats.get("events", 0))
            last_ingested_ts_ms = int(event_stats.get("ts_ms") or last_ingested_ts_ms)
        except Exception as e:
            dur_ms = int(time.time() * 1000) - started_ms
            record_pipeline_status(
                JOB_NAME,
                ok=False,
                raw_rows=int(total_rows),
                event_rows=int(total_event_rows),
                last_ingested_ts_ms=int(last_ingested_ts_ms or started_ms),
                error=str(e),
                meta={
                    "surface_rows": int(total_surface),
                    "feature_rows": int(total_feature_rows),
                    "underlying_limit": int(OPT_SYMBOL_LIMIT),
                    "provider": "polygon_snapshot",
                },
                latency_ms=int(dur_ms),
            )
            raise

        dur_ms = int(time.time() * 1000) - started_ms
        record_pipeline_status(
            JOB_NAME,
            ok=True,
            raw_rows=int(total_rows),
            event_rows=int(total_event_rows),
            last_ingested_ts_ms=int(last_ingested_ts_ms or started_ms),
            meta={
                "surface_rows": int(total_surface),
                "feature_rows": int(total_feature_rows),
                "underlying_limit": int(OPT_SYMBOL_LIMIT),
                "provider": "polygon_snapshot",
            },
            latency_ms=int(dur_ms),
        )
        logging.info(
            "underlyings=%s rows=%s surface=%s feature_rows=%s events=%s errs=%s dur_ms=%s",
            len(underlyings),
            total_rows,
            total_surface,
            total_feature_rows,
            total_event_rows,
            total_errs,
            dur_ms,
        )

    finally:
        try:
            release_job_lock(JOB_NAME, OWNER, PID)
        except Exception as e:
            _warn_nonfatal(
                "INGEST_OPTIONS_RELEASE_LOCK_FAILED",
                e,
                once_key="ingest_options_release_lock",
            )


if __name__ == "__main__":
    main()
