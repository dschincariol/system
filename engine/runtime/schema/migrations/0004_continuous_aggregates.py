"""Dashboard continuous aggregates and refresh policies."""

from __future__ import annotations

import os

id = 4
description = "dashboard continuous aggregates"

MINUTE_MS = 60_000
HOUR_MS = 3_600_000
DAY_MS = 86_400_000
YEAR_MS = 365 * DAY_MS
THREE_YEARS_MS = 3 * YEAR_MS


CONTINUOUS_AGGREGATES = (
    "cagg_prices_5m",
    "cagg_prices_1h",
    "cagg_decision_volume",
    "cagg_runtime_metrics_5m",
)


def _env_truthy(name: str) -> bool:
    return str(os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def _relation_exists(conn, relation_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(?)", (str(relation_name),)).fetchone()
    return bool(row and row[0] is not None)


def _is_hypertable(conn, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM timescaledb_information.hypertables
        WHERE hypertable_schema = ANY (current_schemas(false))
          AND hypertable_name = ?
        """,
        (str(table_name),),
    ).fetchone()
    return bool(row)


def _try_execute(conn, savepoint_name: str, sql: str, params=None) -> bool:
    conn.execute(f"SAVEPOINT {savepoint_name}")
    try:
        conn.execute(sql, params)
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        return True
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        return False


def _create_prices_5m(conn) -> None:
    if _relation_exists(conn, "cagg_prices_5m") or not _is_hypertable(conn, "prices"):
        return
    created = _try_execute(
        conn,
        "sp_cagg_prices_5m_first_last",
        """
        CREATE MATERIALIZED VIEW cagg_prices_5m
        WITH (timescaledb.continuous) AS
        SELECT
          time_bucket(300000, ts_ms) AS bucket,
          symbol,
          first(COALESCE(price, px), ts_ms) AS open,
          max(COALESCE(price, px)) AS high,
          min(COALESCE(price, px)) AS low,
          last(COALESCE(price, px), ts_ms) AS close,
          count(*)::bigint AS volume
        FROM prices
        GROUP BY bucket, symbol
        WITH NO DATA
        """,
    )
    if not created:
        conn.execute(
            """
            CREATE MATERIALIZED VIEW cagg_prices_5m
            WITH (timescaledb.continuous) AS
            SELECT
              time_bucket(300000, ts_ms) AS bucket,
              symbol,
              min(COALESCE(price, px)) AS open,
              max(COALESCE(price, px)) AS high,
              min(COALESCE(price, px)) AS low,
              max(COALESCE(price, px)) AS close,
              count(*)::bigint AS volume
            FROM prices
            GROUP BY bucket, symbol
            WITH NO DATA
            """
        )


def _create_prices_1h(conn) -> None:
    if _relation_exists(conn, "cagg_prices_1h") or not _relation_exists(conn, "cagg_prices_5m"):
        return
    created = _try_execute(
        conn,
        "sp_cagg_prices_1h_first_last",
        """
        CREATE MATERIALIZED VIEW cagg_prices_1h
        WITH (timescaledb.continuous) AS
        SELECT
          time_bucket(3600000, bucket) AS bucket,
          symbol,
          first(open, bucket) AS open,
          max(high) AS high,
          min(low) AS low,
          last(close, bucket) AS close,
          sum(volume)::bigint AS volume
        FROM cagg_prices_5m
        GROUP BY time_bucket(3600000, bucket), symbol
        WITH NO DATA
        """,
    )
    if not created:
        conn.execute(
            """
            CREATE MATERIALIZED VIEW cagg_prices_1h
            WITH (timescaledb.continuous) AS
            SELECT
              time_bucket(3600000, ts_ms) AS bucket,
              symbol,
              min(COALESCE(price, px)) AS open,
              max(COALESCE(price, px)) AS high,
              min(COALESCE(price, px)) AS low,
              max(COALESCE(price, px)) AS close,
              count(*)::bigint AS volume
            FROM prices
            GROUP BY bucket, symbol
            WITH NO DATA
            """
        )


def _create_decision_volume(conn) -> None:
    if _relation_exists(conn, "cagg_decision_volume") or not _is_hypertable(conn, "decision_log"):
        return
    conn.execute(
        """
        CREATE MATERIALIZED VIEW cagg_decision_volume
        WITH (timescaledb.continuous) AS
        SELECT
          time_bucket(3600000, ts_ms) AS bucket,
          COALESCE(extra_json->>'family', explain_json->>'family', model_name, 'unknown') AS family,
          count(*)::bigint AS decisions
        FROM decision_log
        GROUP BY bucket, family
        WITH NO DATA
        """
    )


def _create_runtime_metrics_5m(conn) -> None:
    if _relation_exists(conn, "cagg_runtime_metrics_5m") or not _is_hypertable(conn, "runtime_metrics"):
        return
    created = _try_execute(
        conn,
        "sp_cagg_runtime_p99",
        """
        CREATE MATERIALIZED VIEW cagg_runtime_metrics_5m
        WITH (timescaledb.continuous) AS
        SELECT
          time_bucket(300000, ts_ms) AS bucket,
          metric,
          avg(value_num) AS mean_value,
          percentile_cont(0.99) WITHIN GROUP (ORDER BY value_num) AS p99_value,
          count(*)::bigint AS samples
        FROM runtime_metrics
        WHERE value_num IS NOT NULL
        GROUP BY bucket, metric
        WITH NO DATA
        """,
    )
    if not created:
        conn.execute(
            """
            CREATE MATERIALIZED VIEW cagg_runtime_metrics_5m
            WITH (timescaledb.continuous) AS
            SELECT
              time_bucket(300000, ts_ms) AS bucket,
              metric,
              avg(value_num) AS mean_value,
              max(value_num) AS p99_value,
              count(*)::bigint AS samples
            FROM runtime_metrics
            WHERE value_num IS NOT NULL
            GROUP BY bucket, metric
            WITH NO DATA
            """
        )


def _add_refresh_policy(conn, view_name: str, start_offset_ms: int, end_offset_ms: int, schedule: str) -> None:
    if not _relation_exists(conn, view_name):
        return
    _try_execute(
        conn,
        f"sp_refresh_{view_name}",
        """
        SELECT add_continuous_aggregate_policy(
          ?::regclass,
          start_offset => ?::bigint,
          end_offset => ?::bigint,
          schedule_interval => ?::interval,
          if_not_exists => TRUE
        )
        """,
        (str(view_name), int(start_offset_ms), int(end_offset_ms), str(schedule)),
    )


def _add_retention_policy(conn, view_name: str, retain_ms: int) -> None:
    if not _relation_exists(conn, view_name):
        return
    _try_execute(
        conn,
        f"sp_retain_{view_name}",
        "SELECT add_retention_policy(?::regclass, ?::bigint, if_not_exists => TRUE)",
        (str(view_name), int(retain_ms)),
    )


def _install_policies(conn) -> None:
    _add_refresh_policy(conn, "cagg_prices_5m", DAY_MS, 5 * MINUTE_MS, "1 minute")
    _add_refresh_policy(conn, "cagg_prices_1h", THREE_YEARS_MS, HOUR_MS, "5 minutes")
    _add_refresh_policy(conn, "cagg_decision_volume", THREE_YEARS_MS, HOUR_MS, "5 minutes")
    _add_refresh_policy(conn, "cagg_runtime_metrics_5m", 180 * DAY_MS, 5 * MINUTE_MS, "1 minute")

    _add_retention_policy(conn, "cagg_prices_5m", YEAR_MS)
    _add_retention_policy(conn, "cagg_prices_1h", YEAR_MS)
    _add_retention_policy(conn, "cagg_decision_volume", THREE_YEARS_MS)
    _add_retention_policy(conn, "cagg_runtime_metrics_5m", 180 * DAY_MS)


def up(conn) -> None:  # type: ignore[no-redef]
    if _env_truthy("TRADING_UNIT_TEST_SCHEMA_FAST"):
        return
    _create_prices_5m(conn)
    _create_prices_1h(conn)
    _create_decision_volume(conn)
    _create_runtime_metrics_5m(conn)
    _install_policies(conn)


__all__ = ["CONTINUOUS_AGGREGATES", "up"]
