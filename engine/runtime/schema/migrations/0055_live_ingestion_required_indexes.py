"""Required schema-contract indexes enforced by production preflight."""

from __future__ import annotations

id = 55
description = "required production contract indexes"


def _table_has_columns(conn, table_name: str, *column_names: str) -> bool:
    if not column_names:
        return True
    for column_name in column_names:
        row = conn.execute(
            """
            SELECT 1
            FROM pg_attribute a
            JOIN pg_class c
              ON c.oid = a.attrelid
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE n.nspname = current_schema()
              AND c.relname = ?
              AND a.attname = ?
              AND NOT a.attisdropped
            """,
            (str(table_name), str(column_name)),
        ).fetchone()
        if not row:
            return False
    return True


def _column_names(columns_sql: str) -> tuple[str, ...]:
    return tuple(
        part.strip().strip('"').split()[0].strip('"')
        for part in str(columns_sql).split(",")
        if part.strip()
    )


def _create_index(
    conn,
    table_name: str,
    index_name: str,
    columns_sql: str,
    *,
    unique: bool = False,
    where_sql: str | None = None,
) -> None:
    if not _table_has_columns(
        conn,
        table_name,
        *_column_names(columns_sql),
    ):
        return
    qualifier = "UNIQUE " if unique else ""
    where = f" WHERE {where_sql}" if where_sql else ""
    conn.execute(f"CREATE {qualifier}INDEX IF NOT EXISTS {index_name} ON {table_name} ({columns_sql}){where}")


def up(conn) -> None:
    _create_index(conn, "prices", "idx_prices_symbol_ts", "symbol, ts_ms")
    _create_index(conn, "price_quotes", "idx_price_quotes_symbol_ts", "symbol, ts_ms")
    _create_index(conn, "price_quotes", "idx_price_quotes_ts", "ts_ms")
    _create_index(conn, "price_quotes_raw", "idx_price_quotes_raw_symbol_ts", "symbol, ts_ms")
    _create_index(conn, "price_quotes_raw", "idx_price_quotes_raw_provider_ts", "provider, ts_ms")
    _create_index(conn, "price_quotes_raw", "idx_price_quotes_raw_ts", "ts_ms")
    _create_index(
        conn,
        "price_quotes_raw",
        "idx_price_quotes_raw_provider_event_ts",
        "provider, event_ts_ms",
    )
    _create_index(conn, "price_provider_health", "idx_price_provider_health_ts", "ts_ms")
    _create_index(conn, "price_provider_health", "idx_price_provider_health_provider", "provider")
    _create_index(conn, "ingestion_pipeline_health", "idx_ingestion_pipeline_health_ts", "ts_ms")
    _create_index(conn, "ingestion_pipeline_health", "idx_ingestion_pipeline_health_pipeline", "pipeline")
    _create_index(
        conn,
        "options_symbol_ingestion_state",
        "idx_options_symbol_ingestion_disabled",
        "disabled_until_ts_ms",
    )

    _create_index(conn, "alerts", "uq_alerts_id_prediction_lineage", "id, prediction_id", unique=True)
    _create_index(conn, "temporal_model_eval", "idx_temporal_model_eval_ts", "ts_ms")

    _create_index(conn, "execution_orders", "idx_execution_orders_submit_ts", "submit_ts_ms")
    _create_index(conn, "execution_orders", "idx_execution_orders_source_alert", "source_alert_id")
    _create_index(
        conn,
        "execution_orders",
        "idx_execution_orders_portfolio_order_submit_ts",
        "portfolio_orders_id, submit_ts_ms",
    )
    _create_index(
        conn,
        "execution_orders",
        "idx_execution_orders_prediction_submit_ts",
        "prediction_id, submit_ts_ms",
    )
    _create_index(
        conn,
        "execution_orders",
        "idx_execution_orders_source_alert_prediction_submit_ts",
        "source_alert_id, prediction_id, submit_ts_ms",
    )
    _create_index(conn, "execution_orders", "idx_execution_orders_model_submit_ts", "model_id, submit_ts_ms")
    _create_index(conn, "execution_orders", "idx_execution_orders_symbol_submit_ts", "symbol, submit_ts_ms")
    _create_index(conn, "execution_orders", "idx_execution_orders_order_uid", "order_uid")

    _create_index(conn, "execution_fills", "idx_execution_fills_ts", "fill_ts_ms")
    _create_index(conn, "execution_fills", "idx_execution_fills_client", "client_order_id")
    _create_index(conn, "execution_fills", "idx_execution_fills_model_ts", "model_id, fill_ts_ms")
    _create_index(
        conn,
        "execution_fills",
        "idx_execution_fills_model_symbol_ts",
        "model_id, symbol, fill_ts_ms, id",
    )
    _create_index(
        conn,
        "execution_fills",
        "idx_execution_fills_portfolio_order_ts",
        "portfolio_orders_id, fill_ts_ms",
    )
    _create_index(
        conn,
        "execution_fills",
        "idx_execution_fills_source_alert_ts",
        "source_alert_id, fill_ts_ms",
    )
    _create_index(conn, "execution_fills", "idx_execution_fills_prediction_ts", "prediction_id, fill_ts_ms")
    _create_index(
        conn,
        "execution_fills",
        "idx_execution_fills_source_alert_prediction_ts",
        "source_alert_id, prediction_id, fill_ts_ms",
    )
    _create_index(conn, "execution_fills", "idx_execution_fills_symbol_ts", "symbol, fill_ts_ms")
    _create_index(conn, "execution_fills", "idx_execution_fills_fill_id", "fill_id")
    _create_index(
        conn,
        "execution_fills",
        "uq_execution_fills_client_fillid",
        "client_order_id, fill_id",
        unique=True,
        where_sql="fill_id IS NOT NULL",
    )

    _create_index(conn, "pnl_attribution", "idx_pnl_attribution_prediction_ts", "prediction_id, ts_ms DESC")
    _create_index(conn, "pnl_attribution", "idx_pnl_attribution_ts", "ts_ms DESC")
    _create_index(conn, "pnl_attribution", "idx_pnl_attribution_model_ts", "model_id, ts_ms DESC")
