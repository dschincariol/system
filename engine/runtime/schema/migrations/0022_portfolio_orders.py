"""Create the portfolio order lineage table in the runtime schema."""

from __future__ import annotations

id = 22
description = "portfolio order lineage table"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS portfolio_orders (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT NOT NULL,
            model_id TEXT NOT NULL DEFAULT 'baseline',
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            from_side TEXT NOT NULL,
            to_side TEXT NOT NULL,
            from_weight DOUBLE PRECISION NOT NULL,
            to_weight DOUBLE PRECISION NOT NULL,
            delta_weight DOUBLE PRECISION NOT NULL,
            source_alert_id BIGINT,
            prediction_id BIGINT,
            explain_json JSONB
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_portfolio_orders_ts ON portfolio_orders(ts_ms)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_model_ts "
        "ON portfolio_orders(model_id, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_symbol_ts "
        "ON portfolio_orders(symbol, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_source_alert_ts "
        "ON portfolio_orders(source_alert_id, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_prediction_ts "
        "ON portfolio_orders(prediction_id, ts_ms)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_portfolio_orders_source_alert_prediction_ts "
        "ON portfolio_orders(source_alert_id, prediction_id, ts_ms)"
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_portfolio_orders_id_source_prediction_lineage "
        "ON portfolio_orders(id, source_alert_id, prediction_id, ts_ms)"
    )
