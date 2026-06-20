"""Production drift and calibration monitoring latest-state metrics."""

from __future__ import annotations

id = 59
description = "production drift calibration monitoring metrics"


def up(conn) -> None:
    from engine.strategy.production_monitoring import ensure_production_monitoring_schema

    ensure_production_monitoring_schema(conn)
