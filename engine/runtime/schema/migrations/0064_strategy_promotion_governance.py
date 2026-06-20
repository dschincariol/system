"""Governed strategy promotion candidates."""

from __future__ import annotations

id = 64
description = "strategy promotion governance candidates"


def up(conn) -> None:
    from engine.strategy.strategy_promotion_governance import ensure_strategy_promotion_governance_schema

    ensure_strategy_promotion_governance_schema(conn)
