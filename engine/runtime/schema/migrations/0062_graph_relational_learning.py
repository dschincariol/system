"""Create shadow graph/relational learning snapshot tables."""

from __future__ import annotations


id = 62
description = "shadow graph relational learning snapshots"


def up(conn) -> None:
    from engine.strategy.graph_relational import ensure_graph_relational_schema

    ensure_graph_relational_schema(conn)
