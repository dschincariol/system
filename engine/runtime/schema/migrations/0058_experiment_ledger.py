"""Add append-only generated experiment ledger."""

from __future__ import annotations

id = 58
description = "false-discovery experiment ledger"


def up(conn) -> None:
    from engine.strategy.experiment_ledger import ensure_experiment_ledger_schema

    ensure_experiment_ledger_schema(conn)

