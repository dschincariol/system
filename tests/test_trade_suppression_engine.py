from __future__ import annotations

import sqlite3
from unittest.mock import patch

import pytest

from engine.execution import trade_suppression_engine as tse


class _CloseFailureConnection:
    def __init__(self) -> None:
        self.committed = False
        self.closed = False

    def execute(self, sql, params=()):
        del sql, params
        return None

    def commit(self):
        self.committed = True

    def close(self):
        self.closed = True
        raise sqlite3.DatabaseError("close failed")


class _CommitRollbackFailureConnection:
    def __init__(self) -> None:
        self.rollback_called = False

    def execute(self, sql, params=()):
        del sql, params
        return None

    def commit(self):
        raise sqlite3.DatabaseError("commit failed")

    def rollback(self):
        self.rollback_called = True
        raise sqlite3.DatabaseError("rollback failed")


def _hard_block_patches():
    return (
        patch.object(tse, "init_db"),
        patch.object(tse, "_ensure_tables"),
        patch.object(tse, "get_false_positive_streak", return_value=tse.FP_HARD_THRESHOLD),
        patch.object(
            tse,
            "get_execution_degradation_snapshot",
            return_value={
                "mean_slippage": 0.0,
                "p95_slippage": 0.0,
                "mean_latency": 0.0,
                "p95_latency": 0.0,
            },
        ),
        patch.object(tse, "_compute_slippage_stats", return_value={"mean_bps": 0.0, "vol_bps": 0.0, "z": 0.0}),
        patch.object(tse, "_compute_latency_stats", return_value={"mean_ms": 0.0, "var_ms2": 0.0, "var_z": 0.0}),
        patch.object(tse, "set_state"),
        patch.object(tse.LOG, "exception"),
    )


def test_close_failure_keeps_hard_block_result_and_records_health() -> None:
    con = _CloseFailureConnection()
    patches = _hard_block_patches()

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
        with patch.object(tse, "connect", return_value=con):
            with patch.object(tse, "record_component_health") as health:
                result = tse.evaluate_trade_suppression(actor="test", mode="live", broker="sim")

    assert result["hard_block"] is True
    assert result["state"] == "HARD_BLOCK"
    assert con.committed is True
    assert con.closed is True
    health.assert_called_once()
    args, kwargs = health.call_args
    assert args == ("trade_suppression_engine",)
    assert kwargs["ok"] is False
    assert kwargs["status"] == "degraded"
    assert kwargs["detail"] == "close_failed"


def test_commit_and_rollback_failure_fails_closed_after_hard_block_decision() -> None:
    con = _CommitRollbackFailureConnection()
    patches = _hard_block_patches()

    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
        with patch.object(tse, "record_component_health") as health:
            with pytest.raises(sqlite3.DatabaseError, match="commit failed"):
                tse.evaluate_trade_suppression(con=con, actor="test", mode="live", broker="sim")

    assert con.rollback_called is True
    details = [call.kwargs["detail"] for call in health.call_args_list]
    assert details == ["commit_failed", "rollback_after_commit_failed"]
