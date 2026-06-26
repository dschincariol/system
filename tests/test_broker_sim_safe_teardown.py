from __future__ import annotations

import importlib
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

pytestmark = pytest.mark.safety_critical


def _reload_broker_sim():
    import engine.execution.broker_sim as broker_sim

    return importlib.reload(broker_sim)


class _ClosedRaisingConnection:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.close_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1
        raise sqlite3.ProgrammingError("Cannot operate on a closed database.")

    def close(self) -> None:
        self.close_calls += 1
        raise sqlite3.ProgrammingError("Cannot operate on a closed database.")


class _HealthyConnection:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.close_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class _MarkedClosedConnection:
    closed = True

    def __init__(self) -> None:
        self.commit_calls = 0
        self.close_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.close_calls += 1


class _BrokenCommitConnection:
    def __init__(self) -> None:
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1
        raise sqlite3.OperationalError("disk I/O error")


def test_closed_sqlite_commit_and_close_are_logged_recoverable_teardown_failures() -> None:
    broker_sim = _reload_broker_sim()
    con = _ClosedRaisingConnection()

    with patch.object(broker_sim, "_warn_nonfatal") as warn_nonfatal:
        committed = broker_sim._safe_commit_connection(
            con,
            context="unit_test_commit",
            once_key="unit_test_commit",
        )
        closed = broker_sim._safe_close_connection(
            con,
            context="unit_test_close",
            once_key="unit_test_close",
        )

    assert committed is False
    assert closed is False
    assert con.commit_calls == 1
    assert con.close_calls == 1
    assert [call.args[1] for call in warn_nonfatal.call_args_list] == [
        "BROKER_SIM_COMMIT_SKIPPED_CLOSED_CONNECTION",
        "BROKER_SIM_CONNECTION_CLOSE_SKIPPED_CLOSED",
    ]


def test_healthy_commit_and_close_are_called_exactly_once() -> None:
    broker_sim = _reload_broker_sim()
    con = _HealthyConnection()

    with patch.object(broker_sim, "_warn_nonfatal") as warn_nonfatal:
        committed = broker_sim._safe_commit_connection(
            con,
            context="unit_test_commit",
            once_key="unit_test_commit",
        )
        closed = broker_sim._safe_close_connection(
            con,
            context="unit_test_close",
            once_key="unit_test_close",
        )

    assert committed is True
    assert closed is True
    assert con.commit_calls == 1
    assert con.close_calls == 1
    warn_nonfatal.assert_not_called()


def test_marked_closed_connection_skips_underlying_commit_and_close() -> None:
    broker_sim = _reload_broker_sim()
    con = _MarkedClosedConnection()

    with patch.object(broker_sim, "_warn_nonfatal") as warn_nonfatal:
        committed = broker_sim._safe_commit_connection(
            con,
            context="unit_test_commit",
            once_key="unit_test_commit",
        )
        closed = broker_sim._safe_close_connection(
            con,
            context="unit_test_close",
            once_key="unit_test_close",
        )

    assert committed is False
    assert closed is False
    assert con.commit_calls == 0
    assert con.close_calls == 0
    assert [call.args[1] for call in warn_nonfatal.call_args_list] == [
        "BROKER_SIM_COMMIT_SKIPPED_CLOSED_CONNECTION",
        "BROKER_SIM_CONNECTION_CLOSE_SKIPPED_CLOSED",
    ]


def test_non_closed_commit_errors_are_logged_without_raising() -> None:
    broker_sim = _reload_broker_sim()
    con = _BrokenCommitConnection()

    with patch.object(broker_sim, "_warn_nonfatal") as warn_nonfatal:
        committed = broker_sim._safe_commit_connection(
            con,
            context="unit_test_commit",
            once_key="unit_test_commit",
        )

    assert committed is False
    assert con.commit_calls == 1
    warn_nonfatal.assert_called_once()
    assert warn_nonfatal.call_args.args[1] == "BROKER_SIM_COMMIT_FAILED"


def test_broker_sim_apply_teardown_sites_use_safe_helpers() -> None:
    source = (REPO_ROOT / "engine" / "execution" / "broker_sim.py").read_text(encoding="utf-8")

    expected_sites = [
        '_safe_commit_connection(con, context="options_lifecycle", once_key="options_lifecycle_commit_account")',
        '_safe_commit_connection(con, context="options_lifecycle", once_key="options_lifecycle_commit_mtm")',
        '_safe_commit_connection(con, context="fallback_write_account", once_key="fallback_write_account_commit")',
        '_safe_commit_connection(con, context="apply_new_portfolio_orders", once_key="apply_orders_commit")',
        '_safe_close_connection(con, context="apply_new_portfolio_orders", once_key="apply_orders_close")',
        '_safe_close_connection(con, context="broker_equity_at", once_key="broker_equity_at_close")',
        '_safe_close_connection(con, context="broker_snapshot", once_key="broker_snapshot_close")',
    ]
    for site in expected_sites:
        assert site in source
