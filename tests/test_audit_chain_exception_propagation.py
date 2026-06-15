from __future__ import annotations

from decimal import Decimal

import pytest

from tests.audit_chain_test_utils import connect, create_audit_table

from engine.audit.chain import append_chain_row


def test_append_chain_row_propagates_canonical_serialization_error() -> None:
    con = connect()
    create_audit_table(con)

    class BrokenScalar:
        def item(self):
            raise RuntimeError("canonical serialization failed")

    with pytest.raises(RuntimeError, match="canonical serialization failed") as exc_info:
        append_chain_row("audit_test", {"ts_ms": 1, "actor": BrokenScalar()}, con)

    assert not isinstance(exc_info.value, UnboundLocalError)
    assert con.execute("SELECT COUNT(*) FROM audit_test").fetchone()[0] == 0


def test_append_chain_row_propagates_decimal_nan_error_without_unbound_local() -> None:
    con = connect()
    create_audit_table(con)

    with pytest.raises(ValueError, match="non_finite_decimal") as exc_info:
        append_chain_row("audit_test", {"ts_ms": 1, "amount": Decimal("NaN")}, con)

    assert not isinstance(exc_info.value, UnboundLocalError)
    assert con.execute("SELECT COUNT(*) FROM audit_test").fetchone()[0] == 0
