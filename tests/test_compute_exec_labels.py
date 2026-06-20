from __future__ import annotations

from ops.compute_exec_labels import _get_quote_meta_at_or_before


class _Cursor:
    def __init__(self, row=None, rows=None):
        self._row = row
        self._rows = list(rows or [])

    def fetchone(self):
        return self._row

    def fetchall(self):
        return list(self._rows)


class _FakeConnection:
    def __init__(self, *, quote_row=None, prices_columns=None):
        self.quote_row = quote_row
        self.prices_columns = list(prices_columns or ["ts_ms", "symbol", "price", "px", "source"])
        self.statements: list[str] = []

    def execute(self, sql, params=None):
        text = str(sql)
        self.statements.append(text)
        lowered = text.lower()
        if "sqlite_master" in lowered:
            table_name = str((params or ("",))[0])
            return _Cursor(row=(1,) if table_name == "price_quotes" and self.quote_row is not None else None)
        if "select bid, ask, spread" in lowered:
            return _Cursor(row=self.quote_row)
        if "pragma table_info(prices)" in lowered:
            return _Cursor(rows=[(idx, name, "TEXT", 0, None, 0) for idx, name in enumerate(self.prices_columns)])
        if "select extra_json" in lowered:
            raise AssertionError("prices.extra_json should not be queried when the column is absent")
        return _Cursor()


def test_quote_meta_uses_price_quotes_when_available():
    con = _FakeConnection(quote_row=(100.0, 100.2, 0.2))

    meta = _get_quote_meta_at_or_before(con, "AMD", 123)

    assert meta == {"bid": 100.0, "ask": 100.2, "spread": 0.2}


def test_quote_meta_does_not_probe_missing_prices_extra_json():
    con = _FakeConnection(quote_row=None)

    meta = _get_quote_meta_at_or_before(con, "AMD", 123)

    assert meta == {}
    assert not any("SELECT extra_json" in statement for statement in con.statements)
