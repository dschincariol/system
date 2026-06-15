from __future__ import annotations

import importlib
import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def fetchall(self):
        return list(self._rows)


class _Row(dict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._keys = tuple(kwargs.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return dict.__getitem__(self, self._keys[key])
        return dict.__getitem__(self, key)

    def keys(self):
        return self._keys


class _ShortFeatureCon:
    def __init__(self, short_interest_rows):
        self.short_interest_rows = list(short_interest_rows or [])
        self.queries = []

    def execute(self, sql, params=None):
        self.queries.append((str(sql), tuple(params or ())))
        if "FROM finra_short_sale_volume" in str(sql):
            return _Cursor([])
        if "FROM finra_short_interest" in str(sql):
            assert "availability_ts_ms <= ?" in str(sql)
            symbol, anchor_ts_ms, limit = params
            rows = [
                row
                for row in self.short_interest_rows
                if row["symbol"] == symbol and int(row["availability_ts_ms"]) <= int(anchor_ts_ms)
            ]
            rows = sorted(rows, key=lambda row: (int(row["availability_ts_ms"]), int(row["settlement_ts_ms"])), reverse=True)
            return _Cursor(rows[: int(limit)])
        if "FROM earnings_calendar" in str(sql):
            return _Cursor([])
        raise RuntimeError("unsupported fake query")


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def test_parse_finra_short_volume_fixture() -> None:
    (finra_short,) = _reload("engine.data.finra_short")
    text = "\n".join(
        [
            "Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market",
            "20260210|AAPL|100|2|1000|Q,N",
            "20260210|TOTAL|100|2|1000|Q,N",
        ]
    )

    rows = finra_short.parse_short_volume_file(text, source_url="https://example.test/file.txt", ingested_ts_ms=1)

    assert len(rows) == 1
    row = rows[0]
    assert row["symbol"] == "AAPL"
    assert row["trade_date"] == "2026-02-10"
    assert row["short_volume"] == 100.0
    assert row["short_exempt_volume"] == 2.0
    assert row["total_volume"] == 1000.0
    assert row["availability_ts_ms"] > row["trade_ts_ms"]
    assert row["source_record_id"].startswith("finra:")


def test_short_interest_surprise_uses_prior_ewma() -> None:
    (finra_short,) = _reload("engine.data.finra_short")
    readings = [
        {"settlement_ts_ms": 1, "short_interest_shares": 100.0, "days_to_cover": 2.0},
        {"settlement_ts_ms": 2, "short_interest_shares": 120.0, "days_to_cover": 4.0},
        {"settlement_ts_ms": 3, "short_interest_shares": 150.0, "days_to_cover": 7.0},
    ]

    surprise, dtc_delta = finra_short.short_interest_surprise(readings, alpha=0.5)

    expected_std = math.sqrt(((100.0 - 110.0) ** 2 + (120.0 - 110.0) ** 2) / 1.0)
    assert surprise == (150.0 - 110.0) / expected_std
    assert dtc_delta == 3.0


def test_short_feature_registry_round_trips_feature_schema(monkeypatch) -> None:
    monkeypatch.setenv("USE_SHORT_FEATURES", "1")
    (feature_registry,) = _reload("engine.strategy.feature_registry")

    ids = list(feature_registry.SHORT_FEATURE_IDS)
    assert ids == ["short_vol_ratio_z20", "si_surprise", "days_to_cover_delta", "si_surprise_x_earnings_window"]
    assert feature_registry.FEATURE_GROUPS["short"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "short" in feature_registry.feature_set_tag_from_ids(ids).split("+")


def test_finra_ingestion_jobs_import_clean(monkeypatch) -> None:
    monkeypatch.setenv("INGEST_FINRA_SHORT_VOLUME_ENABLED", "0")
    monkeypatch.setenv("INGEST_FINRA_SHORT_INTEREST_ENABLED", "0")
    _reload(
        "engine.data.jobs.ingest_finra_short_volume",
        "engine.data.jobs.ingest_finra_short_interest",
    )


def test_short_interest_dissemination_no_lookahead(monkeypatch) -> None:
    monkeypatch.setenv("USE_SHORT_FEATURES", "1")
    _reload("engine.strategy.feature_registry")
    (snapshots,) = _reload("engine.strategy.model_feature_snapshots")
    (finra_short,) = _reload("engine.data.finra_short")

    rows = [
        _Row(
            symbol="AAPL",
            settlement_date="2026-01-15",
            settlement_ts_ms=finra_short.date_to_ms("2026-01-15"),
            dissemination_date="2026-01-25",
            dissemination_ts_ms=finra_short.parse_ts_ms("2026-01-25T18:00:00-05:00"),
            availability_ts_ms=finra_short.parse_ts_ms("2026-01-25T18:00:00-05:00"),
            short_interest_shares=100.0,
            days_to_cover=2.0,
        ),
        _Row(
            symbol="AAPL",
            settlement_date="2026-01-31",
            settlement_ts_ms=finra_short.date_to_ms("2026-01-31"),
            dissemination_date="2026-02-08",
            dissemination_ts_ms=finra_short.parse_ts_ms("2026-02-08T18:00:00-05:00"),
            availability_ts_ms=finra_short.parse_ts_ms("2026-02-08T18:00:00-05:00"),
            short_interest_shares=100.0,
            days_to_cover=2.0,
        ),
        _Row(
            symbol="AAPL",
            settlement_date="2026-02-15",
            settlement_ts_ms=finra_short.date_to_ms("2026-02-15"),
            dissemination_date="2026-02-25",
            dissemination_ts_ms=finra_short.parse_ts_ms("2026-02-25T18:00:00-05:00"),
            availability_ts_ms=finra_short.parse_ts_ms("2026-02-25T18:00:00-05:00"),
            short_interest_shares=200.0,
            days_to_cover=5.0,
        ),
    ]
    con = _ShortFeatureCon(rows)
    ids = list(snapshots.SHORT_FEATURE_IDS)

    before = snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=finra_short.parse_ts_ms("2026-02-20T12:00:00-05:00"),
        feature_ids=ids,
        con=con,
    )
    after = snapshots.build_model_feature_snapshot(
        symbol="AAPL",
        ts_ms=finra_short.parse_ts_ms("2026-02-26T12:00:00-05:00"),
        feature_ids=ids,
        con=con,
    )

    assert before["features"]["si_surprise"] == 0.0
    assert before["source_timestamps"]["short"]["latest_short_interest_settlement_date"] == "2026-01-31"
    assert after["features"]["si_surprise"] == 10.0
    assert after["source_timestamps"]["short"]["latest_short_interest_settlement_date"] == "2026-02-15"
    assert snapshots.summarize_model_feature_snapshots([before, after])["lookahead_violations"] == 0
