from __future__ import annotations

import importlib
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _FeatureCon:
    def __init__(self, rows):
        self.rows = list(rows or [])

    def execute(self, sql, params=None):
        text = str(sql)
        if "FROM factor_features" in text:
            feature_id, asof_ts, effective_ts = params
            rows = [
                (
                    row["value"],
                    row["asof_ts"],
                    row["effective_ts"],
                    json.dumps(row.get("meta") or {}, separators=(",", ":"), sort_keys=True),
                )
                for row in self.rows
                if row["feature_id"] == feature_id
                and int(row["asof_ts"]) <= int(asof_ts)
                and int(row["effective_ts"]) <= int(effective_ts)
            ]
            rows.sort(key=lambda row: (int(row[1]), int(row[2])), reverse=True)
            return _Cursor(rows[:128])
        if "FROM gdelt_macro_features" in text:
            return _Cursor([])
        raise RuntimeError(f"unsupported fake query: {text[:80]}")


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _test_spec(factor_id: str = "macro.test_level"):
    (factor_ingestion,) = _reload("engine.data.factor_ingestion")
    return factor_ingestion.MacroSeriesSpec(
        factor_id=factor_id,
        source_series_id="TEST",
        family="macro",
        name="Test vintage macro",
        cadence="monthly",
        applies_to="test",
        units="index",
        transform="initial_release_level",
        release_hour_et=0,
        release_minute_et=0,
        is_revisioned=True,
        history_start="2026-01-01",
        z_window=3,
        delta_lag=1,
    )


def test_macro_vintage_revision_no_lookahead_and_snapshot_parity(monkeypatch) -> None:
    monkeypatch.setenv("MACRO_PIT_MODE", "on")
    (factor_ingestion,) = _reload("engine.data.factor_ingestion")
    (snapshots,) = _reload("engine.strategy.model_feature_snapshots")
    spec = _test_spec()

    v1 = factor_ingestion._et_ts_ms(date(2026, 1, 10), 0, 0)
    v2 = factor_ingestion._et_ts_ms(date(2026, 2, 10), 0, 0)
    obs_ts = factor_ingestion._effective_ts_ms(date(2026, 1, 1))
    factor_rows = factor_ingestion.build_factor_rows_from_vintage_records(
        spec,
        [
            {
                "obs_date": "2026-01-01",
                "obs_ts_ms": obs_ts,
                "vintage_date": "2026-01-10",
                "availability_ts_ms": v1,
                "value": 100.0,
            },
            {
                "obs_date": "2026-01-01",
                "obs_ts_ms": obs_ts,
                "vintage_date": "2026-02-10",
                "availability_ts_ms": v2,
                "value": 110.0,
            },
        ],
    )
    assert [row["value"] for row in factor_rows] == [100.0, 110.0]

    con = _FeatureCon(
        [
            {
                "feature_id": spec.factor_id,
                "asof_ts": int(row["asof_ts"]),
                "effective_ts": int(row["effective_ts"]),
                "value": float(row["value"]),
                "meta": dict(row.get("meta") or {}),
            }
            for row in factor_rows
        ]
    )

    assert factor_ingestion.macro_feature_row_asof(con, feature_id=spec.factor_id, ts_ms=v1 + 1)[0] == 100.0
    assert factor_ingestion.macro_feature_row_asof(con, feature_id=spec.factor_id, ts_ms=v2 - 1)[0] == 100.0
    assert factor_ingestion.macro_feature_row_asof(con, feature_id=spec.factor_id, ts_ms=v2 + 1)[0] == 110.0
    assert snapshots._factor_feature_row_asof(con, feature_id=spec.factor_id, ts_ms=v2 - 1)[0] == 100.0
    assert snapshots._factor_feature_row_asof(con, feature_id=spec.factor_id, ts_ms=v2 + 1)[0] == 110.0


def test_macro_old_observation_revision_does_not_become_latest_level(monkeypatch) -> None:
    monkeypatch.setenv("MACRO_PIT_MODE", "on")
    (factor_ingestion,) = _reload("engine.data.factor_ingestion")
    spec = _test_spec()

    jan_vintage = factor_ingestion._et_ts_ms(date(2026, 1, 10), 0, 0)
    feb_vintage = factor_ingestion._et_ts_ms(date(2026, 2, 10), 0, 0)
    mar_revision = factor_ingestion._et_ts_ms(date(2026, 3, 10), 0, 0)
    rows = factor_ingestion.build_factor_rows_from_vintage_records(
        spec,
        [
            {
                "obs_date": "2026-01-01",
                "obs_ts_ms": factor_ingestion._effective_ts_ms(date(2026, 1, 1)),
                "vintage_date": "2026-01-10",
                "availability_ts_ms": jan_vintage,
                "value": 100.0,
            },
            {
                "obs_date": "2026-02-01",
                "obs_ts_ms": factor_ingestion._effective_ts_ms(date(2026, 2, 1)),
                "vintage_date": "2026-02-10",
                "availability_ts_ms": feb_vintage,
                "value": 200.0,
            },
            {
                "obs_date": "2026-01-01",
                "obs_ts_ms": factor_ingestion._effective_ts_ms(date(2026, 1, 1)),
                "vintage_date": "2026-03-10",
                "availability_ts_ms": mar_revision,
                "value": 110.0,
            },
        ],
    )

    assert [(row["effective_ts"], row["value"]) for row in rows] == [
        (factor_ingestion._effective_ts_ms(date(2026, 1, 1)), 100.0),
        (factor_ingestion._effective_ts_ms(date(2026, 2, 1)), 200.0),
    ]


def test_fred_vintage_fetch_requests_all_observations_by_vintage(monkeypatch) -> None:
    (factor_ingestion,) = _reload("engine.data.factor_ingestion")
    calls = []

    class _Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "count": 1,
                "observations": [
                    {
                        "date": "2026-01-01",
                        "TEST_20260110": "100.0",
                        "TEST_20260210": "110.0",
                    }
                ],
            }

    def fake_get(url, *, params, timeout):
        calls.append((url, params, timeout))
        return _Response()

    monkeypatch.setattr(factor_ingestion.requests, "get", fake_get)

    rows = factor_ingestion._fetch_fred_observation_vintages(
        series_id="TEST",
        obs_start="2026-01-01",
        obs_end="2026-01-31",
        realtime_start="1776-07-04",
        realtime_end="9999-12-31",
    )

    assert calls[0][1]["output_type"] == 2
    assert rows == [
        {
            "series_id": "TEST",
            "obs_date": "2026-01-01",
            "vintage_date": "2026-01-10",
            "realtime_end": None,
            "value": 100.0,
            "payload": {
                "date": "2026-01-01",
                "value": "100.0",
                "column": "TEST_20260110",
            },
        },
        {
            "series_id": "TEST",
            "obs_date": "2026-01-01",
            "vintage_date": "2026-02-10",
            "realtime_end": None,
            "value": 110.0,
            "payload": {
                "date": "2026-01-01",
                "value": "110.0",
                "column": "TEST_20260210",
            },
        }
    ]


def test_macro_snapshot_resolver_is_the_shared_train_serve_path(monkeypatch) -> None:
    (factor_ingestion,) = _reload("engine.data.factor_ingestion")
    (snapshots,) = _reload("engine.strategy.model_feature_snapshots")

    def fake_macro_feature_row_asof(con, *, feature_id: str, ts_ms: int):
        assert feature_id == "macro.cpi_yoy"
        assert ts_ms == 123
        return 42.0, 111, 99

    monkeypatch.setattr(factor_ingestion, "macro_feature_row_asof", fake_macro_feature_row_asof)

    assert snapshots._factor_feature_row_asof(object(), feature_id="macro.cpi_yoy", ts_ms=123) == (42.0, 111, 99)


def test_macro_vintage_backfill_is_resumable(monkeypatch) -> None:
    (factor_ingestion,) = _reload("engine.data.factor_ingestion")
    spec = _test_spec("macro.test_backfill")
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE factor_registry (
            factor_id TEXT PRIMARY KEY,
            family TEXT,
            name TEXT,
            cadence TEXT,
            release_lag_sec INTEGER,
            applies_to TEXT,
            units TEXT,
            transform TEXT,
            is_revisioned INTEGER,
            source TEXT,
            enabled INTEGER
        )
        """
    )
    con.execute(
        """
        CREATE TABLE factor_features (
            feature_id TEXT,
            asof_ts BIGINT,
            effective_ts BIGINT,
            value DOUBLE PRECISION,
            meta_json TEXT
        )
        """
    )

    fetch_calls = []

    def fake_fetch(fetch_spec, *, obs_end: str, backfill: bool):
        fetch_calls.append((fetch_spec.source_series_id, backfill, obs_end))
        return [
            {
                "series_id": "TEST",
                "obs_date": "2026-01-01",
                "vintage_date": "2026-01-10",
                "value": 100.0,
                "payload": {"value": "100.0"},
            }
        ]

    monkeypatch.setattr(factor_ingestion, "MACRO_SERIES_SPECS", [spec])
    monkeypatch.setattr(factor_ingestion, "_fetch_vintage_rows_for_spec", fake_fetch)
    monkeypatch.setattr(factor_ingestion, "run_write_txn", lambda fn, **_kwargs: fn(con))

    first = factor_ingestion.backfill_macro_vintages(now_ms=1)
    second = factor_ingestion.backfill_macro_vintages(now_ms=2)

    assert first["series"] == 1
    assert first["vintage_rows"] == 1
    assert first["feature_rows"] == 3
    assert second["skipped"] == 1
    assert len(fetch_calls) == 1
    state = con.execute("SELECT status, last_vintage_date FROM macro_vintage_backfill_state WHERE series_id = ?", ("TEST",)).fetchone()
    assert state == ("complete", "2026-01-10")


def test_macro_feature_schema_round_trip_and_jobs_registered(monkeypatch) -> None:
    (feature_registry,) = _reload("engine.strategy.feature_registry")
    (job_registry,) = _reload("engine.runtime.job_registry")

    ids = list(feature_registry.MACRO_FEATURE_IDS)
    assert feature_registry.FEATURE_GROUPS["macro"][: len(ids)] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert "macro" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["poll_macro"][3]["cadence_seconds"] == 21600
    assert job_registry.ALLOWED_JOBS["backfill_macro_vintages"][1] == "oneshot"
