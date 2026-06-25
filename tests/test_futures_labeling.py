from __future__ import annotations

import importlib
import math
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from unittest import mock
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

CT = ZoneInfo("America/Chicago")


def _ms_ct(year: int, month: int, day: int, hour: int, minute: int = 0) -> int:
    return int(datetime(year, month, day, hour, minute, tzinfo=CT).timestamp() * 1000)


def _init_db(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(
            """
            CREATE TABLE labels(
              event_id INTEGER,
              horizon_s INTEGER,
              symbol TEXT,
              baseline_ret REAL,
              realized_ret REAL,
              impact_z REAL,
              created_at_ms INTEGER,
              vol_proxy REAL,
              regime TEXT,
              PRIMARY KEY(event_id, horizon_s, symbol)
            )
            """
        )
        con.execute(
            """
            CREATE TABLE futures_roll_calendar(
              root TEXT NOT NULL,
              roll_ts_ms INTEGER NOT NULL,
              from_contract TEXT NOT NULL,
              to_contract TEXT NOT NULL,
              gap_ratio REAL NOT NULL,
              method TEXT NOT NULL,
              ingested_ts_ms INTEGER,
              PRIMARY KEY(root, roll_ts_ms)
            )
            """
        )
        con.commit()
    finally:
        con.close()


def _init_label_due_db(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.executescript(
            """
            CREATE TABLE events(id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL, title TEXT);
            CREATE TABLE labels(
              event_id INTEGER,
              horizon_s INTEGER,
              symbol TEXT,
              baseline_ret REAL,
              realized_ret REAL,
              impact_z REAL,
              created_at_ms INTEGER,
              vol_proxy REAL,
              regime TEXT,
              PRIMARY KEY(event_id, horizon_s, symbol)
            );
            CREATE TABLE symbols(
              symbol TEXT PRIMARY KEY,
              status TEXT,
              score REAL,
              updated_ts_ms INTEGER
            );
            CREATE TABLE prices(
              symbol TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,
              price REAL,
              px REAL,
              PRIMARY KEY(symbol, ts_ms)
            );
            CREATE TABLE price_quotes(
              symbol TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,
              bid REAL,
              ask REAL,
              last REAL
            );
            CREATE TABLE futures_roll_calendar(
              root TEXT NOT NULL,
              roll_ts_ms INTEGER NOT NULL,
              from_contract TEXT NOT NULL,
              to_contract TEXT NOT NULL,
              gap_ratio REAL NOT NULL,
              method TEXT NOT NULL,
              ingested_ts_ms INTEGER,
              PRIMARY KEY(root, roll_ts_ms)
            );
            CREATE TABLE futures_continuous_bars(
              continuous_symbol TEXT NOT NULL,
              ts_ms INTEGER NOT NULL,
              adj_method TEXT NOT NULL,
              open REAL,
              high REAL,
              low REAL,
              close REAL,
              volume REAL,
              roll_flag INTEGER NOT NULL DEFAULT 0,
              source_contract TEXT,
              PRIMARY KEY(continuous_symbol, ts_ms, adj_method)
            );
            """
        )
        con.commit()
    finally:
        con.close()


def _rows(path: Path, *, event_id: int | None = None) -> list[tuple]:
    con = sqlite3.connect(path)
    try:
        if event_id is None:
            return con.execute(
                "SELECT event_id, symbol, horizon_s, realized_ret, impact_z FROM labels ORDER BY event_id, symbol, horizon_s"
            ).fetchall()
        return con.execute(
            """
            SELECT event_id, symbol, horizon_s, realized_ret, impact_z
            FROM labels
            WHERE event_id=?
            ORDER BY symbol, horizon_s
            """,
            (int(event_id),),
        ).fetchall()
    finally:
        con.close()


def _series(event_ts: int, p0: float, p5m: float, p1h: float | None = None) -> list[dict]:
    rows = [
        {"ts_ms": int(event_ts) + 1, "price": float(p0)},
        {"ts_ms": int(event_ts) + 300_000 + 1, "price": float(p5m)},
    ]
    if p1h is not None:
        rows.append({"ts_ms": int(event_ts) + 3_600_000 + 1, "price": float(p1h)})
    return rows


def _seed_non_crossing_roll_calendar(path: Path, *, root: str, event_ts: int) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(
            """
            INSERT OR REPLACE INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            (str(root), int(event_ts) - 86_400_000, f"{root}Z25", f"{root}H26", 1.0, "oi_volume", int(event_ts)),
        )
        con.commit()
    finally:
        con.close()


def test_futures_label_uses_supplied_ratio_adjusted_continuous_series(tmp_path: Path) -> None:
    db_path = tmp_path / "labels.db"
    _init_db(db_path)
    labeling = importlib.reload(importlib.import_module("engine.strategy.labeling"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    _seed_non_crossing_roll_calendar(db_path, root="ES", event_ts=event_ts)
    continuous = _series(event_ts, 110.0, 112.2)
    raw_front_month_return = 125.0 / 100.0 - 1.0
    continuous_return = 112.2 / 110.0 - 1.0

    with mock.patch.object(labeling, "connect", side_effect=lambda: sqlite3.connect(db_path)):
        labeling.label_event(7001, event_ts, {"ES.c.0": continuous})

    rows = _rows(db_path, event_id=7001)
    assert len(rows) == 1
    assert rows[0][1:3] == ("ES.c.0", 300)
    assert math.isclose(float(rows[0][3]), continuous_return, rel_tol=1e-12)
    assert not math.isclose(float(rows[0][3]), raw_front_month_return, rel_tol=1e-12)


def test_futures_label_skips_roll_and_closed_gap_windows(tmp_path: Path) -> None:
    db_path = tmp_path / "labels.db"
    _init_db(db_path)
    labeling = importlib.reload(importlib.import_module("engine.strategy.labeling"))
    roll_event_ts = _ms_ct(2026, 1, 5, 10, 0)
    maintenance_event_ts = _ms_ct(2026, 1, 5, 15, 59)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", roll_event_ts + 120_000, "ESH26", "ESM26", 1.01, "oi_volume", roll_event_ts),
        )
        con.commit()
    finally:
        con.close()

    with mock.patch.object(labeling, "connect", side_effect=lambda: sqlite3.connect(db_path)):
        labeling.label_event(7002, roll_event_ts, {"ES.c.0": _series(roll_event_ts, 100.0, 101.0, 102.0)})
        labeling.label_event(
            7003,
            maintenance_event_ts,
            {"ES.c.0": _series(maintenance_event_ts, 100.0, 101.0, 102.0)},
        )

    assert _rows(db_path, event_id=7002) == []
    assert _rows(db_path, event_id=7003) == []


def test_equity_labeling_path_matches_existing_compute_return(tmp_path: Path) -> None:
    db_path = tmp_path / "labels.db"
    _init_db(db_path)
    labeling = importlib.reload(importlib.import_module("engine.strategy.labeling"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    series = _series(event_ts, 100.0, 101.0, 103.0)

    with mock.patch.object(labeling, "connect", side_effect=lambda: sqlite3.connect(db_path)):
        labeling.label_event(7004, event_ts, {"SPY": series})

    rows = _rows(db_path, event_id=7004)
    realized_by_horizon = {int(row[2]): float(row[3]) for row in rows}
    assert set(realized_by_horizon) == {300, 3600}
    assert math.isclose(realized_by_horizon[300], labeling.compute_return(series, event_ts, 300_000), rel_tol=1e-12)
    assert math.isclose(realized_by_horizon[3600], labeling.compute_return(series, event_ts, 3_600_000), rel_tol=1e-12)


def test_label_due_events_uses_ratio_adjusted_continuous_bars_for_futures(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8001, event_ts, "futures test"))
        con.execute(
            """
            INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", event_ts - 86_400_000, "ESZ25", "ESH26", 1.0, "oi_volume", event_ts),
        )
        con.executemany(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            [("ES.c.0", "ACTIVE", 10.0, event_ts), ("SPY", "ACTIVE", 9.0, event_ts)],
        )
        con.executemany(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,?)",
            [
                ("ES.c.0", event_ts + 1, 100.0, 100.0),
                ("ES.c.0", event_ts + 300_001, 125.0, 125.0),
                ("ES.c.0", event_ts + 3_600_001, 130.0, 130.0),
                ("SPY", event_ts + 1, 100.0, 100.0),
                ("SPY", event_ts + 300_001, 101.0, 101.0),
                ("SPY", event_ts + 3_600_001, 103.0, 103.0),
            ],
        )
        con.executemany(
            """
            INSERT INTO futures_continuous_bars(
              continuous_symbol, ts_ms, adj_method, open, high, low, close, volume, roll_flag, source_contract
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                ("ES.c.0", event_ts + 1, "ratio", 110.0, 110.0, 110.0, 110.0, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 300_001, "ratio", 112.2, 112.2, 112.2, 112.2, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 3_600_001, "ratio", 115.5, 115.5, 115.5, 115.5, 1.0, 0, "ESH26"),
            ],
        )
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 4
    rows = _rows(db_path, event_id=8001)
    realized = {(row[1], int(row[2])): float(row[3]) for row in rows}
    assert math.isclose(realized[("ES.C.0", 300)], 112.2 / 110.0 - 1.0, rel_tol=1e-12)
    assert not math.isclose(realized[("ES.C.0", 300)], 125.0 / 100.0 - 1.0, rel_tol=1e-12)
    assert math.isclose(realized[("SPY", 300)], 101.0 / 100.0 - 1.0, rel_tol=1e-12)


def test_label_due_events_futures_fail_closed_without_continuous_or_roll_calendar(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events_fail_closed.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8003, event_ts, "futures fail closed"))
        con.execute(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            ("ES.c.0", "ACTIVE", 10.0, event_ts),
        )
        con.executemany(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,?)",
            [
                ("ES.c.0", event_ts + 1, 100.0, 100.0),
                ("ES.c.0", event_ts + 300_001, 125.0, 125.0),
                ("ES.c.0", event_ts + 3_600_001, 130.0, 130.0),
            ],
        )
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 0
    assert _rows(db_path, event_id=8003) == []


def test_label_due_events_futures_fail_closed_without_continuous_bars(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events_no_continuous.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8007, event_ts, "missing cont"))
        con.execute(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            ("ES.c.0", "ACTIVE", 10.0, event_ts),
        )
        con.executemany(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,?)",
            [
                ("ES.c.0", event_ts + 1, 100.0, 100.0),
                ("ES.c.0", event_ts + 300_001, 125.0, 125.0),
                ("ES.c.0", event_ts + 3_600_001, 130.0, 130.0),
            ],
        )
        con.execute(
            """
            INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", event_ts - 86_400_000, "ESZ25", "ESH26", 1.0, "oi_volume", event_ts),
        )
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 0
    assert _rows(db_path, event_id=8007) == []


def test_label_due_events_futures_fail_closed_without_roll_calendar(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events_no_roll_calendar.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8008, event_ts, "missing calendar"))
        con.execute(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            ("ES.c.0", "ACTIVE", 10.0, event_ts),
        )
        con.executemany(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,?)",
            [
                ("ES.c.0", event_ts + 1, 100.0, 100.0),
                ("ES.c.0", event_ts + 300_001, 125.0, 125.0),
                ("ES.c.0", event_ts + 3_600_001, 130.0, 130.0),
            ],
        )
        con.executemany(
            """
            INSERT INTO futures_continuous_bars(
              continuous_symbol, ts_ms, adj_method, open, high, low, close, volume, roll_flag, source_contract
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                ("ES.c.0", event_ts + 1, "ratio", 110.0, 110.0, 110.0, 110.0, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 300_001, "ratio", 112.2, 112.2, 112.2, 112.2, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 3_600_001, "ratio", 115.5, 115.5, 115.5, 115.5, 1.0, 0, "ESH26"),
            ],
        )
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 0
    assert _rows(db_path, event_id=8008) == []


def test_label_due_events_equity_matches_raw_price_compute_return(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events_equity.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8004, event_ts, "equity unchanged"))
        con.execute(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            ("SPY", "ACTIVE", 10.0, event_ts),
        )
        con.executemany(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?,?,?,?)",
            [
                ("SPY", event_ts + 1, 100.0, 100.0),
                ("SPY", event_ts + 300_001, 101.0, 101.0),
                ("SPY", event_ts + 3_600_001, 103.0, 103.0),
            ],
        )
        expected = {
            300: label_job.compute_return(con, "SPY", event_ts, 300_000),
            3600: label_job.compute_return(con, "SPY", event_ts, 3_600_000),
        }
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 2
    rows = _rows(db_path, event_id=8004)
    realized = {int(row[2]): float(row[3]) for row in rows}
    assert realized == {horizon: float(value) for horizon, value in expected.items()}


def test_label_due_events_skips_futures_closed_gap_windows(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events_closed_gap.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 15, 59)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8009, event_ts, "closed gap"))
        con.execute(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            ("ES.c.0", "ACTIVE", 10.0, event_ts),
        )
        con.executemany(
            """
            INSERT INTO futures_continuous_bars(
              continuous_symbol, ts_ms, adj_method, open, high, low, close, volume, roll_flag, source_contract
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                ("ES.c.0", event_ts + 1, "ratio", 100.0, 100.0, 100.0, 100.0, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 300_001, "ratio", 101.0, 101.0, 101.0, 101.0, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 3_600_001, "ratio", 102.0, 102.0, 102.0, 102.0, 1.0, 0, "ESH26"),
            ],
        )
        con.execute(
            """
            INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", event_ts - 86_400_000, "ESZ25", "ESH26", 1.0, "oi_volume", event_ts),
        )
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 0
    assert _rows(db_path, event_id=8009) == []


def test_label_due_events_skips_futures_roll_spanning_windows(tmp_path: Path) -> None:
    db_path = tmp_path / "label_due_events_roll.db"
    _init_label_due_db(db_path)
    label_job = importlib.reload(importlib.import_module("engine.data.jobs.label_due_events"))
    event_ts = _ms_ct(2026, 1, 5, 10, 0)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO events(id, ts_ms, title) VALUES (?,?,?)", (8002, event_ts, "roll test"))
        con.execute(
            "INSERT INTO symbols(symbol, status, score, updated_ts_ms) VALUES (?,?,?,?)",
            ("ES.c.0", "ACTIVE", 10.0, event_ts),
        )
        con.executemany(
            """
            INSERT INTO futures_continuous_bars(
              continuous_symbol, ts_ms, adj_method, open, high, low, close, volume, roll_flag, source_contract
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            [
                ("ES.c.0", event_ts + 1, "ratio", 100.0, 100.0, 100.0, 100.0, 1.0, 0, "ESH26"),
                ("ES.c.0", event_ts + 300_001, "ratio", 101.0, 101.0, 101.0, 101.0, 1.0, 0, "ESM26"),
                ("ES.c.0", event_ts + 3_600_001, "ratio", 102.0, 102.0, 102.0, 102.0, 1.0, 0, "ESM26"),
            ],
        )
        con.execute(
            """
            INSERT INTO futures_roll_calendar(root, roll_ts_ms, from_contract, to_contract, gap_ratio, method, ingested_ts_ms)
            VALUES (?,?,?,?,?,?,?)
            """,
            ("ES", event_ts + 120_000, "ESH26", "ESM26", 1.01, "oi_volume", event_ts),
        )
        con.commit()
    finally:
        con.close()

    with (
        mock.patch.object(label_job, "connect", side_effect=lambda: sqlite3.connect(db_path)),
        mock.patch.object(label_job.time, "time", return_value=(event_ts + 7_200_000) / 1000.0),
    ):
        inserted = label_job.label_due_events_internal()

    assert inserted == 0
    assert _rows(db_path, event_id=8002) == []
