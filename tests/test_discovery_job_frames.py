from __future__ import annotations

import sqlite3

import pytest

from engine.strategy.jobs.discover_features import (
    _load_symbol_frame,
    _resolve_feature_ids_for_discovery,
    default_discoverers,
)


def _make_labeled_db() -> sqlite3.Connection:
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE events(id INTEGER PRIMARY KEY, ts_ms INTEGER, title TEXT, body TEXT, source TEXT)")
    con.execute(
        """
        CREATE TABLE labels(
          event_id INTEGER,
          symbol TEXT,
          horizon_s INTEGER,
          impact_z REAL,
          realized_ret REAL,
          baseline_ret REAL
        )
        """
    )
    con.execute(
        """
        CREATE TABLE labels_exec(
          event_id INTEGER,
          symbol TEXT,
          horizon_s INTEGER,
          ts_ms INTEGER,
          source TEXT,
          net_z REAL,
          gross_z REAL,
          net_ret REAL,
          gross_ret REAL
        )
        """
    )
    con.execute("CREATE TABLE prices(symbol TEXT, ts_ms INTEGER, price REAL, px REAL)")
    for idx in range(12):
        event_id = idx + 1
        ts_ms = 1_700_000_000_000 + idx * 60_000
        con.execute(
            "INSERT INTO events(id, ts_ms, title, body, source) VALUES (?, ?, ?, ?, ?)",
            (event_id, ts_ms, f"event {idx}", "body text", "fixture"),
        )
        con.execute(
            """
            INSERT INTO labels(event_id, symbol, horizon_s, impact_z, realized_ret, baseline_ret)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_id, "AAPL", 3600, float(idx) / 10.0, float(idx) / 100.0, 0.0),
        )
        con.execute(
            """
            INSERT INTO labels_exec(event_id, symbol, horizon_s, ts_ms, source, net_z, gross_z, net_ret, gross_ret)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, "AAPL", 3600, ts_ms, "fixture", float(idx) / 8.0, None, None, None),
        )
        con.execute(
            "INSERT INTO prices(symbol, ts_ms, price, px) VALUES (?, ?, ?, ?)",
            ("AAPL", ts_ms, 100.0 + idx, None),
        )
    con.commit()
    return con


def test_symbol_frame_prefers_labeled_registry_feature_matrix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import engine.strategy.feature_registry as feature_registry

    def fake_snapshot(*, event, symbol, feature_ids):  # noqa: ANN001
        del symbol
        idx = int(event["id"])
        return {
            "base.normalized_text_len": float(idx) / 10.0,
            "tech.rsi": 50.0 + idx,
        }

    monkeypatch.setattr(feature_registry, "compute_feature_snapshot", fake_snapshot)
    monkeypatch.setenv("DISCOVERY_MIN_LABELED_ROWS", "2")
    con = _make_labeled_db()

    frame = _load_symbol_frame(
        "AAPL",
        con=con,
        feature_ids=["base.normalized_text_len", "tech.rsi"],
        limit=20,
    )

    assert len(frame.index) == 12
    assert {"target", "close", "base.normalized_text_len", "tech.rsi"}.issubset(frame.columns)
    assert frame["target"].iloc[-1] == pytest.approx(11.0 / 8.0)


def test_discovery_feature_ids_use_live_registry_features(monkeypatch: pytest.MonkeyPatch) -> None:
    import engine.strategy.feature_registry as feature_registry

    calls: list[bool] = []

    def fake_registered_feature_ids(*, include_shadow: bool = True):
        calls.append(bool(include_shadow))
        return ["feature.a", "feature.b", "feature.a"]

    monkeypatch.delenv("DISCOVERY_FEATURE_IDS", raising=False)
    monkeypatch.setenv("DISCOVERY_MAX_REGISTRY_FEATURES", "10")
    monkeypatch.setattr(feature_registry, "registered_feature_ids", fake_registered_feature_ids)

    assert _resolve_feature_ids_for_discovery() == ["feature.a", "feature.b"]
    assert calls == [False]


def test_default_pysr_discoverer_receives_registry_primitives() -> None:
    engines = default_discoverers(feature_ids=["feature.a", "feature.b"])
    pysr = next(engine for engine in engines if getattr(engine, "source", "") == "pysr")

    assert tuple(pysr.primitive_columns) == ("feature.a", "feature.b")
