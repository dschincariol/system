from __future__ import annotations

import ast
import importlib
import os
import sys
import threading
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


_INGEST_CREDENTIAL_NAMES = {
    "POLYGON_API_KEY",
    "POLYGON_KEY",
    "TRADIER_API_TOKEN",
    "FMP_API_KEY",
    "FINNHUB_API_KEY",
    "FRED_API_KEY",
    "ALPHA_VANTAGE_API_KEY",
    "GDELT_API_KEY",
    "NEWSAPI_KEY",
    "WEATHER_API_KEY",
    "OPENWEATHER_API_KEY",
    "REDDIT_CLIENT_ID",
    "REDDIT_CLIENT_SECRET",
}


def test_data_credential_helper_uses_loader_and_ttl_cache(monkeypatch):
    creds = importlib.import_module("engine.data._credentials")
    creds.clear_data_credential_cache()
    monkeypatch.setenv("TS_SECRETS_PROVIDER", "plaintext")

    calls: list[str] = []

    def fake_load_secret(name: str) -> bytes:
        calls.append(str(name))
        return f"secret-for-{name}".encode("utf-8")

    monkeypatch.setattr(creds, "load_secret", fake_load_secret)

    first = creds.get_data_credential("POLYGON_API_KEY", ttl_s=300)
    second = creds.get_data_credential("POLYGON_API_KEY", ttl_s=300)

    assert first == "secret-for-POLYGON_API_KEY"
    assert second == first
    assert calls == ["POLYGON_API_KEY"]


def test_ingesters_do_not_read_provider_credentials_from_environ_directly():
    checked_roots = [REPO_ROOT / "engine" / "data", REPO_ROOT / "engine" / "jobs"]
    allowed = {
        (REPO_ROOT / "engine" / "data" / "_credentials.py").resolve(),
    }
    offenders: list[str] = []

    for root in checked_roots:
        for path in root.rglob("*.py"):
            if path.resolve() in allowed:
                continue
            if path.name == "__init__.py":
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                is_environ_get = (
                    isinstance(func, ast.Attribute)
                    and func.attr == "get"
                    and isinstance(func.value, ast.Attribute)
                    and func.value.attr == "environ"
                    and isinstance(func.value.value, ast.Name)
                    and func.value.value.id == "os"
                )
                is_getenv = (
                    isinstance(func, ast.Attribute)
                    and func.attr == "getenv"
                    and isinstance(func.value, ast.Name)
                    and func.value.id == "os"
                )
                if not (is_environ_get or is_getenv):
                    continue
                if not node.args or not isinstance(node.args[0], ast.Constant):
                    continue
                if str(node.args[0].value) in _INGEST_CREDENTIAL_NAMES:
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{node.lineno}:{node.args[0].value}")

    assert offenders == []


def test_polygon_session_snapshot_cap_drops_oldest_records():
    module = importlib.import_module("engine.data.provider_sessions.polygon_ws_session")
    session = object.__new__(module.PolygonWSSession)
    session._lock = threading.RLock()
    session._last = {f"S{i}": {"ts_ms": i} for i in range(10)}

    dropped = session.cap_snapshot_records(4)

    assert dropped == 6
    assert set(session._last) == {"S6", "S7", "S8", "S9"}


class _FakeSnapshotSession:
    def __init__(self) -> None:
        self._last: dict[str, dict[str, int]] = {}
        self._desired = {"S0", "S1", "S2"}
        self._subscribed = {"S0", "S1", "S2"}
        self.unsubscribe_calls: list[list[str]] = []

    def add_events(self, start: int, count: int) -> None:
        for idx in range(start, start + count):
            self._last[f"S{idx}"] = {"ts_ms": idx}

    def cap_snapshot_records(self, max_records: int) -> int:
        limit = max(1, int(max_records))
        excess = max(0, len(self._last) - limit)
        for symbol in sorted(self._last, key=lambda key: (int(self._last[key]["ts_ms"]), key))[:excess]:
            self._last.pop(symbol, None)
        return excess

    def snapshot(self) -> dict[str, dict[str, int]]:
        return {symbol: dict(record) for symbol, record in self._last.items()}

    def desired_symbols(self) -> set[str]:
        return set(self._desired)

    def subscribed_symbols(self) -> set[str]:
        return set(self._subscribed)

    def replace_desired_symbols(self, symbols) -> None:
        self._desired = {str(symbol) for symbol in symbols}

    def unsubscribe(self, symbols) -> None:
        clean = [str(symbol) for symbol in symbols]
        self.unsubscribe_calls.append(clean)
        self._subscribed -= set(clean)


class _FakeSnapshotManager:
    def __init__(self, session: _FakeSnapshotSession) -> None:
        self.session = session

    def snapshot(self) -> dict[str, dict[str, int]]:
        return self.session.snapshot()

    def ensure_subscriptions(self, symbols) -> None:
        clean = {str(symbol) for symbol in symbols}
        self.session._desired = set(clean)
        self.session._subscribed = set(clean)


def test_snapshot_cap_bounds_failed_flushes_and_pauses_subscriptions(monkeypatch):
    monkeypatch.setenv("ENGINE_SUPERVISED", "1")
    stream = importlib.import_module("engine.jobs.stream_prices_polygon_ws")
    session = _FakeSnapshotSession()
    manager = _FakeSnapshotManager(session)
    cap_state = {"streak": 0, "subscriptions_paused": False, "paused_desired_symbols": []}
    emitted: list[tuple[str, int, dict]] = []

    def fake_emit_counter(metric, value=1, **kwargs):
        emitted.append((str(metric), int(value), dict(kwargs)))

    monkeypatch.setattr(stream, "emit_counter", fake_emit_counter)

    for cycle in range(3):
        session.add_events(cycle * 20, 20)
        snap, dropped = stream._cap_manager_snapshot(manager, manager.snapshot(), 5)
        if dropped > 0:
            cap_state["streak"] = int(cap_state.get("streak") or 0) + 1
            stream._emit_snapshot_cap_metric(dropped, snapshot_size=20, max_records=5)
        assert len(snap) <= 5
        assert len(session._last) <= 5

        with pytest.raises(RuntimeError):
            raise RuntimeError("db_locked")
        if int(cap_state.get("streak") or 0) >= stream.SNAPSHOT_CAP_PAUSE_STREAK:
            stream._pause_snapshot_subscriptions(manager, cap_state, "db_locked")

    assert [row[0] for row in emitted] == ["stream_prices_snapshot_capped"] * 3
    assert all(row[1] > 0 for row in emitted)
    assert cap_state["subscriptions_paused"] is True
    assert session._desired == set()
    assert session._subscribed == set()
    assert session.unsubscribe_calls
