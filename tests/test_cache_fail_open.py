from __future__ import annotations

import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_read_falls_through_to_loader_and_alerts_once(monkeypatch):
    store = importlib.reload(importlib.import_module("engine.cache.store"))
    circuit_mod = importlib.reload(importlib.import_module("engine.cache.circuit"))
    alerts = []
    circuit_mod.set_alert_handler(alerts.append)
    circuit = circuit_mod.CircuitBreaker(failure_threshold=1, cooldown_s=60)

    class DownRedis:
        def get(self, _key):
            raise RuntimeError("down")

        def set(self, *_args, **_kwargs):
            raise RuntimeError("down")

    monkeypatch.setattr(store, "redis_pool", lambda: DownRedis())
    monkeypatch.setattr(store, "cache_circuit", lambda: circuit)

    loaded = store.read("k", lambda: b"postgres", ttl_s=1)
    loaded_again = store.read("k", lambda: b"postgres", ttl_s=1)

    assert loaded == b"postgres"
    assert loaded_again == b"postgres"
    assert [a.code for a in alerts] == ["CACHE_REDIS_UNAVAILABLE"]
    circuit_mod.set_alert_handler(None)
