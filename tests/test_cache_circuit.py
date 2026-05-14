from __future__ import annotations

import time
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest


def test_circuit_opens_after_failures_and_closes_after_probe():
    from engine.cache.circuit import CacheUnavailable, CircuitBreaker, set_alert_handler

    alerts = []
    set_alert_handler(alerts.append)
    circuit = CircuitBreaker(failure_threshold=2, cooldown_s=0.01, name="unit")

    def boom():
        raise RuntimeError("redis down")

    with pytest.raises(CacheUnavailable):
        circuit.call(boom)
    with pytest.raises(CacheUnavailable):
        circuit.call(boom)

    assert circuit.state == CircuitBreaker.OPEN
    assert [a.code for a in alerts] == ["CACHE_REDIS_UNAVAILABLE"]

    with pytest.raises(CacheUnavailable):
        circuit.call(lambda: "blocked")

    time.sleep(0.02)
    assert circuit.call(lambda: "ok") == "ok"
    assert circuit.state == CircuitBreaker.CLOSED
    assert [a.code for a in alerts] == ["CACHE_REDIS_UNAVAILABLE", "CACHE_REDIS_RECOVERED"]
    set_alert_handler(None)
