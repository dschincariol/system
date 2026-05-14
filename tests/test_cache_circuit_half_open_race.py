from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_half_open_probe_is_serialized_under_race():
    from engine.cache.circuit import CacheUnavailable, CircuitBreaker, set_alert_handler

    set_alert_handler(lambda _alert: None)
    try:
        for _ in range(100):
            circuit = CircuitBreaker(failure_threshold=1, cooldown_s=60.0, name="race")
            circuit.force_open("race")
            # Avoid millisecond sleep flakiness; the race starts with cooldown elapsed.
            with circuit._lock:
                circuit._opened_at = time.monotonic() - circuit.cooldown_s - 1.0

            n_threads = 8
            start = threading.Barrier(n_threads + 1)
            condition = threading.Condition()
            probes = 0
            successes = 0
            fail_fast = 0
            errors: list[BaseException] = []

            def blocked_probe() -> str:
                nonlocal probes
                with condition:
                    probes += 1
                    if probes > 1:
                        condition.notify_all()
                        return "ok"

                    deadline = time.monotonic() + 1.0
                    while fail_fast < n_threads - 1 and probes == 1:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        condition.wait(remaining)
                return "ok"

            def worker() -> None:
                nonlocal fail_fast, successes
                try:
                    start.wait(timeout=2.0)
                    circuit.call(blocked_probe)
                except CacheUnavailable:
                    with condition:
                        fail_fast += 1
                        condition.notify_all()
                except BaseException as exc:
                    with condition:
                        errors.append(exc)
                        condition.notify_all()
                else:
                    with condition:
                        successes += 1
                        condition.notify_all()

            threads = [threading.Thread(target=worker) for _ in range(n_threads)]
            for thread in threads:
                thread.start()

            start.wait(timeout=2.0)

            for thread in threads:
                thread.join(timeout=2.0)

            assert not any(thread.is_alive() for thread in threads)
            assert errors == []
            assert probes == 1
            assert successes == 1
            assert fail_fast == n_threads - 1
            assert circuit.state == CircuitBreaker.CLOSED
    finally:
        set_alert_handler(None)
