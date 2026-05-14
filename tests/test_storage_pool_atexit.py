from __future__ import annotations

import importlib


def test_storage_pool_registers_atexit_cleanup(monkeypatch):
    from engine.runtime import storage_pool

    storage_pool.close_pool()
    registered = []
    monkeypatch.setattr(storage_pool.atexit, "register", lambda func: registered.append(func))

    reloaded = importlib.reload(storage_pool)

    assert registered
    assert registered[-1] is reloaded.close_pooled_connections
    reloaded.close_pool()


def test_close_pooled_connections_is_idempotent_when_shutdown_and_atexit_both_fire(monkeypatch):
    from engine.runtime import storage_pg, storage_pool

    storage_pool.close_pool()

    class FakePool:
        def __init__(self) -> None:
            self.close_count = 0

        def close(self, timeout=None) -> None:
            del timeout
            self.close_count += 1

    pool = FakePool()
    monkeypatch.setattr(storage_pool, "_POOL", pool)

    storage_pg.close_pooled_connections()
    storage_pool.close_pooled_connections()

    assert pool.close_count == 1
    assert storage_pool._POOL is None
