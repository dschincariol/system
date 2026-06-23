from __future__ import annotations

import importlib
import json
import os
import threading
import time


def _load_stream_module():
    os.environ["ENGINE_SUPERVISED"] = "1"
    module = importlib.import_module("engine.jobs.stream_prices_polygon_ws")
    return importlib.reload(module)


class _RecordingRLock:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._owner: int | None = None
        self._depth = 0
        self._entered_at = 0.0
        self.hold_durations: list[float] = []

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if timeout == -1:
            acquired = self._lock.acquire(blocking)
        else:
            acquired = self._lock.acquire(blocking, timeout)
        if acquired:
            ident = threading.get_ident()
            if self._depth == 0:
                self._owner = ident
                self._entered_at = time.perf_counter()
            self._depth += 1
        return acquired

    def release(self) -> None:
        if self._depth == 1:
            self.hold_durations.append(time.perf_counter() - self._entered_at)
            self._owner = None
            self._entered_at = 0.0
        self._depth -= 1
        self._lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *_exc) -> None:
        self.release()

    def is_owned_by_current_thread(self) -> bool:
        return self._owner == threading.get_ident()


def _new_polygon_session():
    module = importlib.import_module("engine.data.provider_sessions.polygon_ws_session")
    session = object.__new__(module.PolygonWSSession)
    module.BaseProviderSession.__init__(session, "polygon_ws")
    session.subscribe_trades = True
    session.subscribe_quotes = True
    session._last = {}
    session._last_event_ts_by_stream = {}
    session._pending_events = module.deque()
    session._max_pending_events = 250000
    session._last_latency_ms = 0
    session._last_transport_ts_ms = 0
    session._last_ping_ts_ms = 0
    session._last_pong_ts_ms = 0
    session._opened = threading.Event()
    session._auth_event = threading.Event()
    return session


def test_polygon_ws_message_decode_uses_runtime_codec_for_bytes_and_normalizes_numbers(monkeypatch):
    module = importlib.import_module("engine.data.provider_sessions.polygon_ws_session")
    session = _new_polygon_session()
    decode_calls = []
    real_loads = module._json_loads

    def recording_loads(payload):
        decode_calls.append(type(payload).__name__)
        return real_loads(payload)

    monkeypatch.setattr(module, "_json_loads", recording_loads)

    session._on_message(None, b'{"ev":"T","sym":"SPY","t":1000,"p":451.25,"s":10}')

    assert decode_calls == ["bytes"]
    snap = session.snapshot()
    assert snap["SPY"]["last"] == 451.25
    assert snap["SPY"]["volume"] == 10.0
    queued = session.drain_pending_events()
    assert queued[0]["event_ts_ms"] == 1000
    assert queued[0]["price"] == 451.25
    assert queued[0]["size"] == 10.0


def test_polygon_ws_invalid_json_preserves_warning_and_drops_message(monkeypatch):
    module = importlib.import_module("engine.data.provider_sessions.polygon_ws_session")
    session = _new_polygon_session()
    warnings = []

    def capture_warning(event, code, error, **extra):
        warnings.append((event, code, type(error).__name__, extra))

    monkeypatch.setattr(module, "_warn_nonfatal", capture_warning)

    session._on_message(None, b'{"ev":')

    assert session.snapshot() == {}
    assert warnings
    assert warnings[0][0] == "polygon_ws_session_message_parse_failed"
    assert warnings[0][1] == "POLYGON_WS_SESSION_MESSAGE_PARSE_FAILED"
    assert "message" in warnings[0][3]


def test_polygon_ws_control_frames_use_runtime_json_codec(monkeypatch):
    module = importlib.import_module("engine.data.provider_sessions.polygon_ws_session")
    session = _new_polygon_session()
    sent = []
    encode_calls = []

    class _Sock:
        connected = True

    class _Ws:
        sock = _Sock()

        def send(self, payload):
            sent.append(payload)

    def recording_dumps(payload, **kwargs):
        encode_calls.append((payload, kwargs))
        return "encoded-control-frame"

    monkeypatch.setattr(module, "_json_dumps_text", recording_dumps)
    session._ws = _Ws()
    session._opened.set()
    session._auth_event.set()

    session.subscribe(["SPY"])

    assert sent == ["encoded-control-frame"]
    assert encode_calls == [({"action": "subscribe", "params": "T.SPY,Q.SPY"}, {})]
    assert session.subscribed_symbols() == {"SPY"}


def test_polygon_ws_pauses_on_async_writer_high_watermark(monkeypatch):
    stream = _load_stream_module()
    states = []
    counters = []

    monkeypatch.setattr(stream, "WS_RECONNECT_BASE_S", 0.5)
    monkeypatch.setattr(stream, "WS_RECONNECT_MAX_S", 5.0)
    monkeypatch.setattr(stream, "set_state", lambda state, detail: states.append((state, detail)))
    monkeypatch.setattr(stream, "emit_counter", lambda *args, **kwargs: counters.append((args, kwargs)))

    pause_s = stream._async_persistence_backpressure_pause_s(
        {
            "async_persistence": {
                "attempted": True,
                "accepted": True,
                "backpressure": True,
                "reason": "high_watermark",
            }
        },
        phase="live",
    )

    assert pause_s == 0.5
    assert states
    assert states[0][0] == stream.DEGRADED
    assert "high_watermark" in states[0][1]
    assert counters


def test_polygon_ws_rejected_async_enqueue_raises_for_requeue(monkeypatch):
    stream = _load_stream_module()

    monkeypatch.setattr(stream, "set_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stream, "emit_counter", lambda *_args, **_kwargs: None)

    try:
        stream._async_persistence_backpressure_pause_s(
            {
                "async_persistence": {
                    "attempted": True,
                    "accepted": False,
                    "backpressure": True,
                    "reason": "enqueue_rejected",
                }
            },
            phase="live",
        )
    except RuntimeError as exc:
        assert "async_price_writer_enqueue_rejected:live:enqueue_rejected" in str(exc)
    else:
        raise AssertionError("rejected async enqueue did not raise")


def test_polygon_ws_message_normalization_does_not_hold_snapshot_lock():
    session = _new_polygon_session()
    recording_lock = _RecordingRLock()
    session._lock = recording_lock
    original_normalize = session._normalize_ws_event
    normalizer_started = threading.Event()
    release_normalizer = threading.Event()

    def slow_normalize(ev, ts_now):
        assert not recording_lock.is_owned_by_current_thread()
        normalizer_started.set()
        assert release_normalizer.wait(timeout=2.0)
        return original_normalize(ev, ts_now)

    session._normalize_ws_event = slow_normalize
    payload = json.dumps({"ev": "T", "sym": "SPY", "t": 1000, "p": "451.25", "s": "10"})
    worker = threading.Thread(target=session._on_message, args=(None, payload))

    worker.start()
    try:
        assert normalizer_started.wait(timeout=1.0)
        started = time.perf_counter()
        assert session.snapshot() == {}
        snapshot_elapsed = time.perf_counter() - started
        assert snapshot_elapsed < 0.05
    finally:
        release_normalizer.set()
        worker.join(timeout=2.0)

    assert not worker.is_alive()
    assert float(max(recording_lock.hold_durations or [0.0])) < 0.05
    assert session.snapshot()["SPY"]["last"] == 451.25


def test_polygon_ws_different_symbol_messages_do_not_serialize_cpu_work():
    session = _new_polygon_session()
    recording_lock = _RecordingRLock()
    session._lock = recording_lock
    original_normalize = session._normalize_ws_event
    entered_lock = threading.Lock()
    first_entered = threading.Event()
    both_entered = threading.Event()
    release_normalizers = threading.Event()
    entered_count = 0

    def slow_normalize(ev, ts_now):
        nonlocal entered_count
        assert not recording_lock.is_owned_by_current_thread()
        with entered_lock:
            entered_count += 1
            if entered_count == 1:
                first_entered.set()
            elif entered_count == 2:
                both_entered.set()
        assert release_normalizers.wait(timeout=2.0)
        return original_normalize(ev, ts_now)

    session._normalize_ws_event = slow_normalize
    payload_a = json.dumps({"ev": "Q", "sym": "SPY", "t": 2000, "bp": "451.20", "ap": "451.30", "bs": "3", "as": "4"})
    payload_b = json.dumps({"ev": "Q", "sym": "QQQ", "t": 2001, "bp": "381.10", "ap": "381.16", "bs": "5", "as": "6"})
    thread_a = threading.Thread(target=session._on_message, args=(None, payload_a))
    thread_b = threading.Thread(target=session._on_message, args=(None, payload_b))

    thread_a.start()
    try:
        assert first_entered.wait(timeout=1.0)
        thread_b.start()
        assert both_entered.wait(timeout=1.0)
    finally:
        release_normalizers.set()
        thread_a.join(timeout=2.0)
        thread_b.join(timeout=2.0)

    assert not thread_a.is_alive()
    assert not thread_b.is_alive()
    snap = session.snapshot()
    assert set(snap) == {"SPY", "QQQ"}
    assert abs(float(snap["SPY"]["spread"]) - 0.10) < 1e-9
    assert abs(float(snap["QQQ"]["spread"]) - 0.06) < 1e-9


def test_polygon_ws_record_build_does_not_hold_snapshot_lock():
    session = _new_polygon_session()
    recording_lock = _RecordingRLock()
    session._lock = recording_lock
    original_build = session._build_event_record
    build_started = threading.Event()
    release_build = threading.Event()

    def slow_build(event, base_rec, now_applied_ms):
        assert not recording_lock.is_owned_by_current_thread()
        build_started.set()
        assert release_build.wait(timeout=2.0)
        return original_build(event, base_rec, now_applied_ms)

    session._build_event_record = slow_build
    worker = threading.Thread(
        target=session._on_message,
        args=(None, json.dumps({"ev": "Q", "sym": "SPY", "t": 3000, "bp": "10.00", "ap": "10.05", "bs": "1", "as": "2"})),
    )
    worker.start()

    try:
        assert build_started.wait(timeout=1.0)
        snapshot_result = []
        snapshot_thread = threading.Thread(target=lambda: snapshot_result.append(session.snapshot()))
        snapshot_thread.start()
        snapshot_thread.join(timeout=0.2)
        assert not snapshot_thread.is_alive()
        assert snapshot_result == [{}]
    finally:
        release_build.set()
        worker.join(timeout=2.0)

    assert not worker.is_alive()
    assert session.snapshot()["SPY"]["ask"] == 10.05


def test_polygon_ws_same_symbol_ordering_and_flush_queue_are_preserved():
    session = _new_polygon_session()

    session._on_message(
        None,
        json.dumps(
            [
                {"ev": "T", "sym": "SPY", "t": 1000, "p": "100.50", "s": "7", "i": "trade-1"},
                {"ev": "Q", "sym": "SPY", "t": 1005, "bp": "100.40", "ap": "100.60", "bs": "3", "as": "4", "q": 10},
            ]
        ),
    )

    snap = session.snapshot()
    assert set(snap) == {"SPY"}
    assert snap["SPY"]["last"] == 100.50
    assert snap["SPY"]["volume"] == 7.0
    assert snap["SPY"]["bid"] == 100.40
    assert snap["SPY"]["ask"] == 100.60
    assert abs(float(snap["SPY"]["spread"]) - 0.20) < 1e-9
    assert snap["SPY"]["trade_ts_ms"] == 1000
    assert snap["SPY"]["quote_ts_ms"] == 1005
    assert snap["SPY"]["ts_ms"] == 1005

    queued = session.drain_pending_events()
    assert [row["event_type"] for row in queued] == ["T", "Q"]
    assert [row["event_ts_ms"] for row in queued] == [1000, 1005]

    session._on_message(None, json.dumps({"ev": "T", "sym": "SPY", "t": 999, "p": "80.00", "s": "99", "i": "older"}))

    snap_after_stale = session.snapshot()
    assert snap_after_stale["SPY"]["last"] == 100.50
    assert snap_after_stale["SPY"]["trade_ts_ms"] == 1000
    assert session.drain_pending_events() == []


def test_polygon_ws_stale_and_duplicate_events_skip_record_build():
    session = _new_polygon_session()
    first_payload = json.dumps({"ev": "Q", "sym": "SPY", "t": 1000, "bp": "10.00", "ap": "10.02", "q": 1})
    session._on_message(None, first_payload)

    build_calls = 0

    def fail_record_build(*_args, **_kwargs):
        nonlocal build_calls
        build_calls += 1
        raise AssertionError("stale/duplicate event should not build a record")

    session._build_event_record = fail_record_build

    session._on_message(None, json.dumps({"ev": "Q", "sym": "SPY", "t": 999, "bp": "9.00", "ap": "9.02", "q": 2}))
    session._on_message(None, first_payload)

    assert build_calls == 0
    assert session._dedup_drop_count == 1
    assert session.snapshot()["SPY"]["bid"] == 10.00


def test_polygon_ws_gap_counter_increments_only_for_new_gap():
    session = _new_polygon_session()

    session._on_message(None, json.dumps({"ev": "Q", "sym": "SPY", "t": 1000, "bp": "10.00", "ap": "10.02"}))
    session._on_message(None, json.dumps({"ev": "Q", "sym": "SPY", "t": 62001, "bp": "10.01", "ap": "10.03"}))
    assert session._gap_event_count == 1
    assert session.snapshot()["SPY"]["gap_detected"] is True

    session._on_message(None, json.dumps({"ev": "Q", "sym": "SPY", "t": 62002, "bp": "10.02", "ap": "10.04"}))

    assert session._gap_event_count == 1
    assert "_gap_event_new" not in session.snapshot()["SPY"]


def test_legacy_polygon_ws_microstructure_runs_outside_snapshot_lock(monkeypatch):
    stream = _load_stream_module()
    ingest = object.__new__(stream._WsIngest)
    recording_lock = _RecordingRLock()
    ingest._lock = recording_lock
    ingest._last = {}
    ingest._last_event_ts_by_stream = {}
    ingest._last_msg_ts_ms = 0
    ingest._session_started_ts_ms = stream._now_ms()
    ingest._last_error = None

    monkeypatch.setattr(stream, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stream, "meta_set", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stream, "meta_set_if_missing", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(stream, "set_state", lambda *_args, **_kwargs: None)

    original_quote_microstructure = stream._update_quote_microstructure
    original_trade_microstructure = stream._update_trade_microstructure
    quote_called = threading.Event()
    trade_called = threading.Event()

    def checked_quote_microstructure(rec):
        assert not recording_lock.is_owned_by_current_thread()
        quote_called.set()
        return original_quote_microstructure(rec)

    def checked_trade_microstructure(rec, trade_px, trade_sz):
        assert not recording_lock.is_owned_by_current_thread()
        trade_called.set()
        return original_trade_microstructure(rec, trade_px, trade_sz)

    monkeypatch.setattr(stream, "_update_quote_microstructure", checked_quote_microstructure)
    monkeypatch.setattr(stream, "_update_trade_microstructure", checked_trade_microstructure)

    ingest._on_message(
        None,
        json.dumps(
            [
                {"ev": "Q", "sym": "SPY", "t": 2000, "bp": "100.40", "ap": "100.60", "bs": "3", "as": "4"},
                {"ev": "T", "sym": "SPY", "t": 2001, "p": "100.62", "s": "9"},
            ]
        ),
    )

    assert quote_called.is_set()
    assert trade_called.is_set()
    snap = ingest.snapshot()
    assert snap["SPY"]["quote_ts_ms"] == 2000
    assert snap["SPY"]["trade_ts_ms"] == 2001
    assert snap["SPY"]["ts_ms"] == 2001
    assert snap["SPY"]["last"] == 100.62
    assert "mid_px" in snap["SPY"]
    assert "trade_aggressor_imbalance" in snap["SPY"]


def test_legacy_polygon_ws_stale_and_duplicate_events_skip_record_build(monkeypatch):
    stream = _load_stream_module()
    ingest = object.__new__(stream._WsIngest)
    ingest._lock = threading.RLock()
    ingest._last = {}
    ingest._last_event_ts_by_stream = {}
    ingest._last_event_key_by_stream = {}
    ingest._dedup_drop_count = 0
    ingest._last_msg_ts_ms = 0
    ingest._session_started_ts_ms = stream._now_ms()
    ingest._last_error = None

    monkeypatch.setattr(stream, "emit_counter", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stream, "meta_set", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(stream, "meta_set_if_missing", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(stream, "set_state", lambda *_args, **_kwargs: None)

    first_payload = json.dumps({"ev": "Q", "sym": "SPY", "t": 1000, "bp": "10.00", "ap": "10.02", "q": 1})
    ingest._on_message(None, first_payload)

    build_calls = 0

    def fail_record_build(*_args, **_kwargs):
        nonlocal build_calls
        build_calls += 1
        raise AssertionError("legacy stale/duplicate event should not build a record")

    monkeypatch.setattr(stream, "_build_legacy_ws_record", fail_record_build)

    ingest._on_message(None, json.dumps({"ev": "Q", "sym": "SPY", "t": 999, "bp": "9.00", "ap": "9.02", "q": 2}))
    ingest._on_message(None, first_payload)

    assert build_calls == 0
    assert ingest._dedup_drop_count == 1
    assert ingest.snapshot()["SPY"]["bid"] == 10.00
