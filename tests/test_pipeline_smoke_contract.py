from tools import pipeline_smoke_test


def test_operator_token_header_is_sent_when_configured(monkeypatch):
    seen = {}

    class _Response:
        headers = {"content-type": "application/json"}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return b'{"ok":true}'

    def _open(req, timeout=None):
        seen["token"] = req.get_header("X-operator-token") or req.get_header("X-Operator-token")
        return _Response()

    monkeypatch.setattr(pipeline_smoke_test, "OPERATOR_TOKEN", "operator-secret")
    monkeypatch.setattr(pipeline_smoke_test.urllib.request, "urlopen", _open)

    payload = pipeline_smoke_test._operator_req("/api/operator/start", method="POST", body={"mode": "safe"})

    assert payload["ok"] is True
    assert seen["token"] == "operator-secret"


def test_http_error_json_body_is_returned_for_operator_reconciliation(monkeypatch):
    class _Headers:
        def get(self, _name, _default=None):
            return "application/json"

    class _HttpError(Exception):
        code = 422
        headers = _Headers()

        def read(self):
            return b'{"ok":false,"error":"start_failed","reason":"OPERATOR_DISABLE_INTERNAL_ENGINE_START"}'

    def _raise(*_args, **_kwargs):
        raise _HttpError()

    monkeypatch.setattr(pipeline_smoke_test.urllib.error, "HTTPError", _HttpError)
    monkeypatch.setattr(pipeline_smoke_test.urllib.request, "urlopen", _raise)

    payload = pipeline_smoke_test._req_to("http://operator", "/api/operator/start", method="POST")

    assert payload["ok"] is False
    assert payload["reason"] == "OPERATOR_DISABLE_INTERNAL_ENGINE_START"
    assert payload["meta"]["status"] == 422


def test_operator_start_proxy_only_response_is_reconciled():
    payload = {
        "ok": False,
        "error": "start_failed",
        "steps": [
            {"id": "preflight", "ok": True},
            {
                "id": "spawn",
                "ok": False,
                "detail": {
                    "ok": False,
                    "disabled": True,
                    "reason": "OPERATOR_DISABLE_INTERNAL_ENGINE_START",
                },
            },
        ],
    }

    assert pipeline_smoke_test._operator_start_is_proxy_only(payload)


def test_operator_start_regular_failure_is_not_reconciled():
    payload = {
        "ok": False,
        "error": "start_failed",
        "steps": [{"id": "spawn", "ok": False, "detail": {"error": "port_in_use"}}],
    }

    assert not pipeline_smoke_test._operator_start_is_proxy_only(payload)


def test_health_ready_for_smoke_accepts_advisory_health_reasons_without_critical_blockers():
    payload = {
        "ok": False,
        "body": {
            "db": {"ok": True},
            "prices": {"last_ts_ms": 1_700_000_000_000, "age_s": 12.0},
            "critical_blockers": [],
            "reasons": ["data_gate:model_inputs_valid"],
        },
    }

    assert pipeline_smoke_test._health_ready_for_smoke(payload)


def test_health_ready_for_smoke_accepts_provider_only_critical_blockers():
    payload = {
        "ok": False,
        "body": {
            "db": {"ok": True},
            "prices": {"last_ts_ms": 1_700_000_000_000, "age_s": 12.0},
            "critical_blockers": ["providers_not_ok"],
        },
    }

    assert pipeline_smoke_test._health_ready_for_smoke(payload)


def test_health_ready_for_smoke_rejects_execution_critical_blocker():
    payload = {
        "ok": False,
        "body": {
            "db": {"ok": True},
            "prices": {"last_ts_ms": 1_700_000_000_000, "age_s": 12.0},
            "critical_blockers": ["execution_supervisor_critical"],
        },
    }

    assert not pipeline_smoke_test._health_ready_for_smoke(payload)


def test_start_job_and_wait_accepts_successful_history_when_job_summary_is_stale(monkeypatch):
    snapshots = iter(
        [
            {"update_universe": {"running": False, "last_event_ts_ms": 100}},
            {"update_universe": {"running": False, "last_event_ts_ms": 100, "last_exit_code": 1}},
        ]
    )
    histories = iter(
        [
            [{"event": "exit", "exit_code": 0, "ts_ms": 100}],
            [{"event": "exit", "exit_code": 0, "ts_ms": 200}],
        ]
    )

    monkeypatch.setattr(pipeline_smoke_test, "_jobs_snapshot", lambda: next(snapshots))
    monkeypatch.setattr(pipeline_smoke_test, "_job_history", lambda _name, limit=10: next(histories))
    monkeypatch.setattr(pipeline_smoke_test, "_req", lambda *_args, **_kwargs: {"ok": True})

    result = pipeline_smoke_test._start_job_and_wait("update_universe", timeout_s=5)

    assert result["ok"] is True
    assert result["history"]["ts_ms"] == 200


def test_start_job_and_wait_accepts_immediate_successful_start_completion(monkeypatch):
    requests = []
    monkeypatch.setattr(
        pipeline_smoke_test,
        "_jobs_snapshot",
        lambda: {"compute_drift": {"running": False, "last_event_ts_ms": 100}},
    )
    monkeypatch.setattr(
        pipeline_smoke_test,
        "_job_history",
        lambda _name, limit=10: [{"event": "exit", "exit_code": 0, "ts_ms": 100}],
    )

    def _req(path, *args, **kwargs):
        requests.append((path, kwargs))
        return {"ok": True, "exit_code": 0}

    monkeypatch.setattr(pipeline_smoke_test, "_req", _req)

    result = pipeline_smoke_test._start_job_and_wait("compute_drift", timeout_s=5)

    assert result["ok"] is True
    assert result["start"]["exit_code"] == 0
    assert result["history"] is None
    assert requests[0][1]["body"]["confirmation"] == "JOB_ACTION"
    assert requests[0][1]["body"]["consequence_ack"] is True
