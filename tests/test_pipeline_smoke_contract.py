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
