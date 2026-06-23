from __future__ import annotations

import json

import pytest

from engine.runtime import json_codec


def test_json_codec_loads_accepts_text_and_bytes_like_inputs(monkeypatch):
    monkeypatch.setattr(json_codec, "_orjson", None)

    expected = {"int": 10, "float": 451.25, "items": [1, 2, 3]}

    assert json_codec.loads(json.dumps(expected, separators=(",", ":"))) == expected
    assert json_codec.loads(json.dumps(expected, separators=(",", ":")).encode("utf-8")) == expected
    assert json_codec.loads(bytearray(json.dumps(expected, separators=(",", ":")).encode("utf-8"))) == expected
    assert json_codec.loads(memoryview(json.dumps(expected, separators=(",", ":")).encode("utf-8"))) == expected


def test_json_codec_invalid_json_raises_decode_error(monkeypatch):
    monkeypatch.setattr(json_codec, "_orjson", None)

    with pytest.raises(json.JSONDecodeError):
        json_codec.loads(b'{"broken":')


def test_json_codec_orjson_fast_path_keeps_text_contract(monkeypatch):
    class _FakeOrjson:
        OPT_SORT_KEYS = 1

        def __init__(self) -> None:
            self.loads_payloads = []
            self.dumps_options = []

        def loads(self, payload):
            self.loads_payloads.append(payload)
            return {"decoded": True, "payload_type": type(payload).__name__}

        def dumps(self, value, *, option=0, default=None):
            self.dumps_options.append(int(option))
            rendered = json.dumps(
                value,
                separators=(",", ":"),
                sort_keys=bool(option & self.OPT_SORT_KEYS),
                default=default,
            )
            return rendered.encode("utf-8")

    fake_orjson = _FakeOrjson()
    monkeypatch.setattr(json_codec, "_orjson", fake_orjson)

    assert json_codec.codec_name() == "orjson"
    assert json_codec.loads(b'{"ignored":true}') == {"decoded": True, "payload_type": "bytes"}
    assert json_codec.dumps_text({"z": 1, "a": 2}, sort_keys=True) == '{"a":2,"z":1}'
    assert json_codec.dumps_bytes({"z": 1, "a": 2}) == b'{"z":1,"a":2}'
    assert fake_orjson.loads_payloads == [b'{"ignored":true}']
    assert fake_orjson.dumps_options == [_FakeOrjson.OPT_SORT_KEYS, 0]
