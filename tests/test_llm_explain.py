from __future__ import annotations

import importlib
import sys
import types

import pytest


def test_run_llm_explain_truncates_prompt_and_response(monkeypatch) -> None:
    seen_prompts: list[str] = []
    fake_llm = types.ModuleType("llm")

    def _explain(prompt: str) -> str:
        seen_prompts.append(prompt)
        return "abcdefg"

    fake_llm.llmExplain = _explain
    monkeypatch.setitem(sys.modules, "llm", fake_llm)
    monkeypatch.setenv("VOICE_ENABLED", "1")
    monkeypatch.setenv("VOICE_MAX_PROMPT_CHARS", "4")
    monkeypatch.setenv("VOICE_MAX_RESPONSE_CHARS", "5")

    module = importlib.import_module("engine.strategy.llm_explain")
    module = importlib.reload(module)

    assert module.run_llm_explain_with_timeout("123456", 1.0) == "abcde"
    assert seen_prompts == ["1234"]


def test_run_llm_explain_respects_disabled_flag(monkeypatch) -> None:
    monkeypatch.setenv("VOICE_ENABLED", "0")

    module = importlib.import_module("engine.strategy.llm_explain")
    module = importlib.reload(module)

    with pytest.raises(RuntimeError, match="disabled"):
        module.run_llm_explain_with_timeout("hello", 1.0)
