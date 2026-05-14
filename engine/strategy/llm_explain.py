"""
FILE: llm_explain.py

Best-effort wrapper around an external `llmExplain` helper. This is a guarded
utility for human-readable explanations and is designed never to block the main
runtime indefinitely.
"""

import os
import threading
import importlib

VOICE_ENABLED = os.environ.get("VOICE_ENABLED", "1") == "1"
VOICE_TIMEOUT_S = float(os.environ.get("VOICE_TIMEOUT_S", "6.0"))
VOICE_MAX_PROMPT_CHARS = int(os.environ.get("VOICE_MAX_PROMPT_CHARS", "8000"))
VOICE_MAX_RESPONSE_CHARS = int(os.environ.get("VOICE_MAX_RESPONSE_CHARS", "700"))


def run_llm_explain_with_timeout(prompt: str, timeout_s: float) -> str:
    """
    Runs `llmExplain(prompt)` in a worker thread with a timeout.
    """
    if not VOICE_ENABLED:
        raise RuntimeError("llmExplain disabled by VOICE_ENABLED=0")

    prompt = str(prompt or "")[:VOICE_MAX_PROMPT_CHARS]

    result = {}
    error = {}

    def _worker():
        try:
            try:
                llm_module = importlib.import_module("llm")
            except Exception as e:
                raise RuntimeError("llmExplain unavailable") from e

            explain_fn = getattr(llm_module, "llmExplain", None)
            if not callable(explain_fn):
                raise RuntimeError("llmExplain unavailable")
            txt = explain_fn(prompt)
            result["text"] = (str(txt) if txt is not None else "")[:VOICE_MAX_RESPONSE_CHARS]
        except Exception as e:
            error["error"] = str(e)

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    t.join(timeout=float(timeout_s))

    if t.is_alive():
        raise RuntimeError("llmExplain timeout")

    if error.get("error"):
        raise RuntimeError(error["error"])

    return str(result.get("text") or "")
