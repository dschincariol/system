"""ONNX Runtime + VitisAI (NPU) inference backend for FinBERT.

This is the NPU counterpart to the torch path in ``engine.data.finbert_sentiment``.
The XDNA2 NPU runs INT8-quantized models through ONNX Runtime's VitisAI execution
provider; it is reached here, never through torch.

Design contract:
- Opt-in and fail-closed. The execution provider list always ends in
  ``CPUExecutionProvider`` so a missing/partial VitisAI install degrades to CPU
  ONNX rather than crashing. ``engine.runtime.npu.resolve_nlp_backend`` decides
  whether this backend is selected at all.
- Inference only. No training, no order path, no secrets.
- Same output shape as the torch scorer: a list of ``{label: probability}`` maps.

Everything heavy (onnxruntime, transformers, numpy) is imported lazily so this
module is safe to import on a host without the NPU stack installed.
"""

from __future__ import annotations

import os
import threading
from typing import Any, Dict, List, Optional, Sequence

from engine.runtime.npu import probe_npu_stack

FINBERT_ONNX_PATH_ENV = "FINBERT_ONNX_PATH"
VITISAI_CONFIG_ENV = "VITISAI_CONFIG"
DEFAULT_MAX_TOKENS = 256

_SESSION_LOCK = threading.Lock()
_SESSION_CACHE: Dict[Any, Dict[str, Any]] = {}


class NpuBackendUnavailable(RuntimeError):
    """Raised when the ONNX/VitisAI FinBERT backend cannot be constructed."""


def _model_path(model_path: Optional[str]) -> str:
    path = str(model_path or os.environ.get(FINBERT_ONNX_PATH_ENV) or "").strip()
    if not path:
        raise NpuBackendUnavailable("finbert_onnx_path_unset:set FINBERT_ONNX_PATH to the quantized model")
    if not os.path.exists(path):
        raise NpuBackendUnavailable(f"finbert_onnx_model_missing:{path}")
    return path


def _load_id2label(model_path: str) -> Dict[int, str]:
    """Read the label order from the model's config.json next to the ONNX file.

    The label index order is model-specific (ProsusAI/finbert is
    {0: positive, 1: negative, 2: neutral}), so it must come from config, not a
    guess. Falls back to the ProsusAI default rather than an arbitrary order.
    """
    default = {0: "positive", 1: "negative", 2: "neutral"}
    try:
        import json

        config_path = os.path.join(os.path.dirname(model_path), "config.json")
        with open(config_path, "r", encoding="utf-8") as handle:
            raw = dict(json.load(handle).get("id2label") or {})
        mapping = {int(k): str(v).strip().lower() for k, v in raw.items()}
        return mapping or default
    except Exception:
        return default


def _provider_list(*, prefer_npu: bool) -> List[Any]:
    """VitisAI first when available, always CPU last as the fail-closed floor."""
    providers: List[Any] = []
    if prefer_npu:
        try:
            import onnxruntime as ort  # type: ignore

            if "VitisAIExecutionProvider" in set(ort.get_available_providers()):
                config = str(os.environ.get(VITISAI_CONFIG_ENV) or "").strip()
                provider_options = {"config_file": config} if config else {}
                providers.append(("VitisAIExecutionProvider", provider_options))
        except Exception:
            pass  # no-op-guard: fall through to CPU EP if ORT/EP probing fails.
    providers.append("CPUExecutionProvider")
    return providers


def load_finbert_onnx_session(
    *,
    model_path: Optional[str] = None,
    tokenizer_name: Optional[str] = None,
    prefer_npu: bool = True,
) -> Dict[str, Any]:
    """Load and cache an ORT session + tokenizer for the quantized FinBERT model."""
    resolved_path = _model_path(model_path)
    cache_key = (resolved_path, bool(prefer_npu), str(tokenizer_name or ""))
    with _SESSION_LOCK:
        cached = _SESSION_CACHE.get(cache_key)
        if cached is not None:
            return cached
        try:
            import onnxruntime as ort  # type: ignore
            import transformers  # type: ignore
        except Exception as exc:
            raise NpuBackendUnavailable(f"onnx_runtime_or_transformers_missing:{type(exc).__name__}: {exc}") from exc

        providers = _provider_list(prefer_npu=prefer_npu)
        try:
            session = ort.InferenceSession(resolved_path, providers=[p if isinstance(p, str) else p[0] for p in providers])
            # Re-apply provider options when present (VitisAI config file).
            opts = {p[0]: p[1] for p in providers if not isinstance(p, str)}
            if opts:
                session = ort.InferenceSession(resolved_path, providers=providers)
        except Exception as exc:
            raise NpuBackendUnavailable(f"onnx_session_init_failed:{type(exc).__name__}: {exc}") from exc

        resolved_tokenizer = str(tokenizer_name or os.environ.get("FINBERT_MODEL_NAME") or "ProsusAI/finbert")
        tokenizer = transformers.AutoTokenizer.from_pretrained(resolved_tokenizer)
        id2label = _load_id2label(resolved_path)
        bundle = {
            "session": session,
            "tokenizer": tokenizer,
            "id2label": id2label,
            "providers": session.get_providers(),
            "model_path": resolved_path,
            "input_names": [i.name for i in session.get_inputs()],
        }
        _SESSION_CACHE[cache_key] = bundle
        return bundle


def _softmax(logits: Any) -> Any:
    import numpy as np

    shifted = logits - np.max(logits, axis=-1, keepdims=True)
    exp = np.exp(shifted)
    return exp / np.sum(exp, axis=-1, keepdims=True)


def score_texts_onnx(
    texts: Sequence[str],
    *,
    model_path: Optional[str] = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    prefer_npu: bool = True,
) -> List[Dict[str, float]]:
    """Return per-text ``{label: probability}`` maps via ONNX Runtime (NPU/CPU)."""
    items = [str(t or "") for t in texts]
    if not items:
        return []
    import numpy as np

    bundle = load_finbert_onnx_session(model_path=model_path, prefer_npu=prefer_npu)
    tokenizer = bundle["tokenizer"]
    session = bundle["session"]
    id2label = bundle["id2label"]
    wanted = set(bundle["input_names"])

    encoded = tokenizer(
        items,
        return_tensors="np",
        padding="max_length",
        truncation=True,
        max_length=int(max_tokens),
    )
    feeds = {name: np.asarray(value).astype("int64") for name, value in encoded.items() if name in wanted}
    logits = session.run(None, feeds)[0]
    probs = _softmax(np.asarray(logits, dtype="float32"))
    out: List[Dict[str, float]] = []
    for row in probs:
        out.append({str(id2label.get(int(i), str(i))): float(row[int(i)]) for i in range(len(row))})
    return out


def npu_backend_ready(*, model_path: Optional[str] = None) -> Dict[str, Any]:
    """Non-fatal readiness report for the ONNX/VitisAI FinBERT backend."""
    snap = probe_npu_stack()
    path = str(model_path or os.environ.get(FINBERT_ONNX_PATH_ENV) or "").strip()
    model_present = bool(path and os.path.exists(path))
    return {
        "npu_inference_ready": bool(snap.get("inference_ready")),
        "onnx_model_present": model_present,
        "onnx_model_path": path,
        "ready": bool(snap.get("inference_ready") and model_present),
        "next_step": snap.get("next_step") if not snap.get("inference_ready") else (
            "set_FINBERT_ONNX_PATH" if not model_present else "ready"
        ),
    }


__all__ = [
    "NpuBackendUnavailable",
    "load_finbert_onnx_session",
    "score_texts_onnx",
    "npu_backend_ready",
]
