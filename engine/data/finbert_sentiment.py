"""
Lazy FinBERT sentiment scoring and persisted feature resolution helpers.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import logging
import math
import os
import threading
from typing import Any, Dict, List, Optional, Sequence, Tuple

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.hardware import resolve_torch_device
from engine.runtime.logging import get_logger
from engine.runtime.npu import NPU_BACKEND_NAME, resolve_nlp_backend

LOG = get_logger("engine.data.finbert_sentiment")
_WARNED_NONFATAL_KEYS: set[str] = set()

USE_FINBERT_SENTIMENT = os.environ.get("USE_FINBERT_SENTIMENT", "0") == "1"
FINBERT_MODEL_NAME = str(os.environ.get("FINBERT_MODEL_NAME", "ProsusAI/finbert") or "ProsusAI/finbert").strip() or "ProsusAI/finbert"
FINBERT_BATCH_SIZE = max(1, int(os.environ.get("FINBERT_BATCH_SIZE", "16")))
FINBERT_MAX_TEXT_LEN = max(64, int(os.environ.get("FINBERT_MAX_TEXT_LEN", "4000")))
FINBERT_USE_PERSISTED_ENRICHMENT = os.environ.get("FINBERT_USE_PERSISTED_ENRICHMENT", "1") == "1"
FINBERT_LIVE_INFERENCE_ENABLED = os.environ.get("FINBERT_LIVE_INFERENCE_ENABLED", "0") == "1"

FINBERT_FEATURE_IDS = [
    "sentiment.finbert.label",
    "sentiment.finbert.score",
    "sentiment.finbert.pos",
    "sentiment.finbert.neg",
    "sentiment.finbert.neu",
    "sentiment.finbert.confidence",
]

_MODEL_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
_MODEL_LOCK = threading.Lock()


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: object) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=logging.WARNING,
        component="engine.data.finbert_sentiment",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


def _round_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    if not math.isfinite(out):
        return float(default)
    return float(round(out, 6))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def _safe_int_or_none(value: Any) -> Optional[int]:
    out = _safe_int(value, 0)
    return int(out) if out > 0 else None


def _normalize_symbol(value: Any) -> Optional[str]:
    symbol = str(value or "").upper().strip()
    return symbol or None


def _label_key(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if "pos" in raw:
        return "positive"
    if "neg" in raw:
        return "negative"
    if "neu" in raw:
        return "neutral"
    if raw == "missing":
        return "missing"
    return "neutral"


def _label_value(label: str) -> float:
    key = _label_key(label)
    if key == "positive":
        return 1.0
    if key == "negative":
        return -1.0
    return 0.0


def _zero_feature_map() -> Dict[str, float]:
    return {fid: 0.0 for fid in FINBERT_FEATURE_IDS}


def _jsonable_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _clean_text(value: Any) -> str:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return ""
    limit = int(max(64, FINBERT_MAX_TEXT_LEN))
    if len(text) <= limit:
        return text
    clipped = text[:limit].rstrip()
    if " " in clipped and limit < len(text):
        clipped = clipped.rsplit(" ", 1)[0].rstrip() or clipped
    return clipped


def _compose_text(payload: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    title = _clean_text(payload.get("title"))
    body = _clean_text(payload.get("body") or payload.get("summary"))
    text = _clean_text(payload.get("text"))
    if text:
        normalized = text
    else:
        normalized = "\n".join(part for part in (title, body) if part).strip()
    raw_text = " ".join(
        part
        for part in (
            str(payload.get("text") or "").strip(),
            str(payload.get("title") or "").strip(),
            str(payload.get("body") or payload.get("summary") or "").strip(),
        )
        if part
    ).strip()
    truncated = bool(raw_text) and (len(normalized) < len(" ".join(raw_text.split())))
    meta = {
        "missing_text": not bool(normalized),
        "text_len": int(len(normalized)),
        "text_hash": hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16] if normalized else "",
        "truncated": bool(truncated),
    }
    return normalized, meta


def _source_identifier(payload: Dict[str, Any]) -> str:
    for key in ("source_identifier", "source_id", "event_key", "event_id"):
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _resolved_device(requested: Any = None) -> Tuple[Any, str]:
    torch = importlib.import_module("torch")
    resolution = resolve_torch_device(
        torch,
        requested=requested,
        env_var="FINBERT_DEVICE",
        fallback_envs=("NLP_DEVICE", "TORCH_DEVICE"),
    )
    return torch, resolution.resolved


def load_finbert_model(
    model_name: Optional[str] = None,
    *,
    device: Optional[str] = None,
    local_files_only: Optional[bool] = None,
) -> Dict[str, Any]:
    """Load and cache the FinBERT pipeline plus lightweight model metadata."""
    resolved_model_name = str(model_name or FINBERT_MODEL_NAME).strip() or FINBERT_MODEL_NAME
    torch, resolved_device = _resolved_device(device)
    cache_key = (resolved_model_name, resolved_device)
    with _MODEL_LOCK:
        cached = _MODEL_CACHE.get(cache_key)
        if cached is not None:
            return cached

        transformers = importlib.import_module("transformers")
        tokenizer = transformers.AutoTokenizer.from_pretrained(
            resolved_model_name,
            local_files_only=bool(local_files_only) if local_files_only is not None else False,
        )
        model = transformers.AutoModelForSequenceClassification.from_pretrained(
            resolved_model_name,
            local_files_only=bool(local_files_only) if local_files_only is not None else False,
        )
        if hasattr(model, "to"):
            model = model.to(resolved_device)
        if hasattr(model, "eval"):
            model.eval()
        raw_id2label = dict(getattr(getattr(model, "config", None), "id2label", {}) or {})
        id2label = {int(idx): _label_key(label) for idx, label in raw_id2label.items()}
        if not id2label:
            id2label = {0: "negative", 1: "neutral", 2: "positive"}
        max_token_len = int(getattr(tokenizer, "model_max_length", 512) or 512)
        if max_token_len <= 0 or max_token_len > 4096:
            max_token_len = 512
        bundle = {
            "device": str(resolved_device),
            "id2label": dict(id2label),
            "max_token_len": int(min(512, max_token_len)),
            "model": model,
            "model_name": str(resolved_model_name),
            "model_version": str(getattr(transformers, "__version__", "")),
            "tokenizer": tokenizer,
            "torch": torch,
        }
        _MODEL_CACHE[cache_key] = bundle
        return bundle


def _try_npu_probabilities(texts: Sequence[str]) -> Optional[List[Dict[str, float]]]:
    """Opt-in, fail-closed NPU (ONNX/VitisAI) inference path.

    Returns ``None`` to fall back to the torch CPU/ROCm path whenever the NPU is
    not both explicitly selected AND fully installed, or if NPU scoring errors.
    The default runtime never reaches the NPU branch.
    """
    try:
        if resolve_nlp_backend().get("backend") != NPU_BACKEND_NAME:
            return None
        from engine.data.finbert_onnx_backend import score_texts_onnx

        raw = score_texts_onnx(list(texts))
    except Exception as exc:
        _warn_nonfatal("finbert_npu_backend_fallback", exc, once_key="finbert_npu_backend")
        return None
    if len(raw) != len(list(texts)):
        return None
    normalized: List[Dict[str, float]] = []
    for row in raw:
        probs_by_label = {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
        for label, value in dict(row or {}).items():
            key = _label_key(label)
            if key in probs_by_label:
                probs_by_label[key] = _round_float(value, 0.0)
        normalized.append(probs_by_label)
    return normalized


def _probabilities_for_texts(texts: Sequence[str], *, model_name: Optional[str] = None) -> List[Dict[str, float]]:
    if not texts:
        return []
    npu_rows = _try_npu_probabilities(texts)
    if npu_rows is not None:
        return npu_rows
    bundle = load_finbert_model(model_name=model_name)
    torch = bundle["torch"]
    tokenizer = bundle["tokenizer"]
    model = bundle["model"]
    device = str(bundle["device"])
    encoded = tokenizer(
        list(texts),
        padding=True,
        truncation=True,
        max_length=int(bundle["max_token_len"]),
        return_tensors="pt",
    )
    if hasattr(encoded, "to"):
        encoded = encoded.to(device)
    else:
        encoded = {
            key: value.to(device) if hasattr(value, "to") else value
            for key, value in dict(encoded or {}).items()
        }
    with torch.no_grad():
        outputs = model(**encoded)
        logits = outputs.logits
        probs = torch.nn.functional.softmax(logits, dim=-1)
    rows = probs.detach().cpu().tolist() if hasattr(probs, "detach") else []
    id2label = dict(bundle.get("id2label") or {})
    normalized: List[Dict[str, float]] = []
    for row in rows:
        probs_by_label = {"positive": 0.0, "negative": 0.0, "neutral": 0.0}
        for idx, value in enumerate(list(row or [])):
            probs_by_label[_label_key(id2label.get(int(idx), idx))] = _round_float(value, 0.0)
        normalized.append(probs_by_label)
    return normalized


def _missing_summary(metadata: Optional[Dict[str, Any]] = None, *, reason: str = "missing_text") -> Dict[str, Any]:
    payload = dict(metadata or {})
    text_meta = _jsonable_payload(payload.get("payload_json"))
    text_meta.update({"missing_text": True, "reason": str(reason), "text_hash": "", "text_len": 0, "truncated": False})
    if payload.get("source"):
        text_meta["source"] = str(payload.get("source") or "")
    if payload.get("event_type"):
        text_meta["event_type"] = str(payload.get("event_type") or "")
    return {
        "event_id": _safe_int_or_none(payload.get("event_id")),
        "source_identifier": _source_identifier(payload),
        "symbol": _normalize_symbol(payload.get("symbol")),
        "ts_ms": _safe_int(payload.get("ts_ms"), 0),
        "label": "missing",
        "score": 0.0,
        "confidence": 0.0,
        "pos": 0.0,
        "neg": 0.0,
        "neu": 0.0,
        "model_name": str(payload.get("model_name") or FINBERT_MODEL_NAME),
        "model_version": str(payload.get("model_version") or ""),
        "payload_json": text_meta,
    }


def _summary_from_probabilities(
    probabilities: Dict[str, float],
    *,
    metadata: Optional[Dict[str, Any]] = None,
    model_name: Optional[str] = None,
    model_version: Optional[str] = None,
    text_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = dict(metadata or {})
    pos = _round_float(probabilities.get("positive"), 0.0)
    neg = _round_float(probabilities.get("negative"), 0.0)
    neu = _round_float(probabilities.get("neutral"), 0.0)
    label = max(
        (("positive", pos), ("negative", neg), ("neutral", neu)),
        key=lambda item: (float(item[1]), item[0]),
    )[0]
    diagnostics = _jsonable_payload(payload.get("payload_json"))
    diagnostics.update(text_meta or {})
    diagnostics["label_value"] = _round_float(_label_value(label), 0.0)
    if payload.get("source"):
        diagnostics["source"] = str(payload.get("source") or "")
    if payload.get("event_type"):
        diagnostics["event_type"] = str(payload.get("event_type") or "")
    return {
        "event_id": _safe_int_or_none(payload.get("event_id")),
        "source_identifier": _source_identifier(payload),
        "symbol": _normalize_symbol(payload.get("symbol")),
        "ts_ms": _safe_int(payload.get("ts_ms"), 0),
        "label": str(label),
        "score": _round_float(pos - neg, 0.0),
        "confidence": _round_float(max(pos, neg, neu), 0.0),
        "pos": pos,
        "neg": neg,
        "neu": neu,
        "model_name": str(model_name or payload.get("model_name") or FINBERT_MODEL_NAME),
        "model_version": str(model_version or payload.get("model_version") or ""),
        "payload_json": diagnostics,
    }


def score_financial_text(text: str) -> Dict[str, Any]:
    """Score one financial text blob and return a normalized sentiment summary."""
    return score_event_rows([{"text": text}])[0]


def summarize_document_sentiment(text: str, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Summarize one document into sentiment probabilities and derived features."""
    payload = dict(metadata or {})
    payload["text"] = str(text or "")
    return score_event_rows([payload])[0]


def score_event_rows(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Batch-score event or news rows and attach normalized sentiment payloads."""
    prepared: List[Tuple[int, Dict[str, Any], str, Dict[str, Any]]] = []
    output: List[Optional[Dict[str, Any]]] = [None] * len(list(rows or []))
    row_list = [dict(row or {}) for row in (rows or [])]
    for idx, payload in enumerate(row_list):
        normalized_text, text_meta = _compose_text(payload)
        if not normalized_text:
            output[idx] = _missing_summary(payload)
            continue
        prepared.append((idx, payload, normalized_text, text_meta))

    if not prepared:
        return [dict(row or _missing_summary()) for row in output]

    bundle = load_finbert_model(model_name=FINBERT_MODEL_NAME)
    batch_size = int(max(1, FINBERT_BATCH_SIZE))
    for start in range(0, len(prepared), batch_size):
        batch = prepared[start : start + batch_size]
        probs = _probabilities_for_texts([item[2] for item in batch], model_name=str(bundle["model_name"]))
        for (idx, payload, _text, text_meta), prob_row in zip(batch, probs):
            output[idx] = _summary_from_probabilities(
                prob_row,
                metadata=payload,
                model_name=str(bundle["model_name"]),
                model_version=str(bundle["model_version"]),
                text_meta=text_meta,
            )

    return [dict(row or _missing_summary()) for row in output]


def finbert_feature_map_from_row(row: Optional[Dict[str, Any]]) -> Dict[str, float]:
    """Project a persisted FinBERT enrichment row into numeric feature values."""
    if not isinstance(row, dict):
        return _zero_feature_map()
    return {
        "sentiment.finbert.label": _round_float(_label_value(str(row.get("label") or "")), 0.0),
        "sentiment.finbert.score": _round_float(row.get("score"), 0.0),
        "sentiment.finbert.pos": _round_float(row.get("pos"), 0.0),
        "sentiment.finbert.neg": _round_float(row.get("neg"), 0.0),
        "sentiment.finbert.neu": _round_float(row.get("neu"), 0.0),
        "sentiment.finbert.confidence": _round_float(row.get("confidence"), 0.0),
    }


def resolve_finbert_sentiment_snapshot(
    *,
    symbol: str,
    ts_ms: int,
    event: Optional[Dict[str, Any]] = None,
    con=None,
) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    """Resolve the best available FinBERT features for one symbol and timestamp."""
    feature_map = _zero_feature_map()
    meta: Dict[str, Any] = {
        "event_id": None,
        "label": None,
        "model_name": FINBERT_MODEL_NAME,
        "model_version": None,
        "source": "disabled" if not USE_FINBERT_SENTIMENT else "none",
        "ts_ms": None,
    }
    if not USE_FINBERT_SENTIMENT:
        return feature_map, meta, False

    symbol_key = _normalize_symbol(symbol)
    anchor_ts_ms = _safe_int(ts_ms, _safe_int((event or {}).get("ts_ms"), 0))
    event_id = _safe_int_or_none((event or {}).get("event_id") if isinstance(event, dict) else None)
    if FINBERT_USE_PERSISTED_ENRICHMENT and event_id is not None:
        try:
            from engine.runtime.storage import load_finbert_sentiment_enrichment_for_event

            row = load_finbert_sentiment_enrichment_for_event(
                event_id=int(event_id),
                model_name=str(FINBERT_MODEL_NAME),
                con=con,
            )
        except Exception as exc:
            _warn_nonfatal(
                "FINBERT_PERSISTED_EVENT_LOOKUP_FAILED",
                exc,
                once_key="finbert_persisted_event_lookup_failed",
                event_id=int(event_id),
            )
            row = None
        if isinstance(row, dict):
            meta = {
                "event_id": _safe_int_or_none(row.get("event_id")),
                "label": str(row.get("label") or ""),
                "model_name": str(row.get("model_name") or FINBERT_MODEL_NAME),
                "model_version": str(row.get("model_version") or ""),
                "source": "persisted_event",
                "ts_ms": _safe_int(row.get("ts_ms"), 0),
            }
            return finbert_feature_map_from_row(row), meta, True

    if FINBERT_USE_PERSISTED_ENRICHMENT and symbol_key and anchor_ts_ms > 0:
        try:
            from engine.runtime.storage import load_latest_finbert_sentiment_enrichment

            row = load_latest_finbert_sentiment_enrichment(
                symbol=str(symbol_key),
                ts_ms=int(anchor_ts_ms),
                model_name=str(FINBERT_MODEL_NAME),
                con=con,
            )
        except Exception as exc:
            _warn_nonfatal(
                "FINBERT_PERSISTED_LOOKUP_FAILED",
                exc,
                once_key="finbert_persisted_lookup_failed",
                symbol=str(symbol_key),
                ts_ms=int(anchor_ts_ms),
            )
            row = None
        if isinstance(row, dict):
            meta = {
                "event_id": _safe_int_or_none(row.get("event_id")),
                "label": str(row.get("label") or ""),
                "model_name": str(row.get("model_name") or FINBERT_MODEL_NAME),
                "model_version": str(row.get("model_version") or ""),
                "source": "persisted",
                "ts_ms": _safe_int(row.get("ts_ms"), 0),
            }
            return finbert_feature_map_from_row(row), meta, True

    if not FINBERT_LIVE_INFERENCE_ENABLED or not isinstance(event, dict):
        return feature_map, meta, False

    try:
        summary = summarize_document_sentiment(
            _compose_text(event)[0],
            metadata={
                "event_id": event.get("event_id"),
                "event_type": event.get("event_type"),
                "source": event.get("source"),
                "source_id": event.get("source_id"),
                "symbol": symbol_key or event.get("symbol"),
                "text": event.get("text"),
                "title": event.get("title"),
                "body": event.get("body"),
                "ts_ms": anchor_ts_ms,
            },
        )
    except Exception as exc:
        _warn_nonfatal(
            "FINBERT_LIVE_LOOKUP_FAILED",
            exc,
            once_key="finbert_live_lookup_failed",
            symbol=str(symbol_key or ""),
            ts_ms=int(anchor_ts_ms),
        )
        return feature_map, meta, False
    payload = _jsonable_payload(summary.get("payload_json"))
    if payload.get("missing_text"):
        return feature_map, meta, False
    meta = {
        "event_id": _safe_int_or_none(summary.get("event_id")),
        "label": str(summary.get("label") or ""),
        "model_name": str(summary.get("model_name") or FINBERT_MODEL_NAME),
        "model_version": str(summary.get("model_version") or ""),
        "source": "live_inference",
        "ts_ms": _safe_int(summary.get("ts_ms"), 0),
    }
    return finbert_feature_map_from_row(summary), meta, True
