"""Lazy local transformer encoders used by offline NLP jobs."""

from __future__ import annotations

import gc
import importlib
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Iterable, Sequence

import numpy as np

from engine.runtime.hardware import resolve_torch_device
from engine.runtime.platform import default_local_models_dir


DEFAULT_MODEL_CACHE_DIR = Path(
    os.environ.get("NLP_MODEL_CACHE_DIR", str((default_local_models_dir() / "nlp").resolve()))
)
FINBERT_LABEL_ORDER = ("positive", "negative", "neutral")


def _default_cache_dir() -> str:
    DEFAULT_MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return str(DEFAULT_MODEL_CACHE_DIR)


def _as_text_list(texts: Sequence[str] | Iterable[str]) -> list[str]:
    return [str(text or "") for text in list(texts or [])]


def _normalize_probability_rows(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    if arr.shape[1] != 3:
        raise ValueError(f"expected_finbert_probability_dim_3:{arr.shape}")
    arr = np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0)
    arr = np.clip(arr, 0.0, 1.0)
    sums = arr.sum(axis=1, keepdims=True)
    bad = sums.squeeze(axis=1) <= 0.0
    if np.any(bad):
        arr[bad] = np.array([0.0, 0.0, 1.0], dtype=np.float32)
        sums = arr.sum(axis=1, keepdims=True)
    return (arr / sums).astype(np.float32)


def _collect_released_model_memory(torch_module: object | None = None) -> None:
    gc.collect()
    if torch_module is None:
        try:
            torch_module = importlib.import_module("torch")
        except Exception:
            return
    cuda = getattr(torch_module, "cuda", None)
    if cuda is None:
        return
    is_available = getattr(cuda, "is_available", None)
    try:
        if callable(is_available) and not bool(is_available()):
            return
    except Exception:
        return
    empty_cache = getattr(cuda, "empty_cache", None)
    if callable(empty_cache):
        try:
            empty_cache()
        except Exception:
            return


class Encoder(ABC):
    """Common encoder contract for batched text-to-array transforms."""

    model_name: str

    @abstractmethod
    def encode(self, texts: list[str]) -> np.ndarray:
        """Encode a batch of texts into a numeric array."""

    @abstractmethod
    def release(self) -> None:
        """Release cached model state and any accelerator cache."""


class FinBertSentimentEncoder(Encoder):
    """ProsusAI FinBERT wrapper returning probabilities in pos/neg/neutral order."""

    def __init__(
        self,
        model_name: str = "ProsusAI/finbert",
        *,
        batch_size: int = 32,
        cache_dir: str | os.PathLike[str] | None = None,
        device: str | None = None,
        local_files_only: bool = False,
        predict_fn: Callable[[list[str]], np.ndarray] | None = None,
    ) -> None:
        self.model_name = str(model_name or "ProsusAI/finbert")
        self.batch_size = max(1, int(batch_size or 32))
        self.cache_dir = str(cache_dir) if cache_dir is not None else _default_cache_dir()
        self.device = str(device).strip() if device else ""
        self.local_files_only = bool(local_files_only)
        self._predict_fn = predict_fn
        self._bundle: dict[str, object] | None = None

    def __enter__(self) -> "FinBertSentimentEncoder":
        if self._predict_fn is None:
            self._load_bundle()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()

    def release(self) -> None:
        torch = self._bundle.get("torch") if self._bundle is not None else None
        self._bundle = None
        _collect_released_model_memory(torch)

    def encode(self, texts: list[str]) -> np.ndarray:
        rows = _as_text_list(texts)
        if not rows:
            return np.zeros((0, 3), dtype=np.float32)
        outputs: list[np.ndarray] = []
        for start in range(0, len(rows), self.batch_size):
            batch = rows[start : start + self.batch_size]
            if self._predict_fn is not None:
                probs = self._predict_fn(batch)
            else:
                probs = self._encode_batch(batch)
            outputs.append(_normalize_probability_rows(np.asarray(probs, dtype=np.float32)))
        return np.vstack(outputs).astype(np.float32)

    def scores(self, texts: list[str]) -> np.ndarray:
        return self.score_from_probabilities(self.encode(texts))

    @staticmethod
    def score_from_probabilities(probabilities: np.ndarray) -> np.ndarray:
        probs = _normalize_probability_rows(probabilities)
        return (probs[:, 0] - probs[:, 1]).astype(np.float32)

    @staticmethod
    def labels_from_probabilities(probabilities: np.ndarray) -> list[str]:
        probs = _normalize_probability_rows(probabilities)
        return [FINBERT_LABEL_ORDER[int(idx)] for idx in np.argmax(probs, axis=1)]

    def _resolve_device(self):
        torch = importlib.import_module("torch")
        resolution = resolve_torch_device(
            torch,
            requested=self.device or None,
            env_var="FINBERT_DEVICE",
            fallback_envs=("NLP_DEVICE", "TORCH_DEVICE"),
        )
        return torch, resolution.resolved

    def _load_bundle(self) -> dict[str, object]:
        if self._bundle is not None:
            return self._bundle
        transformers = importlib.import_module("transformers")
        torch, device = self._resolve_device()
        tokenizer = transformers.AutoTokenizer.from_pretrained(
            self.model_name,
            cache_dir=self.cache_dir,
            local_files_only=self.local_files_only,
        )
        model = transformers.AutoModelForSequenceClassification.from_pretrained(
            self.model_name,
            cache_dir=self.cache_dir,
            local_files_only=self.local_files_only,
        )
        if hasattr(model, "to"):
            model = model.to(device)
        if hasattr(model, "eval"):
            model.eval()
        raw_id2label = dict(getattr(getattr(model, "config", None), "id2label", {}) or {})
        id2label: dict[int, str] = {}
        for idx, label in raw_id2label.items():
            text = str(label or "").strip().lower()
            if "pos" in text:
                id2label[int(idx)] = "positive"
            elif "neg" in text:
                id2label[int(idx)] = "negative"
            elif "neu" in text:
                id2label[int(idx)] = "neutral"
        if not id2label:
            id2label = {0: "positive", 1: "negative", 2: "neutral"}
        max_len = int(getattr(tokenizer, "model_max_length", 512) or 512)
        if max_len <= 0 or max_len > 4096:
            max_len = 512
        self._bundle = {
            "device": device,
            "id2label": id2label,
            "max_length": min(512, max_len),
            "model": model,
            "tokenizer": tokenizer,
            "torch": torch,
        }
        return self._bundle

    def _encode_batch(self, texts: list[str]) -> np.ndarray:
        bundle = self._load_bundle()
        tokenizer = bundle["tokenizer"]
        model = bundle["model"]
        torch = bundle["torch"]
        device = str(bundle["device"])
        encoded = tokenizer(
            list(texts),
            padding=True,
            truncation=True,
            max_length=int(bundle["max_length"]),
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
            logits = model(**encoded).logits
            probs = torch.nn.functional.softmax(logits, dim=-1)
        raw = probs.detach().cpu().numpy()
        out = np.zeros((len(texts), 3), dtype=np.float32)
        id2label = dict(bundle.get("id2label") or {})
        label_to_idx = {label: idx for idx, label in enumerate(FINBERT_LABEL_ORDER)}
        for raw_idx in range(raw.shape[1]):
            label = str(id2label.get(int(raw_idx), "") or "").lower()
            if label not in label_to_idx:
                continue
            out[:, label_to_idx[label]] = raw[:, raw_idx]
        return out


class SentenceTransformerEncoder(Encoder):
    """Sentence-transformer wrapper for filings and transcript embeddings."""

    def __init__(
        self,
        model_name: str = "all-MiniLM-L6-v2",
        *,
        batch_size: int = 64,
        cache_dir: str | os.PathLike[str] | None = None,
        device: str | None = None,
        encode_fn: Callable[[list[str]], np.ndarray] | None = None,
    ) -> None:
        self.model_name = str(model_name or "all-MiniLM-L6-v2")
        self.batch_size = max(1, int(batch_size or 64))
        self.cache_dir = str(cache_dir) if cache_dir is not None else _default_cache_dir()
        self.device = str(device).strip() if device else ""
        self._encode_fn = encode_fn
        self._model = None

    def __enter__(self) -> "SentenceTransformerEncoder":
        if self._encode_fn is None:
            self._load_model()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.release()

    def release(self) -> None:
        self._model = None
        _collect_released_model_memory()

    def encode(self, texts: list[str]) -> np.ndarray:
        rows = _as_text_list(texts)
        if not rows:
            return np.zeros((0, 0), dtype=np.float32)
        if self._encode_fn is not None:
            return np.asarray(self._encode_fn(rows), dtype=np.float32)
        model = self._load_model()
        return np.asarray(
            model.encode(
                rows,
                batch_size=self.batch_size,
                convert_to_numpy=True,
                normalize_embeddings=False,
                show_progress_bar=False,
            ),
            dtype=np.float32,
        )

    def _load_model(self):
        if self._model is not None:
            return self._model
        module = importlib.import_module("sentence_transformers")
        torch = importlib.import_module("torch")
        resolution = resolve_torch_device(
            torch,
            requested=self.device or None,
            env_var="NLP_DEVICE",
            fallback_envs=("EMBED_DEVICE", "TORCH_DEVICE"),
        )
        kwargs = {"cache_folder": self.cache_dir}
        kwargs["device"] = resolution.resolved
        self._model = module.SentenceTransformer(self.model_name, **kwargs)
        return self._model
