"""Lazy local transformer encoders used by offline NLP jobs."""

from __future__ import annotations

import gc
import hashlib
import importlib
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np

from engine.runtime.hardware import resolve_torch_device
from engine.runtime.platform import default_local_models_dir


DEFAULT_MODEL_CACHE_DIR = Path(
    os.environ.get("NLP_MODEL_CACHE_DIR", str((default_local_models_dir() / "nlp").resolve()))
)
FINBERT_LABEL_ORDER = ("positive", "negative", "neutral")
DEFAULT_FINBERT_MODEL = "ProsusAI/finbert"
DEFAULT_SENTENCE_MODEL = "all-MiniLM-L6-v2"
DEFAULT_OPENAI_EMBED_MODEL = "text-embedding-3-small"
DEFAULT_FINANCIAL_EMBED_MODEL = "FinanceMTEB/FinE5"
DEFAULT_HASHING_MODEL = "hashing-v1"


class EncoderUnavailableError(RuntimeError):
    """Raised when an optional encoder backend is requested but unavailable."""


@dataclass(frozen=True)
class ModelCardMetadata:
    backend: str
    model_name: str
    task: str
    model_card_url: str
    license: str
    license_review_status: str
    default_role: str
    direct_trading_authority: bool = False
    notes: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "backend": self.backend,
            "model_name": self.model_name,
            "task": self.task,
            "model_card_url": self.model_card_url,
            "license": self.license,
            "license_review_status": self.license_review_status,
            "default_role": self.default_role,
            "direct_trading_authority": bool(self.direct_trading_authority),
            "notes": self.notes,
        }


_MODEL_CARD_METADATA: dict[tuple[str, str], ModelCardMetadata] = {
    ("finbert", DEFAULT_FINBERT_MODEL.lower()): ModelCardMetadata(
        backend="finbert",
        model_name=DEFAULT_FINBERT_MODEL,
        task="finance_sentiment",
        model_card_url="https://huggingface.co/ProsusAI/finbert",
        license="operator-review-required",
        license_review_status="legacy_conservative_fallback_review_required",
        default_role="sentiment_fallback",
        notes="Existing conservative fallback. Review upstream model card/license before new production rollout.",
    ),
    ("sentence_transformer", DEFAULT_SENTENCE_MODEL.lower()): ModelCardMetadata(
        backend="sentence_transformer",
        model_name=DEFAULT_SENTENCE_MODEL,
        task="general_text_embedding",
        model_card_url="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2",
        license="apache-2.0",
        license_review_status="reviewed_public_model_card",
        default_role="legacy_embedding_fallback",
        notes="Legacy sentence-transformer embedding fallback; not finance-domain specialized.",
    ),
    ("openai", DEFAULT_OPENAI_EMBED_MODEL.lower()): ModelCardMetadata(
        backend="openai",
        model_name=DEFAULT_OPENAI_EMBED_MODEL,
        task="api_text_embedding",
        model_card_url="https://platform.openai.com/docs/guides/embeddings",
        license="service-terms",
        license_review_status="operator_service_terms_review_required",
        default_role="optional_api_embedding",
        notes="Requires explicit API credentials and service-terms review.",
    ),
    ("financial_sentence_transformer", DEFAULT_FINANCIAL_EMBED_MODEL.lower()): ModelCardMetadata(
        backend="financial_sentence_transformer",
        model_name=DEFAULT_FINANCIAL_EMBED_MODEL,
        task="finance_text_embedding",
        model_card_url="https://huggingface.co/FinanceMTEB/FinE5",
        license="model-card-review-required",
        license_review_status="operator_ack_required",
        default_role="finance_embedding_candidate",
        notes="Finance-domain candidate. Keep local_files_only and require operator model-card/license acknowledgement before use.",
    ),
    ("hashing", DEFAULT_HASHING_MODEL.lower()): ModelCardMetadata(
        backend="hashing",
        model_name=DEFAULT_HASHING_MODEL,
        task="deterministic_text_embedding",
        model_card_url="internal://engine.nlp.encoder.HashingEmbeddingEncoder",
        license="internal",
        license_review_status="not_applicable",
        default_role="test_and_degraded_fallback",
        notes="Deterministic local hashing fallback for tests/degraded operation; never benchmark-promotable by itself.",
    ),
}


@dataclass(frozen=True)
class TextEmbeddingConfig:
    backend: str
    model_name: str
    batch_size: int = 64
    cache_dir: str | None = None
    device: str = ""
    local_files_only: bool = False
    fallback_policy: str = "skip"
    dim: int = 128
    api_key: str = ""

    @property
    def namespace(self) -> str:
        return embedding_namespace(self.backend, self.model_name)

    @property
    def metadata(self) -> dict[str, Any]:
        return model_card_metadata(self.backend, self.model_name).to_dict()


@dataclass(frozen=True)
class SentimentEncoderConfig:
    backend: str
    model_name: str
    fallback_model_name: str = DEFAULT_FINBERT_MODEL
    batch_size: int = 32
    cache_dir: str | None = None
    device: str = ""
    local_files_only: bool = False

    @property
    def namespace(self) -> str:
        return embedding_namespace(self.backend, self.model_name)

    @property
    def metadata(self) -> dict[str, Any]:
        return model_card_metadata(self.backend, self.model_name).to_dict()


@dataclass(frozen=True)
class EncodedTextBatch:
    values: np.ndarray
    requested_config: TextEmbeddingConfig
    effective_config: TextEmbeddingConfig
    degraded: bool = False
    errors: tuple[str, ...] = ()

    @property
    def backend(self) -> str:
        return self.effective_config.backend

    @property
    def model_name(self) -> str:
        return self.effective_config.model_name

    @property
    def namespace(self) -> str:
        return self.effective_config.namespace


def _default_cache_dir() -> str:
    DEFAULT_MODEL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return str(DEFAULT_MODEL_CACHE_DIR)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(str(name))
    if raw is None:
        return bool(default)
    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return bool(default)


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    try:
        value = int(os.environ.get(str(name), str(default)) or default)
    except Exception:
        value = int(default)
    return max(int(minimum), int(value))


def canonical_embedding_backend(raw_backend: Any, *, default: str = "sentence_transformer") -> str:
    text = str(raw_backend or default or "sentence_transformer").strip().lower().replace("-", "_")
    aliases = {
        "current": "finbert",
        "default": "finbert",
        "nlp_finbert": "finbert",
        "hf_finbert": "finbert",
        "sentence": "sentence_transformer",
        "sentence_transformers": "sentence_transformer",
        "sbert": "sentence_transformer",
        "st": "sentence_transformer",
        "finance": "financial_sentence_transformer",
        "fin_e5": "financial_sentence_transformer",
        "fine5": "financial_sentence_transformer",
        "financial": "financial_sentence_transformer",
        "financial_sentence": "financial_sentence_transformer",
        "financial_sentence_transformers": "financial_sentence_transformer",
        "api": "openai",
        "openai_api": "openai",
        "hash": "hashing",
        "test_hash": "hashing",
    }
    return aliases.get(text, text or str(default or "sentence_transformer"))


def embedding_namespace(backend: Any, model_name: Any) -> str:
    backend_key = canonical_embedding_backend(backend, default="unknown")
    model_key = " ".join(str(model_name or "").split()).strip() or "unknown"
    return f"{backend_key}:{model_key}"


def encoder_namespace(encoder: Any) -> str:
    try:
        explicit = vars(encoder).get("namespace")
    except TypeError:
        explicit = None
    if explicit:
        return str(explicit)
    backend = getattr(encoder, "backend", None) or "legacy"
    model_name = getattr(encoder, "model_name", None) or "unknown"
    return embedding_namespace(backend, model_name)


def model_card_metadata(backend: Any, model_name: Any) -> ModelCardMetadata:
    backend_key = canonical_embedding_backend(backend, default="unknown")
    model_key = str(model_name or "").strip() or "unknown"
    known = _MODEL_CARD_METADATA.get((backend_key, model_key.lower()))
    if known is not None:
        return known
    if backend_key in {"sentence_transformer", "financial_sentence_transformer", "finbert"} and "/" in model_key:
        url = f"https://huggingface.co/{model_key}"
    elif backend_key == "openai":
        url = "https://platform.openai.com/docs/guides/embeddings"
    else:
        url = ""
    return ModelCardMetadata(
        backend=backend_key,
        model_name=model_key,
        task="text_embedding" if backend_key != "finbert" else "finance_sentiment",
        model_card_url=url,
        license="operator-review-required",
        license_review_status="operator_review_required",
        default_role="operator_configured",
        notes="Model was supplied by configuration; persist this metadata and review the upstream card/license before promotion use.",
    )


def _model_metadata_dict(backend: Any, model_name: Any) -> dict[str, Any]:
    data = model_card_metadata(backend, model_name).to_dict()
    data["namespace"] = embedding_namespace(backend, model_name)
    return data


def _require_financial_model_review(config: TextEmbeddingConfig) -> None:
    if canonical_embedding_backend(config.backend) != "financial_sentence_transformer":
        return
    if _env_bool("NLP_FINANCIAL_EMBED_LICENSE_REVIEW_ACK", False) or _env_bool(
        "FINANCIAL_TEXT_MODEL_LICENSE_REVIEW_ACK",
        False,
    ):
        return
    raise EncoderUnavailableError(
        "financial_embedding_model_license_review_required:"
        f"{config.model_name}:set_NLP_FINANCIAL_EMBED_LICENSE_REVIEW_ACK=1_after_model_card_review"
    )


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

    backend: str = "unknown"
    model_name: str

    @property
    def namespace(self) -> str:
        return encoder_namespace(self)

    @property
    def model_metadata(self) -> dict[str, Any]:
        return _model_metadata_dict(getattr(self, "backend", "unknown"), getattr(self, "model_name", "unknown"))

    @abstractmethod
    def encode(self, texts: list[str]) -> np.ndarray:
        """Encode a batch of texts into a numeric array."""

    @abstractmethod
    def release(self) -> None:
        """Release cached model state and any accelerator cache."""


class FinBertSentimentEncoder(Encoder):
    """Finance sequence-classifier wrapper returning probabilities in pos/neg/neutral order."""

    def __init__(
        self,
        model_name: str = DEFAULT_FINBERT_MODEL,
        *,
        backend: str = "finbert",
        batch_size: int = 32,
        cache_dir: str | os.PathLike[str] | None = None,
        device: str | None = None,
        local_files_only: bool = False,
        predict_fn: Callable[[list[str]], np.ndarray] | None = None,
    ) -> None:
        self.backend = canonical_embedding_backend(backend, default="finbert")
        self.model_name = str(model_name or DEFAULT_FINBERT_MODEL)
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
        model_name: str = DEFAULT_SENTENCE_MODEL,
        *,
        backend: str = "sentence_transformer",
        batch_size: int = 64,
        cache_dir: str | os.PathLike[str] | None = None,
        device: str | None = None,
        local_files_only: bool = False,
        encode_fn: Callable[[list[str]], np.ndarray] | None = None,
    ) -> None:
        self.backend = canonical_embedding_backend(backend, default="sentence_transformer")
        self.model_name = str(model_name or DEFAULT_SENTENCE_MODEL)
        self.batch_size = max(1, int(batch_size or 64))
        self.cache_dir = str(cache_dir) if cache_dir is not None else _default_cache_dir()
        self.device = str(device).strip() if device else ""
        self.local_files_only = bool(local_files_only)
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
        if self.local_files_only:
            kwargs["local_files_only"] = True
        self._model = module.SentenceTransformer(self.model_name, **kwargs)
        return self._model


class HashingEmbeddingEncoder(Encoder):
    """Deterministic local hashing encoder for tests and degraded operation."""

    def __init__(self, model_name: str = DEFAULT_HASHING_MODEL, *, dim: int = 128) -> None:
        self.backend = "hashing"
        self.model_name = str(model_name or DEFAULT_HASHING_MODEL)
        self.dim = max(1, int(dim or 128))

    def release(self) -> None:
        return None

    def encode(self, texts: list[str]) -> np.ndarray:
        rows = _as_text_list(texts)
        if not rows:
            return np.zeros((0, self.dim), dtype=np.float32)
        vectors: list[np.ndarray] = []
        for text in rows:
            vec = np.zeros(self.dim, dtype=np.float32)
            for token in str(text or "").lower().split():
                digest = hashlib.blake2b(token.encode("utf-8", errors="ignore"), digest_size=8).digest()
                bucket = int.from_bytes(digest[:4], "little") % self.dim
                sign = 1.0 if (digest[4] % 2) == 0 else -1.0
                vec[bucket] += float(sign)
            vectors.append(vec)
        return np.vstack(vectors).astype(np.float32)


class OpenAIEmbeddingEncoder(Encoder):
    """Lazy OpenAI embedding API wrapper."""

    def __init__(
        self,
        model_name: str = DEFAULT_OPENAI_EMBED_MODEL,
        *,
        batch_size: int = 128,
        api_key: str | None = None,
    ) -> None:
        self.backend = "openai"
        self.model_name = str(model_name or DEFAULT_OPENAI_EMBED_MODEL)
        self.batch_size = max(1, int(batch_size or 128))
        self.api_key = str(api_key or "").strip()
        self._client = None

    def release(self) -> None:
        self._client = None

    def _resolve_api_key(self) -> str:
        if self.api_key:
            return self.api_key
        try:
            from engine.data._credentials import get_data_credential

            value = str(get_data_credential("OPENAI_API_KEY") or "").strip()
            if value:
                return value
        except Exception:
            pass  # no-op-guard: allow - credential provider lookup is optional before env fallback.
        return str(os.environ.get("OPENAI_API_KEY") or "").strip()

    def _load_client(self):
        if self._client is not None:
            return self._client
        api_key = self._resolve_api_key()
        if not api_key:
            raise EncoderUnavailableError("openai_embedding_backend_requires_OPENAI_API_KEY")
        try:
            module = importlib.import_module("openai")
        except Exception as exc:
            raise EncoderUnavailableError("openai_embedding_backend_missing_openai_package") from exc
        self._client = module.OpenAI(api_key=api_key)
        return self._client

    def encode(self, texts: list[str]) -> np.ndarray:
        rows = _as_text_list(texts)
        if not rows:
            return np.zeros((0, 0), dtype=np.float32)
        client = self._load_client()
        vectors: list[list[float]] = []
        for start in range(0, len(rows), self.batch_size):
            batch = rows[start : start + self.batch_size]
            response = client.embeddings.create(model=self.model_name, input=batch)
            vectors.extend([list(item.embedding) for item in response.data])
        return np.asarray(vectors, dtype=np.float32)


def _normalize_encoded_matrix(values: np.ndarray) -> np.ndarray:
    arr = np.asarray(values, dtype=np.float32)
    if arr.ndim == 1:
        arr = arr.reshape(1, -1)
    return np.nan_to_num(arr, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)


def resolve_text_embedding_config(
    *,
    kind: str = "news",
    backend: str | None = None,
    model_name: str | None = None,
) -> TextEmbeddingConfig:
    kind_key = str(kind or "news").strip().lower()
    if backend is None:
        if kind_key == "news":
            backend = os.environ.get("NEWS_EMBED_BACKEND", "current")
        else:
            backend = os.environ.get("NLP_EMBED_BACKEND", "sentence_transformer")
    backend_key = canonical_embedding_backend(backend, default="sentence_transformer")

    if model_name is None:
        if backend_key == "finbert":
            model_name = (
                os.environ.get("NEWS_EMBED_FINBERT_MODEL")
                or os.environ.get("NLP_SENTIMENT_MODEL_NAME")
                or os.environ.get("NLP_FINBERT_MODEL_NAME")
                or os.environ.get("FINBERT_MODEL_NAME")
                or DEFAULT_FINBERT_MODEL
            )
        elif backend_key == "sentence_transformer":
            env_name = "NEWS_EMBED_SENTENCE_MODEL" if kind_key == "news" else "NLP_SENTENCE_MODEL_NAME"
            model_name = os.environ.get(env_name) or os.environ.get("NLP_SENTENCE_MODEL_NAME") or DEFAULT_SENTENCE_MODEL
        elif backend_key == "financial_sentence_transformer":
            model_name = (
                os.environ.get("NEWS_EMBED_FINANCIAL_MODEL")
                or os.environ.get("NLP_FINANCIAL_EMBED_MODEL")
                or DEFAULT_FINANCIAL_EMBED_MODEL
            )
        elif backend_key == "openai":
            model_name = os.environ.get("NEWS_EMBED_OPENAI_MODEL") or os.environ.get("NLP_OPENAI_EMBED_MODEL") or DEFAULT_OPENAI_EMBED_MODEL
        elif backend_key == "hashing":
            model_name = os.environ.get("NEWS_EMBED_HASHING_MODEL") or DEFAULT_HASHING_MODEL
        else:
            model_name = os.environ.get("NEWS_EMBED_MODEL") or os.environ.get("NLP_EMBED_MODEL") or str(backend_key)

    default_batch = 32 if backend_key == "finbert" else 64
    if backend_key == "openai":
        default_batch = 128
    return TextEmbeddingConfig(
        backend=backend_key,
        model_name=str(model_name or "").strip() or str(backend_key),
        batch_size=_env_int("NEWS_EMBED_BATCH_SIZE" if kind_key == "news" else "NLP_EMBED_BATCH_SIZE", default_batch),
        cache_dir=str(os.environ.get("NLP_MODEL_CACHE_DIR", "") or "") or None,
        device=str(os.environ.get("NLP_DEVICE", os.environ.get("EMBED_DEVICE", "")) or "").strip(),
        local_files_only=_env_bool("NEWS_EMBED_LOCAL_FILES_ONLY", _env_bool("NLP_EMBED_LOCAL_FILES_ONLY", False)),
        fallback_policy=str(os.environ.get("NEWS_EMBED_FALLBACK_POLICY", os.environ.get("NLP_EMBED_FALLBACK_POLICY", "skip")) or "skip").strip().lower(),
        dim=_env_int("NEWS_EMBED_HASHING_DIM", _env_int("NLP_HASHING_EMBED_DIM", 128), minimum=1),
        api_key=str(os.environ.get("OPENAI_API_KEY", "") or "").strip(),
    )


def current_sentiment_config(
    *,
    model_name: str | None = None,
    backend: str | None = None,
) -> SentimentEncoderConfig:
    backend_key = canonical_embedding_backend(os.environ.get("NLP_SENTIMENT_BACKEND", backend or "finbert"), default="finbert")
    if backend_key not in {"finbert", "transformers", "hf_sequence_classifier"}:
        backend_key = "finbert"
    resolved_model = str(
        model_name
        or os.environ.get("NLP_SENTIMENT_MODEL_NAME")
        or os.environ.get("NLP_FINBERT_MODEL_NAME")
        or os.environ.get("FINBERT_MODEL_NAME")
        or DEFAULT_FINBERT_MODEL
    ).strip() or DEFAULT_FINBERT_MODEL
    return SentimentEncoderConfig(
        backend="finbert" if backend_key in {"transformers", "hf_sequence_classifier"} else backend_key,
        model_name=resolved_model,
        fallback_model_name=str(os.environ.get("NLP_SENTIMENT_FALLBACK_MODEL_NAME", DEFAULT_FINBERT_MODEL) or DEFAULT_FINBERT_MODEL),
        batch_size=_env_int("NLP_SENTIMENT_BATCH_SIZE", _env_int("FINBERT_BATCH_SIZE", 32), minimum=1),
        cache_dir=str(os.environ.get("NLP_MODEL_CACHE_DIR", "") or "") or None,
        device=str(os.environ.get("FINBERT_DEVICE", os.environ.get("NLP_DEVICE", "")) or "").strip(),
        local_files_only=_env_bool("NLP_SENTIMENT_LOCAL_FILES_ONLY", _env_bool("FINBERT_LOCAL_FILES_ONLY", False)),
    )


def build_text_embedding_encoder(config: TextEmbeddingConfig | Mapping[str, Any] | None = None) -> Encoder:
    if config is None:
        cfg = resolve_text_embedding_config()
    elif isinstance(config, TextEmbeddingConfig):
        cfg = config
    else:
        cfg = TextEmbeddingConfig(**dict(config))
    backend_key = canonical_embedding_backend(cfg.backend, default="sentence_transformer")
    if backend_key == "hashing":
        return HashingEmbeddingEncoder(model_name=cfg.model_name or DEFAULT_HASHING_MODEL, dim=int(cfg.dim or 128))
    if backend_key == "openai":
        return OpenAIEmbeddingEncoder(model_name=cfg.model_name or DEFAULT_OPENAI_EMBED_MODEL, batch_size=cfg.batch_size, api_key=cfg.api_key)
    if backend_key == "finbert":
        return FinBertSentimentEncoder(
            model_name=cfg.model_name or DEFAULT_FINBERT_MODEL,
            backend="finbert",
            batch_size=cfg.batch_size,
            cache_dir=cfg.cache_dir,
            device=cfg.device,
            local_files_only=cfg.local_files_only,
        )
    if backend_key in {"sentence_transformer", "financial_sentence_transformer"}:
        if backend_key == "financial_sentence_transformer":
            _require_financial_model_review(cfg)
        return SentenceTransformerEncoder(
            model_name=cfg.model_name or DEFAULT_SENTENCE_MODEL,
            backend=backend_key,
            batch_size=cfg.batch_size,
            cache_dir=cfg.cache_dir,
            device=cfg.device,
            local_files_only=cfg.local_files_only,
        )
    raise EncoderUnavailableError(f"unsupported_text_embedding_backend:{backend_key}")


def build_sentiment_encoder(config: SentimentEncoderConfig | Mapping[str, Any] | None = None) -> FinBertSentimentEncoder:
    if config is None:
        cfg = current_sentiment_config()
    elif isinstance(config, SentimentEncoderConfig):
        cfg = config
    else:
        cfg = SentimentEncoderConfig(**dict(config))
    return FinBertSentimentEncoder(
        model_name=cfg.model_name or cfg.fallback_model_name or DEFAULT_FINBERT_MODEL,
        backend=cfg.backend or "finbert",
        batch_size=cfg.batch_size,
        cache_dir=cfg.cache_dir,
        device=cfg.device,
        local_files_only=cfg.local_files_only,
    )


def encode_texts_with_config(
    texts: Sequence[str],
    config: TextEmbeddingConfig | None = None,
) -> EncodedTextBatch:
    rows = _as_text_list(texts)
    cfg = config or resolve_text_embedding_config(kind="news")
    try:
        encoder = build_text_embedding_encoder(cfg)
        try:
            values = _normalize_encoded_matrix(encoder.encode(rows))
        finally:
            encoder.release()
        return EncodedTextBatch(values=values, requested_config=cfg, effective_config=cfg)
    except Exception as exc:
        fallback = str(cfg.fallback_policy or "skip").strip().lower()
        message = f"{type(exc).__name__}:{exc}"
        if fallback == "raise":
            raise
        if fallback in {"hash", "hashing"}:
            fallback_cfg = TextEmbeddingConfig(
                backend="hashing",
                model_name=DEFAULT_HASHING_MODEL,
                batch_size=cfg.batch_size,
                fallback_policy="skip",
                dim=cfg.dim,
            )
            encoder = HashingEmbeddingEncoder(model_name=fallback_cfg.model_name, dim=fallback_cfg.dim)
            return EncodedTextBatch(
                values=_normalize_encoded_matrix(encoder.encode(rows)),
                requested_config=cfg,
                effective_config=fallback_cfg,
                degraded=True,
                errors=(message,),
            )
        if fallback in {"zero", "zeros"}:
            fallback_cfg = TextEmbeddingConfig(
                backend="hashing",
                model_name="zero-vector-fallback",
                batch_size=cfg.batch_size,
                fallback_policy="skip",
                dim=max(1, int(cfg.dim or 128)),
            )
            return EncodedTextBatch(
                values=np.zeros((len(rows), fallback_cfg.dim), dtype=np.float32),
                requested_config=cfg,
                effective_config=fallback_cfg,
                degraded=True,
                errors=(message,),
            )
        return EncodedTextBatch(
            values=np.zeros((0, 0), dtype=np.float32),
            requested_config=cfg,
            effective_config=cfg,
            degraded=True,
            errors=(message,),
        )
