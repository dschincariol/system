from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _FakeCuda:
    def __init__(self) -> None:
        self.empty_cache_calls = 0
        self._memory_allocated = 0

    def is_available(self) -> bool:
        return True

    def empty_cache(self) -> None:
        self.empty_cache_calls += 1
        self._memory_allocated = 0

    def memory_allocated(self) -> int:
        return self._memory_allocated


class _FakeNoGrad:
    def __enter__(self) -> "_FakeNoGrad":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None


class _FakeTensor:
    def __init__(self, values: np.ndarray) -> None:
        self._values = np.asarray(values, dtype=np.float32)

    def detach(self) -> "_FakeTensor":
        return self

    def cpu(self) -> "_FakeTensor":
        return self

    def numpy(self) -> np.ndarray:
        return np.array(self._values, dtype=np.float32, copy=True)


def _softmax(logits: np.ndarray, dim: int = -1) -> _FakeTensor:
    arr = np.asarray(logits, dtype=np.float32)
    arr = arr - np.max(arr, axis=dim, keepdims=True)
    exp = np.exp(arr)
    return _FakeTensor(exp / exp.sum(axis=dim, keepdims=True))


class _FakeTorch:
    def __init__(self) -> None:
        self.cuda = _FakeCuda()
        self.nn = SimpleNamespace(functional=SimpleNamespace(softmax=_softmax))

    def no_grad(self) -> _FakeNoGrad:
        return _FakeNoGrad()


class _FakeTokenizer:
    model_max_length = 512

    def __call__(self, texts: list[str], **_kwargs: object) -> dict[str, list[str]]:
        return {"texts": list(texts)}


class _FakeFinBertModel:
    def __init__(self) -> None:
        self.config = SimpleNamespace(id2label={0: "positive", 1: "negative", 2: "neutral"})

    def to(self, _device: str) -> "_FakeFinBertModel":
        return self

    def eval(self) -> None:
        return None

    def __call__(self, **encoded: object) -> SimpleNamespace:
        rows = []
        for text in encoded.get("texts", []):
            lower = str(text).lower()
            if "beat" in lower:
                rows.append([4.0, 0.5, 1.0])
            elif "warning" in lower:
                rows.append([0.5, 4.0, 1.0])
            else:
                rows.append([1.0, 1.0, 4.0])
        return SimpleNamespace(logits=np.asarray(rows, dtype=np.float32))


class _FakeTransformers:
    def __init__(self) -> None:
        self.model_loads = 0
        self.tokenizer_loads = 0
        self.AutoTokenizer = SimpleNamespace(from_pretrained=self._load_tokenizer)
        self.AutoModelForSequenceClassification = SimpleNamespace(from_pretrained=self._load_model)

    def _load_tokenizer(self, *_args: object, **_kwargs: object) -> _FakeTokenizer:
        self.tokenizer_loads += 1
        return _FakeTokenizer()

    def _load_model(self, *_args: object, **_kwargs: object) -> _FakeFinBertModel:
        self.model_loads += 1
        return _FakeFinBertModel()


class _FakeSentenceModel:
    def encode(self, texts: list[str], **_kwargs: object) -> np.ndarray:
        rows = []
        for text in texts:
            cleaned = str(text)
            rows.append([float(len(cleaned)), float(sum(ord(ch) for ch in cleaned) % 17), 1.0])
        return np.asarray(rows, dtype=np.float32)


class _FakeSentenceTransformers:
    def __init__(self) -> None:
        self.model_loads = 0

    def SentenceTransformer(self, *_args: object, **_kwargs: object) -> _FakeSentenceModel:
        self.model_loads += 1
        return _FakeSentenceModel()


def _install_fake_model_imports(monkeypatch):
    from engine.nlp import encoder as encoder_module

    fake_torch = _FakeTorch()
    fake_transformers = _FakeTransformers()
    fake_sentence_transformers = _FakeSentenceTransformers()
    original_import_module = encoder_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "torch":
            return fake_torch
        if name == "transformers":
            return fake_transformers
        if name == "sentence_transformers":
            return fake_sentence_transformers
        return original_import_module(name)

    monkeypatch.setattr(encoder_module.importlib, "import_module", fake_import_module)
    return encoder_module, fake_torch, fake_transformers, fake_sentence_transformers


def test_finbert_release_drops_bundle_and_allows_fresh_load(monkeypatch, tmp_path) -> None:
    encoder_module, fake_torch, fake_transformers, _fake_sentence_transformers = _install_fake_model_imports(monkeypatch)
    encoder = encoder_module.FinBertSentimentEncoder(model_name="unit-finbert", cache_dir=tmp_path)

    first = encoder.encode(["Revenue beat expectations.", "Management issued a warning."])
    assert encoder._bundle is not None
    assert fake_transformers.model_loads == 1

    fake_torch.cuda._memory_allocated = 4096
    encoder.release()
    assert encoder._bundle is None
    assert fake_torch.cuda.memory_allocated() == 0
    assert fake_torch.cuda.empty_cache_calls == 1

    second = encoder.encode(["Revenue beat expectations.", "Management issued a warning."])
    assert encoder._bundle is not None
    assert fake_transformers.model_loads == 2
    np.testing.assert_allclose(second, first)

    encoder.release()
    assert encoder._bundle is None
    assert fake_torch.cuda.empty_cache_calls == 2


def test_sentence_transformer_release_drops_model_and_allows_fresh_load(monkeypatch, tmp_path) -> None:
    encoder_module, fake_torch, _fake_transformers, fake_sentence_transformers = _install_fake_model_imports(monkeypatch)
    encoder = encoder_module.SentenceTransformerEncoder(model_name="unit-sentence-model", cache_dir=tmp_path)

    first = encoder.encode(["Filing paragraph text.", "Earnings call section."])
    assert encoder._model is not None
    assert fake_sentence_transformers.model_loads == 1

    fake_torch.cuda._memory_allocated = 2048
    encoder.release()
    assert encoder._model is None
    assert fake_torch.cuda.memory_allocated() == 0
    assert fake_torch.cuda.empty_cache_calls == 1

    second = encoder.encode(["Filing paragraph text.", "Earnings call section."])
    assert encoder._model is not None
    assert fake_sentence_transformers.model_loads == 2
    np.testing.assert_allclose(second, first)

    encoder.release()
    assert encoder._model is None
    assert fake_torch.cuda.empty_cache_calls == 2


def test_encoder_context_manager_loads_then_releases(monkeypatch, tmp_path) -> None:
    encoder_module, _fake_torch, fake_transformers, fake_sentence_transformers = _install_fake_model_imports(monkeypatch)

    with encoder_module.FinBertSentimentEncoder(model_name="unit-finbert", cache_dir=tmp_path) as finbert:
        assert finbert._bundle is not None
        assert fake_transformers.model_loads == 1
        assert finbert.encode(["Revenue beat expectations."]).shape == (1, 3)
    assert finbert._bundle is None

    with encoder_module.SentenceTransformerEncoder(model_name="unit-sentence-model", cache_dir=tmp_path) as sentence:
        assert sentence._model is not None
        assert fake_sentence_transformers.model_loads == 1
        assert sentence.encode(["Filing paragraph text."]).shape == (1, 3)
    assert sentence._model is None


def test_missing_optional_embedding_backend_degrades_without_crash(monkeypatch) -> None:
    from engine.nlp import encoder as encoder_module

    original_import_module = encoder_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "sentence_transformers":
            raise ImportError("missing optional sentence-transformers")
        return original_import_module(name)

    monkeypatch.setattr(encoder_module.importlib, "import_module", fake_import_module)
    cfg = encoder_module.TextEmbeddingConfig(
        backend="sentence_transformer",
        model_name="unit-missing-model",
        fallback_policy="skip",
    )

    encoded = encoder_module.encode_texts_with_config(["Revenue beat expectations"], cfg)

    assert encoded.degraded is True
    assert encoded.values.shape == (0, 0)
    assert "missing optional sentence-transformers" in encoded.errors[0]


def test_missing_optional_embedding_backend_can_fallback_to_hashing(monkeypatch) -> None:
    from engine.nlp import encoder as encoder_module

    original_import_module = encoder_module.importlib.import_module

    def fake_import_module(name: str):
        if name == "sentence_transformers":
            raise ImportError("missing optional sentence-transformers")
        return original_import_module(name)

    monkeypatch.setattr(encoder_module.importlib, "import_module", fake_import_module)
    cfg = encoder_module.TextEmbeddingConfig(
        backend="sentence_transformer",
        model_name="unit-missing-model",
        fallback_policy="hashing",
        dim=16,
    )

    encoded = encoder_module.encode_texts_with_config(["Revenue beat expectations"], cfg)

    assert encoded.degraded is True
    assert encoded.effective_config.backend == "hashing"
    assert encoded.values.shape == (1, 16)


def test_financial_embedding_candidate_requires_license_review_ack(monkeypatch) -> None:
    from engine.nlp import encoder as encoder_module

    monkeypatch.delenv("NLP_FINANCIAL_EMBED_LICENSE_REVIEW_ACK", raising=False)
    cfg = encoder_module.TextEmbeddingConfig(
        backend="financial_sentence_transformer",
        model_name=encoder_module.DEFAULT_FINANCIAL_EMBED_MODEL,
    )

    encoded = encoder_module.encode_texts_with_config(["cash flow guidance"], cfg)

    assert encoded.degraded is True
    assert "license_review_required" in encoded.errors[0]
