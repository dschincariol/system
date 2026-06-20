from __future__ import annotations

import sys
import types
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_finbert_encoder_probability_contract_and_score_signs() -> None:
    from engine.nlp.encoder import FinBertSentimentEncoder

    def fake_predict(texts: list[str]) -> np.ndarray:
        rows = []
        for text in texts:
            lower = text.lower()
            if "beat" in lower or "surged" in lower:
                rows.append([0.86, 0.04, 0.10])
            elif "missed" in lower or "warning" in lower:
                rows.append([0.05, 0.81, 0.14])
            else:
                rows.append([0.10, 0.08, 0.82])
        return np.asarray(rows, dtype=np.float32)

    encoder = FinBertSentimentEncoder(predict_fn=fake_predict)
    texts = [
        "The company beat earnings expectations and shares surged.",
        "The company missed guidance and issued a profit warning.",
        "The company filed its quarterly report.",
    ]
    probs = encoder.encode(texts)
    scores = encoder.score_from_probabilities(probs)

    assert probs.shape == (3, 3)
    np.testing.assert_allclose(probs.sum(axis=1), np.ones(3), atol=1e-5)
    assert float(scores[0]) > 0.0
    assert float(scores[1]) < 0.0
    assert -1.0 <= float(scores[2]) <= 1.0


def test_sentence_transformer_encoder_stable_vectors() -> None:
    from engine.nlp.encoder import SentenceTransformerEncoder

    def fake_encode(texts: list[str]) -> np.ndarray:
        rows = []
        for text in texts:
            base = float(len(text))
            rows.append([base, base / 2.0, 1.0])
        return np.asarray(rows, dtype=np.float32)

    encoder = SentenceTransformerEncoder(encode_fn=fake_encode)
    first = encoder.encode(["filing paragraph", "earnings call section"])
    second = encoder.encode(["filing paragraph", "earnings call section"])

    assert first.shape == (2, 3)
    np.testing.assert_allclose(first, second)
    assert np.all(np.linalg.norm(first, axis=1) > 0.0)


def test_sentence_transformer_encoder_passes_cpu_device_by_default(monkeypatch) -> None:
    from engine.nlp.encoder import SentenceTransformerEncoder

    captured = {}

    class _FakeCuda:
        def is_available(self) -> bool:
            return True

    class _FakeTorch:
        cuda = _FakeCuda()

    class _FakeSentenceTransformer:
        def __init__(self, model_name: str, **kwargs) -> None:
            captured["model_name"] = model_name
            captured.update(kwargs)

        def encode(self, *_args, **_kwargs) -> np.ndarray:
            return np.asarray([[1.0]], dtype=np.float32)

    monkeypatch.delenv("NLP_DEVICE", raising=False)
    monkeypatch.delenv("EMBED_DEVICE", raising=False)
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.setitem(sys.modules, "torch", _FakeTorch())
    monkeypatch.setitem(
        sys.modules,
        "sentence_transformers",
        types.SimpleNamespace(SentenceTransformer=_FakeSentenceTransformer),
    )

    encoder = SentenceTransformerEncoder()
    encoder.encode(["text"])

    assert captured["device"] == "cpu"
