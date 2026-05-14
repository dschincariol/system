from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def test_nlp_groups_are_registered_and_legacy_sentiment_is_deprecated(monkeypatch) -> None:
    feature_registry = importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
    monkeypatch.setattr(feature_registry, "_discovered_feature_ids", lambda *args, **kwargs: [])

    groups = feature_registry.list_groups()
    assert "nlp_finbert_news_v1" in groups
    assert "nlp_filings_v1" in groups
    assert "nlp_transcripts_v1" in groups
    assert "lexical_sentiment_v0" in groups
    assert groups["lexical_sentiment_v0"].get("deprecated_after")
    assert "nlp.finbert_news_v1.score_mean" in feature_registry.registered_feature_ids()


def test_legacy_sentiment_deprecation_marker_is_sha_or_iso(monkeypatch) -> None:
    feature_registry = importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
    warnings = []

    def capture_warning(message: str, *args: object, **_kwargs: object) -> None:
        warnings.append(message % args if args else message)

    monkeypatch.setattr(feature_registry.LOG, "warning", capture_warning)

    assert feature_registry.validate_lexical_sentiment_deprecation_marker()
    assert feature_registry.validate_lexical_sentiment_deprecation_marker("2026-05-05")
    assert not feature_registry.validate_lexical_sentiment_deprecation_marker("TBD")
    assert not feature_registry.validate_lexical_sentiment_deprecation_marker("")
    assert any("startup_warning" in warning for warning in warnings)


def test_predictor_source_does_not_import_transformers() -> None:
    source = (REPO_ROOT / "engine" / "strategy" / "predictor.py").read_text(encoding="utf-8")
    assert "import transformers" not in source
    assert "sentence_transformers" not in source
