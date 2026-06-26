from __future__ import annotations

import importlib
import time


def test_pipeline_health_summary_rejects_simulated_only_as_healthy(monkeypatch) -> None:
    ingestion_status = importlib.reload(importlib.import_module("engine.runtime.ingestion_status"))
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        ingestion_status,
        "get_all_pipeline_statuses",
        lambda: {
            "poll_prices": {
                "pipeline": "poll_prices",
                "ok": True,
                "updated_ts_ms": now_ms,
                "meta": {
                    "providers": ["simulated"],
                    "provider_result_counts": {"simulated": 2},
                },
            }
        },
    )

    summary = ingestion_status.pipeline_health_summary(stale_after_s=900)
    poll_prices = summary["pipelines"]["poll_prices"]

    assert summary["ok"] is False
    assert summary["healthy"] == 0
    assert summary["not_live"] == 1
    assert summary["simulated"] == 1
    assert poll_prices["live_market_data_ok"] is False
    assert poll_prices["live_feed_status"] == "simulated"
    assert poll_prices["live_feed_classification"] == "simulated_not_live"


def test_pipeline_health_summary_rejects_yfinance_only_as_live(monkeypatch) -> None:
    ingestion_status = importlib.reload(importlib.import_module("engine.runtime.ingestion_status"))
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        ingestion_status,
        "get_all_pipeline_statuses",
        lambda: {
            "poll_prices": {
                "pipeline": "poll_prices",
                "ok": True,
                "updated_ts_ms": now_ms,
                "meta": {
                    "providers": ["yfinance"],
                    "provider_result_counts": {"yfinance": 2},
                },
            }
        },
    )

    summary = ingestion_status.pipeline_health_summary(stale_after_s=900)
    poll_prices = summary["pipelines"]["poll_prices"]

    assert summary["ok"] is False
    assert summary["healthy"] == 0
    assert summary["not_live"] == 1
    assert summary["simulated"] == 0
    assert poll_prices["live_market_data_ok"] is False
    assert poll_prices["live_feed_status"] == "fallback"
    assert poll_prices["live_feed_classification"] == "fallback_not_live"


def test_pipeline_health_summary_counts_stubbed_live_provider_as_healthy(monkeypatch) -> None:
    ingestion_status = importlib.reload(importlib.import_module("engine.runtime.ingestion_status"))
    now_ms = int(time.time() * 1000)
    monkeypatch.setattr(
        ingestion_status,
        "get_all_pipeline_statuses",
        lambda: {
            "poll_prices": {
                "pipeline": "poll_prices",
                "ok": True,
                "updated_ts_ms": now_ms,
                "meta": {
                    "providers": ["polygon"],
                    "provider_result_counts": {"polygon": 2},
                },
            }
        },
    )

    summary = ingestion_status.pipeline_health_summary(stale_after_s=900)
    poll_prices = summary["pipelines"]["poll_prices"]

    assert summary["ok"] is True
    assert summary["healthy"] == 1
    assert summary["not_live"] == 0
    assert poll_prices["live_market_data_ok"] is True
    assert poll_prices["live_feed_status"] == "live"


def test_provider_map_liveness_does_not_count_yfinance_as_live() -> None:
    feed_truth = importlib.reload(importlib.import_module("engine.runtime.feed_truth"))

    result = feed_truth.annotate_provider_map_liveness(
        {
            "yfinance": {"ok": True},
            "polygon": {"ok": True},
        }
    )

    assert result["raw_healthy_providers"] == 2
    assert result["live_healthy_providers"] == 1
    assert result["fallback_healthy_providers"] == 1
    assert result["live_market_data_ok"] is True
    assert result["providers"]["yfinance"]["raw_ok"] is True
    assert result["providers"]["yfinance"]["ok"] is False
    assert result["providers"]["yfinance"]["live_feed_classification"] == "fallback_not_live"
    assert result["providers"]["polygon"]["ok"] is True
