"""Shared live-vs-simulated market-data truth helpers."""

from __future__ import annotations

from typing import Any, Iterable, Mapping


SIMULATED_PROVIDER_NAMES = frozenset({"simulated"})
FALLBACK_NOT_LIVE_PROVIDER_NAMES = frozenset({"yfinance"})
MARKET_DATA_SOURCE_TYPES = frozenset({"price_provider", "options_provider"})
MARKET_DATA_PIPELINES = frozenset(
    {
        "poll_prices",
        "stream_prices_polygon_ws",
        "options_poll",
        "ingest_simulated_prices",
        "populate_simulated_prices",
    }
)


def dedupe_strings(values: Iterable[Any] | None) -> list[str]:
    out: list[str] = []
    for value in values or ():
        text = str(value or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def is_simulated_provider(provider_name: Any) -> bool:
    return str(provider_name or "").strip().lower() in SIMULATED_PROVIDER_NAMES


def is_fallback_not_live_provider(provider_name: Any) -> bool:
    return str(provider_name or "").strip().lower() in FALLBACK_NOT_LIVE_PROVIDER_NAMES


def is_live_market_data_provider(provider_name: Any) -> bool:
    provider = str(provider_name or "").strip().lower()
    return bool(provider and provider not in SIMULATED_PROVIDER_NAMES and provider not in FALLBACK_NOT_LIVE_PROVIDER_NAMES)


def classify_market_data_liveness(
    *,
    provider_name: Any = "",
    ok: bool = False,
    stale: bool = False,
    simulated: bool = False,
    fallback_not_live: bool = False,
    missing_credential_env_vars: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Classify whether a feed status proves live market-data connectivity.

    Fresh simulated rows can be useful for safe/test mode, but they are never a
    live-feed success. Missing required credentials are also non-green even when
    a fallback provider produced rows.
    """

    missing = dedupe_strings(missing_credential_env_vars)
    simulated_feed = bool(simulated or is_simulated_provider(provider_name))
    fallback_feed = bool(fallback_not_live or is_fallback_not_live_provider(provider_name))
    raw_ok = bool(ok) and not bool(stale)
    live_ok = bool(raw_ok and not simulated_feed and not fallback_feed and not missing)
    if live_ok:
        status = "live"
        classification = "live"
        reason = "live_provider_ok"
    elif simulated_feed:
        status = "simulated"
        classification = "simulated_not_live"
        reason = "simulated_market_data_not_live"
    elif fallback_feed:
        status = "fallback"
        classification = "fallback_not_live"
        reason = "fallback_market_data_not_live"
    elif missing:
        status = "missing_credentials"
        classification = "missing_credentials"
        reason = "missing_live_market_data_credentials"
    elif bool(stale):
        status = "stale"
        classification = "stale"
        reason = "market_data_stale"
    else:
        status = "degraded"
        classification = "not_live"
        reason = "live_market_data_not_proven"
    return {
        "live_market_data_ok": bool(live_ok),
        "live_feed_status": status,
        "live_feed_classification": classification,
        "live_feed_reason": reason,
        "simulated": bool(simulated_feed),
        "fallback_not_live": bool(fallback_feed),
        "missing_credential_env_vars": missing,
    }


def missing_live_market_credentials_from_sources(sources: Iterable[Mapping[str, Any]] | None) -> list[str]:
    """Return manager-computed missing env vars for enabled real market feeds."""

    missing: list[str] = []
    for source in sources or ():
        if not isinstance(source, Mapping):
            continue
        source_type = str(source.get("source_type") or "").strip()
        if source_type not in MARKET_DATA_SOURCE_TYPES:
            continue
        provider_name = str(source.get("provider_name") or source.get("source_key") or "").strip()
        if is_simulated_provider(provider_name) or is_fallback_not_live_provider(provider_name):
            continue
        if not bool(source.get("enabled")):
            continue
        missing.extend(source.get("missing_credential_env_vars") or [])
    return dedupe_strings(missing)


def classify_pipeline_liveness(status: Mapping[str, Any]) -> dict[str, Any]:
    """Classify a persisted ingestion pipeline row for live-feed aggregation."""

    pipeline = str(status.get("pipeline") or status.get("pipeline_name") or "").strip()
    meta = status.get("meta") if isinstance(status.get("meta"), Mapping) else {}
    if pipeline not in MARKET_DATA_PIPELINES and not bool((meta or {}).get("simulated")):
        return {"applies": False}

    provider_counts_raw = (meta or {}).get("provider_result_counts")
    provider_counts = provider_counts_raw if isinstance(provider_counts_raw, Mapping) else {}
    providers = dedupe_strings(
        list((meta or {}).get("providers") or [])
        + [str(name) for name in provider_counts.keys()]
        + ([str((meta or {}).get("provider"))] if (meta or {}).get("provider") else [])
    )
    successful_providers = [
        str(name)
        for name, value in provider_counts.items()
        if str(name or "").strip() and int(value or 0) > 0
    ]
    active_providers = dedupe_strings(successful_providers or providers)
    simulated = bool((meta or {}).get("simulated")) or (
        bool(active_providers) and all(is_simulated_provider(name) for name in active_providers)
    )
    if pipeline in {"ingest_simulated_prices", "populate_simulated_prices"}:
        simulated = True

    error_classes_raw = (meta or {}).get("provider_error_classifications")
    error_classes = error_classes_raw if isinstance(error_classes_raw, Mapping) else {}
    missing_providers = [
        str(name)
        for name, classification in error_classes.items()
        if str(classification or "").strip() == "missing_credentials"
    ]
    live_successes = [name for name in successful_providers if is_live_market_data_provider(name)]
    fallback_successes = [name for name in successful_providers if is_fallback_not_live_provider(name)]
    simulated_successes = [name for name in successful_providers if is_simulated_provider(name)]
    truth = classify_market_data_liveness(
        provider_name=(active_providers[0] if len(active_providers) == 1 else ""),
        ok=bool(status.get("ok")) and bool(live_successes or fallback_successes or simulated_successes or simulated),
        stale=bool(status.get("stale")),
        simulated=bool((simulated_successes or simulated) and not live_successes and not fallback_successes),
        fallback_not_live=bool(fallback_successes and not live_successes),
        missing_credential_env_vars=missing_providers if not live_successes else [],
    )
    truth["applies"] = True
    truth["live_provider_successes"] = list(live_successes)
    truth["fallback_provider_successes"] = list(fallback_successes)
    truth["simulated_provider_successes"] = list(simulated_successes)
    truth["missing_credential_providers"] = missing_providers
    return truth


def annotate_provider_map_liveness(
    providers: Mapping[str, Any] | None,
    *,
    missing_credential_env_vars: Iterable[Any] | None = None,
) -> dict[str, Any]:
    """Annotate provider rows and return live-provider counts."""

    missing_env = dedupe_strings(missing_credential_env_vars)
    annotated: dict[str, Any] = {}
    raw_healthy = 0
    live_healthy = 0
    simulated_healthy = 0
    fallback_healthy = 0
    for name, raw in (providers or {}).items():
        row = dict(raw or {}) if isinstance(raw, Mapping) else {}
        raw_ok = bool(row.get("ok"))
        raw_healthy += 1 if raw_ok else 0
        row_missing = dedupe_strings(row.get("missing_credential_env_vars") or [])
        truth = classify_market_data_liveness(
            provider_name=name,
            ok=raw_ok,
            stale=bool(row.get("stale")),
            simulated=bool(row.get("simulated")),
            missing_credential_env_vars=row_missing,
        )
        row.update(truth)
        row.setdefault("raw_ok", raw_ok)
        row["ok"] = bool(truth["live_market_data_ok"])
        if truth["live_market_data_ok"]:
            live_healthy += 1
        elif truth["simulated"] and raw_ok:
            simulated_healthy += 1
        elif truth["fallback_not_live"] and raw_ok:
            fallback_healthy += 1
        annotated[str(name)] = row
    return {
        "providers": annotated,
        "raw_healthy_providers": int(raw_healthy),
        "live_healthy_providers": int(live_healthy),
        "simulated_healthy_providers": int(simulated_healthy),
        "fallback_healthy_providers": int(fallback_healthy),
        "missing_credential_env_vars": missing_env,
        "live_market_data_ok": bool(live_healthy > 0 and not missing_env),
        "live_feed_status": "live" if live_healthy > 0 and not missing_env else (
            "missing_credentials" if missing_env else (
                "simulated" if simulated_healthy > 0 else ("fallback" if fallback_healthy > 0 else "degraded")
            )
        ),
    }
