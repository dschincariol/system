"""Structured LLM financial-event extraction.

LLMs in this module are bounded extractors, not predictors.  The batch path
selects only already-available source documents, asks a provider adapter for a
strict JSON object, validates every field deterministically, persists accepted
rows with lineage, and projects shadow-only feature rows into
``structured_document_events``.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Protocol, Sequence

import requests

from engine.data._credentials import get_data_credential
from engine.data.structured_document_events import EVENT_FEATURE_ID, put_structured_document_events
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


LOG = get_logger("engine.data.llm_event_extraction")

EXTRACTOR_NAME = "llm_event_extraction"
SCHEMA_VERSION = "llm_financial_event_v1"
PROMPT_VERSION = "llm_event_extraction_prompt_v1"
DEFAULT_MODEL = "gpt-4.1-mini"
DEFAULT_PROVIDER = "openai"
MAX_RAW_RESPONSE_EXCERPT_CHARS = 2048
MS_DAY = 24 * 60 * 60 * 1000

SUPPORTED_EVENT_TYPES = tuple(sorted(EVENT_FEATURE_ID))
SUPPORTED_DIRECTIONS = frozenset({"positive", "negative", "neutral", "increase", "decrease", "risk"})
DOCUMENT_TYPES = frozenset({"filing", "transcript", "news", "earnings", "macro"})


LLM_EVENT_RESPONSE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["events"],
    "properties": {
        "events": {
            "type": "array",
            "maxItems": 12,
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "entity",
                    "ticker",
                    "event_type",
                    "direction",
                    "polarity",
                    "magnitude",
                    "confidence",
                    "source_doc_id",
                    "source_url",
                    "source_ts_ms",
                    "availability_ts_ms",
                    "evidence_span",
                    "extraction_model",
                    "prompt_version",
                    "schema_version",
                    "contamination_pit_guard",
                ],
                "properties": {
                    "entity": {"type": "string", "minLength": 1, "maxLength": 160},
                    "ticker": {"type": "string", "minLength": 0, "maxLength": 24},
                    "event_type": {"type": "string", "enum": list(SUPPORTED_EVENT_TYPES)},
                    "direction": {"type": "string", "enum": sorted(SUPPORTED_DIRECTIONS)},
                    "polarity": {"type": "number", "minimum": -1.0, "maximum": 1.0},
                    "magnitude": {
                        "anyOf": [
                            {"type": "null"},
                            {
                                "type": "object",
                                "additionalProperties": False,
                                "required": ["value", "unit"],
                                "properties": {
                                    "value": {"type": "number"},
                                    "unit": {"type": "string", "minLength": 1, "maxLength": 40},
                                    "period": {"type": "string", "maxLength": 80},
                                },
                            },
                        ]
                    },
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "source_doc_id": {"type": "string", "minLength": 1, "maxLength": 512},
                    "source_url": {"type": "string", "maxLength": 1200},
                    "source_ts_ms": {"type": "integer", "minimum": 0},
                    "availability_ts_ms": {"type": "integer", "minimum": 0},
                    "evidence_span": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["start", "end", "text"],
                        "properties": {
                            "start": {"type": "integer", "minimum": 0},
                            "end": {"type": "integer", "minimum": 0},
                            "text": {"type": "string", "minLength": 1, "maxLength": 800},
                        },
                    },
                    "extraction_model": {"type": "string", "minLength": 1, "maxLength": 120},
                    "prompt_version": {"type": "string", "enum": [PROMPT_VERSION]},
                    "schema_version": {"type": "string", "enum": [SCHEMA_VERSION]},
                    "contamination_pit_guard": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": [
                            "pit_safe",
                            "decision_ts_ms",
                            "source_available_by_decision_ts",
                            "source_hash",
                            "direct_trading_authority",
                        ],
                        "properties": {
                            "pit_safe": {"type": "boolean"},
                            "decision_ts_ms": {"type": "integer", "minimum": 0},
                            "source_available_by_decision_ts": {"type": "boolean"},
                            "source_hash": {"type": "string", "minLength": 32, "maxLength": 128},
                            "direct_trading_authority": {"type": "boolean"},
                        },
                    },
                },
            },
        }
    },
}


@dataclass(frozen=True)
class SourceDocument:
    source_doc_id: str
    source_event_id: int | None
    symbol: str
    entity: str
    document_type: str
    source: str
    title: str
    text: str
    source_url: str
    source_ts_ms: int
    availability_ts_ms: int
    source_hash: str


@dataclass(frozen=True)
class LLMEventExtractionConfig:
    enabled: bool = False
    provider: str = DEFAULT_PROVIDER
    model: str = DEFAULT_MODEL
    max_docs: int = 25
    max_input_chars: int = 6000
    max_output_tokens: int = 1400
    max_cost_usd: float = 0.25
    input_cost_per_1k: float = 0.00015
    output_cost_per_1k: float = 0.00060
    min_interval_ms: int = 250
    timeout_s: float = 30.0
    decision_ts_ms: int | None = None
    symbols: tuple[str, ...] = ()

    @classmethod
    def from_env(cls) -> "LLMEventExtractionConfig":
        return cls(
            enabled=_env_bool("LLM_EVENT_EXTRACT_ENABLED", False),
            provider=str(os.environ.get("LLM_EVENT_EXTRACT_PROVIDER") or DEFAULT_PROVIDER).strip().lower() or DEFAULT_PROVIDER,
            model=str(os.environ.get("LLM_EVENT_EXTRACT_MODEL") or DEFAULT_MODEL).strip() or DEFAULT_MODEL,
            max_docs=_bounded_int(os.environ.get("LLM_EVENT_EXTRACT_MAX_DOCS"), 25, low=1, high=500),
            max_input_chars=_bounded_int(os.environ.get("LLM_EVENT_EXTRACT_MAX_INPUT_CHARS"), 6000, low=500, high=40000),
            max_output_tokens=_bounded_int(os.environ.get("LLM_EVENT_EXTRACT_MAX_OUTPUT_TOKENS"), 1400, low=256, high=8000),
            max_cost_usd=_bounded_float(os.environ.get("LLM_EVENT_EXTRACT_MAX_COST_USD"), 0.25, low=0.0, high=100.0),
            input_cost_per_1k=_bounded_float(os.environ.get("LLM_EVENT_EXTRACT_INPUT_COST_PER_1K_USD"), 0.00015, low=0.0, high=10.0),
            output_cost_per_1k=_bounded_float(os.environ.get("LLM_EVENT_EXTRACT_OUTPUT_COST_PER_1K_USD"), 0.00060, low=0.0, high=10.0),
            min_interval_ms=_bounded_int(os.environ.get("LLM_EVENT_EXTRACT_MIN_INTERVAL_MS"), 250, low=0, high=60000),
            timeout_s=_bounded_float(os.environ.get("LLM_EVENT_EXTRACT_TIMEOUT_S"), 30.0, low=1.0, high=300.0),
            decision_ts_ms=None,
            symbols=_symbols_from_env(os.environ.get("LLM_EVENT_EXTRACT_SYMBOLS")),
        )


@dataclass(frozen=True)
class AdapterResponse:
    text: str
    provider: str
    model: str
    raw_excerpt: str = ""
    input_tokens: int = 0
    output_tokens: int = 0


class LLMEventExtractionAdapter(Protocol):
    provider: str
    model: str

    def extract(self, *, prompt: str, schema: Mapping[str, Any], config: LLMEventExtractionConfig) -> AdapterResponse:
        ...


class OpenAIResponsesAdapter:
    provider = "openai"

    def __init__(self, *, api_key: str, model: str) -> None:
        self.api_key = str(api_key or "").strip()
        self.model = str(model or DEFAULT_MODEL)

    def extract(self, *, prompt: str, schema: Mapping[str, Any], config: LLMEventExtractionConfig) -> AdapterResponse:
        if not self.api_key:
            raise RuntimeError("llm_event_extract_missing_OPENAI_API_KEY")
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key, timeout=float(config.timeout_s))
        response = client.responses.create(
            model=self.model,
            input=[
                {
                    "role": "system",
                    "content": "Extract only the requested financial events. Return strict JSON matching the schema.",
                },
                {"role": "user", "content": prompt},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "financial_event_extractions",
                    "schema": dict(schema),
                    "strict": True,
                }
            },
            max_output_tokens=int(config.max_output_tokens),
        )
        text = str(getattr(response, "output_text", "") or "")
        if not text:
            try:
                text = json.dumps(response.model_dump(), separators=(",", ":"), sort_keys=True, default=str)
            except Exception:
                text = str(response)
        usage = getattr(response, "usage", None)
        input_tokens = _safe_int(getattr(usage, "input_tokens", 0), 0) if usage is not None else 0
        output_tokens = _safe_int(getattr(usage, "output_tokens", 0), 0) if usage is not None else 0
        return AdapterResponse(
            text=text,
            provider=self.provider,
            model=self.model,
            raw_excerpt=_bounded_excerpt(text),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
        )


class AnthropicToolAdapter:
    provider = "anthropic"

    def __init__(self, *, api_key: str, model: str) -> None:
        self.api_key = str(api_key or "").strip()
        self.model = str(model or "claude-sonnet-4-6")

    def extract(self, *, prompt: str, schema: Mapping[str, Any], config: LLMEventExtractionConfig) -> AdapterResponse:
        if not self.api_key:
            raise RuntimeError("llm_event_extract_missing_ANTHROPIC_API_KEY")
        body = {
            "model": self.model,
            "max_tokens": int(config.max_output_tokens),
            "messages": [{"role": "user", "content": prompt}],
            "tools": [
                {
                    "name": "record_financial_events",
                    "description": "Return structured financial events from the provided source document.",
                    "input_schema": dict(schema),
                }
            ],
            "tool_choice": {"type": "tool", "name": "record_financial_events"},
        }
        response = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            data=json.dumps(body, separators=(",", ":"), sort_keys=True),
            timeout=float(config.timeout_s),
        )
        response.raise_for_status()
        payload = response.json()
        extracted: Any = None
        for item in list(payload.get("content") or []):
            if isinstance(item, dict) and item.get("type") == "tool_use" and item.get("name") == "record_financial_events":
                extracted = item.get("input")
                break
        text = json.dumps(extracted if extracted is not None else payload, separators=(",", ":"), sort_keys=True, default=str)
        usage = dict(payload.get("usage") or {})
        return AdapterResponse(
            text=text,
            provider=self.provider,
            model=self.model,
            raw_excerpt=_bounded_excerpt(text),
            input_tokens=_safe_int(usage.get("input_tokens"), 0),
            output_tokens=_safe_int(usage.get("output_tokens"), 0),
        )


class FakeLLMEventExtractionAdapter:
    """Deterministic test fixture for sample documents."""

    provider = "fake"
    model = "fake-llm-event-extractor-v1"

    def extract(self, *, prompt: str, schema: Mapping[str, Any], config: LLMEventExtractionConfig) -> AdapterResponse:
        del schema, config
        marker = "SOURCE_DOCUMENT_JSON="
        doc = _json_loads(prompt.split(marker, 1)[1]) if marker in prompt else {}
        text = str(doc.get("text") or "")
        source_doc_id = str(doc.get("source_doc_id") or "")
        source_url = str(doc.get("source_url") or "")
        source_ts_ms = _safe_int(doc.get("source_ts_ms"), 0)
        availability_ts_ms = _safe_int(doc.get("availability_ts_ms"), source_ts_ms)
        decision_ts_ms = _safe_int(doc.get("decision_ts_ms"), availability_ts_ms)
        source_hash = str(doc.get("source_hash") or _sha256(text))
        symbol = str(doc.get("symbol") or "").upper()
        entity = str(doc.get("entity") or symbol or "market")
        events: list[dict[str, Any]] = []
        probes = (
            ("guidance_cut", "negative", -1.0, "lowered guidance"),
            ("margin_pressure", "negative", -1.0, "margin pressure"),
            ("buyback_increase", "positive", 0.5, "buyback"),
            ("supply_chain_exposure", "risk", -0.75, "supply chain"),
            ("macro_negative_surprise", "negative", -0.4, "worse than expected"),
        )
        lower = text.lower()
        for event_type, direction, polarity, needle in probes:
            idx = lower.find(needle)
            if idx < 0:
                continue
            end = idx + len(needle)
            events.append(
                {
                    "entity": entity,
                    "ticker": symbol,
                    "event_type": event_type,
                    "direction": direction,
                    "polarity": polarity,
                    "magnitude": None,
                    "confidence": 0.86,
                    "source_doc_id": source_doc_id,
                    "source_url": source_url,
                    "source_ts_ms": source_ts_ms,
                    "availability_ts_ms": availability_ts_ms,
                    "evidence_span": {"start": idx, "end": end, "text": text[idx:end]},
                    "extraction_model": self.model,
                    "prompt_version": PROMPT_VERSION,
                    "schema_version": SCHEMA_VERSION,
                    "contamination_pit_guard": {
                        "pit_safe": True,
                        "decision_ts_ms": decision_ts_ms,
                        "source_available_by_decision_ts": availability_ts_ms <= decision_ts_ms,
                        "source_hash": source_hash,
                        "direct_trading_authority": False,
                    },
                }
            )
        response = json.dumps({"events": events}, separators=(",", ":"), sort_keys=True)
        return AdapterResponse(text=response, provider=self.provider, model=self.model, raw_excerpt=response)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.environ.get(name, "")).strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def _bounded_int(value: Any, default: int, *, low: int, high: int) -> int:
    try:
        out = int(float(value))
    except Exception:
        out = int(default)
    return max(int(low), min(int(high), int(out)))


def _bounded_float(value: Any, default: float, *, low: float, high: float) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if not math.isfinite(out):
        out = float(default)
    return max(float(low), min(float(high), float(out)))


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return float(default)
    return float(out) if math.isfinite(out) else float(default)


def _symbols_from_env(value: Any) -> tuple[str, ...]:
    return tuple(str(part).strip().upper() for part in str(value or "").split(",") if str(part).strip())


def _json_loads(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if value in (None, "", b"", bytearray()):
        return {}
    try:
        parsed = json.loads(value.decode("utf-8", "replace") if isinstance(value, (bytes, bytearray)) else str(value))
    except Exception:
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _json_dumps(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def _sha256(value: Any) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8", errors="ignore")).hexdigest()


def _content_hash(value: Any) -> str:
    return _sha256(_json_dumps(value))


def _bounded_excerpt(value: Any, limit: int = MAX_RAW_RESPONSE_EXCERPT_CHARS) -> str:
    text = str(value or "")
    return text[: max(0, int(limit))]


def _norm_symbol(value: Any) -> str:
    return str(value or "").strip().upper()


def _row_get(row: Any, key: str, idx: int, default: Any = None) -> Any:
    try:
        if hasattr(row, "keys"):
            return row[key]
    except Exception:
        pass
    try:
        return row[idx]
    except Exception:
        return default


def _table_columns(con, table_name: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table_name})").fetchall() or []
        return {str(row[1] or "") for row in rows if len(row) > 1}
    except Exception:
        pass
    try:
        rows = con.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name=?
            """,
            (str(table_name),),
        ).fetchall()
        return {str(row[0] or "") for row in rows}
    except Exception:
        return set()


def ensure_llm_event_extraction_schema(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_extracted_events (
            id BIGSERIAL PRIMARY KEY,
            event_uid TEXT NOT NULL UNIQUE,
            source_doc_id TEXT NOT NULL,
            source_event_id BIGINT,
            entity TEXT NOT NULL,
            symbol TEXT NOT NULL DEFAULT '',
            event_type TEXT NOT NULL,
            direction TEXT NOT NULL,
            polarity DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            magnitude_value DOUBLE PRECISION,
            magnitude_unit TEXT,
            magnitude_period TEXT,
            confidence DOUBLE PRECISION NOT NULL,
            source_url TEXT,
            source_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            evidence_start BIGINT NOT NULL,
            evidence_end BIGINT NOT NULL,
            evidence_text TEXT NOT NULL,
            extraction_provider TEXT NOT NULL,
            extraction_model TEXT NOT NULL,
            prompt_version TEXT NOT NULL,
            schema_version TEXT NOT NULL,
            prompt_hash TEXT NOT NULL,
            model_hash TEXT NOT NULL,
            source_hash TEXT NOT NULL,
            feature_id TEXT NOT NULL,
            direct_trading_authority BIGINT NOT NULL DEFAULT 0,
            created_ts_ms BIGINT NOT NULL,
            payload_json JSONB,
            pit_metadata_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_llm_extracted_events_symbol_avail
          ON llm_extracted_events(symbol, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_llm_extracted_events_doc
          ON llm_extracted_events(source_doc_id)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS llm_event_extraction_audit (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT NOT NULL,
            decision_ts_ms BIGINT NOT NULL,
            source_doc_id TEXT,
            source_event_id BIGINT,
            source_hash TEXT,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            prompt_hash TEXT,
            model_hash TEXT,
            schema_version TEXT NOT NULL,
            status TEXT NOT NULL,
            rejection_reason TEXT,
            cost_estimate_usd DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            input_chars BIGINT NOT NULL DEFAULT 0,
            output_chars BIGINT NOT NULL DEFAULT 0,
            events_accepted BIGINT NOT NULL DEFAULT 0,
            raw_response_excerpt TEXT,
            diagnostics_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_llm_event_extraction_audit_status_ts
          ON llm_event_extraction_audit(status, ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_llm_event_extraction_audit_doc_ts
          ON llm_event_extraction_audit(source_doc_id, ts_ms DESC)
        """
    )


def _document_type(event_type: str, source: str, meta: Mapping[str, Any]) -> str:
    event = str(event_type or "").strip().lower()
    src = str(source or "").strip().lower()
    if event == "filing" or src.startswith("sec"):
        return "filing"
    if event == "transcript" or src == "fmp_transcript" or bool(meta.get("transcript")) or "transcript" in src:
        return "transcript"
    if event == "earnings":
        return "earnings"
    if event == "macro" or src.startswith("macro") or src == "gdelt":
        return "macro"
    return "news"


def _source_doc_id(row: Mapping[str, Any], meta: Mapping[str, Any]) -> str:
    for key in ("source_id", "event_key", "dedupe_hash", "url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    raw = meta.get("raw_payload") if isinstance(meta.get("raw_payload"), dict) else {}
    for key in ("source_id", "accession", "url"):
        value = str((raw or {}).get(key) or "").strip()
        if value:
            return value
    return f"event:{_sha256([row.get('id'), row.get('source'), row.get('title')])}"


def _availability_ts(row: Mapping[str, Any], meta: Mapping[str, Any], raw_payload: Mapping[str, Any]) -> int:
    event_ts = _safe_int(row.get("ts_ms") or row.get("timestamp"), 0)
    pipeline = meta.get("pipeline_timing") if isinstance(meta.get("pipeline_timing"), dict) else {}
    return _safe_int(
        row.get("availability_ts_ms")
        or raw_payload.get("availability_ts_ms")
        or raw_payload.get("filing_ts_ms")
        or pipeline.get("db_observed_ts_ms")
        or event_ts,
        event_ts,
    )


def _source_hash_for(parts: Mapping[str, Any]) -> str:
    return _content_hash(
        {
            "source_doc_id": parts.get("source_doc_id"),
            "source_event_id": parts.get("source_event_id"),
            "source_url": parts.get("source_url"),
            "source_ts_ms": parts.get("source_ts_ms"),
            "availability_ts_ms": parts.get("availability_ts_ms"),
            "text": parts.get("text"),
        }
    )


def select_source_documents(
    con,
    *,
    decision_ts_ms: int,
    limit: int = 25,
    symbols: Sequence[str] | None = None,
) -> list[SourceDocument]:
    """Return PIT-safe source documents available by ``decision_ts_ms``."""

    anchor = int(decision_ts_ms)
    max_rows = max(1, min(1000, int(limit or 25) * 8))
    sym_filter = {_norm_symbol(sym) for sym in list(symbols or []) if _norm_symbol(sym)}
    where = "WHERE COALESCE(ts_ms, timestamp, 0) <= ?"
    params: list[Any] = [anchor]
    if sym_filter:
        placeholders = ",".join("?" for _ in sym_filter)
        where += f" AND UPPER(COALESCE(symbol,'')) IN ({placeholders})"
        params.extend(sorted(sym_filter))
    rows = con.execute(
        f"""
        SELECT id, ts_ms, timestamp, event_type, symbol, source, title, body, url,
               importance_score, raw_payload, derived_features, meta_json, source_id,
               dedupe_hash, event_key
        FROM events
        {where}
        ORDER BY COALESCE(importance_score, 0.0) DESC, COALESCE(ts_ms, timestamp, 0) DESC, id DESC
        LIMIT ?
        """,
        tuple(params) + (int(max_rows),),
    ).fetchall()
    out: list[SourceDocument] = []
    for row in rows or []:
        payload = {
            "id": _row_get(row, "id", 0),
            "ts_ms": _row_get(row, "ts_ms", 1),
            "timestamp": _row_get(row, "timestamp", 2),
            "event_type": _row_get(row, "event_type", 3),
            "symbol": _row_get(row, "symbol", 4),
            "source": _row_get(row, "source", 5),
            "title": _row_get(row, "title", 6),
            "body": _row_get(row, "body", 7),
            "url": _row_get(row, "url", 8),
            "importance_score": _row_get(row, "importance_score", 9),
            "raw_payload": _row_get(row, "raw_payload", 10),
            "derived_features": _row_get(row, "derived_features", 11),
            "meta_json": _row_get(row, "meta_json", 12),
            "source_id": _row_get(row, "source_id", 13),
            "dedupe_hash": _row_get(row, "dedupe_hash", 14),
            "event_key": _row_get(row, "event_key", 15),
        }
        meta = _json_loads(payload.get("meta_json"))
        raw_payload = dict(meta.get("raw_payload") or {}) if isinstance(meta.get("raw_payload"), dict) else _json_loads(payload.get("raw_payload"))
        document_type = _document_type(str(payload.get("event_type") or ""), str(payload.get("source") or ""), meta)
        if document_type not in DOCUMENT_TYPES:
            continue
        source_ts = _safe_int(payload.get("ts_ms") or payload.get("timestamp"), 0)
        availability_ts = max(source_ts, _availability_ts(payload, meta, raw_payload))
        if source_ts > anchor or availability_ts > anchor:
            continue
        title = str(payload.get("title") or raw_payload.get("title") or "").strip()
        body = str(payload.get("body") or raw_payload.get("body") or raw_payload.get("summary") or "").strip()
        text = "\n".join(part for part in (title, body) if part).strip()
        if not text:
            continue
        source_doc_id = _source_doc_id(payload, meta)
        doc_parts = {
            "source_doc_id": source_doc_id,
            "source_event_id": payload.get("id"),
            "source_url": payload.get("url") or raw_payload.get("url") or raw_payload.get("primary_doc_url") or "",
            "source_ts_ms": source_ts,
            "availability_ts_ms": availability_ts,
            "text": text,
        }
        out.append(
            SourceDocument(
                source_doc_id=str(source_doc_id),
                source_event_id=_safe_int(payload.get("id"), 0) or None,
                symbol=_norm_symbol(payload.get("symbol") or raw_payload.get("symbol") or meta.get("matched_symbol")),
                entity=str(raw_payload.get("company_name") or meta.get("entity") or payload.get("symbol") or "").strip(),
                document_type=document_type,
                source=str(payload.get("source") or "unknown"),
                title=title,
                text=text,
                source_url=str(doc_parts["source_url"] or ""),
                source_ts_ms=int(source_ts),
                availability_ts_ms=int(availability_ts),
                source_hash=_source_hash_for(doc_parts),
            )
        )
        if len(out) >= int(limit):
            break
    return out


def build_extraction_prompt(doc: SourceDocument, *, decision_ts_ms: int, max_input_chars: int) -> tuple[str, str]:
    text = str(doc.text or "")[: max(1, int(max_input_chars))]
    prompt_doc = {
        "source_doc_id": doc.source_doc_id,
        "source_event_id": doc.source_event_id,
        "symbol": doc.symbol,
        "entity": doc.entity or doc.symbol,
        "document_type": doc.document_type,
        "source": doc.source,
        "source_url": doc.source_url,
        "source_ts_ms": int(doc.source_ts_ms),
        "availability_ts_ms": int(doc.availability_ts_ms),
        "decision_ts_ms": int(decision_ts_ms),
        "source_hash": doc.source_hash,
        "text": text,
    }
    instructions = {
        "task": "Extract financial events only when directly supported by the source text.",
        "event_types": list(SUPPORTED_EVENT_TYPES),
        "rules": [
            "Return an empty events array when no supported event is explicitly evidenced.",
            "Use byte/character offsets into the provided text for evidence_span.",
            "Do not infer future information or use outside knowledge.",
            "Set direct_trading_authority=false for every event.",
        ],
        "prompt_version": PROMPT_VERSION,
        "schema_version": SCHEMA_VERSION,
    }
    prompt = (
        "Return strict JSON matching the supplied schema.\n"
        f"INSTRUCTIONS={_json_dumps(instructions)}\n"
        f"SOURCE_DOCUMENT_JSON={_json_dumps(prompt_doc)}"
    )
    prompt_hash = _content_hash({"prompt": prompt, "schema": LLM_EVENT_RESPONSE_SCHEMA, "prompt_version": PROMPT_VERSION})
    return prompt, prompt_hash


def _parse_response_json(text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(str(text or ""))
    except Exception as exc:
        raise ValueError("invalid_json") from exc
    if not isinstance(parsed, dict):
        raise ValueError("json_root_not_object")
    return parsed


def _require_keys(obj: Mapping[str, Any], keys: Sequence[str], *, context: str) -> None:
    missing = [key for key in keys if key not in obj]
    if missing:
        raise ValueError(f"{context}_missing_keys:{','.join(missing)}")


def _reject_extra_keys(obj: Mapping[str, Any], allowed: set[str], *, context: str) -> None:
    extra = sorted(set(str(key) for key in obj.keys()) - set(allowed))
    if extra:
        raise ValueError(f"{context}_extra_keys:{','.join(extra[:8])}")


def validate_extraction_payload(
    payload: Mapping[str, Any],
    *,
    doc: SourceDocument,
    decision_ts_ms: int,
    provider: str,
    model: str,
    prompt_hash: str,
    model_hash: str,
) -> list[dict[str, Any]]:
    _reject_extra_keys(payload, {"events"}, context="response")
    events = payload.get("events")
    if not isinstance(events, list):
        raise ValueError("events_not_array")
    if len(events) > 12:
        raise ValueError("events_too_many")
    out: list[dict[str, Any]] = []
    event_allowed = set(LLM_EVENT_RESPONSE_SCHEMA["properties"]["events"]["items"]["properties"].keys())
    required = list(LLM_EVENT_RESPONSE_SCHEMA["properties"]["events"]["items"]["required"])
    guard_required = ["pit_safe", "decision_ts_ms", "source_available_by_decision_ts", "source_hash", "direct_trading_authority"]
    for idx, raw in enumerate(events):
        if not isinstance(raw, dict):
            raise ValueError(f"event_{idx}_not_object")
        _reject_extra_keys(raw, event_allowed, context=f"event_{idx}")
        _require_keys(raw, required, context=f"event_{idx}")
        event_type = str(raw.get("event_type") or "")
        if event_type not in EVENT_FEATURE_ID:
            raise ValueError(f"event_{idx}_unsupported_event_type:{event_type}")
        direction = str(raw.get("direction") or "").strip().lower()
        if direction not in SUPPORTED_DIRECTIONS:
            raise ValueError(f"event_{idx}_invalid_direction:{direction}")
        confidence = _safe_float(raw.get("confidence"), -1.0)
        if confidence < 0.0 or confidence > 1.0:
            raise ValueError(f"event_{idx}_invalid_confidence")
        polarity = _safe_float(raw.get("polarity"), 999.0)
        if polarity < -1.0 or polarity > 1.0:
            raise ValueError(f"event_{idx}_invalid_polarity")
        if str(raw.get("source_doc_id") or "") != doc.source_doc_id:
            raise ValueError(f"event_{idx}_source_doc_mismatch")
        source_ts = _safe_int(raw.get("source_ts_ms"), -1)
        availability_ts = _safe_int(raw.get("availability_ts_ms"), -1)
        if source_ts != int(doc.source_ts_ms) or availability_ts != int(doc.availability_ts_ms):
            raise ValueError(f"event_{idx}_source_timestamp_mismatch")
        if source_ts > int(decision_ts_ms) or availability_ts > int(decision_ts_ms):
            raise ValueError(f"event_{idx}_future_source_document")
        span = raw.get("evidence_span")
        if not isinstance(span, dict):
            raise ValueError(f"event_{idx}_evidence_span_not_object")
        _reject_extra_keys(span, {"start", "end", "text"}, context=f"event_{idx}_evidence_span")
        _require_keys(span, ["start", "end", "text"], context=f"event_{idx}_evidence_span")
        start = _safe_int(span.get("start"), -1)
        end = _safe_int(span.get("end"), -1)
        evidence = str(span.get("text") or "")
        if start < 0 or end <= start or end > len(doc.text):
            raise ValueError(f"event_{idx}_invalid_evidence_offsets")
        source_slice = str(doc.text[start:end])
        if evidence not in source_slice and source_slice not in evidence:
            raise ValueError(f"event_{idx}_evidence_text_mismatch")
        guard = raw.get("contamination_pit_guard")
        if not isinstance(guard, dict):
            raise ValueError(f"event_{idx}_pit_guard_not_object")
        _reject_extra_keys(guard, set(guard_required), context=f"event_{idx}_pit_guard")
        _require_keys(guard, guard_required, context=f"event_{idx}_pit_guard")
        if guard.get("pit_safe") is not True or guard.get("source_available_by_decision_ts") is not True:
            raise ValueError(f"event_{idx}_pit_guard_not_safe")
        if guard.get("direct_trading_authority") is not False:
            raise ValueError(f"event_{idx}_direct_trading_authority_not_false")
        if _safe_int(guard.get("decision_ts_ms"), -1) != int(decision_ts_ms):
            raise ValueError(f"event_{idx}_decision_ts_mismatch")
        if str(guard.get("source_hash") or "") != doc.source_hash:
            raise ValueError(f"event_{idx}_source_hash_mismatch")
        if str(raw.get("schema_version") or "") != SCHEMA_VERSION:
            raise ValueError(f"event_{idx}_schema_version_mismatch")
        if str(raw.get("prompt_version") or "") != PROMPT_VERSION:
            raise ValueError(f"event_{idx}_prompt_version_mismatch")
        if str(raw.get("extraction_model") or "") != str(model):
            raise ValueError(f"event_{idx}_model_mismatch")
        magnitude = raw.get("magnitude")
        if magnitude is not None:
            if not isinstance(magnitude, dict):
                raise ValueError(f"event_{idx}_magnitude_not_object")
            _reject_extra_keys(magnitude, {"value", "unit", "period"}, context=f"event_{idx}_magnitude")
            _require_keys(magnitude, ["value", "unit"], context=f"event_{idx}_magnitude")
            value = _safe_float(magnitude.get("value"), math.nan)
            if not math.isfinite(value):
                raise ValueError(f"event_{idx}_invalid_magnitude_value")
            if not str(magnitude.get("unit") or "").strip():
                raise ValueError(f"event_{idx}_invalid_magnitude_unit")
        event_uid = _content_hash(
            {
                "source_doc_id": doc.source_doc_id,
                "symbol": _norm_symbol(raw.get("ticker") or doc.symbol),
                "event_type": event_type,
                "evidence_start": start,
                "evidence_end": end,
                "prompt_hash": prompt_hash,
                "model_hash": model_hash,
                "schema_version": SCHEMA_VERSION,
            }
        )
        normalized = {
            "event_uid": event_uid,
            "source_doc_id": doc.source_doc_id,
            "source_event_id": doc.source_event_id,
            "entity": str(raw.get("entity") or doc.entity or doc.symbol),
            "symbol": _norm_symbol(raw.get("ticker") or doc.symbol),
            "event_type": event_type,
            "direction": direction,
            "polarity": float(polarity),
            "magnitude_value": None if magnitude is None else _safe_float(magnitude.get("value"), 0.0),
            "magnitude_unit": None if magnitude is None else str(magnitude.get("unit") or ""),
            "magnitude_period": None if magnitude is None else str(magnitude.get("period") or ""),
            "confidence": float(confidence),
            "source_url": str(raw.get("source_url") or doc.source_url),
            "source_ts_ms": int(source_ts),
            "availability_ts_ms": int(availability_ts),
            "evidence_start": int(start),
            "evidence_end": int(end),
            "evidence_text": evidence,
            "extraction_provider": str(provider),
            "extraction_model": str(model),
            "prompt_version": PROMPT_VERSION,
            "schema_version": SCHEMA_VERSION,
            "prompt_hash": str(prompt_hash),
            "model_hash": str(model_hash),
            "source_hash": str(doc.source_hash),
            "feature_id": EVENT_FEATURE_ID[event_type],
            "direct_trading_authority": 0,
            "created_ts_ms": _now_ms(),
            "payload_json": dict(raw),
            "pit_metadata_json": {
                "pit_eligible": True,
                "source_timestamp_field": "source_ts_ms",
                "availability_timestamp_field": "availability_ts_ms",
                "source_event_ts_ms": int(source_ts),
                "availability_ts_ms": int(availability_ts),
                "decision_ts_ms": int(decision_ts_ms),
                "source_available_by_decision_ts": True,
                "source_hash": str(doc.source_hash),
                "prompt_hash": str(prompt_hash),
                "model_hash": str(model_hash),
                "schema_version": SCHEMA_VERSION,
                "prompt_version": PROMPT_VERSION,
                "extractor_name": EXTRACTOR_NAME,
                "extractor_version": SCHEMA_VERSION,
                "direct_trading_authority": False,
            },
        }
        out.append(normalized)
    return out


def _audit_row(
    con,
    *,
    decision_ts_ms: int,
    doc: SourceDocument | None,
    provider: str,
    model: str,
    prompt_hash: str = "",
    model_hash: str = "",
    status: str,
    rejection_reason: str = "",
    cost_estimate_usd: float = 0.0,
    input_chars: int = 0,
    output_chars: int = 0,
    events_accepted: int = 0,
    raw_response_excerpt: str = "",
    diagnostics: Mapping[str, Any] | None = None,
) -> None:
    ensure_llm_event_extraction_schema(con)
    con.execute(
        """
        INSERT INTO llm_event_extraction_audit(
          ts_ms, decision_ts_ms, source_doc_id, source_event_id, source_hash,
          provider, model, prompt_hash, model_hash, schema_version, status,
          rejection_reason, cost_estimate_usd, input_chars, output_chars,
          events_accepted, raw_response_excerpt, diagnostics_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            _now_ms(),
            int(decision_ts_ms),
            None if doc is None else str(doc.source_doc_id),
            None if doc is None else doc.source_event_id,
            None if doc is None else str(doc.source_hash),
            str(provider or ""),
            str(model or ""),
            str(prompt_hash or ""),
            str(model_hash or ""),
            SCHEMA_VERSION,
            str(status or ""),
            str(rejection_reason or ""),
            float(cost_estimate_usd or 0.0),
            int(input_chars or 0),
            int(output_chars or 0),
            int(events_accepted or 0),
            _bounded_excerpt(raw_response_excerpt),
            _json_dumps(dict(diagnostics or {})),
        ),
    )


def persist_llm_extracted_events(con, events: Sequence[Mapping[str, Any]], *, doc: SourceDocument) -> int:
    clean = [dict(event or {}) for event in list(events or []) if event]
    if not clean:
        return 0
    ensure_llm_event_extraction_schema(con)
    written = 0
    for row in clean:
        con.execute(
            """
            INSERT INTO llm_extracted_events(
              event_uid, source_doc_id, source_event_id, entity, symbol, event_type,
              direction, polarity, magnitude_value, magnitude_unit, magnitude_period,
              confidence, source_url, source_ts_ms, availability_ts_ms, evidence_start,
              evidence_end, evidence_text, extraction_provider, extraction_model,
              prompt_version, schema_version, prompt_hash, model_hash, source_hash,
              feature_id, direct_trading_authority, created_ts_ms, payload_json, pit_metadata_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(event_uid) DO UPDATE SET
              confidence=excluded.confidence,
              polarity=excluded.polarity,
              magnitude_value=excluded.magnitude_value,
              magnitude_unit=excluded.magnitude_unit,
              magnitude_period=excluded.magnitude_period,
              evidence_text=excluded.evidence_text,
              created_ts_ms=excluded.created_ts_ms,
              payload_json=excluded.payload_json,
              pit_metadata_json=excluded.pit_metadata_json
            """,
            (
                row["event_uid"],
                row["source_doc_id"],
                row.get("source_event_id"),
                row["entity"],
                row["symbol"],
                row["event_type"],
                row["direction"],
                float(row["polarity"]),
                row.get("magnitude_value"),
                row.get("magnitude_unit"),
                row.get("magnitude_period"),
                float(row["confidence"]),
                row.get("source_url"),
                int(row["source_ts_ms"]),
                int(row["availability_ts_ms"]),
                int(row["evidence_start"]),
                int(row["evidence_end"]),
                row["evidence_text"],
                row["extraction_provider"],
                row["extraction_model"],
                row["prompt_version"],
                row["schema_version"],
                row["prompt_hash"],
                row["model_hash"],
                row["source_hash"],
                row["feature_id"],
                int(row.get("direct_trading_authority") or 0),
                int(row["created_ts_ms"]),
                _json_dumps(row.get("payload_json") or {}),
                _json_dumps(row.get("pit_metadata_json") or {}),
            ),
        )
        written += 1
    structured_rows = [
        {
            "source_document_id": row["source_doc_id"],
            "source_event_id": row.get("source_event_id"),
            "symbol": row["symbol"],
            "document_type": doc.document_type,
            "source": doc.source,
            "event_type": row["event_type"],
            "event_ts_ms": int(row["source_ts_ms"]),
            "availability_ts_ms": int(row["availability_ts_ms"]),
            "extraction_confidence": float(row["confidence"]),
            "polarity": float(row["polarity"]),
            "feature_id": row["feature_id"],
            "evidence": row["evidence_text"],
            "extractor_name": EXTRACTOR_NAME,
            "extractor_version": SCHEMA_VERSION,
            "created_ts_ms": int(row["created_ts_ms"]),
            "payload_json": {
                "llm_event_uid": row["event_uid"],
                "provider": row["extraction_provider"],
                "model": row["extraction_model"],
                "prompt_hash": row["prompt_hash"],
                "model_hash": row["model_hash"],
                "source_hash": row["source_hash"],
                "evidence_start": int(row["evidence_start"]),
                "evidence_end": int(row["evidence_end"]),
            },
            "pit_metadata_json": dict(row.get("pit_metadata_json") or {}),
        }
        for row in clean
    ]
    put_structured_document_events(con, structured_rows)
    return int(written)


def _adapter_from_config(config: LLMEventExtractionConfig) -> LLMEventExtractionAdapter | None:
    provider = str(config.provider or "").strip().lower()
    if provider == "openai":
        api_key = get_data_credential("OPENAI_API_KEY", ttl_s=300)
        return OpenAIResponsesAdapter(api_key=api_key, model=config.model) if api_key else None
    if provider == "anthropic":
        api_key = get_data_credential("ANTHROPIC_API_KEY", ttl_s=300)
        return AnthropicToolAdapter(api_key=api_key, model=config.model) if api_key else None
    if provider == "fake":
        return FakeLLMEventExtractionAdapter()
    raise ValueError(f"unsupported_llm_event_extract_provider:{provider}")


def _estimated_cost(input_chars: int, output_tokens: int, config: LLMEventExtractionConfig) -> float:
    input_tokens_est = max(1.0, float(input_chars) / 4.0)
    return float((input_tokens_est / 1000.0) * float(config.input_cost_per_1k) + (float(output_tokens) / 1000.0) * float(config.output_cost_per_1k))


def run_llm_event_extraction_batch(
    *,
    con=None,
    adapter: LLMEventExtractionAdapter | None = None,
    config: LLMEventExtractionConfig | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    """Run one bounded PIT-safe extraction batch."""

    cfg = config or LLMEventExtractionConfig.from_env()
    owns = con is None
    if owns:
        from engine.runtime.storage import connect, init_db

        init_db()
        con = connect(readonly=False)
    try:
        ensure_llm_event_extraction_schema(con)
        decision_ts_ms = int(cfg.decision_ts_ms if cfg.decision_ts_ms is not None else _now_ms())
        if not bool(cfg.enabled) and adapter is None:
            _audit_row(
                con,
                decision_ts_ms=decision_ts_ms,
                doc=None,
                provider=str(cfg.provider),
                model=str(cfg.model),
                status="disabled",
                rejection_reason="LLM_EVENT_EXTRACT_ENABLED_not_set",
            )
            if owns:
                con.commit()
            return {"ok": True, "status": "disabled", "processed_docs": 0, "events_written": 0, "errors": []}

        resolved_adapter = adapter or _adapter_from_config(cfg)
        if resolved_adapter is None:
            _audit_row(
                con,
                decision_ts_ms=decision_ts_ms,
                doc=None,
                provider=str(cfg.provider),
                model=str(cfg.model),
                status="missing_key",
                rejection_reason=f"missing_{str(cfg.provider).upper()}_credential",
            )
            if owns:
                con.commit()
            return {"ok": True, "status": "missing_key", "processed_docs": 0, "events_written": 0, "errors": []}

        docs = select_source_documents(con, decision_ts_ms=decision_ts_ms, limit=int(cfg.max_docs), symbols=cfg.symbols)
        if not docs:
            _audit_row(
                con,
                decision_ts_ms=decision_ts_ms,
                doc=None,
                provider=resolved_adapter.provider,
                model=resolved_adapter.model,
                status="no_sources",
            )
            if owns:
                con.commit()
            return {"ok": True, "status": "no_sources", "processed_docs": 0, "events_written": 0, "errors": []}

        spent = 0.0
        processed = 0
        written = 0
        rejected = 0
        errors: list[str] = []
        model_hash = _content_hash({"provider": resolved_adapter.provider, "model": resolved_adapter.model, "schema_version": SCHEMA_VERSION})
        last_call_ms = 0
        for doc in docs:
            prompt, prompt_hash = build_extraction_prompt(doc, decision_ts_ms=decision_ts_ms, max_input_chars=int(cfg.max_input_chars))
            input_chars = len(prompt)
            pre_cost = _estimated_cost(input_chars, int(cfg.max_output_tokens), cfg)
            if spent + pre_cost > float(cfg.max_cost_usd):
                _audit_row(
                    con,
                    decision_ts_ms=decision_ts_ms,
                    doc=doc,
                    provider=resolved_adapter.provider,
                    model=resolved_adapter.model,
                    prompt_hash=prompt_hash,
                    model_hash=model_hash,
                    status="cost_exhausted",
                    rejection_reason="max_cost_usd_exceeded",
                    cost_estimate_usd=pre_cost,
                    input_chars=input_chars,
                )
                break
            now = _now_ms()
            wait_ms = int(cfg.min_interval_ms) - max(0, now - int(last_call_ms))
            if last_call_ms and wait_ms > 0:
                if sleep_fn is None:
                    time.sleep(wait_ms / 1000.0)
                else:
                    sleep_fn(wait_ms / 1000.0)
            last_call_ms = _now_ms()
            try:
                response = resolved_adapter.extract(prompt=prompt, schema=LLM_EVENT_RESPONSE_SCHEMA, config=cfg)
                parsed = _parse_response_json(response.text)
                events = validate_extraction_payload(
                    parsed,
                    doc=doc,
                    decision_ts_ms=decision_ts_ms,
                    provider=response.provider,
                    model=response.model,
                    prompt_hash=prompt_hash,
                    model_hash=model_hash,
                )
                accepted = persist_llm_extracted_events(con, events, doc=doc)
                output_chars = len(response.text)
                cost = _estimated_cost(input_chars, int(response.output_tokens or cfg.max_output_tokens), cfg)
                spent += cost
                processed += 1
                written += accepted
                _audit_row(
                    con,
                    decision_ts_ms=decision_ts_ms,
                    doc=doc,
                    provider=response.provider,
                    model=response.model,
                    prompt_hash=prompt_hash,
                    model_hash=model_hash,
                    status="accepted",
                    cost_estimate_usd=cost,
                    input_chars=input_chars,
                    output_chars=output_chars,
                    events_accepted=accepted,
                    raw_response_excerpt=response.raw_excerpt,
                    diagnostics={"input_tokens": response.input_tokens, "output_tokens": response.output_tokens},
                )
            except Exception as exc:
                rejected += 1
                errors.append(str(exc))
                _audit_row(
                    con,
                    decision_ts_ms=decision_ts_ms,
                    doc=doc,
                    provider=resolved_adapter.provider,
                    model=resolved_adapter.model,
                    prompt_hash=prompt_hash,
                    model_hash=model_hash,
                    status="rejected",
                    rejection_reason=str(exc)[:500],
                    input_chars=input_chars,
                )
                log_failure(
                    LOG,
                    event="llm_event_extraction_rejected",
                    code="LLM_EVENT_EXTRACTION_REJECTED",
                    message=str(exc),
                    error=exc,
                    component="engine.data.llm_event_extraction",
                    persist=False,
                )
        if owns:
            con.commit()
        return {
            "ok": True,
            "status": "ok",
            "processed_docs": int(processed),
            "events_written": int(written),
            "rejected_docs": int(rejected),
            "cost_estimate_usd": round(float(spent), 8),
            "errors": errors[:10],
            "direct_trading_authority": False,
            "feature_stage": "shadow",
        }
    finally:
        if owns and con is not None:
            con.close()


__all__ = [
    "AdapterResponse",
    "AnthropicToolAdapter",
    "EXTRACTOR_NAME",
    "FakeLLMEventExtractionAdapter",
    "LLM_EVENT_RESPONSE_SCHEMA",
    "LLMEventExtractionConfig",
    "OpenAIResponsesAdapter",
    "PROMPT_VERSION",
    "SCHEMA_VERSION",
    "SUPPORTED_EVENT_TYPES",
    "build_extraction_prompt",
    "ensure_llm_event_extraction_schema",
    "persist_llm_extracted_events",
    "run_llm_event_extraction_batch",
    "select_source_documents",
    "validate_extraction_payload",
]
