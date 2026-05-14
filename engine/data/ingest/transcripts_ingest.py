import datetime as dt
import logging
from typing import Any, Dict, List, Sequence, Tuple

import requests

from engine.artifacts.store import LocalArtifactStore
from engine.data._credentials import get_data_credential
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger

FMP_BASE = "https://financialmodelingprep.com/api/v3"
LOG = get_logger("engine.data.ingest.transcripts_ingest")


def _fmp_key() -> str:
    return get_data_credential("FMP_API_KEY")


def _store_transcript_body(*, transcript_id: str, symbol: str, body: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    if not body:
        return {}
    alias = f"transcript:fmp:{transcript_id}"
    try:
        ref = LocalArtifactStore().put(
            body.encode("utf-8"),
            content_type="text/plain; charset=utf-8",
            kind="transcript",
            alias=alias,
            metadata={
                **dict(metadata or {}),
                "symbol": str(symbol),
                "transcript_id": str(transcript_id),
                "provider": "fmp",
            },
        )
        return {
            "artifact_alias": alias,
            "artifact_sha256": ref.sha256,
            "artifact_size_bytes": int(ref.size),
            "content_type": ref.content_type,
        }
    except Exception as exc:
        log_failure(
            LOG,
            event="transcripts_ingest_artifact_store_failed",
            code="TRANSCRIPTS_INGEST_ARTIFACT_STORE_FAILED",
            message="Transcript artifact storage failed.",
            error=exc,
            level=logging.WARNING,
            component="engine.data.ingest.transcripts_ingest",
            extra={"symbol": symbol, "transcript_id": transcript_id},
            persist=False,
        )
        return {}


def _quarter_windows(now: dt.date) -> List[Tuple[int, int]]:
    quarter = ((now.month - 1) // 3) + 1
    year = now.year
    windows: List[Tuple[int, int]] = []
    for _ in range(3):
        windows.append((year, quarter))
        quarter -= 1
        if quarter <= 0:
            year -= 1
            quarter = 4
    return windows


def ingest_transcripts(
    symbols: Sequence[str],
    *,
    timeout_s: int = 20,
    max_items_per_symbol: int = 2,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    fmp_key = _fmp_key()
    if not fmp_key:
        return [], []

    session = requests.Session()
    today = dt.date.today()
    out: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for raw_symbol in symbols or []:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        captured = 0
        for year, quarter in _quarter_windows(today):
            if captured >= max(1, int(max_items_per_symbol)):
                break
            try:
                response = session.get(
                    f"{FMP_BASE}/earning_call_transcript/{symbol}",
                    params={"year": int(year), "quarter": int(quarter), "apikey": fmp_key},
                    timeout=timeout_s,
                )
                if response.status_code == 404:
                    continue
                response.raise_for_status()
                payload = response.json()
                items = payload if isinstance(payload, list) else []
            except Exception as exc:
                errors.append(
                    {
                        "source_name": f"fmp_transcript:{symbol}:{year}Q{quarter}",
                        "error": repr(exc),
                    }
                )
                log_failure(
                    LOG,
                    event="transcripts_ingest_fetch_failed",
                    code="TRANSCRIPTS_INGEST_FETCH_FAILED",
                    message="Transcript fetch failed.",
                    error=exc,
                    level=logging.WARNING,
                    component="engine.data.ingest.transcripts_ingest",
                    extra={"symbol": symbol, "year": int(year), "quarter": int(quarter)},
                    persist=False,
                )
                continue

            for item in items:
                if captured >= max(1, int(max_items_per_symbol)):
                    break
                body = str(item.get("content") or item.get("text") or "").strip()
                title = str(item.get("title") or "").strip() or f"{symbol} earnings call transcript"
                date_text = str(item.get("date") or "").strip()
                ts_ms = 0
                if date_text:
                    try:
                        ts_ms = int(dt.datetime.fromisoformat(date_text.replace("Z", "+00:00")).timestamp() * 1000)
                    except Exception:
                        try:
                            ts_ms = int(dt.datetime.strptime(date_text[:10], "%Y-%m-%d").timestamp() * 1000)
                        except Exception:
                            ts_ms = 0
                transcript_id = str(item.get("symbol") or symbol).strip().upper()
                transcript_id = f"{transcript_id}:{year}Q{quarter}:{item.get('date') or captured}"
                if not body:
                    continue
                artifact_meta = _store_transcript_body(
                    transcript_id=transcript_id,
                    symbol=symbol,
                    body=body,
                    metadata={
                        "quarter": int(quarter),
                        "year": int(year),
                        "transcript_date": date_text,
                    },
                )
                out.append(
                    {
                        "ts_ms": ts_ms,
                        "event_type": "news",
                        "symbol": symbol,
                        "source": "fmp_transcript",
                        "source_id": transcript_id,
                        "title": title,
                        "body": body[:8000],
                        "url": str(item.get("url") or "").strip() or None,
                        "event_key": f"fmp_transcript:{transcript_id}",
                        "meta_json": {
                            "provider": "fmp",
                            "transcript": True,
                            "quarter": int(quarter),
                            "year": int(year),
                            "transcript_date": date_text,
                            **artifact_meta,
                        },
                    }
                )
                captured += 1
    return out, errors
