from __future__ import annotations

import re
from typing import Any


_LEVEL_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\b(?:CRIT|CRITICAL|FATAL)\b", re.IGNORECASE), "CRIT"),
    (re.compile(r"\b(?:ERROR|ERR)\b", re.IGNORECASE), "ERROR"),
    (re.compile(r"\b(?:WARN|WARNING)\b", re.IGNORECASE), "WARN"),
    (re.compile(r"\bINFO\b", re.IGNORECASE), "INFO"),
    (re.compile(r"\bDEBUG\b", re.IGNORECASE), "DEBUG"),
    (re.compile(r"\bTRACE\b", re.IGNORECASE), "TRACE"),
]


def coerce_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(int(minimum), min(int(maximum), out))


def normalize_level(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"", "ALL"}:
        return ""
    if raw in {"CRIT", "CRITICAL", "FATAL"}:
        return "CRIT"
    if raw in {"ERROR", "ERR"}:
        return "ERROR"
    if raw in {"WARN", "WARNING", "HIGH"}:
        return "WARN"
    if raw in {"INFO", "DEBUG", "TRACE"}:
        return raw
    return raw


def detect_level(line: Any) -> str:
    text = str(line or "")
    for pattern, level in _LEVEL_PATTERNS:
        if pattern.search(text):
            return level
    return ""


def ensure_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    if isinstance(value, dict):
        if isinstance(value.get("lines"), list):
            return [str(item) for item in value.get("lines", [])]
        if value.get("text") is not None:
            return str(value.get("text") or "").splitlines()
        if value.get("log") is not None:
            return str(value.get("log") or "").splitlines()
    return str(value or "").splitlines()


def lines_to_text(lines: list[str]) -> str:
    return "\n".join([str(line) for line in (lines or []) if line is not None])


def filter_lines(lines: list[str], *, level: Any = "", query: Any = "", limit: Any = 0) -> list[str]:
    normalized_level = normalize_level(level)
    needle = str(query or "").strip().lower()
    out: list[str] = []

    for raw_line in lines or []:
        line = str(raw_line or "")
        if normalized_level and detect_level(line) != normalized_level:
            continue
        if needle and needle not in line.lower():
            continue
        out.append(line)

    try:
        capped = int(limit)
    except Exception:
        capped = 0
    if capped > 0 and len(out) > capped:
        out = out[-capped:]

    return out
