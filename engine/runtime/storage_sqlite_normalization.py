"""Pure normalization and SQL classification helpers for SQLite test storage."""

from __future__ import annotations

import json
import re
from typing import Any


def env_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def adapt_json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)


def is_read_statement(sql: str) -> bool:
    return bool(
        re.match(r"^\s*(?:SELECT|WITH|PRAGMA)\b", str(sql or ""), flags=re.IGNORECASE)
    )


def is_auto_write_statement(sql: str) -> bool:
    return bool(
        re.match(
            r"^\s*(?:INSERT|UPDATE|DELETE|REPLACE)\b",
            str(sql or ""),
            flags=re.IGNORECASE,
        )
    )


def normalized_sql_signature(sql: str) -> str:
    return re.sub(r"\s+", "", str(sql or "")).upper()


def normalize_param(value: Any) -> Any:
    if isinstance(value, (dict, list, tuple)):
        return adapt_json(value)
    if isinstance(value, memoryview):
        return bytes(value)
    return value


def normalize_params(params: Any) -> Any:
    if params is None:
        return None
    if isinstance(params, dict):
        return {str(key): normalize_param(value) for key, value in params.items()}
    if isinstance(params, (tuple, list)):
        return tuple(normalize_param(value) for value in params)
    return normalize_param(params)
