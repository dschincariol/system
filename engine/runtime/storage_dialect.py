"""SQL dialect helpers for the Postgres-backed storage facade."""

from __future__ import annotations

import re
from functools import lru_cache


_DOLLAR_QUOTE_RE = re.compile(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$")
_JSON_EXPR_END_CHARS = set(")]}\"'")


def _is_ident_char(ch: str) -> bool:
    return ch.isalnum() or ch in {"_", "."}


def _skip_ws(sql: str, idx: int) -> int:
    n = len(sql)
    while idx < n and sql[idx].isspace():
        idx += 1
    return idx


def _prev_significant_char(sql: str, idx: int) -> str:
    pos = idx - 1
    while pos >= 0 and sql[pos].isspace():
        pos -= 1
    return sql[pos] if pos >= 0 else ""


def _next_starts_json_key(sql: str, idx: int) -> bool:
    idx = _skip_ws(sql, idx)
    if idx >= len(sql):
        return False
    if sql[idx] == "'":
        return True
    if sql[idx] == "$":
        return _DOLLAR_QUOTE_RE.match(sql, idx) is not None
    if sql[idx] == '"':
        return True
    return _is_ident_char(sql[idx])


def _is_json_question_operator(sql: str, idx: int) -> bool:
    if idx + 1 < len(sql) and sql[idx + 1] in {"|", "&"}:
        return True

    prev = _prev_significant_char(sql, idx)
    if not prev or (not _is_ident_char(prev) and prev not in _JSON_EXPR_END_CHARS):
        return False
    return _next_starts_json_key(sql, idx + 1)


def _dollar_quote_at(sql: str, idx: int) -> str | None:
    match = _DOLLAR_QUOTE_RE.match(sql, idx)
    return str(match.group(0)) if match else None


@lru_cache(maxsize=1024)
def to_pg_params(sql: str) -> str:
    """Rewrite DB-API qmark placeholders to psycopg's ``%s`` placeholders.

    The walk tracks SQL string literals, quoted identifiers, dollar-quoted
    strings, and comments explicitly so only real qmark placeholders are
    rewritten. Existing call sites can keep using ``?`` placeholders while the
    runtime uses psycopg underneath.
    """

    text = str(sql or "")
    out: list[str] = []
    idx = 0
    n = len(text)
    while idx < n:
        ch = text[idx]

        if text.startswith("--", idx):
            end = text.find("\n", idx + 2)
            if end < 0:
                out.append(text[idx:])
                break
            out.append(text[idx : end + 1])
            idx = end + 1
            continue

        if text.startswith("/*", idx):
            end = text.find("*/", idx + 2)
            if end < 0:
                out.append(text[idx:])
                break
            out.append(text[idx : end + 2])
            idx = end + 2
            continue

        dollar_quote = _dollar_quote_at(text, idx) if ch == "$" else None
        if dollar_quote:
            end = text.find(dollar_quote, idx + len(dollar_quote))
            if end < 0:
                out.append(text[idx:])
                break
            end += len(dollar_quote)
            out.append(text[idx:end])
            idx = end
            continue

        if ch == "'":
            start = idx
            idx += 1
            while idx < n:
                if text[idx] == "\\" and idx + 1 < n:
                    idx += 2
                    continue
                if text[idx] == "'" and idx + 1 < n and text[idx + 1] == "'":
                    idx += 2
                    continue
                if text[idx] == "'":
                    idx += 1
                    break
                idx += 1
            out.append(text[start:idx])
            continue

        if ch == '"':
            start = idx
            idx += 1
            while idx < n:
                if text[idx] == "\\" and idx + 1 < n:
                    idx += 2
                    continue
                if text[idx] == '"' and idx + 1 < n and text[idx + 1] == '"':
                    idx += 2
                    continue
                if text[idx] == '"':
                    idx += 1
                    break
                idx += 1
            out.append(text[start:idx])
            continue

        if ch == "?":
            out.append("?" if _is_json_question_operator(text, idx) else "%s")
        else:
            out.append(ch)
        idx += 1
    return "".join(out)


def bigserial() -> str:
    return "BIGSERIAL"


def jsonb() -> str:
    return "JSONB"


def bytea() -> str:
    return "BYTEA"


def timestamptz() -> str:
    return "TIMESTAMPTZ"
