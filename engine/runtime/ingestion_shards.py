"""Shard helpers for supervised ingestion workers."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Mapping, MutableMapping, Sequence, TypeVar


_T = TypeVar("_T")


@dataclass(frozen=True)
class IngestionShard:
    index: int = 0
    count: int = 1

    @property
    def enabled(self) -> bool:
        return int(self.count) > 1

    @property
    def label(self) -> str:
        return f"shard:{int(self.index)}-of-{int(self.count)}"

    def as_dict(self) -> dict[str, int | bool | str]:
        return {
            "index": int(self.index),
            "count": int(self.count),
            "enabled": bool(self.enabled),
            "label": str(self.label),
        }


def _parse_int_env(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(str(name))
    if raw is None or str(raw).strip() == "":
        return int(default)
    text = str(raw).strip()
    try:
        return int(text, 10)
    except Exception as exc:
        raise ValueError(f"{name}_invalid_integer:{text}") from exc


def parse_ingestion_shard_env(env: Mapping[str, str] | None = None) -> IngestionShard:
    source = os.environ if env is None else env
    count = _parse_int_env(source, "INGESTION_SHARD_COUNT", 1)
    index = _parse_int_env(source, "INGESTION_SHARD_INDEX", 0)
    if count < 1:
        raise ValueError(f"INGESTION_SHARD_COUNT_out_of_range:{count}:expected>=1")
    if index < 0:
        raise ValueError(f"INGESTION_SHARD_INDEX_out_of_range:{index}:expected>=0")
    if index >= count:
        raise ValueError(f"INGESTION_SHARD_INDEX_out_of_range:{index}:expected<INGESTION_SHARD_COUNT:{count}")
    return IngestionShard(index=int(index), count=int(count))


def current_ingestion_shard() -> IngestionShard:
    return parse_ingestion_shard_env(os.environ)


def canonical_shard_env(shard: IngestionShard | None = None) -> dict[str, str]:
    selected = shard or current_ingestion_shard()
    return {
        "INGESTION_SHARD_INDEX": str(int(selected.index)),
        "INGESTION_SHARD_COUNT": str(int(selected.count)),
    }


def ingestion_shard_job_name(job_name: str, shard: IngestionShard | None = None) -> str:
    base = str(job_name or "").strip()
    if not base:
        raise ValueError("ingestion_shard_job_name_empty")
    selected = shard or current_ingestion_shard()
    if not selected.enabled:
        return base
    return f"{base}:{selected.label}"


def ingestion_state_key(base_key: str = "ingestion_state", shard: IngestionShard | None = None) -> str:
    key = str(base_key or "").strip()
    if not key:
        raise ValueError("ingestion_state_key_empty")
    selected = shard or current_ingestion_shard()
    if not selected.enabled:
        return key
    return f"{key}::{selected.label}"


def normalize_shard_symbol(symbol: object) -> str:
    return str(symbol or "").strip().upper()


def stable_symbol_shard(symbol: object, shard_count: int) -> int:
    count = int(shard_count)
    if count < 1:
        raise ValueError(f"shard_count_out_of_range:{count}:expected>=1")
    normalized = normalize_shard_symbol(symbol)
    digest = hashlib.sha256(normalized.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=False) % count


def symbol_belongs_to_shard(symbol: object, shard: IngestionShard | None = None) -> bool:
    selected = shard or current_ingestion_shard()
    if not selected.enabled:
        return True
    normalized = normalize_shard_symbol(symbol)
    if not normalized:
        return False
    return stable_symbol_shard(normalized, int(selected.count)) == int(selected.index)


def filter_symbols_for_shard(symbols: Sequence[_T], shard: IngestionShard | None = None) -> list[_T]:
    selected = shard or current_ingestion_shard()
    if not selected.enabled:
        return list(symbols or [])
    return [symbol for symbol in list(symbols or []) if symbol_belongs_to_shard(symbol, selected)]


def filter_symbol_mapping_for_shard(
    mapping: Mapping[str, _T] | MutableMapping[str, _T],
    shard: IngestionShard | None = None,
) -> dict[str, _T]:
    selected = shard or current_ingestion_shard()
    if not selected.enabled:
        return dict(mapping or {})
    return {
        str(symbol): value
        for symbol, value in dict(mapping or {}).items()
        if symbol_belongs_to_shard(symbol, selected)
    }
