"""UTC timestamp helpers for ingestion boundaries."""

from __future__ import annotations

from datetime import datetime, timezone


def assert_utc_datetime(value: datetime, *, field_name: str = "timestamp") -> datetime:
    """Return `value` normalized to UTC, raising when it is naive."""
    if not isinstance(value, datetime):
        raise TypeError(f"{field_name}_must_be_datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"naive_datetime_not_utc:{field_name}")
    return value.astimezone(timezone.utc)


def utc_ms_from_datetime(value: datetime, *, field_name: str = "timestamp") -> int:
    dt_utc = assert_utc_datetime(value, field_name=field_name)
    return int(dt_utc.timestamp() * 1000)
