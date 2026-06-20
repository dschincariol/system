"""CLI wrapper for feature-store backfills."""

from engine.data.jobs import backfill_features as _impl
from engine.data.jobs.backfill_features import *  # noqa: F401,F403
from engine.data.jobs.backfill_features import main

__all__ = [name for name in dir(_impl) if not name.startswith("_")]


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_impl)))


if __name__ == "__main__":
    raise SystemExit(main())
