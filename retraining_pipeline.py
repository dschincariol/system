"""CLI wrapper for the strategy retraining pipeline."""

from engine.strategy import retraining_pipeline as _impl
from engine.strategy.retraining_pipeline import *  # noqa: F401,F403
from engine.strategy.retraining_pipeline import main

__all__ = [name for name in dir(_impl) if not name.startswith("_")]


def __getattr__(name: str):
    return getattr(_impl, name)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(dir(_impl)))


if __name__ == "__main__":
    raise SystemExit(main())
