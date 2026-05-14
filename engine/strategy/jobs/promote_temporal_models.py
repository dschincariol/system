"""CLI/job entrypoint that delegates to temporal model promotion logic."""

from engine.strategy.promote_temporal_models import main


if __name__ == "__main__":
    raise SystemExit(main())
