"""CLI/job entrypoint that delegates to temporal predictor training."""

from engine.strategy.train_temporal_predictor import main


if __name__ == "__main__":
    raise SystemExit(main())
