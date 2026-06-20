"""Job entrypoint for learned alpha decay/capacity/crowding estimates."""

from engine.strategy.learned_alpha_decay import main


if __name__ == "__main__":
    raise SystemExit(main())
