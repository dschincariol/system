"""Build triple-barrier meta-label rows for matured primary intents."""

from __future__ import annotations

from engine.strategy.meta_labeling import run_label_job

JOB_NAME = "triple_barrier_labels"


def run() -> dict:
    return run_label_job()


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
