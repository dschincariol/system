"""Train governed meta-label classifiers from triple-barrier labels."""

from __future__ import annotations

from engine.strategy.meta_labeling import run_train_job

JOB_NAME = "train_meta_label_model"


def run() -> dict:
    return run_train_job()


def main() -> None:
    print(run())


if __name__ == "__main__":
    main()
