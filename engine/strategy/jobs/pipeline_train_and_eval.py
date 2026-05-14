"""
FILE: pipeline_train_and_eval.py

Job entrypoint wrapper for the main strategy train-and-evaluate pipeline.
"""

from engine.strategy.pipeline_train_and_eval import main

if __name__ == "__main__":
    raise SystemExit(main())
