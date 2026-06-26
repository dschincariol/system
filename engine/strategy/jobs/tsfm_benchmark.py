"""Job entrypoint for governed TSFM benchmark runs."""

from __future__ import annotations

import argparse
import json
import os
from typing import Sequence


def _set_if_present(env_name: str, value: str | None) -> None:
    if value is not None and str(value).strip():
        os.environ[str(env_name)] = str(value).strip()


def main(argv: Sequence[str] | None = None) -> dict:
    parser = argparse.ArgumentParser(description="Run governed TSFM benchmark")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols")
    parser.add_argument("--backends", default=None, help="Comma-separated TSFM backends")
    parser.add_argument("--tasks", default=None, help="Comma-separated benchmark tasks")
    parser.add_argument("--horizon-rows", default=None)
    parser.add_argument("--context-rows", default=None)
    parser.add_argument("--max-eval-points", default=None)
    parser.add_argument("--fallback", default=None, choices=("skip", "fake"))
    args = parser.parse_args(list(argv) if argv is not None else None)

    _set_if_present("TSFM_BENCHMARK_SYMBOLS", args.symbols)
    _set_if_present("TSFM_BENCHMARK_BACKENDS", args.backends)
    _set_if_present("TSFM_BENCHMARK_TASKS", args.tasks)
    _set_if_present("TSFM_BENCHMARK_HORIZON_ROWS", args.horizon_rows)
    _set_if_present("TSFM_BENCHMARK_CONTEXT_ROWS", args.context_rows)
    _set_if_present("TSFM_BENCHMARK_MAX_EVAL_POINTS", args.max_eval_points)
    _set_if_present("TSFM_BENCHMARK_FALLBACK", args.fallback)

    from engine.strategy.tsfm_benchmark import config_from_env, run_tsfm_benchmark

    summary = run_tsfm_benchmark(config_from_env())
    print(json.dumps(summary, sort_keys=True))
    return summary


if __name__ == "__main__":
    main()
