from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.runtime.telemetry_migration_validation import build_telemetry_migration_validation_snapshot


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare recent SQLite and Timescale telemetry windows.")
    parser.add_argument("--lookback-minutes", type=int, default=60)
    parser.add_argument("--max-count-delta", type=int, default=0)
    parser.add_argument("--max-last-ts-lag-ms", type=int, default=5000)
    parser.add_argument("--require-healthy-mirror", action="store_true")
    parser.add_argument("--require-healthy-timescale", action="store_true")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when the comparison snapshot is not ok.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    comparison = build_telemetry_migration_validation_snapshot(
        lookback_minutes=max(1, int(args.lookback_minutes)),
        max_count_delta=max(0, int(args.max_count_delta)),
        max_last_ts_lag_ms=max(0, int(args.max_last_ts_lag_ms)),
        require_healthy_mirror=bool(args.require_healthy_mirror),
        require_healthy_timescale=bool(args.require_healthy_timescale),
    )
    if args.json:
        print(json.dumps(comparison, indent=2, sort_keys=True))
    else:
        print(json.dumps(comparison, indent=2, sort_keys=True))
    return 0 if (not bool(args.strict) or bool(comparison.get("ok"))) else 1


if __name__ == "__main__":
    raise SystemExit(main())
