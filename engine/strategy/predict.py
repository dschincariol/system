"""
FILE: predict.py

Human-readable purpose:
Small CLI helper that trains lightweight relevance stats from labels and prints
the resulting model statistics. It is useful for manual inspection and quick
local sanity checks, not as the primary production prediction path.
"""

from engine.strategy.learning import (
    learn_relevance_stats as train_stats_from_labels,
    get_model_stats,
    confidence_from_n,
)


def main() -> int:
    trained = train_stats_from_labels()
    print("trained_rows =", trained)

    # This output is intentionally human-readable so an operator can see what
    # the simple model learned without opening the database directly.
    rows = get_model_stats()
    for sym, h, n, mean_z, updated_at in rows:
        conf = confidence_from_n(int(n))
        print(f"{sym} horizon_s={h} n={n} mean_impact_z={mean_z:+.3f} confidence={conf:.2f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
