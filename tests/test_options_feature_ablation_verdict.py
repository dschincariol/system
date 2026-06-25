import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.options_feature_ablation import (
    VALID_VERDICTS,
    VERDICT_ABSTAIN_INSUFFICIENT_DATA,
    VERDICT_ENABLE_NOT_SUPPORTED,
    VERDICT_ENABLE_SUPPORTED,
    evaluate_enablement,
)


class OptionsFeatureAblationVerdictTests(unittest.TestCase):
    def test_verdict_set_is_exactly_documented_values(self):
        self.assertEqual(
            VALID_VERDICTS,
            {
                "ENABLE_SUPPORTED",
                "ENABLE_NOT_SUPPORTED",
                "ABSTAIN_INSUFFICIENT_DATA",
            },
        )

    def test_positive_delta_and_stability_support_enablement(self):
        report = {
            "status": "ok",
            "dataset": {"usable_rows": 1000, "gex_flow_nonzero_coverage": 0.85},
            "metrics": {"training_status": "ok"},
            "delta": {"rank_ic_mean": 0.04, "positive_rank_ic_delta_fraction": 0.8},
        }

        verdict = evaluate_enablement(
            report,
            thresholds={
                "min_rows": 500,
                "min_rank_ic_delta": 0.01,
                "min_stability_fraction": 0.6,
                "min_gex_coverage": 0.5,
            },
        )

        self.assertEqual(verdict["verdict"], VERDICT_ENABLE_SUPPORTED)

    def test_zero_or_negative_delta_does_not_support_enablement(self):
        report = {
            "status": "ok",
            "dataset": {"usable_rows": 1000, "gex_flow_nonzero_coverage": 0.85},
            "metrics": {"training_status": "ok"},
            "delta": {"rank_ic_mean": -0.001, "positive_rank_ic_delta_fraction": 0.8},
        }

        verdict = evaluate_enablement(
            report,
            thresholds={
                "min_rows": 500,
                "min_rank_ic_delta": 0.01,
                "min_stability_fraction": 0.6,
                "min_gex_coverage": 0.5,
            },
        )

        self.assertEqual(verdict["verdict"], VERDICT_ENABLE_NOT_SUPPORTED)

    def test_abstain_dominates_when_rows_below_floor(self):
        report = {
            "status": "ok",
            "dataset": {"usable_rows": 10, "gex_flow_nonzero_coverage": 1.0},
            "metrics": {"training_status": "ok"},
            "delta": {"rank_ic_mean": 0.50, "positive_rank_ic_delta_fraction": 1.0},
        }

        verdict = evaluate_enablement(
            report,
            thresholds={
                "min_rows": 500,
                "min_rank_ic_delta": 0.01,
                "min_stability_fraction": 0.6,
                "min_gex_coverage": 0.5,
            },
        )

        self.assertEqual(verdict["verdict"], VERDICT_ABSTAIN_INSUFFICIENT_DATA)
        self.assertIn("rows_below_floor", verdict["reasons"])


if __name__ == "__main__":
    unittest.main()
