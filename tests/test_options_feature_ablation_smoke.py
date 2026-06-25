import json
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools import options_feature_ablation as ablation


def _lightgbm_available() -> bool:
    try:
        import lightgbm  # noqa: F401
    except Exception:
        return False
    return True


class OptionsFeatureAblationSmokeTests(unittest.TestCase):
    def test_synthetic_harness_writes_structured_report(self):
        if not _lightgbm_available():
            self.skipTest("LightGBM unavailable; harness abstains in runtime without fabricating a pass")
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = Path(tmp) / "report"
            code = ablation.main(
                [
                    "--synthetic",
                    "--synthetic-rows",
                    "80",
                    "--n-splits",
                    "4",
                    "--n-test-splits",
                    "1",
                    "--min-rows",
                    "40",
                    "--min-gex-coverage",
                    "0.1",
                    "--min-rank-ic-delta",
                    "0.0",
                    "--min-stability-fraction",
                    "0.0",
                    "--out",
                    str(out_dir),
                ]
            )

            self.assertEqual(code, 0)
            json_path = out_dir / "options_feature_ablation_report.json"
            text_path = out_dir / "options_feature_ablation_report.txt"
            self.assertTrue(json_path.exists())
            self.assertTrue(text_path.exists())
            report = json.loads(json_path.read_text(encoding="utf-8"))
            self.assertIn(report["verdict"], ablation.VALID_VERDICTS)
            self.assertIn("dataset", report)
            self.assertIn("feature_sets", report)
            self.assertIn("metrics", report)
            self.assertIn("delta", report)


if __name__ == "__main__":
    unittest.main()
