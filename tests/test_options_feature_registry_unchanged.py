import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class OptionsFeatureRegistryUnchangedTests(unittest.TestCase):
    def test_use_options_features_default_stays_off(self):
        script = textwrap.dedent(
            """
            import importlib
            import os
            os.environ.pop("USE_OPTIONS_FEATURES", None)
            import engine.strategy.feature_registry as fr
            fr = importlib.reload(fr)
            assert os.environ.get("USE_OPTIONS_FEATURES", "0") == "0"
            assert fr.USE_OPTIONS_FEATURES is False
            assert fr.OPTIONS_FEATURE_IDS == list(fr._BASE_OPTIONS_FEATURE_IDS)
            assert len(fr._BASE_OPTIONS_FEATURE_IDS) == 8
            assert all(fid not in fr.OPTIONS_FEATURE_IDS for fid in fr._OPTIONS_GEX_FLOW_FEATURE_IDS)
            assert fr.FEATURE_GROUPS["options"] == fr.FEATURE_GROUPS["options_symbol"]
            """
        )

        proc = subprocess.run(
            [sys.executable, "-c", script],
            cwd=str(REPO_ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)


if __name__ == "__main__":
    unittest.main()
