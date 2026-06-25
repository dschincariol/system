import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.strategy import feature_registry
from tools import options_feature_ablation as ablation


class OptionsFeatureAblationFeatureSetTests(unittest.TestCase):
    def test_with_set_is_without_plus_registry_gex_flow_ids(self):
        feature_sets = ablation.resolve_feature_sets(["core.alpha", "core.beta"])

        self.assertEqual(
            feature_sets["gex_flow"],
            [
                "options_symbol.gex_norm_z",
                "options_symbol.gex_sign",
                "options_symbol.opt_flow_imbalance_z",
            ],
        )
        self.assertEqual(
            feature_sets["with"],
            feature_sets["without"] + feature_sets["gex_flow"],
        )

    def test_tool_uses_registry_private_split_not_literals(self):
        feature_sets = ablation.resolve_feature_sets([])

        self.assertIs(ablation._BASE_OPTIONS_FEATURE_IDS, feature_registry._BASE_OPTIONS_FEATURE_IDS)
        self.assertIs(ablation._OPTIONS_GEX_FLOW_FEATURE_IDS, feature_registry._OPTIONS_GEX_FLOW_FEATURE_IDS)
        self.assertEqual(feature_sets["base_options"], list(feature_registry._BASE_OPTIONS_FEATURE_IDS))
        self.assertEqual(feature_sets["gex_flow"], list(feature_registry._OPTIONS_GEX_FLOW_FEATURE_IDS))


if __name__ == "__main__":
    unittest.main()
