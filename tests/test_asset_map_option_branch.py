from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_asset_map():
    return importlib.reload(importlib.import_module("engine.data.asset_map"))


class AssetMapOptionBranchTests(unittest.TestCase):
    def test_occ_contract_classifies_as_option_without_reclassifying_bare_roots(self) -> None:
        with patch.dict(os.environ, {"ASSET_CLASS_MAP_JSON": ""}, clear=False):
            asset_map = _reload_asset_map()

            self.assertEqual(asset_map.asset_class_for_symbol("SPY240920C00450000"), "OPTION")
            self.assertEqual(asset_map.asset_class_for_symbol("SPY"), "EQUITY")
            self.assertEqual(asset_map.asset_class_for_symbol("GC"), "COMMODITY")
            self.assertEqual(asset_map.asset_class_for_symbol("ZN"), "RATES")
            self.assertEqual(asset_map.asset_class_for_symbol("TLT"), "RATES")
            self.assertEqual(asset_map.asset_class_for_symbol("EURUSD"), "FX")
            self.assertEqual(asset_map.asset_class_for_symbol("NOTREAL"), "UNKNOWN")

    def test_asset_class_override_wins_over_option_heuristic(self) -> None:
        override = '{"SPY240920C00450000":"EQUITY"}'
        with patch.dict(os.environ, {"ASSET_CLASS_MAP_JSON": override}, clear=False):
            asset_map = _reload_asset_map()
            self.assertEqual(asset_map.asset_class_for_symbol("SPY240920C00450000"), "EQUITY")

        with patch.dict(os.environ, {"ASSET_CLASS_MAP_JSON": ""}, clear=False):
            _reload_asset_map()


if __name__ == "__main__":
    unittest.main()
