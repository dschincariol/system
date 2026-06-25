from __future__ import annotations

import importlib
import inspect
import sys
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_asset_map():
    import engine.data.asset_map as asset_map

    return importlib.reload(asset_map)


class FuturesAssetClassDerivationTests(unittest.TestCase):
    def test_explicit_futures_forms_classify_as_futures(self) -> None:
        import os

        os.environ.pop("ASSET_CLASS_MAP_JSON", None)
        asset_map = _reload_asset_map()

        for symbol in ("ES.c.0", "ESZ26", "CL.c.0"):
            with self.subTest(symbol=symbol):
                result = asset_map.asset_class_for_symbol(symbol)
                self.assertIsInstance(result, str)
                self.assertEqual(result, "FUTURES")

    def test_non_futures_classifications_are_unchanged(self) -> None:
        import os

        os.environ.pop("ASSET_CLASS_MAP_JSON", None)
        asset_map = _reload_asset_map()

        expected = {
            "SPY": "EQUITY",
            "BTC": "CRYPTO",
            "GC": "COMMODITY",
            "ZN": "RATES",
            "EURUSD": "FX",
            "ZZZ": "UNKNOWN",
        }
        for symbol, asset_class in expected.items():
            with self.subTest(symbol=symbol):
                result = asset_map.asset_class_for_symbol(symbol)
                self.assertIsInstance(result, str)
                self.assertEqual(result, asset_class)

    def test_asset_class_override_still_wins_over_futures_branch(self) -> None:
        import os

        os.environ["ASSET_CLASS_MAP_JSON"] = '{"ES.c.0":"CUSTOM"}'
        asset_map = _reload_asset_map()

        self.assertEqual(asset_map.asset_class_for_symbol("ES.c.0"), "CUSTOM")

    def test_asset_class_signature_is_unchanged(self) -> None:
        import os

        os.environ.pop("ASSET_CLASS_MAP_JSON", None)
        asset_map = _reload_asset_map()

        signature = inspect.signature(asset_map.asset_class_for_symbol)
        self.assertEqual(list(signature.parameters), ["symbol"])
        self.assertIsInstance(asset_map.asset_class_for_symbol("ES.c.0"), str)


if __name__ == "__main__":
    unittest.main()
