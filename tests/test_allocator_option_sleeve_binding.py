from __future__ import annotations

import importlib
import os
from pathlib import Path
import sys
import unittest
from unittest import mock


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class AllocatorOptionSleeveBindingTest(unittest.TestCase):
    ENV_KEYS = ("HIER_ALLOC_BIND_ASSET_CLASS_SLEEVE", "STRATEGY_SLEEVE_MAP_JSON")

    def setUp(self) -> None:
        self.env_backup = {key: os.environ.get(key) for key in self.ENV_KEYS}

    def tearDown(self) -> None:
        for key, value in self.env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _reload_allocator(self, **env: str):
        for key in self.ENV_KEYS:
            os.environ.pop(key, None)
        for key, value in env.items():
            os.environ[key] = str(value)

        import engine.runtime.hierarchical_allocator as hierarchical_allocator

        return importlib.reload(hierarchical_allocator)

    def test_option_symbol_binds_to_options_sleeve(self) -> None:
        allocator = self._reload_allocator()

        with mock.patch.object(allocator, "asset_class_for_symbol", return_value="OPTION") as lookup:
            sleeve = allocator._strategy_to_sleeve(
                None,
                "option_strategy",
                {},
                {"option_strategy": {"symbol": "SPY270115C00500000"}},
            )

        self.assertEqual(sleeve, "options")
        lookup.assert_called_with("SPY270115C00500000")

    def test_explicit_and_registry_sleeve_mappings_win(self) -> None:
        allocator = self._reload_allocator()

        with mock.patch.object(allocator, "asset_class_for_symbol", return_value="OPTION"):
            explicit = allocator._strategy_to_sleeve(
                None,
                "option_strategy",
                {"option_strategy": "equities"},
                {"option_strategy": {"symbol": "SPY270115C00500000"}},
            )
            registry = allocator._strategy_to_sleeve(
                None,
                "option_strategy",
                {},
                {"option_strategy": {"sleeve": "bespoke", "symbol": "SPY270115C00500000"}},
            )

        self.assertEqual(explicit, "equities")
        self.assertEqual(registry, "bespoke")

    def test_unmapped_class_and_disabled_binding_return_default(self) -> None:
        allocator = self._reload_allocator()
        with mock.patch.object(allocator, "asset_class_for_symbol", return_value="FX"):
            unmapped = allocator._strategy_to_sleeve(
                None,
                "fx_strategy",
                {},
                {"fx_strategy": {"symbol": "EURUSD"}},
            )

        disabled = self._reload_allocator(HIER_ALLOC_BIND_ASSET_CLASS_SLEEVE="0")
        with mock.patch.object(disabled, "asset_class_for_symbol", return_value="OPTION"):
            disabled_sleeve = disabled._strategy_to_sleeve(
                None,
                "option_strategy",
                {},
                {"option_strategy": {"symbol": "SPY270115C00500000"}},
            )

        self.assertEqual(unmapped, "default")
        self.assertEqual(disabled_sleeve, "default")


if __name__ == "__main__":
    unittest.main()
