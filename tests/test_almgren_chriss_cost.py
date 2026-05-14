import math
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from engine.execution.cost_models import almgren_chriss
from engine.execution.cost_models.almgren_chriss import AlmgrenChrissCost


def test_cost_bps_matches_hand_computed_reference_value():
    model = AlmgrenChrissCost()

    value = model.cost_bps(
        notional=1_000_000.0,
        adv=20_000_000.0,
        sigma_daily=0.02,
        participation=0.10,
        half_spread_bps=1.5,
    )

    expected = 1.5 + (0.142 * 0.02 * math.sqrt(0.05)) + (0.314 * 0.05)
    assert abs(value - expected) <= 1e-6


def test_cost_bps_is_monotonic_in_notional_and_participation():
    model = AlmgrenChrissCost()
    base = dict(adv=20_000_000.0, sigma_daily=0.02, half_spread_bps=1.0)

    small = model.cost_bps(notional=100_000.0, participation=0.10, **base)
    large = model.cost_bps(notional=2_000_000.0, participation=0.10, **base)
    slow = model.cost_bps(notional=1_000_000.0, participation=0.05, **base)
    fast = model.cost_bps(notional=1_000_000.0, participation=0.25, **base)

    assert large > small
    assert fast > slow


def test_cost_bps_participation_is_monotonic_and_capped(monkeypatch):
    model = AlmgrenChrissCost()
    base = dict(
        notional=1_000_000.0,
        adv=20_000_000.0,
        sigma_daily=0.02,
        half_spread_bps=1.0,
    )

    values = [model.cost_bps(participation=p, **base) for p in (0.0, 0.10, 0.25, 0.50, 0.75, 1.0)]
    assert all(cur >= prev for prev, cur in zip(values, values[1:]))

    almgren_chriss._PARTICIPATION_CLAMP_WARNED = False
    warning_calls = []
    monkeypatch.setattr(almgren_chriss.LOGGER, "warning", lambda *args, **kwargs: warning_calls.append(args))
    capped = model.cost_bps(participation=1.5, **base)
    capped_again = model.cost_bps(participation=2.0, **base)

    assert abs(capped - values[-1]) <= 1e-12
    assert abs(capped_again - values[-1]) <= 1e-12
    assert warning_calls == [
        ("ALMGREN_CHRISS_PARTICIPATION_CLAMPED: participation=%s clamped=%s", 1.5, 1.0)
    ]
    components = model.components_bps(participation=1.5, **base)
    assert components["participation"] == 1.0


def test_asset_class_override_uses_configured_coefficients():
    model = AlmgrenChrissCost(asset_class_coefficients={"FUTURES": (0.2, 0.1)})

    value = model.cost_bps(
        notional=500_000.0,
        adv=10_000_000.0,
        sigma_daily=0.015,
        participation=0.10,
        half_spread_bps=0.5,
        asset_class="futures",
    )

    expected = 0.5 + (0.2 * 0.015 * math.sqrt(0.05)) + (0.1 * 0.05)
    assert abs(value - expected) <= 1e-6
