import math

import pytest
from hypothesis import given, settings, strategies as st

import engine.risk.monte_carlo_risk_engine as mc
import engine.risk.portfolio_risk_engine as pre


pytestmark = pytest.mark.safety_critical

EPS = 1e-9
CAP_EPS = 1e-6
PROPERTY_SETTINGS = settings(max_examples=200, deadline=None)

FINITE_PNL_LISTS = st.lists(
    st.floats(
        min_value=-1e6,
        max_value=1e6,
        allow_nan=False,
        allow_infinity=False,
    ),
    min_size=1,
    max_size=200,
)

WEIGHT_LISTS = st.lists(
    st.floats(
        min_value=-5.0,
        max_value=5.0,
        allow_nan=False,
        allow_infinity=False,
    ),
    min_size=1,
    max_size=12,
)


def _desired_from_weights(weights: list[float]) -> dict[str, dict[str, float]]:
    return {f"S{i}": {"weight": float(w)} for i, w in enumerate(weights)}


def _raw_weights(rows: dict[str, dict[str, float]]) -> dict[str, float]:
    return {sym: float((row or {}).get("weight", 0.0) or 0.0) for sym, row in rows.items()}


def _signed_weights(rows: dict[str, dict[str, float]]) -> dict[str, float]:
    return {sym: pre._signed_weight(row) for sym, row in rows.items()}


def _sign(value: float) -> int:
    if math.isclose(float(value), 0.0, abs_tol=1e-12):
        return 0
    return 1 if float(value) > 0.0 else -1


def test_monte_carlo_empty_tail_helpers_return_zero() -> None:
    assert mc._pct([], 0.05) == 0.0
    assert mc._cvar([], 0.05) == 0.0
    assert mc._upper_cvar([], 0.95) == 0.0


@PROPERTY_SETTINGS
@given(xs=FINITE_PNL_LISTS)
def test_monte_carlo_lower_tail_cvar_is_no_greater_than_var(xs: list[float]) -> None:
    for q in (0.05, 0.01):
        var = mc._pct(xs, q)
        cvar = mc._cvar(xs, q)

        assert cvar <= var + EPS
        assert min(xs) - EPS <= cvar <= var + EPS


@PROPERTY_SETTINGS
@given(xs=FINITE_PNL_LISTS)
def test_monte_carlo_upper_tail_cvar_is_no_less_than_percentile(xs: list[float]) -> None:
    for q in (0.95, 0.99):
        pct = mc._pct(xs, q)
        cvar = mc._upper_cvar(xs, q)

        assert cvar >= pct - EPS
        assert pct - EPS <= cvar <= max(xs) + EPS


@PROPERTY_SETTINGS
@given(weights=WEIGHT_LISTS)
def test_portfolio_caps_are_sound_sign_preserving_and_idempotent(weights: list[float]) -> None:
    desired = _desired_from_weights(weights)
    original_weights = _raw_weights(desired)
    original_signed_weights = _signed_weights(desired)
    pre_gross = pre._gross(desired)
    pre_net = pre._net(desired)

    out = pre._apply_portfolio_caps(dict(desired), info={})

    if float(pre.MAX_GROSS) > 0.0:
        assert pre._gross(out) <= float(pre.MAX_GROSS) + CAP_EPS
    if float(pre.MAX_NET) > 0.0:
        assert abs(pre._net(out)) <= float(pre.MAX_NET) + CAP_EPS

    for sym, original_weight in original_weights.items():
        assert sym in out
        assert _sign(pre._signed_weight(out[sym])) == _sign(original_signed_weights[sym])

    already_within_gross = float(pre.MAX_GROSS) <= 0.0 or pre_gross <= float(pre.MAX_GROSS) + EPS
    already_within_net = float(pre.MAX_NET) <= 0.0 or abs(pre_net) <= float(pre.MAX_NET) + EPS
    if already_within_gross and already_within_net:
        for sym, original_weight in original_weights.items():
            assert float((out[sym] or {}).get("weight", 0.0) or 0.0) == pytest.approx(
                original_weight,
                abs=1e-12,
            )


@PROPERTY_SETTINGS
@given(
    weights=WEIGHT_LISTS,
    scale=st.floats(
        min_value=1.0,
        max_value=10.0,
        allow_nan=False,
        allow_infinity=False,
    ),
)
def test_portfolio_cap_gross_is_monotone_after_a_cap_binds(
    weights: list[float],
    scale: float,
) -> None:
    desired = _desired_from_weights(weights)
    scaled_desired = _desired_from_weights([float(scale) * float(w) for w in weights])

    pre_gross = pre._gross(desired)
    pre_net = pre._net(desired)
    cap_binds = (
        float(pre.MAX_GROSS) > 0.0
        and pre_gross > float(pre.MAX_GROSS) + EPS
    ) or (
        float(pre.MAX_NET) > 0.0
        and abs(pre_net) > float(pre.MAX_NET) + EPS
    )
    if not cap_binds:
        return

    clamped = pre._apply_portfolio_caps(dict(desired), info={})
    scaled_clamped = pre._apply_portfolio_caps(dict(scaled_desired), info={})

    assert pre._gross(scaled_clamped) <= pre._gross(clamped) + CAP_EPS


# The per-symbol vol-cap clamp is intentionally not property-tested here:
# _apply_symbol_vol_caps resolves volatility through _symbol_vol_input(con, ...),
# which depends on DB-backed forecast/trailing-vol paths. Existing example tests
# cover that integration path; this file keeps HG-6 invariants pure and socket-free.
