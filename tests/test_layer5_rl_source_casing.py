"""Layer 5 negative test: RL source-marker casing must not bypass the
broker router gate.

The broker router rejects any order whose `source` (or fallback
`order_source`) starts with `rl.` or `rl_`. This sweep confirms the
case-insensitive guard catches every plausible variant — uppercase,
mixed-case, padded with whitespace, and with trailing detail.

If this test fails, an attacker (or a buggy caller) can put a
shadow-only RL policy's order through the live broker by manipulating
casing alone. P0.
"""

from __future__ import annotations

import pytest

# Casing variants and shapes that must all be rejected.
_RL_SOURCES = [
    "rl.",
    "rl_",
    "RL.",
    "RL_",
    "Rl.",
    "rL.",
    "rl.portfolio",
    "RL.portfolio",
    "rl_portfolio",
    "RL_PORTFOLIO",
    "rl.shadow.v3",
    "rl_shadow_v3",
    " rl. ",  # leading whitespace, lower
    "  RL_  ",  # padded uppercase
    "Rl.live",
    "rL_live",
]

# Negative controls — these must NOT be blocked by the RL guard.
_NON_RL_SOURCES = [
    "supervised",
    "operator",
    "rules_engine",
    "rule.engine",
    "earl_grey",  # contains 'rl' but doesn't start with rl. or rl_
    "real_time",
    "RL",  # no separator
    "rlbroker",  # no separator
]


@pytest.mark.parametrize("source", _RL_SOURCES)
def test_router_rl_guardrail_rejects_casing_variant(source: str) -> None:
    from engine.execution import broker_router

    orders = [{"symbol": "TEST", "qty": 1, "side": "buy", "source": source}]
    result = broker_router._rl_source_block(orders)
    assert result is not None, (
        f"order with source={source!r} not blocked; expected rejection"
    )
    # Reason payload should mention the rl-block (sanity check).
    blob = repr(result).lower()
    assert "rl" in blob, f"reason payload missing rl marker: {result!r}"


@pytest.mark.parametrize("source", _RL_SOURCES)
def test_router_rl_guardrail_rejects_via_order_source_fallback(source: str) -> None:
    """The same casing sweep, but the marker is set in `order_source`
    (the fallback field) rather than `source`. The guard must honour
    both fields with identical semantics."""
    from engine.execution import broker_router

    orders = [{"symbol": "TEST", "qty": 1, "side": "buy", "order_source": source}]
    result = broker_router._rl_source_block(orders)
    assert result is not None, (
        f"order with order_source={source!r} not blocked"
    )


@pytest.mark.parametrize("source", _NON_RL_SOURCES)
def test_router_rl_guardrail_does_not_block_non_rl_sources(source: str) -> None:
    """Negative control. These sources must pass through unblocked so
    we know the guard isn't pattern-broad enough to false-positive
    legitimate sources."""
    from engine.execution import broker_router

    orders = [{"symbol": "TEST", "qty": 1, "side": "buy", "source": source}]
    result = broker_router._rl_source_block(orders)
    assert result is None, (
        f"non-RL source {source!r} was blocked; guard is over-broad: {result!r}"
    )


def test_router_rl_guardrail_returns_none_for_empty_input() -> None:
    """No orders → no block. Sanity check."""
    from engine.execution import broker_router

    assert broker_router._rl_source_block(None) is None
    assert broker_router._rl_source_block([]) is None
