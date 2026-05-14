from __future__ import annotations

import pytest

from engine.causal.dag import CausalDAG


def test_causal_dag_round_trips_json() -> None:
    dag = CausalDAG(
        name="macro_liquidity_to_returns",
        nodes=("liquidity", "vol", "forward_ret"),
        edges=(("liquidity", "forward_ret"), ("vol", "liquidity"), ("vol", "forward_ret")),
        treatment="liquidity",
        outcome="forward_ret",
        confounders=("vol",),
    )

    restored = CausalDAG.from_json(dag.to_json())

    assert restored == dag
    assert restored.to_dict()["edges"] == [["liquidity", "forward_ret"], ["vol", "liquidity"], ["vol", "forward_ret"]]
    assert '"liquidity" -> "forward_ret"' in restored.to_dot()


def test_causal_dag_rejects_cycles() -> None:
    with pytest.raises(ValueError, match="cycle"):
        CausalDAG(
            name="bad",
            nodes=("x", "y"),
            edges=(("x", "y"), ("y", "x")),
            treatment="x",
            outcome="y",
        )
