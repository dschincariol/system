from __future__ import annotations

import json

from engine.risk import monte_carlo_risk_engine as mc


class _ZeroRandom:
    def gauss(self, _mu: float, _sigma: float) -> float:
        return 0.0


class _Conn:
    def close(self) -> None:
        return None


def test_simulation_builds_fan_rows_and_distribution_buckets(monkeypatch) -> None:
    monkeypatch.setattr(mc.random, "Random", lambda: _ZeroRandom())
    monkeypatch.setattr(mc, "MC_SIMULATIONS", 4)
    monkeypatch.setattr(mc, "MC_HORIZON", 3)

    pnl, drawdowns, fan = mc._simulate([1.0], [0.0], [0.01], [[1.0]])
    distribution = mc._distribution_buckets(pnl)

    assert pnl == [0.03, 0.03, 0.03, 0.03]
    assert drawdowns == [0.0, 0.0, 0.0, 0.0]
    assert fan == [
        {"step": 1, "p05": 0.01, "p50": 0.01, "p95": 0.01},
        {"step": 2, "p05": 0.02, "p50": 0.02, "p95": 0.02},
        {"step": 3, "p05": 0.03, "p50": 0.03, "p95": 0.03},
    ]
    assert distribution == [
        {
            "bucket": "3.00%",
            "lower": 0.03,
            "upper": 0.03,
            "value": 0.03,
            "count": 4,
            "probability": 1.0,
        }
    ]


def test_worker_persists_monte_carlo_chart_artifacts(monkeypatch) -> None:
    states: dict[str, str] = {}

    monkeypatch.setattr(mc.random, "Random", lambda: _ZeroRandom())
    monkeypatch.setattr(mc, "MC_SIMULATIONS", 4)
    monkeypatch.setattr(mc, "MC_HORIZON", 3)
    monkeypatch.setattr(mc, "connect", lambda: _Conn())
    monkeypatch.setattr(mc, "_now_ms", lambda: 123456)
    monkeypatch.setattr(
        mc,
        "_build_inputs",
        lambda _con, _desired: (
            ["SPY"],
            [1.0],
            [0.0],
            [0.01],
            [[1.0]],
            {"SPY": {"source": "unit"}},
        ),
    )
    monkeypatch.setattr(mc, "set_state", lambda key, value: states.__setitem__(str(key), str(value)))

    mc._worker({"SPY": {"weight": 1.0}})

    payload = json.loads(states["monte_carlo_risk_info"])
    assert payload["ready"] is True
    assert payload["status"] == "ok"
    assert payload["fan"][0] == {"step": 1, "p05": 0.01, "p50": 0.01, "p95": 0.01}
    assert sum(int(row["count"]) for row in payload["distribution"]) == 4
    assert states["monte_carlo_risk_pending"] == "0"
