from __future__ import annotations

from typing import Any


def passing_deconfounded_payload(n_obs: int, *, effect: float = 0.20) -> dict[str, Any]:
    n = max(8, int(n_obs))
    signal: list[float] = []
    outcome: list[float] = []
    controls: list[dict[str, Any]] = []
    sectors = ["tech", "financials", "healthcare"]
    regimes = ["risk_on", "risk_off", "neutral"]
    for idx in range(n):
        beta = float(((idx * 3) % 11) - 5) / 10.0
        sector = sectors[idx % len(sectors)]
        size = 9.5 + float(idx % 7) * 0.2
        volatility = 0.10 + float((idx * 2) % 9) * 0.01
        liquidity = 1_000_000.0 + float((idx * 5) % 13) * 10_000.0
        regime = regimes[(idx // 3) % len(regimes)]
        exposure = (0.15 * beta) + (0.02 if sector == "tech" else -0.01)
        residual_signal = float(((idx * 7) % 17) - 8) / 8.0
        candidate_signal = residual_signal + (0.20 * beta) + (0.05 * exposure)
        realized = (float(effect) * residual_signal) + (0.03 * beta) + (0.01 * exposure)
        signal.append(float(candidate_signal))
        outcome.append(float(realized))
        controls.append(
            {
                "beta": beta,
                "sector": sector,
                "size": size,
                "volatility": volatility,
                "liquidity": liquidity,
                "regime": regime,
                "existing_model_exposure": exposure,
            }
        )
    bucket = max(3, n // 4)
    return {
        "candidate_signal": signal,
        "outcome": outcome,
        "controls": controls,
        "stability_labels": [f"era_{idx // bucket}" for idx in range(n)],
    }


def confounded_deconfounded_payload(n_obs: int) -> dict[str, Any]:
    n = max(8, int(n_obs))
    signal: list[float] = []
    outcome: list[float] = []
    controls: list[dict[str, Any]] = []
    sectors = ["tech", "financials"]
    regimes = ["risk_on", "risk_off"]
    for idx in range(n):
        beta = float(((idx * 5) % 13) - 6) / 6.0
        sector = sectors[idx % len(sectors)]
        size = 10.0 + float(idx % 5)
        volatility = 0.12 + float(idx % 4) * 0.02
        liquidity = 900_000.0 + float(idx % 6) * 25_000.0
        regime = regimes[(idx // 4) % len(regimes)]
        exposure = (0.6 * beta) + (0.2 if sector == "tech" else -0.2)
        candidate_signal = beta + exposure
        realized = (0.08 * beta) + (0.04 * exposure)
        signal.append(float(candidate_signal))
        outcome.append(float(realized))
        controls.append(
            {
                "beta": beta,
                "sector": sector,
                "size": size,
                "volatility": volatility,
                "liquidity": liquidity,
                "regime": regime,
                "existing_model_exposure": exposure,
            }
        )
    return {
        "candidate_signal": signal,
        "outcome": outcome,
        "controls": controls,
        "stability_labels": [f"era_{idx // max(4, n // 3)}" for idx in range(n)],
    }
