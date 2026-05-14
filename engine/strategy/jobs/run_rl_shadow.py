"""Run one shadow-evaluation pass for the portfolio RL policy."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Sequence

from engine.rl.shadow_runner import RLShadowRunner, ShadowRunnerConfig


def _live_portfolio_weights() -> Dict[str, float]:
    try:
        from engine.strategy.portfolio import get_portfolio_snapshot

        snap = get_portfolio_snapshot(limit_orders=1) or {}
    except Exception:
        return {}
    out: Dict[str, float] = {}
    for row in list((snap or {}).get("state") or []):
        sym = str((row or {}).get("symbol") or "").upper().strip()
        if not sym:
            continue
        side = str((row or {}).get("side") or "FLAT").upper()
        try:
            weight = float((row or {}).get("weight", 0.0) or 0.0)
        except Exception:
            weight = 0.0
        if side == "SHORT":
            weight = -abs(weight)
        elif side == "FLAT":
            weight = 0.0
        out[sym] = float(weight)
    return out


def _csv(value: str, default: Sequence[str]) -> list[str]:
    parts = [p.strip().upper() for p in str(value or "").split(",") if p.strip()]
    return parts or [str(x).upper() for x in default]


def main(argv: Sequence[str] | None = None) -> Dict[str, Any]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--algo", default=os.environ.get("RL_PORTFOLIO_ALGO", "ppo"), choices=["ppo", "sac"])
    parser.add_argument("--symbols", default=os.environ.get("RL_PORTFOLIO_SYMBOLS", "SPY,AAPL,MSFT"))
    parser.add_argument("--model-root", default=os.environ.get("RL_PORTFOLIO_MODEL_ROOT", "models/rl"))
    parser.add_argument("--checkpoint", default=os.environ.get("RL_PORTFOLIO_CHECKPOINT", ""))
    args = parser.parse_args(list(argv) if argv is not None else None)

    config = ShadowRunnerConfig(
        universe=_csv(args.symbols, ["SPY", "AAPL", "MSFT"]),
        algo=str(args.algo),
        model_root=str(args.model_root),
        checkpoint_path=(str(args.checkpoint) if str(args.checkpoint or "").strip() else None),
        max_w=float(os.environ.get("RL_PORTFOLIO_MAX_W", "0.35")),
        leverage_cap=float(os.environ.get("RL_PORTFOLIO_LEVERAGE_CAP", "1.0")),
        seed=int(os.environ.get("RL_PORTFOLIO_SEED", "7")),
        live_decision_fn=_live_portfolio_weights,
    )
    result = RLShadowRunner(config).run_once()
    print(json.dumps(result, indent=2, sort_keys=True))
    return result


if __name__ == "__main__":
    main()
