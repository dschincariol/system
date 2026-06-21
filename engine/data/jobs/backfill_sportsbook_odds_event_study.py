"""Run research-only sportsbook odds event studies after realistic costs."""

from __future__ import annotations

import json
import os
import time

from engine.data.sportsbook_odds import (
    evaluate_sportsbook_odds_go_gate,
    parse_list,
    run_sportsbook_odds_event_study,
    run_sportsbook_odds_promotion_research,
)
from engine.runtime.storage import connect, init_db


JOB_NAME = "backfill_sportsbook_odds_event_study"


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)) or default)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, str(default)) or default)
    except Exception:
        return float(default)


def _symbols() -> list[str]:
    return [symbol.upper() for symbol in parse_list(os.environ.get("SPORTSBOOK_ODDS_EVENT_STUDY_SYMBOLS"))]


def run_backfill(
    *,
    start_ts_ms: int,
    end_ts_ms: int,
    horizon_s: int,
    latency_ms: int,
    fee_bps: float,
    slippage_bps: float,
    symbols: list[str],
) -> dict:
    con = connect(readonly=False)
    try:
        event_study = run_sportsbook_odds_event_study(
            con,
            symbols=list(symbols or []),
            start_ts_ms=int(start_ts_ms),
            end_ts_ms=int(end_ts_ms),
            horizon_s=int(horizon_s),
            latency_ms=int(latency_ms),
            fee_bps=float(fee_bps),
            slippage_bps=float(slippage_bps),
            persist=True,
        )
        promotion_evidence = run_sportsbook_odds_promotion_research(
            con,
            symbols=list(symbols or []),
            start_ts_ms=int(start_ts_ms),
            end_ts_ms=int(end_ts_ms),
            horizon_s=int(horizon_s),
            latency_ms=int(latency_ms),
            fee_bps=float(fee_bps),
            slippage_bps=float(slippage_bps),
            persist=True,
        )
        go, gate = evaluate_sportsbook_odds_go_gate(
            con,
            feature_ids=[
                "sports_odds_sector_v1.no_vig_probability_level",
                "sports_odds_sector_v1.no_vig_probability_move",
            ],
            symbols=list(symbols or []),
        )
        con.commit()
        return {
            "ok": True,
            "research_only": True,
            "direct_trading_authority": False,
            "event_study": event_study,
            "promotion_evidence": promotion_evidence,
            "go_for_production_features": bool(go),
            "promotion_gate": gate,
        }
    finally:
        con.close()


def main() -> None:
    init_db()
    now_ms = int(time.time() * 1000)
    result = run_backfill(
        start_ts_ms=_env_int("SPORTSBOOK_ODDS_EVENT_STUDY_START_TS_MS", now_ms - 180 * 24 * 60 * 60 * 1000),
        end_ts_ms=_env_int("SPORTSBOOK_ODDS_EVENT_STUDY_END_TS_MS", now_ms),
        horizon_s=_env_int("SPORTSBOOK_ODDS_EVENT_STUDY_HORIZON_S", 86_400),
        latency_ms=_env_int("SPORTSBOOK_ODDS_EVENT_STUDY_LATENCY_MS", 15 * 60 * 1000),
        fee_bps=_env_float("SPORTSBOOK_ODDS_EVENT_STUDY_FEE_BPS", 1.0),
        slippage_bps=_env_float("SPORTSBOOK_ODDS_EVENT_STUDY_SLIPPAGE_BPS", 5.0),
        symbols=_symbols(),
    )
    print(json.dumps(result, separators=(",", ":"), sort_keys=True))


if __name__ == "__main__":
    main()
