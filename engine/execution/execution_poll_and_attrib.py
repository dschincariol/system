"""
FILE: execution_poll_and_attrib.py

Execution subsystem module for `execution_poll_and_attrib`.
"""

"""
Poll broker fills + compute slippage metrics + pnl attribution snapshots.

Run every 60s (or 300s) in production.

Env:
  EXEC_POLL_LOOKBACK_S=3600
"""

import json
import logging
import os
import time

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.runtime_meta import meta_set
from engine.execution.execution_analytics_engine import build_execution_analytics
from engine.execution.execution_ledger import (
    compute_capital_efficiency_snapshot,
    compute_metrics_snapshot,
    compute_pnl_attribution_snapshot,
    init_execution_ledger,
    repair_execution_order_model_identity,
)
from engine.execution.execution_quality_supervisor import (
    refresh_execution_quality_supervisor,
)
from engine.execution.trade_attribution_ledger import (
    attribution_completeness_snapshot,
    suppression_opportunity_snapshot,
    upsert_from_latest_pnl_attribution_snapshot,
)
from engine.runtime.shadow_capital_allocator import (
    compute_and_persist_shadow_capital_scores,
)
from engine.runtime.storage import connect
from engine.strategy.champion_manager import recompute_model_rankings
from engine.strategy.model_marketplace import recompute_marketplace_scores
from engine.strategy.pnl_decomposition_engine import compute_pnl_decomposition_snapshot

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s [execution_poll_and_attrib] %(message)s",
)

POLL_LOOKBACK_S = int(os.environ.get("EXEC_POLL_LOOKBACK_S", "3600"))
LOG = logging.getLogger(__name__)
_WARNED_NONFATAL_KEYS: set[str] = set()
_ALPACA_BROKER_NAMES = {"alpaca"}
_IBKR_BROKER_NAMES = {"ibkr", "interactive_brokers", "interactivebrokers"}


def _warn_nonfatal(event: str, code: str, error: BaseException, *, warn_key: str | None = None, **extra: object) -> None:
    if warn_key and warn_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=event,
        code=code,
        message=event,
        error=error,
        level=logging.WARNING,
        component=__name__,
        extra=extra or None,
        persist=False,
    )
    if warn_key:
        _WARNED_NONFATAL_KEYS.add(warn_key)


def _configured_live_poll_brokers() -> set[str]:
    """Return live broker adapters that are actually reachable for this process."""

    try:
        from engine.execution.broker_router import effective_broker_chain

        chain = [str(item or "").strip().lower() for item in list(effective_broker_chain() or [])]
    except Exception as exc:
        _warn_nonfatal(
            "execution_poll_and_attrib_broker_chain_load_failed",
            "EXECUTION_POLL_AND_ATTRIB_BROKER_CHAIN_LOAD_FAILED",
            exc,
            warn_key="execution_poll_and_attrib_broker_chain_load_failed",
        )
        chain = []

    brokers: set[str] = set()
    for name in chain:
        if name in _ALPACA_BROKER_NAMES:
            brokers.add("alpaca")
        if name in _IBKR_BROKER_NAMES:
            brokers.add("ibkr")
    return brokers

# ------------------------------------------------------------
# Phase 2: Residual Hard Invariant (fail-closed)
# ------------------------------------------------------------
# Absolute $ guard (sum abs residual over latest snapshot)
RESIDUAL_ABS_PNL_MAX = float(os.environ.get("RESIDUAL_ABS_PNL_MAX", "50.0"))

# Ratio guard: sum(|residual|) / sum(|realized_pnl|) over latest snapshot
RESIDUAL_ABS_RATIO_MAX = float(os.environ.get("RESIDUAL_ABS_RATIO_MAX", "0.25"))

# If realized is near 0, use absolute guard only
RESIDUAL_REALIZED_EPS = float(os.environ.get("RESIDUAL_REALIZED_EPS", "1.0"))


def _residual_hard_invariant(*, snapshot_ts_ms: int) -> dict:
    # This is a fail-closed accounting invariant. If decomposition residuals are
    # too large, attribution output is not trustworthy enough to continue silently.
    resid_check = {"ok": True}
    snap_ts = int(snapshot_ts_ms or 0)
    if snap_ts <= 0:
        return resid_check

    con = connect(readonly=True)
    try:
        r = con.execute(
            """
            SELECT
              COUNT(1) AS n,
              SUM(ABS(COALESCE(residual_pnl,0))) AS sum_abs_resid,
              SUM(ABS(COALESCE(realized_pnl,0))) AS sum_abs_realized
            FROM pnl_decomposition
            WHERE ts_ms=?
            """,
            (int(snap_ts),),
        ).fetchone()

        n = int(r[0] or 0) if r else 0
        sum_abs_resid = float(r[1] or 0.0) if r else 0.0
        sum_abs_realized = float(r[2] or 0.0) if r else 0.0

        ratio = None
        if sum_abs_realized >= float(RESIDUAL_REALIZED_EPS):
            ratio = float(sum_abs_resid) / float(sum_abs_realized)

        resid_check = {
            "ok": True,
            "snapshot_ts_ms": int(snap_ts),
            "n": int(n),
            "sum_abs_residual": float(sum_abs_resid),
            "sum_abs_realized": float(sum_abs_realized),
            "ratio": (float(ratio) if ratio is not None else None),
            "abs_max": float(RESIDUAL_ABS_PNL_MAX),
            "ratio_max": float(RESIDUAL_ABS_RATIO_MAX),
            "realized_eps": float(RESIDUAL_REALIZED_EPS),
        }

        # Absolute guard always applies
        if float(sum_abs_resid) > float(RESIDUAL_ABS_PNL_MAX):
            resid_check["ok"] = False
            resid_check["failed"] = "abs_residual_exceeded"

        # Ratio guard applies only if realized sum is meaningful
        if ratio is not None and float(ratio) > float(RESIDUAL_ABS_RATIO_MAX):
            resid_check["ok"] = False
            resid_check["failed"] = "residual_ratio_exceeded"

        if not bool(resid_check["ok"]):
            raise RuntimeError(
                "RESIDUAL_INVARIANT_FAILED "
                f"failed={resid_check.get('failed')} "
                f"sum_abs_residual={sum_abs_resid:.6f} "
                f"sum_abs_realized={sum_abs_realized:.6f} "
                f"ratio={(ratio if ratio is not None else -1.0):.6f}"
            )

        return resid_check
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "execution_poll_and_attrib_residual_invariant_close_failed",
                "EXECUTION_POLL_AND_ATTRIB_RESIDUAL_INVARIANT_CLOSE_FAILED",
                exc,
                warn_key="execution_poll_and_attrib_residual_invariant_close_failed",
            )


def _orphan_pnl_invariant() -> dict:
    # Every pnl_attribution row must have a corresponding ledger attribution row.
    # If not, post-trade explainability has drifted out of sync.
    con = connect(readonly=True)
    try:
        orphan = con.execute(
            """
            SELECT COUNT(1)
            FROM pnl_attribution p
            LEFT JOIN trade_attribution_ledger t
              ON p.ts_ms = t.ts_ms
             AND p.source_alert_id = t.source_alert_id
             AND COALESCE(NULLIF(TRIM(p.model_id), ''), 'baseline') = COALESCE(NULLIF(TRIM(t.model_id), ''), 'baseline')
             AND p.symbol = t.symbol
            WHERE t.id IS NULL
            """
        ).fetchone()[0]
        orphan = int(orphan or 0)
        if orphan > 0:
            raise RuntimeError(f"ATTRIBUTION_INCOMPLETE orphan_rows={orphan}")
        return {"ok": True, "orphan_rows": 0}
    finally:
        try:
            con.close()
        except Exception as exc:
            _warn_nonfatal(
                "execution_poll_and_attrib_orphan_invariant_close_failed",
                "EXECUTION_POLL_AND_ATTRIB_ORPHAN_INVARIANT_CLOSE_FAILED",
                exc,
                warn_key="execution_poll_and_attrib_orphan_invariant_close_failed",
            )


def main() -> int:
    init_execution_ledger()

    after_ts_ms = int(time.time() * 1000) - int(POLL_LOOKBACK_S) * 1000
    live_poll_brokers = _configured_live_poll_brokers()

    # Poll fills from each broker adapter that exists.
    # These are best-effort adapters; absence of one broker should not block
    # analytics for another.
    if "alpaca" in live_poll_brokers:
        try:
            from engine.execution.broker_alpaca_rest import poll_and_log_fills

            poll_and_log_fills(after_ts_ms=after_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "execution_poll_and_attrib_alpaca_poll_failed",
                "EXECUTION_POLL_AND_ATTRIB_ALPACA_POLL_FAILED",
                exc,
                warn_key="execution_poll_and_attrib_alpaca_poll_failed",
                after_ts_ms=int(after_ts_ms),
            )

    if "ibkr" in live_poll_brokers:
        try:
            from engine.execution.broker_ibkr_gateway import poll_and_log_fills as ibkr_poll

            ibkr_poll(after_ts_ms=after_ts_ms)
        except Exception as exc:
            _warn_nonfatal(
                "execution_poll_and_attrib_ibkr_poll_failed",
                "EXECUTION_POLL_AND_ATTRIB_IBKR_POLL_FAILED",
                exc,
                warn_key="execution_poll_and_attrib_ibkr_poll_failed",
                after_ts_ms=int(after_ts_ms),
            )

    # Phase 2: manage open orders (cancel/replace) best-effort
    try:
        from engine.execution.execution_open_order_manager import manage_open_orders

        manage_open_orders()
    except Exception as exc:
        _warn_nonfatal(
            "execution_poll_and_attrib_manage_open_orders_failed",
            "EXECUTION_POLL_AND_ATTRIB_MANAGE_OPEN_ORDERS_FAILED",
            exc,
            warn_key="execution_poll_and_attrib_manage_open_orders_failed",
        )

    # Snapshots are computed in dependency order: raw fills -> metrics ->
    # pnl attribution -> capital efficiency -> analytics/quality.
    # Compute slippage + m2m metrics snapshot
    m = compute_metrics_snapshot(limit_orders=5000)

    # Compute pnl attribution snapshot (by signal/source_alert_id)
    a = compute_pnl_attribution_snapshot(lookback_orders=5000)

    # Capital efficiency snapshot (order + strategy aggregates)
    ce = compute_capital_efficiency_snapshot(limit_orders=5000)

    # Canonical execution analytics / fill-quality / strategy-attribution snapshot
    xa = build_execution_analytics(limit=5000)

    # Runtime execution quality supervisor
    try:
        xs = refresh_execution_quality_supervisor(lookback_n=500)
    except Exception as e:
        _warn_nonfatal(
            "execution_poll_and_attrib_quality_supervisor_failed",
            "EXECUTION_POLL_AND_ATTRIB_QUALITY_SUPERVISOR_FAILED",
            e,
        )
        xs = {"ok": False, "error": str(e)}

    out = {
        "ok": True,
        "metrics": m,
        "attribution": a,
        "capital_efficiency": ce,
        "execution_analytics": xa,
        "execution_supervisor": xs,
        "ts_ms": int(time.time() * 1000),
    }

    repair = repair_execution_order_model_identity(limit=5000)

    # Trade Attribution Ledger: enrich pnl_attribution with alerts + execution_policy_audit
    t = upsert_from_latest_pnl_attribution_snapshot()
    completeness = attribution_completeness_snapshot(limit=5000)

    # Marketplace scoring: realized pnl + unrealized pnl + fill-derived entry/exit/costs
    marketplace = recompute_marketplace_scores()

    try:
        shadow_capital = compute_and_persist_shadow_capital_scores()
    except Exception as e:
        _warn_nonfatal(
            "execution_poll_and_attrib_shadow_capital_failed",
            "EXECUTION_POLL_AND_ATTRIB_SHADOW_CAPITAL_FAILED",
            e,
        )
        shadow_capital = {"ok": False, "error": str(e)}

    try:
        competition = recompute_model_rankings()
    except Exception as e:
        _warn_nonfatal(
            "execution_poll_and_attrib_competition_failed",
            "EXECUTION_POLL_AND_ATTRIB_COMPETITION_FAILED",
            e,
        )
        competition = {"ok": False, "error": str(e)}

    # Phase 2: PnL decomposition (alpha vs costs vs sizing vs residual)
    d = compute_pnl_decomposition_snapshot()

    # Residual Hard Invariant (latest snapshot only; fail-closed)
    resid_check = _residual_hard_invariant(
        snapshot_ts_ms=int((d or {}).get("snapshot_ts_ms") or 0)
    )

    # Hard invariant: every pnl row must have attribution row
    orphan_check = _orphan_pnl_invariant()

    # Suppression opportunity (counterfactual; best-effort)
    s = suppression_opportunity_snapshot(lookback_ms=24 * 60 * 60 * 1000)

    out = {
        "ok": True,
        "metrics": m,
        "attribution": a,
        "capital_efficiency": ce,
        "execution_analytics": xa,
        "execution_supervisor": xs,
        "execution_order_model_identity_repair": repair,
        "trade_attrib": t,
        "attribution_completeness": completeness,
        "marketplace": marketplace,
        "shadow_capital": shadow_capital,
        "competition": competition,
        "pnl_decomp": d,
        "residual_invariant": resid_check,
        "orphan_invariant": orphan_check,
        "suppression_opportunity": s,
        "ts_ms": int(time.time() * 1000),
    }
    try:
        meta_set(
            "attribution_completeness",
            json.dumps(completeness, separators=(",", ":"), sort_keys=True),
        )
        meta_set(
            "execution_order_model_identity_repair",
            json.dumps(repair, separators=(",", ":"), sort_keys=True),
        )
        meta_set(
            "execution_poll_and_attrib_last",
            json.dumps(
                {
                    "ok": True,
                    "ts_ms": int(out.get("ts_ms") or 0),
                    "trade_attrib_ok": bool((t or {}).get("ok")),
                    "competition_ok": bool((competition or {}).get("ok")),
                    "marketplace_ok": bool((marketplace or {}).get("ok")),
                },
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
    except Exception as exc:
        _warn_nonfatal(
            "execution_poll_and_attrib_meta_set_failed",
            "EXECUTION_POLL_AND_ATTRIB_META_SET_FAILED",
            exc,
            warn_key="execution_poll_and_attrib_meta_set_failed",
        )
    print(json.dumps(out, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
