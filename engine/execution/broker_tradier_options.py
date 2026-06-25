"""Tradier live options order adapter.

This adapter handles single-leg listed option orders only. Tradier's order API
uses form-encoded fields for submission; the in-process representation remains
a plain dict so router tests and audit logs can inspect the exact payload before
any HTTP call.
"""

from __future__ import annotations

import json
import math
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Mapping, Optional

from engine.data._credentials import get_data_credential
from engine.data.options_instrument import parse_option_symbol
from engine.execution.options_readiness import is_options_order, live_options_order_block, option_order_metadata
from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.live_execution_control import disabled_live_execution_gate, live_execution_disabled, prelive_reconcile_policy_gate
from engine.runtime.logging import get_logger

LOG = get_logger("execution.broker_tradier_options")
BROKER_NAME = "tradier_options"
DEFAULT_BASE_URL = "https://api.tradier.com/v1"
DEFAULT_TIMEOUT_S = float(os.environ.get("TRADIER_OPTIONS_ORDER_TIMEOUT_S", "15"))
VALID_ORDER_TYPES = {"market", "limit", "stop", "stop_limit"}
VALID_DURATIONS = {"day", "gtc", "pre", "post"}
VALID_TRADIER_OPTION_SIDES = {"buy_to_open", "buy_to_close", "sell_to_open", "sell_to_close"}
_WARNED_NONFATAL_KEYS: set[str] = set()

try:
    from engine.execution.position_reconcile import pre_live_position_reconcile as _prelive_reconcile
except Exception:
    _prelive_reconcile = None  # type: ignore


def _warn_nonfatal(code: str, error: BaseException, *, once_key: str | None = None, **extra: Any) -> None:
    if once_key and once_key in _WARNED_NONFATAL_KEYS:
        return
    log_failure(
        LOG,
        event=str(code).lower(),
        code=str(code),
        message=str(error),
        error=error,
        level=30,
        component="engine.execution.broker_tradier_options",
        extra=extra or None,
        persist=False,
    )
    if once_key:
        _WARNED_NONFATAL_KEYS.add(once_key)


class TradierOptionsOrderError(RuntimeError):
    def __init__(self, message: str, *, status_code: Optional[int] = None, failure_kind: str = "broker") -> None:
        super().__init__(str(message))
        self.status_code = None if status_code is None else int(status_code)
        self.failure_kind = str(failure_kind or "broker")


def _credential_value(name: str) -> str:
    try:
        return str(get_data_credential(str(name)) or "").strip()
    except Exception as exc:
        _warn_nonfatal("TRADIER_OPTIONS_CREDENTIAL_READ_FAILED", exc, once_key=f"credential:{name}", name=str(name))
        return ""


def _tradier_api_token() -> str:
    return _credential_value("TRADIER_API_TOKEN")


def _tradier_account_id() -> str:
    return _credential_value("TRADIER_ACCOUNT_ID") or str(os.environ.get("TRADIER_ACCOUNT_ID") or "").strip()


def _tradier_base_url() -> str:
    return str(os.environ.get("TRADIER_BASE_URL") or DEFAULT_BASE_URL).strip().rstrip("/")


def _credentials_block() -> Optional[Dict[str, Any]]:
    token = _tradier_api_token()
    account_id = _tradier_account_id()
    missing = []
    if not token:
        missing.append("TRADIER_API_TOKEN")
    if not account_id:
        missing.append("TRADIER_ACCOUNT_ID")
    if not missing:
        return None
    return {
        "ok": False,
        "status": "missing_credentials",
        "reason": "tradier_options_credentials_missing",
        "broker": BROKER_NAME,
        "missing": missing,
        "stop_failover": True,
        "retryable": False,
        "failure_kind": "credential",
    }


def _first_text(order: Mapping[str, Any], *names: str) -> str:
    containers: list[Mapping[str, Any]] = [order]
    for key in ("option", "options", "option_contract", "contract", "instrument", "explain"):
        child = order.get(key)
        if isinstance(child, Mapping):
            containers.append(child)
    for container in containers:
        for name in names:
            raw = container.get(name)
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                return text
    return ""


def _first_number(order: Mapping[str, Any], *names: str) -> float:
    for name in names:
        raw = order.get(name)
        if raw in (None, ""):
            continue
        try:
            value = float(raw)
        except Exception:
            continue
        if math.isfinite(value):
            return float(value)
    return math.nan


def _contract_quantity(order: Mapping[str, Any]) -> int:
    qty = _first_number(order, "contracts", "qty", "quantity", "order_qty", "target_contracts")
    if not math.isfinite(qty) or abs(qty) <= 0.0:
        return 0
    return int(abs(qty))


def _normalize_order_type(order: Mapping[str, Any]) -> str:
    raw = _first_text(order, "type", "order_type", "tradier_type").lower()
    if raw in VALID_ORDER_TYPES:
        return raw
    if math.isfinite(_first_number(order, "limit_price", "limit_px", "price")):
        return "limit"
    return "market"


def _normalize_duration(order: Mapping[str, Any]) -> str:
    raw = _first_text(order, "duration", "time_in_force", "tif", "order_duration").lower()
    if raw in {"day", "gtc", "pre", "post"}:
        return raw
    if raw in {"opg", "ioc", "fok"}:
        return ""
    return "day"


def _position_effect(order: Mapping[str, Any]) -> str:
    raw = _first_text(order, "position_effect", "open_close", "effect", "intent").lower().replace("-", "_")
    if raw in {"open", "to_open", "opening"}:
        return "open"
    if raw in {"close", "to_close", "closing"}:
        return "close"
    tradier_side = _first_text(order, "tradier_side", "option_side").lower()
    if tradier_side.endswith("_to_open"):
        return "open"
    if tradier_side.endswith("_to_close"):
        return "close"
    return ""


def _tradier_option_side(order: Mapping[str, Any]) -> str:
    explicit = _first_text(order, "tradier_side", "option_side").lower()
    if explicit in VALID_TRADIER_OPTION_SIDES:
        return explicit

    side = _first_text(order, "side", "action", "order_side").lower()
    effect = _position_effect(order)
    if not effect:
        effect = "open" if side in {"buy", "long", "buy_to_open"} else "close"
    if side in {"buy", "long", "buy_to_open", "buy_to_close"}:
        return "buy_to_open" if effect == "open" else "buy_to_close"
    if side in {"sell", "short", "sell_short", "sell_to_open", "sell_to_close"}:
        return "sell_to_open" if effect == "open" else "sell_to_close"
    qty = _first_number(order, "contracts", "qty", "quantity", "order_qty", "target_contracts")
    if math.isfinite(qty) and qty < 0.0:
        return "sell_to_close"
    return "buy_to_open"


def _client_tag(order: Mapping[str, Any], *, override_order_id: Optional[int], index: int) -> str:
    raw = _first_text(order, "tag", "client_order_id", "client_oid", "order_ref")
    if raw:
        return raw[:255]
    if override_order_id is not None:
        return f"ts-{int(override_order_id)}-{int(index)}"[:255]
    return ""


def build_tradier_option_order_payload(
    order: Mapping[str, Any],
    *,
    override_order_id: Optional[int] = None,
    index: int = 0,
) -> Dict[str, Any]:
    if not is_options_order(order):
        raise ValueError("not_an_option_order")

    meta = option_order_metadata(order)
    parsed = parse_option_symbol(meta.get("contract"))
    if parsed is None:
        raise ValueError("tradier_options_occ_symbol_required")
    qty = _contract_quantity(order)
    if qty <= 0:
        raise ValueError("tradier_options_quantity_required")
    duration = _normalize_duration(order)
    if not duration:
        raise ValueError("tradier_options_duration_unsupported")
    order_type = _normalize_order_type(order)

    payload: Dict[str, Any] = {
        "class": "option",
        "symbol": str(parsed.underlying).upper(),
        "option_symbol": str(parsed.occ_symbol).upper(),
        "side": _tradier_option_side(order),
        "quantity": int(qty),
        "type": order_type,
        "duration": duration,
    }
    if order_type in {"limit", "stop_limit"}:
        price = _first_number(order, "limit_price", "limit_px", "price")
        if not math.isfinite(price) or price <= 0.0:
            raise ValueError("tradier_options_limit_price_required")
        payload["price"] = float(price)
    if order_type in {"stop", "stop_limit"}:
        stop = _first_number(order, "stop", "stop_price", "stop_px")
        if not math.isfinite(stop) or stop <= 0.0:
            raise ValueError("tradier_options_stop_price_required")
        payload["stop"] = float(stop)
    tag = _client_tag(order, override_order_id=override_order_id, index=index)
    if tag:
        payload["tag"] = tag
    return payload


def _headers(token: str) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {str(token).strip()}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _post_order(
    *,
    token: str,
    account_id: str,
    payload: Mapping[str, Any],
    timeout_s: Optional[float] = None,
) -> Dict[str, Any]:
    base = _tradier_base_url()
    url = f"{base}/accounts/{urllib.parse.quote(str(account_id), safe='')}/orders"
    body = urllib.parse.urlencode({key: str(value) for key, value in dict(payload).items()}).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=_headers(token), method="POST")
    timeout = max(0.1, min(30.0, float(timeout_s if timeout_s is not None else DEFAULT_TIMEOUT_S)))
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        try:
            body_text = exc.read().decode("utf-8", "ignore")[:500]
        except Exception:
            body_text = ""
        status_code = int(getattr(exc, "code", 0) or 0)
        failure_kind = "credential" if status_code in {401, 403} else "broker"
        raise TradierOptionsOrderError(
            f"tradier_options_http_error:{status_code}:{body_text}",
            status_code=status_code,
            failure_kind=failure_kind,
        ) from exc
    except Exception as exc:
        raise TradierOptionsOrderError(f"tradier_options_transport_error:{type(exc).__name__}:{exc}") from exc

    if not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise TradierOptionsOrderError(f"tradier_options_invalid_json:{exc}") from exc
    if not isinstance(parsed, dict):
        raise TradierOptionsOrderError("tradier_options_invalid_response")
    return parsed


def _real_trading_gate(
    *,
    symbol: Optional[str] = None,
    regime: Optional[str] = None,
    model_id: Optional[str] = None,
) -> Dict[str, Any]:
    if live_execution_disabled():
        return disabled_live_execution_gate(source="engine.execution.broker_tradier_options")
    try:
        from engine.execution.kill_switch import execution_allowed

        allowed, reason, meta = execution_allowed(
            con=None,
            symbol=symbol,
            regime=regime,
            model_id=model_id,
        )
    except Exception as exc:
        return {
            "ok": False,
            "reason": "kill_switch_execution_allowed_failed",
            "real_trading_allowed": False,
            "allowed": False,
            "error": str(exc),
        }
    return {
        "ok": bool(allowed),
        "reason": str(reason or "ok"),
        "real_trading_allowed": bool(allowed),
        "allowed": bool(allowed),
        "meta": dict(meta or {}),
    }


def _prelive_reconcile_or_block(broker: str = BROKER_NAME) -> Optional[Dict[str, Any]]:
    policy_block = prelive_reconcile_policy_gate(
        source="engine.execution.broker_tradier_options",
        engine_mode="live",
        broker=str(broker),
        audit_override=True,
    )
    if policy_block is not None:
        return policy_block
    if os.environ.get("EXECUTION_PRELIVE_RECONCILE", "1").strip().lower() in {"0", "false", "no", "off"}:
        return None
    if not callable(_prelive_reconcile):
        return {
            "ok": False,
            "status": "prelive_reconcile_unavailable",
            "broker": str(broker),
            "fatal_reconcile": True,
        }
    try:
        gate = _prelive_reconcile(broker=str(broker)) or {}
    except Exception as exc:
        _warn_nonfatal("TRADIER_OPTIONS_PRELIVE_RECONCILE_FAILED", exc, once_key="prelive_reconcile", broker=str(broker))
        return {
            "ok": False,
            "status": "prelive_reconcile_exception",
            "broker": str(broker),
            "fatal_reconcile": True,
            "error": str(exc),
        }
    if bool(gate.get("ok", False)):
        return None
    return {
        "ok": False,
        "status": str(gate.get("status") or "prelive_reconcile_block"),
        "broker": str(broker),
        "fatal_reconcile": True,
        "reconcile": dict(gate or {}),
    }


def _option_scope(orders: List[Dict[str, Any]]) -> Dict[str, str]:
    for order in list(orders or []):
        if not is_options_order(order):
            continue
        meta = option_order_metadata(order)
        return {
            "symbol": str(meta.get("contract") or order.get("symbol") or "").upper().strip(),
            "regime": _first_text(order, "regime", "market_regime"),
            "model_id": _first_text(order, "model_id", "model", "strategy_id"),
        }
    return {"symbol": "", "regime": "", "model_id": ""}


def _build_payloads(
    orders: List[Dict[str, Any]],
    *,
    override_order_id: Optional[int],
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    payloads: list[Dict[str, Any]] = []
    skipped: list[Dict[str, Any]] = []
    for index, order in enumerate(list(orders or [])):
        if not is_options_order(order):
            skipped.append({"index": int(index), "reason": "not_an_option_order", "symbol": str(order.get("symbol") or "")})
            continue
        try:
            payloads.append(build_tradier_option_order_payload(order, override_order_id=override_order_id, index=index))
        except Exception as exc:
            skipped.append({"index": int(index), "reason": str(exc), "symbol": str(order.get("symbol") or "")})
    return payloads, skipped


def apply_latest_portfolio_orders_live(
    dry_run: bool = False,
    override_orders: Optional[List[Dict[str, Any]]] = None,
    override_order_id: Optional[int] = None,
    override_ts_ms: Optional[int] = None,
) -> Dict[str, Any]:
    orders = [dict(order) for order in list(override_orders or []) if isinstance(order, Mapping)]
    if not orders:
        return {"ok": True, "status": "no_orders", "broker": BROKER_NAME}

    payloads, skipped = _build_payloads(orders, override_order_id=override_order_id)
    if skipped:
        return {
            "ok": False,
            "status": "invalid_options_order",
            "reason": str(skipped[0].get("reason") or "invalid_options_order"),
            "broker": BROKER_NAME,
            "skipped": skipped,
            "stop_failover": True,
            "retryable": False,
        }
    if bool(dry_run):
        return {
            "ok": True,
            "status": "dry_run",
            "broker": BROKER_NAME,
            "orders": payloads,
            "dry_run": True,
            "override_order_id": override_order_id,
            "override_ts_ms": override_ts_ms,
        }

    credentials_block = _credentials_block()
    if credentials_block is not None:
        return credentials_block

    readiness_block = live_options_order_block(
        orders,
        broker=BROKER_NAME,
        dry_run=False,
        engine_mode=os.environ.get("ENGINE_MODE", ""),
        execution_mode=os.environ.get("EXECUTION_MODE", ""),
    )
    if readiness_block is not None:
        return readiness_block

    scope = _option_scope(orders)
    gate = _real_trading_gate(
        symbol=scope.get("symbol") or None,
        regime=scope.get("regime") or None,
        model_id=scope.get("model_id") or None,
    )
    if (not bool(gate.get("ok"))) or (not bool(gate.get("real_trading_allowed"))):
        return {"ok": False, "status": "real_trading_blocked", "gate": gate, "broker": BROKER_NAME}

    reconcile_block = _prelive_reconcile_or_block(BROKER_NAME)
    if reconcile_block is not None:
        return reconcile_block

    token = _tradier_api_token()
    account_id = _tradier_account_id()
    submissions: list[Dict[str, Any]] = []
    for payload in payloads:
        try:
            response = _post_order(token=token, account_id=account_id, payload=payload)
        except TradierOptionsOrderError as exc:
            return {
                "ok": False,
                "status": "auth_failed" if exc.failure_kind == "credential" else "tradier_options_submit_failed",
                "reason": str(exc),
                "broker": BROKER_NAME,
                "status_code": exc.status_code,
                "failure_kind": exc.failure_kind,
                "submitted": submissions,
                "stop_failover": True,
                "retryable": False if exc.failure_kind == "credential" else True,
            }

        order_response = response.get("order") if isinstance(response, dict) else None
        order_state = dict(order_response or {}) if isinstance(order_response, Mapping) else {}
        accepted = bool(order_state.get("id") or str(order_state.get("status") or "").lower() in {"ok", "pending", "open"})
        submissions.append({"payload": dict(payload), "response": response, "accepted": bool(accepted), "ts_ms": int(time.time() * 1000)})
        if not accepted:
            return {
                "ok": False,
                "status": "tradier_options_order_rejected",
                "reason": str(order_state.get("reason_description") or order_state.get("status") or "tradier_options_order_rejected"),
                "broker": BROKER_NAME,
                "submitted": submissions,
                "stop_failover": True,
                "retryable": False,
            }

    return {
        "ok": True,
        "status": "submitted",
        "broker": BROKER_NAME,
        "submitted": submissions,
        "order_count": int(len(submissions)),
        "override_order_id": override_order_id,
        "override_ts_ms": override_ts_ms,
    }


__all__ = [
    "apply_latest_portfolio_orders_live",
    "build_tradier_option_order_payload",
]
