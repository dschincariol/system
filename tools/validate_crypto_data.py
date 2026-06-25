#!/usr/bin/env python3
"""Validate crypto funding data wiring without touching live execution paths."""

from __future__ import annotations

import importlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

FEATURE_IDS = [
    "funding_rate_now",
    "funding_z_30d",
    "funding_extreme_flag",
    "funding_cum_3d",
    "perp_basis_pct",
    "basis_z_30d",
]


class _StubFundingExchange:
    has = {"fetchFundingRateHistory": True, "fetchFundingRate": True, "fetchTicker": True}

    def fetchFundingRateHistory(self, market: str, since: int | None = None, limit: int | None = None) -> List[Dict[str, Any]]:
        del since, limit
        return [
            {"symbol": market, "timestamp": 1_000, "fundingRate": 0.001},
            {"symbol": market, "timestamp": 2_000, "fundingRate": -0.002},
        ]

    def fetchFundingRate(self, market: str) -> Dict[str, Any]:
        return {"symbol": market, "timestamp": 3_000, "fundingRate": 0.003, "markPrice": 101.0}

    def fetchTicker(self, market: str) -> Dict[str, Any]:
        if ":" in str(market):
            return {"timestamp": 3_000, "last": 101.0}
        return {"timestamp": 3_000, "last": 100.0}


def _result(name: str, status: str, detail: str = "", **extra: Any) -> Dict[str, Any]:
    return {
        "name": str(name),
        "status": str(status).upper(),
        "detail": str(detail),
        **{str(k): v for k, v in extra.items()},
    }


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _public_ccxt_probe() -> List[Dict[str, Any]]:
    if str(os.environ.get("CRYPTO_DATA_VALIDATE_PUBLIC_PROBE", "1")).strip().lower() in {"0", "false", "off", "no"}:
        return [_result("ccxt_public_probe", "SKIP", "disabled_by_CRYPTO_DATA_VALIDATE_PUBLIC_PROBE")]
    try:
        import ccxt  # type: ignore
    except Exception as exc:
        return [_result("ccxt_public_probe", "SKIP", "ccxt_unavailable", error_type=type(exc).__name__)]

    checks: List[Dict[str, Any]] = []
    try:
        from engine.data.crypto_positioning import parse_env_market_map

        parsed = parse_env_market_map()
    except Exception:
        parsed = []
    funding_exchange_id = str(os.environ.get("CCXT_FUNDING_EXCHANGE_ID") or "binanceusdm").strip() or "binanceusdm"
    spot_exchange_id = str(os.environ.get("CCXT_EXCHANGE_ID") or "kraken").strip() or "kraken"
    perp_symbol = str(parsed[0].perp_market if parsed else "BTC/USDT:USDT")
    spot_symbol = str(parsed[0].spot_market if parsed else "BTC/USD")

    for name, exchange_id, symbol, endpoint in (
        ("ccxt_public_funding_probe", funding_exchange_id, perp_symbol, "fetchFundingRate"),
        ("ccxt_public_spot_probe", spot_exchange_id, spot_symbol, "fetchTicker"),
    ):
        try:
            exchange_cls = getattr(ccxt, exchange_id)
            exchange = exchange_cls({"enableRateLimit": True, "timeout": 5000, "options": {"defaultType": "swap"}})
            has = getattr(exchange, "has", {}) or {}
            if endpoint == "fetchFundingRate" and has.get(endpoint) is False:
                checks.append(_result(name, "SKIP", "endpoint_unavailable", exchange_id=exchange_id, symbol=symbol))
                continue
            if endpoint == "fetchFundingRate":
                payload = exchange.fetch_funding_rate(symbol)
            else:
                payload = exchange.fetch_ticker(symbol)
            if payload:
                checks.append(_result(name, "PASS", "public_endpoint_returned_payload", exchange_id=exchange_id, symbol=symbol))
            else:
                checks.append(_result(name, "SKIP", "empty_public_payload", exchange_id=exchange_id, symbol=symbol))
        except Exception as exc:
            checks.append(_result(name, "SKIP", "public_endpoint_unreachable", exchange_id=exchange_id, symbol=symbol, error_type=type(exc).__name__))
    return checks


def _mocked_pipeline_check() -> Dict[str, Any]:
    old_env = {key: os.environ.get(key) for key in ("TS_STORAGE_BACKEND", "DB_PATH", "USE_FUNDING_FEATURES", "ASSET_CLASS_MAP_JSON")}
    with tempfile.TemporaryDirectory(prefix="crypto-data-validate-") as tmp:
        os.environ["TS_STORAGE_BACKEND"] = "sqlite"
        os.environ["DB_PATH"] = str(Path(tmp) / "crypto_data_validation.sqlite")
        os.environ["USE_FUNDING_FEATURES"] = "1"
        os.environ["ASSET_CLASS_MAP_JSON"] = json.dumps({"BTC": "CRYPTO", "AAPL": "EQUITY"}, separators=(",", ":"))
        try:
            storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
            positioning = importlib.reload(importlib.import_module("engine.data.crypto_positioning"))

            storage.init_db()
            market = positioning.CryptoPerpMarket("BTC", "binanceusdm", "BTC/USDT:USDT", "BTC/USDT")
            rows, errors = positioning.poll_exchange_funding(
                _StubFundingExchange(),
                [market],
                since_ms=1_000,
                history_limit=2,
                now_ms=4_000,
            )
            if errors:
                return _result("mocked_funding_pipeline", "FAIL", "mocked_poller_returned_errors", errors=errors)
            if len(rows) < 3:
                return _result("mocked_funding_pipeline", "FAIL", "mocked_poller_missing_rows", rows=len(rows))

            future = dict(rows[-1])
            future.update(
                {
                    "ts_ms": 9_000,
                    "funding_ts_ms": 9_000,
                    "availability_ts_ms": 9_000,
                    "funding_rate": 0.999,
                    "source_record_id": "crypto_funding:validation_future_row",
                    "is_live": False,
                }
            )

            def _write(con) -> int:
                written = 0
                for row in [*rows, future]:
                    written += int(storage.put_crypto_funding_rate(row, con=con) or 0)
                return int(written)

            written = int(
                storage.run_write_txn(
                    _write,
                    table="crypto_funding_rates",
                    operation="validate_crypto_data",
                    context={"rows": len(rows) + 1},
                )
                or 0
            )
            con = storage.connect(readonly=True)
            try:
                count_row = con.execute("SELECT COUNT(*), MAX(availability_ts_ms) FROM crypto_funding_rates").fetchone()
                row_count = _safe_int((count_row or [0, 0])[0])
                max_availability = _safe_int((count_row or [0, 0])[1])
                columns = [
                    "symbol",
                    "exchange",
                    "perp_market",
                    "spot_market",
                    "funding_ts_ms",
                    "availability_ts_ms",
                    "funding_rate",
                    "perp_basis_pct",
                    "mark_price",
                    "spot_price",
                    "is_live",
                ]
                db_rows = [
                    {columns[idx]: row[idx] for idx in range(len(columns))}
                    for row in con.execute(
                        """
                        SELECT
                          symbol, exchange, perp_market, spot_market, funding_ts_ms,
                          availability_ts_ms, funding_rate, perp_basis_pct, mark_price,
                          spot_price, is_live
                        FROM crypto_funding_rates
                        WHERE symbol = ?
                        ORDER BY funding_ts_ms ASC
                        """,
                        ("BTC",),
                    ).fetchall()
                ]
                computed = positioning.compute_positioning_features(db_rows, asof_ts_ms=3_500)
                missing = [fid for fid in FEATURE_IDS if fid not in computed]
                future_leaked = abs(float(computed.get("funding_rate_now") or 0.0) - 0.999) < 1e-12
                latest_availability = max(
                    [_safe_int(row.get("availability_ts_ms")) for row in db_rows if _safe_int(row.get("availability_ts_ms")) <= 3_500]
                    or [0]
                )
            finally:
                con.close()

            if row_count < 4 or written <= 0:
                return _result("mocked_funding_pipeline", "FAIL", "rows_not_persisted", row_count=row_count, written=written)
            if missing:
                return _result("mocked_funding_pipeline", "FAIL", "feature_ids_missing", missing=missing)
            if future_leaked or latest_availability > 3_500 or max_availability < 9_000:
                return _result(
                    "mocked_funding_pipeline",
                    "FAIL",
                    "point_in_time_violation",
                    computed=computed,
                    latest_availability_ts_ms=latest_availability,
                    max_storage_availability_ts_ms=max_availability,
                )
            return _result(
                "mocked_funding_pipeline",
                "PASS",
                "mocked_rows_persisted_and_features_pit_safe",
                row_count=row_count,
                written=written,
                latest_availability_ts_ms=latest_availability,
                features={fid: float(computed.get(fid) or 0.0) for fid in FEATURE_IDS},
            )
        except Exception as exc:
            return _result("mocked_funding_pipeline", "FAIL", "validation_exception", error_type=type(exc).__name__, error=str(exc)[:500])
        finally:
            for key, value in old_env.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


def main() -> int:
    checks = []
    checks.extend(_public_ccxt_probe())
    checks.append(_mocked_pipeline_check())
    failed = [row for row in checks if str(row.get("status")) == "FAIL"]
    skipped = [row for row in checks if str(row.get("status")) == "SKIP"]
    passed = [row for row in checks if str(row.get("status")) == "PASS"]
    payload = {
        "tool": "validate_crypto_data",
        "status": "FAIL" if failed else ("SKIP" if skipped and not passed else "PASS"),
        "generated_ts_ms": int(time.time() * 1000),
        "summary": {"pass": len(passed), "skip": len(skipped), "fail": len(failed)},
        "checks": checks,
    }
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
