from __future__ import annotations

import importlib
import json
from typing import Any, Dict, List


FEATURE_IDS = [
    "funding_rate_now",
    "funding_z_30d",
    "funding_extreme_flag",
    "funding_cum_3d",
    "perp_basis_pct",
    "basis_z_30d",
]


class _FundingExchange:
    has = {"fetchFundingRateHistory": True, "fetchFundingRate": True, "fetchTicker": True}

    def fetchFundingRateHistory(self, market, since=None, limit=None):
        assert market == "BTC/USDT:USDT"
        return [
            {"symbol": market, "timestamp": 1_000, "fundingRate": 0.001},
            {"symbol": market, "timestamp": 2_000, "fundingRate": -0.002},
        ][: int(limit or 2)]

    def fetchFundingRate(self, market):
        assert market == "BTC/USDT:USDT"
        return {"symbol": market, "timestamp": 3_000, "fundingRate": 0.003, "markPrice": 101.0}

    def fetchTicker(self, market):
        if market == "BTC/USDT:USDT":
            return {"timestamp": 3_000, "last": 101.0}
        if market == "BTC/USDT":
            return {"timestamp": 3_000, "last": 100.0}
        raise AssertionError(market)


def _row_dict(row: Any, columns: List[str]) -> Dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return {column: row[idx] for idx, column in enumerate(columns)}


def _isolated_crypto_modules(monkeypatch, tmp_path):
    monkeypatch.setenv("TS_STORAGE_BACKEND", "sqlite")
    monkeypatch.setenv("DB_PATH", str(tmp_path / "crypto_funding_pipeline.sqlite"))
    monkeypatch.setenv("CCXT_ENABLED", "1")
    monkeypatch.setenv("INGEST_CRYPTO_FUNDING_ENABLED", "1")
    monkeypatch.setenv("USE_FUNDING_FEATURES", "1")
    monkeypatch.setenv("ASSET_CLASS_MAP_JSON", json.dumps({"BTC": "CRYPTO", "AAPL": "EQUITY"}, separators=(",", ":")))
    storage = importlib.reload(importlib.import_module("engine.runtime.storage"))
    storage.init_db()
    positioning = importlib.reload(importlib.import_module("engine.data.crypto_positioning"))
    importlib.reload(importlib.import_module("engine.strategy.feature_registry"))
    snapshots = importlib.reload(importlib.import_module("engine.strategy.model_feature_snapshots"))
    return storage, positioning, snapshots


def test_mocked_funding_poller_persists_rows_and_snapshot_features_are_pit_safe(monkeypatch, tmp_path) -> None:
    storage, positioning, snapshots = _isolated_crypto_modules(monkeypatch, tmp_path)
    market = positioning.CryptoPerpMarket("BTC", "binanceusdm", "BTC/USDT:USDT", "BTC/USDT")
    rows, errors = positioning.poll_exchange_funding(
        _FundingExchange(),
        [market],
        since_ms=1_000,
        history_limit=2,
        now_ms=4_000,
    )
    assert errors == []
    assert len(rows) == 3

    future = dict(rows[-1])
    future.update(
        {
            "ts_ms": 9_000,
            "funding_ts_ms": 9_000,
            "availability_ts_ms": 9_000,
            "funding_rate": 0.999,
            "source_record_id": "crypto_funding:test_future_row",
            "is_live": False,
        }
    )

    def _write(con) -> int:
        written = 0
        for row in [*rows, future]:
            written += int(storage.put_crypto_funding_rate(row, con=con) or 0)
        return int(written)

    written = storage.run_write_txn(
        _write,
        table="crypto_funding_rates",
        operation="test_crypto_funding_pipeline",
        context={"rows": len(rows) + 1},
    )
    assert int(written or 0) >= 1

    con = storage.connect(readonly=True)
    try:
        count = con.execute("SELECT COUNT(*) FROM crypto_funding_rates").fetchone()
        assert int((count or [0])[0] or 0) == 4
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
            _row_dict(row, columns)
            for row in con.execute(
                """
                SELECT
                  symbol, exchange, perp_market, spot_market, funding_ts_ms,
                  availability_ts_ms, funding_rate, perp_basis_pct, mark_price,
                  spot_price, is_live
                FROM crypto_funding_rates
                WHERE symbol = ?
                ORDER BY funding_ts_ms ASC, availability_ts_ms ASC
                """,
                ("BTC",),
            ).fetchall()
        ]
        computed = positioning.compute_positioning_features(db_rows, asof_ts_ms=3_500)
        crypto = snapshots.build_model_feature_snapshot(symbol="BTC", ts_ms=3_500, feature_ids=list(FEATURE_IDS), con=con)
        equity = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=3_500, feature_ids=list(FEATURE_IDS), con=con)
        health = importlib.reload(importlib.import_module("engine.runtime.health"))
        crypto_health = health._crypto_data_readiness_snapshot(
            con,
            now_ms=10_000,
            pipeline_statuses={"ingest_crypto_funding": {"ok": True, "last_ingested_ts_ms": 3_000}},
        )
    finally:
        con.close()

    assert set(FEATURE_IDS).issubset(computed)
    assert computed["funding_rate_now"] == 0.003
    assert computed["funding_rate_now"] != 0.999
    assert crypto["features"]["funding_rate_now"] == 0.003
    assert crypto["source_timestamps"]["crypto_positioning"]["latest_availability_ts_ms"] == 3_000
    assert equity["features"] == {fid: 0.0 for fid in FEATURE_IDS}
    assert snapshots.summarize_model_feature_snapshots([crypto, equity])["lookahead_violations"] == 0
    assert crypto_health["wired"] is True
    assert crypto_health["enabled"] is True
    assert crypto_health["row_count"] == 4
    assert crypto_health["last_row_age_s"] == 1.0
