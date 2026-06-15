from __future__ import annotations

import importlib
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


class _Cursor:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def fetchall(self):
        return list(self._rows)


class _Row(dict):
    def __init__(self, **kwargs):
        super().__init__(kwargs)
        self._keys = tuple(kwargs.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return dict.__getitem__(self, self._keys[key])
        return dict.__getitem__(self, key)

    def keys(self):
        return self._keys


class _FundingExchange:
    has = {"fetchFundingRateHistory": True, "fetchFundingRate": True, "fetchTicker": True}

    def fetchFundingRateHistory(self, market, since=None, limit=None):
        assert market == "BTC/USDT:USDT"
        assert since == 1_000
        assert limit == 2
        return [
            {"symbol": market, "timestamp": 1_000, "fundingRate": 0.001},
            {"symbol": market, "timestamp": 2_000, "fundingRate": -0.002},
        ]

    def fetchFundingRate(self, market):
        assert market == "BTC/USDT:USDT"
        return {"symbol": market, "timestamp": 3_000, "fundingRate": 0.003, "markPrice": 101.0}

    def fetchTicker(self, market):
        if market == "BTC/USDT:USDT":
            return {"timestamp": 3_000, "last": 101.0}
        if market == "BTC/USDT":
            return {"timestamp": 3_000, "last": 100.0}
        raise AssertionError(market)


class _NoFundingExchange:
    has = {"fetchFundingRateHistory": False, "fetchFundingRate": False}


class _CryptoFeatureCon:
    def __init__(self, rows):
        self.rows = list(rows or [])

    def execute(self, sql, params=None):
        if "FROM crypto_funding_rates" in str(sql):
            symbol, anchor_ts_ms, window_start = params
            rows = [
                row
                for row in self.rows
                if row["symbol"] == symbol
                and int(row["availability_ts_ms"]) <= int(anchor_ts_ms)
                and int(row["availability_ts_ms"]) >= int(window_start)
            ]
            rows = sorted(rows, key=lambda row: (int(row["funding_ts_ms"]), int(row["availability_ts_ms"])))
            return _Cursor(rows)
        raise RuntimeError("unsupported fake query")


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def test_mocked_ccxt_funding_poller_parses_history_live_and_basis() -> None:
    (positioning,) = _reload("engine.data.crypto_positioning")
    market = positioning.CryptoPerpMarket(
        symbol="BTC",
        exchange_id="binance",
        perp_market="BTC/USDT:USDT",
        spot_market="BTC/USDT",
    )

    rows, errors = positioning.poll_exchange_funding(
        _FundingExchange(),
        [market],
        since_ms=1_000,
        history_limit=2,
        now_ms=4_000,
    )

    assert errors == []
    assert [row["funding_ts_ms"] for row in rows] == [1_000, 2_000, 3_000]
    assert rows[-1]["is_live"] is True
    assert rows[-1]["availability_ts_ms"] == 3_000
    assert rows[-1]["perp_basis_pct"] == 0.01
    assert rows[-1]["source_record_id"].startswith("crypto_funding:")


def test_mocked_ccxt_poller_gracefully_skips_missing_endpoint() -> None:
    (positioning,) = _reload("engine.data.crypto_positioning")
    market = positioning.CryptoPerpMarket("BTC", "binance", "BTC/USDT:USDT", "BTC/USDT")

    rows, errors = positioning.poll_exchange_funding(_NoFundingExchange(), [market], since_ms=1_000)

    assert rows == []
    assert any("history_endpoint_unavailable" in error for error in errors)
    assert any("live_endpoint_unavailable" in error for error in errors)


def test_crypto_positioning_zscore_and_basis_math() -> None:
    (positioning,) = _reload("engine.data.crypto_positioning")
    rows = [
        {"funding_ts_ms": 1_000, "availability_ts_ms": 1_000, "funding_rate": 0.00, "perp_basis_pct": 0.00},
        {"funding_ts_ms": 2_000, "availability_ts_ms": 2_000, "funding_rate": 0.01, "perp_basis_pct": 0.01},
        {"funding_ts_ms": 3_000, "availability_ts_ms": 3_000, "funding_rate": 0.02, "perp_basis_pct": 0.02},
        {"funding_ts_ms": 4_000, "availability_ts_ms": 4_000, "funding_rate": 0.10, "perp_basis_pct": 0.05},
    ]

    features = positioning.compute_positioning_features(rows, asof_ts_ms=4_000)

    assert features["funding_rate_now"] == 0.10
    assert features["funding_z_30d"] == 9.0
    assert features["funding_extreme_flag"] == 1.0
    assert features["funding_cum_3d"] == 0.13
    assert features["perp_basis_pct"] == 0.05
    assert features["basis_z_30d"] == 4.0


def test_crypto_positioning_registry_round_trip_and_job_registered(monkeypatch) -> None:
    monkeypatch.setenv("USE_FUNDING_FEATURES", "1")
    (feature_registry,) = _reload("engine.strategy.feature_registry")
    (job_registry,) = _reload("engine.runtime.job_registry")

    ids = list(feature_registry.CRYPTO_POSITIONING_FEATURE_IDS)
    assert ids == ["funding_rate_now", "funding_z_30d", "funding_extreme_flag", "funding_cum_3d", "perp_basis_pct", "basis_z_30d"]
    assert feature_registry.FEATURE_GROUPS["crypto_positioning"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert "crypto_positioning" in feature_registry.feature_set_tag_from_ids(ids).split("+")
    assert job_registry.ALLOWED_JOBS["ingest_crypto_funding"][3]["cadence_seconds"] == 28800


def test_crypto_funding_snapshot_no_lookahead_and_non_crypto_safe(monkeypatch) -> None:
    monkeypatch.setenv("USE_FUNDING_FEATURES", "1")
    _reload("engine.strategy.feature_registry")
    (snapshots,) = _reload("engine.strategy.model_feature_snapshots")
    ids = list(snapshots.CRYPTO_POSITIONING_FEATURE_IDS)
    rows = [
        _Row(
            symbol="BTC",
            exchange="binance",
            perp_market="BTC/USDT:USDT",
            spot_market="BTC/USDT",
            funding_ts_ms=1_000,
            availability_ts_ms=1_000,
            funding_rate=0.001,
            perp_basis_pct=0.01,
            mark_price=101.0,
            spot_price=100.0,
            is_live=False,
        ),
        _Row(
            symbol="BTC",
            exchange="binance",
            perp_market="BTC/USDT:USDT",
            spot_market="BTC/USDT",
            funding_ts_ms=2_000,
            availability_ts_ms=2_000,
            funding_rate=0.100,
            perp_basis_pct=0.09,
            mark_price=109.0,
            spot_price=100.0,
            is_live=False,
        ),
    ]
    con = _CryptoFeatureCon(rows)

    before = snapshots.build_model_feature_snapshot(symbol="BTC", ts_ms=1_500, feature_ids=ids, con=con)
    after = snapshots.build_model_feature_snapshot(symbol="BTC", ts_ms=2_500, feature_ids=ids, con=con)
    equity = snapshots.build_model_feature_snapshot(symbol="AAPL", ts_ms=2_500, feature_ids=ids, con=con)

    assert before["features"]["funding_rate_now"] == 0.001
    assert before["source_timestamps"]["crypto_positioning"]["latest_availability_ts_ms"] == 1_000
    assert after["features"]["funding_rate_now"] == 0.100
    assert after["source_timestamps"]["crypto_positioning"]["latest_availability_ts_ms"] == 2_000
    assert equity["features"] == {fid: 0.0 for fid in ids}
    assert snapshots.summarize_model_feature_snapshots([before, after, equity])["lookahead_violations"] == 0
