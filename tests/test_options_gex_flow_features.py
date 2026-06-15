from __future__ import annotations

import importlib
import math
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


class _Rows:
    def __init__(self, rows):
        self._rows = list(rows or [])

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None


class _RegimeCon:
    def __init__(self, *, gex_row=None):
        self.gex_row = gex_row

    def execute(self, sql, params=None):
        text = " ".join(str(sql).lower().split())
        if "from options_symbol_features" in text:
            return _Rows([self.gex_row] if self.gex_row is not None else [])
        if "from prices" in text:
            return _Rows([(100.0,), (101.0,), (99.0,), (102.0,), (101.0,), (103.0,), (104.0,), (103.0,), (105.0,)])
        if "from execution_fills" in text:
            return _Rows([])
        if "from execution_capital_efficiency" in text:
            return _Rows([])
        return _Rows([])


def _make_options_db(options_features):
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE options_chain_v2 (
            ts_ms INTEGER NOT NULL,
            underlying TEXT NOT NULL,
            contract TEXT NOT NULL,
            expiration TEXT,
            contract_type TEXT,
            strike REAL,
            iv REAL,
            open_interest REAL,
            volume REAL,
            bid REAL,
            ask REAL,
            delta REAL,
            gamma REAL,
            theta REAL,
            vega REAL,
            source TEXT,
            PRIMARY KEY(contract, ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE options_symbol_features (
            symbol TEXT NOT NULL,
            bucket_ts_ms INTEGER NOT NULL,
            bucket_sec INTEGER NOT NULL,
            snapshot_ts_ms INTEGER NOT NULL,
            chain_source TEXT,
            contract_count INTEGER NOT NULL DEFAULT 0,
            expiry_count INTEGER NOT NULL DEFAULT 0,
            atm_iv_near REAL NOT NULL DEFAULT 0.0,
            atm_iv_next REAL NOT NULL DEFAULT 0.0,
            iv_rank REAL NOT NULL DEFAULT 0.0,
            iv_rank_short REAL NOT NULL DEFAULT 0.0,
            skew_25d REAL NOT NULL DEFAULT 0.0,
            skew_zscore REAL NOT NULL DEFAULT 0.0,
            term_structure_slope REAL NOT NULL DEFAULT 0.0,
            term_structure_zscore REAL NOT NULL DEFAULT 0.0,
            call_put_volume_ratio REAL NOT NULL DEFAULT 1.0,
            call_put_oi_ratio REAL NOT NULL DEFAULT 1.0,
            unusual_volume_score REAL NOT NULL DEFAULT 0.0,
            unusual_volume_contracts INTEGER NOT NULL DEFAULT 0,
            unusual_volume_ratio REAL NOT NULL DEFAULT 0.0,
            signal_score REAL NOT NULL DEFAULT 0.0,
            meta_json TEXT,
            PRIMARY KEY(symbol, bucket_ts_ms, bucket_sec)
        )
        """
    )
    con.execute(
        """
        CREATE TABLE options_event_features (
            event_id INTEGER PRIMARY KEY,
            ts_ms INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            event_kind TEXT NOT NULL,
            bucket_sec INTEGER NOT NULL DEFAULT 0,
            signal_score REAL NOT NULL DEFAULT 0.0,
            iv_rank REAL NOT NULL DEFAULT 0.0,
            iv_rank_short REAL NOT NULL DEFAULT 0.0,
            skew_25d REAL NOT NULL DEFAULT 0.0,
            skew_zscore REAL NOT NULL DEFAULT 0.0,
            term_structure_slope REAL NOT NULL DEFAULT 0.0,
            term_structure_zscore REAL NOT NULL DEFAULT 0.0,
            unusual_volume_score REAL NOT NULL DEFAULT 0.0,
            call_put_volume_ratio REAL NOT NULL DEFAULT 1.0,
            call_put_oi_ratio REAL NOT NULL DEFAULT 1.0,
            meta_json TEXT
        )
        """
    )
    con.execute("CREATE TABLE options_surface (ts_ms INTEGER, underlying TEXT, atm_iv_near REAL, atm_iv_next REAL, skew_25d REAL, term_structure_slope REAL)")
    con.execute("CREATE TABLE price_quotes (ts_ms INTEGER, symbol TEXT, last REAL, volume REAL)")
    con.execute("CREATE TABLE prices (ts_ms INTEGER, symbol TEXT, px REAL, price REAL)")
    options_features._ensure_options_gex_flow_columns(con)
    return con


def test_gex_math_golden_and_put_heavy_sign() -> None:
    (options_features,) = _reload("engine.data.options_features")
    call_only = options_features.compute_dealer_gex_metrics(
        [{"contract_type": "call", "gamma": 0.01, "open_interest": 10}],
        spot=100.0,
        adv_dollars=10_000.0,
        ts_ms=1_700_000_000_000,
    )
    assert call_only["gex_raw"] == 1000.0
    assert call_only["gex_norm"] == 0.1
    assert call_only["gex_sign"] == 1.0

    put_heavy = options_features.compute_dealer_gex_metrics(
        [
            {"contract_type": "call", "gamma": 0.01, "open_interest": 10},
            {"contract_type": "put", "gamma": 0.02, "open_interest": 20},
        ],
        spot=100.0,
        adv_dollars=10_000.0,
        ts_ms=1_700_000_000_000,
    )
    assert put_heavy["gex_raw"] == -3000.0
    assert put_heavy["gex_sign"] == -1.0


def test_materialized_flow_and_gex_are_finite_when_adv_missing() -> None:
    (options_features,) = _reload("engine.data.options_features")
    con = _make_options_db(options_features)
    rows = [
        (1_000, "SPY", "O:SPY260117C00100000", "2026-01-17", "call", 100.0, 0.20, 10.0, 5.0, 0.5, 0.01),
        (1_000, "SPY", "O:SPY260117P00100000", "2026-01-17", "put", 100.0, 0.20, 10.0, 5.0, -0.5, 0.01),
        (2_000, "SPY", "O:SPY260117C00100000", "2026-01-17", "call", 100.0, 0.20, 25.0, 30.0, 0.5, 0.01),
        (2_000, "SPY", "O:SPY260117P00100000", "2026-01-17", "put", 100.0, 0.20, 12.0, 10.0, -0.5, 0.01),
    ]
    con.executemany(
        """
        INSERT INTO options_chain_v2(
          ts_ms, underlying, contract, expiration, contract_type, strike, iv,
          open_interest, volume, delta, gamma, source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,'polygon')
        """,
        rows,
    )

    stats = options_features.materialize_options_features(con, underlyings=["SPY"])
    assert stats["symbols"] == 1
    row = con.execute(
        """
        SELECT gex_norm, gex_sign, opt_flow_imbalance
        FROM options_symbol_features
        WHERE symbol='SPY' AND bucket_sec=86400
        """
    ).fetchone()
    assert row is not None
    assert math.isfinite(float(row[0]))
    assert float(row[1]) > 0.0
    assert float(row[2]) > 0.0


def test_options_snapshot_no_lookahead_and_registry_round_trip(monkeypatch) -> None:
    monkeypatch.setenv("USE_OPTIONS_FEATURES", "1")
    feature_registry, snapshots, job_registry = _reload(
        "engine.strategy.feature_registry",
        "engine.strategy.model_feature_snapshots",
        "engine.runtime.job_registry",
    )
    con = sqlite3.connect(":memory:")
    con.execute(
        """
        CREATE TABLE options_symbol_features (
            symbol TEXT,
            bucket_ts_ms INTEGER,
            bucket_sec INTEGER,
            snapshot_ts_ms INTEGER,
            iv_rank REAL,
            iv_rank_short REAL,
            skew_25d REAL,
            term_structure_slope REAL,
            unusual_volume_score REAL,
            call_put_volume_ratio REAL,
            call_put_oi_ratio REAL,
            signal_score REAL,
            gex_norm_z REAL,
            gex_sign REAL,
            opt_flow_imbalance_z REAL
        )
        """
    )
    con.executemany(
        """
        INSERT INTO options_symbol_features VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            ("SPY", 0, 86400, 2_000, 0.1, 0.1, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 0.25, 1.0, 0.5),
            ("SPY", 0, 86400, 5_000, 0.9, 0.9, 0.0, 0.0, 0.0, 1.0, 1.0, 0.0, 9.0, -1.0, -9.0),
        ],
    )

    before, before_meta, before_available = snapshots._load_options_group(con, symbol="SPY", ts_ms=3_000)
    after, after_meta, after_available = snapshots._load_options_group(con, symbol="SPY", ts_ms=6_000)

    assert before_available is True
    assert before_meta["snapshot_ts_ms"] == 2_000
    assert before["options_symbol.gex_norm_z"] == 0.25
    assert after_available is True
    assert after_meta["snapshot_ts_ms"] == 5_000
    assert after["options_symbol.gex_sign"] == -1.0

    ids = list(feature_registry.OPTIONS_FEATURE_IDS)
    for fid in ("options_symbol.gex_norm_z", "options_symbol.gex_sign", "options_symbol.opt_flow_imbalance_z"):
        assert fid in ids
    assert feature_registry.FEATURE_GROUPS["options"] == ids
    assert feature_registry.resolve_feature_ids(model_spec={"feature_schema": {"feature_ids": ids}}) == ids
    assert feature_registry.expected_columns(ids, fallback_to_default=False) == ids
    assert job_registry.ALLOWED_JOBS["ingest_options"][3]["cadence_seconds"] == 300


def test_regime_stack_gex_input_present_when_flag_on_absent_when_off(monkeypatch) -> None:
    monkeypatch.setenv("USE_OPTIONS_FEATURES", "1")
    (regime_stack,) = _reload("engine.strategy.regime_stack")
    on = regime_stack.compute_regime_vector(symbol="SPY", ts_ms=6_000, con=_RegimeCon(gex_row=(5_000, -0.2, -2.5, -1.0)), include_hmm=False)
    assert on["micro"]["gex_norm_z"] == -2.5
    assert on["micro"]["gex_sign"] == -1.0

    monkeypatch.setenv("USE_OPTIONS_FEATURES", "0")
    (regime_stack,) = _reload("engine.strategy.regime_stack")
    off = regime_stack.compute_regime_vector(symbol="SPY", ts_ms=6_000, con=_RegimeCon(gex_row=(5_000, -0.2, -2.5, -1.0)), include_hmm=False)
    assert "gex_norm_z" not in off["micro"]
    assert "gex_sign" not in off["micro"]
