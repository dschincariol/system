"""
FILE: repair_schema.py

Job entrypoint or scheduled task for `repair_schema`.
"""

import sys
import time
from pathlib import Path

from engine.runtime.config_schema import load_runtime_config
from engine.runtime.storage_live_ingestion_schema import ensure_prices_schema
from engine.runtime.storage import SCHEMA_VERSION as STORAGE_SCHEMA_VERSION

SCHEMA_VERSION = int(STORAGE_SCHEMA_VERSION)


def _warn_nonfatal(code: str, error: BaseException, **extra: object) -> None:
    details = " ".join(f"{key}={value}" for key, value in sorted((extra or {}).items()))
    suffix = f" {details}" if details else ""
    sys.stderr.write(f"[repair_schema] {code}:{type(error).__name__}:{error}{suffix}\n")


def _apply_v2(cur) -> None:
    _ensure_required_runtime_tables(cur)


def _apply_v3(cur) -> None:
    _ensure_required_runtime_tables(cur)


def _apply_v4(cur) -> None:
    _ensure_required_runtime_tables(cur)


def _apply_v5(cur) -> None:
    _ensure_required_runtime_tables(cur)


def _apply_v6(cur) -> None:
    _ensure_required_runtime_tables(cur)


def _ensure_version_tables(cur) -> None:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS runtime_meta (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_ts_ms INTEGER
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schema_version (
        version INTEGER PRIMARY KEY,
        applied_ts_ms INTEGER NOT NULL,
        status TEXT NOT NULL,
        notes TEXT
    )
    """)


def _read_effective_version(cur) -> int:
    row = cur.execute(
        """
        SELECT version
        FROM schema_version
        WHERE status = 'applied'
        ORDER BY version DESC
        LIMIT 1
        """
    ).fetchone()

    if row and row[0] is not None:
        try:
            return int(row[0])
        except Exception as e:
            print(f"[repair_schema] schema_version_row_parse_failed: {type(e).__name__}: {e}")

    legacy = cur.execute(
        "SELECT value FROM runtime_meta WHERE key = 'schema_version'"
    ).fetchone()

    if legacy and legacy[0] is not None:
        try:
            return int(float(str(legacy[0]).strip()))
        except Exception as e:
            sys.stderr.write(
                f"[repair_schema] legacy_schema_version_parse_failed:"
                f"{type(e).__name__}:{e}\n"
            )
            return 0

    return 0


def _read_last_non_applied(cur):
    return cur.execute(
        """
        SELECT version, status, notes
        FROM schema_version
        WHERE status <> 'applied'
        ORDER BY version DESC
        LIMIT 1
        """
    ).fetchone()


def _table_columns(cur, table_name: str) -> set[str]:
    return {
        str(row[1])
        for row in cur.execute(f"PRAGMA table_info({table_name})").fetchall()
        if row and len(row) >= 2 and str(row[1]).strip()
    }


def _ensure_columns(cur, table_name: str, columns: tuple[tuple[str, str], ...]) -> None:
    existing = _table_columns(cur, table_name)
    for column_name, column_ddl in columns:
        if str(column_name) not in existing:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_ddl}")
            existing.add(str(column_name))


def _set_runtime_meta_schema_version(cur, version: int, now: int) -> None:
    cur.execute(
        """
        INSERT INTO runtime_meta(key, value, updated_ts_ms)
        VALUES(?,?,?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_ts_ms=excluded.updated_ts_ms
        """,
        ("schema_version", str(int(version)), int(now)),
    )


def _mark_schema_version(cur, version: int, now: int, status: str, notes: str) -> None:
    cur.execute(
        """
        INSERT INTO schema_version(version, applied_ts_ms, status, notes)
        VALUES(?,?,?,?)
        ON CONFLICT(version) DO UPDATE SET
            applied_ts_ms=excluded.applied_ts_ms,
            status=excluded.status,
            notes=excluded.notes
        """,
        (int(version), int(now), str(status), str(notes)),
    )


def _mark_schema_version_applied(cur, version: int, now: int, notes: str) -> None:
    _mark_schema_version(cur, version, now, "applied", notes)


def _apply_v1(cur) -> None:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        severity TEXT,
        symbol TEXT,
        horizon_s INTEGER,
        message TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_equity_state (
        ts_ms INTEGER PRIMARY KEY,
        equity REAL NOT NULL DEFAULT 0,
        drawdown REAL NOT NULL DEFAULT 0
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS broker_account (
        ts_ms INTEGER PRIMARY KEY,
        equity REAL NOT NULL DEFAULT 0,
        buying_power REAL NOT NULL DEFAULT 0
    )
    """)

    labels_exists = cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='labels'"
    ).fetchone()

    if labels_exists:
        cur.execute("PRAGMA table_info(labels)")
        cols = [str(r[1]) for r in cur.fetchall()]

        if "label" not in cols:
            cur.execute("ALTER TABLE labels ADD COLUMN label TEXT")

        if "ts_ms" not in cols:
            cur.execute("ALTER TABLE labels ADD COLUMN ts_ms INTEGER")


def _ensure_required_runtime_tables(cur) -> None:
    con = getattr(cur, "connection", None)
    if con is None:
        raise RuntimeError("repair_schema_missing_cursor_connection")
    ensure_prices_schema(con, warn_nonfatal=_warn_nonfatal)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        severity TEXT,
        symbol TEXT,
        horizon_s INTEGER,
        message TEXT
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS portfolio_state (
        ts_ms INTEGER PRIMARY KEY,
        cash REAL NOT NULL DEFAULT 0,
        gross_exposure REAL NOT NULL DEFAULT 0,
        net_exposure REAL NOT NULL DEFAULT 0,
        leverage REAL NOT NULL DEFAULT 0,
        positions_json TEXT NOT NULL DEFAULT '{}'
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS job_heartbeats (
        job_name TEXT PRIMARY KEY,
        owner TEXT NOT NULL DEFAULT '',
        pid INTEGER NOT NULL,
        ts_ms INTEGER NOT NULL,
        status TEXT,
        extra_json TEXT
    )
    """)
    cur.execute("PRAGMA table_info(job_heartbeats)")
    heartbeat_cols = {str(row[1]) for row in cur.fetchall()}
    if "owner" not in heartbeat_cols:
        cur.execute("ALTER TABLE job_heartbeats ADD COLUMN owner TEXT NOT NULL DEFAULT ''")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        qty REAL NOT NULL DEFAULT 0,
        price REAL NOT NULL DEFAULT 0,
        order_id TEXT,
        source_order_id TEXT,
        broker_order_id TEXT,
        note TEXT
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_trades_symbol_ts
    ON trades(symbol, ts_ms DESC)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_trades_ts
    ON trades(ts_ms DESC)
    """)

    for _options_table in ("options_symbol_features", "options_event_features"):
        try:
            exists = cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (str(_options_table),),
            ).fetchone()
        except Exception:
            exists = None
        if exists:
            try:
                _ensure_columns(
                    cur,
                    str(_options_table),
                    (
                        ("gex_raw", "REAL"),
                        ("gex_norm", "REAL NOT NULL DEFAULT 0.0"),
                        ("gex_norm_z", "REAL NOT NULL DEFAULT 0.0"),
                        ("gex_sign", "REAL NOT NULL DEFAULT 0.0"),
                        ("opt_flow_imbalance", "REAL NOT NULL DEFAULT 0.0"),
                        ("opt_flow_imbalance_z", "REAL NOT NULL DEFAULT 0.0"),
                        ("gex_zero_gamma_flip", "REAL"),
                    ),
                )
            except Exception as e:
                _warn_nonfatal("REPAIR_SCHEMA_OPTIONS_GEX_FLOW_COLUMNS_FAILED", e, table=str(_options_table))

    cur.execute("""
    CREATE TABLE IF NOT EXISTS insider_transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        symbol TEXT,
        event_id INTEGER,
        source_transaction_id TEXT,
        created_ts_ms INTEGER,
        ingested_ts_ms INTEGER,
        source TEXT,
        filing_accession TEXT,
        filing_identifier TEXT,
        filing_url TEXT,
        filing_ts_ms INTEGER,
        availability_ts_ms INTEGER,
        filing_date TEXT,
        filing_accepted_at TEXT,
        transaction_ts_ms INTEGER,
        transaction_date TEXT,
        issuer_name TEXT,
        issuer_cik TEXT,
        insider_name TEXT,
        insider_cik TEXT,
        insider_role TEXT,
        insider_title TEXT,
        transaction_code TEXT,
        transaction_type TEXT,
        direction TEXT,
        security_type TEXT,
        shares REAL,
        price REAL,
        value REAL,
        ownership_nature TEXT,
        is_10b5_1_plan INTEGER,
        entity_id TEXT,
        resolution_status TEXT,
        resolution_method TEXT,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "insider_transactions",
        (
            ("id", "BIGSERIAL"),
            ("ts_ms", "INTEGER"),
            ("symbol", "TEXT"),
            ("event_id", "INTEGER"),
            ("source_transaction_id", "TEXT"),
            ("created_ts_ms", "INTEGER"),
            ("ingested_ts_ms", "INTEGER"),
            ("source", "TEXT"),
            ("filing_accession", "TEXT"),
            ("filing_identifier", "TEXT"),
            ("filing_url", "TEXT"),
            ("filing_ts_ms", "INTEGER"),
            ("availability_ts_ms", "INTEGER"),
            ("filing_date", "TEXT"),
            ("filing_accepted_at", "TEXT"),
            ("transaction_ts_ms", "INTEGER"),
            ("transaction_date", "TEXT"),
            ("issuer_name", "TEXT"),
            ("issuer_cik", "TEXT"),
            ("insider_name", "TEXT"),
            ("insider_cik", "TEXT"),
            ("insider_role", "TEXT"),
            ("insider_title", "TEXT"),
            ("transaction_code", "TEXT"),
            ("transaction_type", "TEXT"),
            ("direction", "TEXT"),
            ("security_type", "TEXT"),
            ("shares", "REAL"),
            ("price", "REAL"),
            ("value", "REAL"),
            ("ownership_nature", "TEXT"),
            ("is_10b5_1_plan", "INTEGER"),
            ("entity_id", "TEXT"),
            ("resolution_status", "TEXT"),
            ("resolution_method", "TEXT"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_ts
    ON insider_transactions(symbol, transaction_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_insider_transactions_symbol_availability
    ON insider_transactions(symbol, availability_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_insider_transactions_resolution_ts
    ON insider_transactions(resolution_status, transaction_ts_ms DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS congressional_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        symbol TEXT,
        event_id INTEGER,
        source_trade_id TEXT,
        source_record_id TEXT,
        source_url TEXT,
        created_ts_ms INTEGER,
        ingested_ts_ms INTEGER,
        source TEXT,
        chamber TEXT,
        office TEXT,
        politician_name TEXT,
        owner_name TEXT,
        issuer_name TEXT,
        transaction_type_raw TEXT,
        transaction_type TEXT,
        direction TEXT,
        amount_range TEXT,
        amount_low REAL,
        amount_high REAL,
        amount_mid REAL,
        transaction_date TEXT,
        transaction_ts_ms INTEGER,
        disclosure_date TEXT,
        disclosure_ts_ms INTEGER,
        entity_id TEXT,
        resolution_status TEXT,
        resolution_method TEXT,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "congressional_trades",
        (
            ("id", "BIGSERIAL"),
            ("ts_ms", "INTEGER"),
            ("symbol", "TEXT"),
            ("event_id", "INTEGER"),
            ("source_trade_id", "TEXT"),
            ("source_record_id", "TEXT"),
            ("source_url", "TEXT"),
            ("created_ts_ms", "INTEGER"),
            ("ingested_ts_ms", "INTEGER"),
            ("source", "TEXT"),
            ("chamber", "TEXT"),
            ("office", "TEXT"),
            ("politician_name", "TEXT"),
            ("owner_name", "TEXT"),
            ("issuer_name", "TEXT"),
            ("transaction_type_raw", "TEXT"),
            ("transaction_type", "TEXT"),
            ("direction", "TEXT"),
            ("amount_range", "TEXT"),
            ("amount_low", "REAL"),
            ("amount_high", "REAL"),
            ("amount_mid", "REAL"),
            ("transaction_date", "TEXT"),
            ("transaction_ts_ms", "INTEGER"),
            ("disclosure_date", "TEXT"),
            ("disclosure_ts_ms", "INTEGER"),
            ("entity_id", "TEXT"),
            ("resolution_status", "TEXT"),
            ("resolution_method", "TEXT"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_congressional_trades_symbol_ts
    ON congressional_trades(symbol, transaction_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_congressional_trades_resolution_ts
    ON congressional_trades(resolution_status, transaction_ts_ms DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS finra_short_sale_volume (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        symbol TEXT,
        trade_date TEXT,
        trade_ts_ms INTEGER,
        availability_ts_ms INTEGER,
        source_record_id TEXT,
        source_url TEXT,
        ingested_ts_ms INTEGER,
        short_volume REAL,
        short_exempt_volume REAL,
        total_volume REAL,
        market TEXT,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "finra_short_sale_volume",
        (
            ("id", "BIGSERIAL"),
            ("ts_ms", "INTEGER"),
            ("symbol", "TEXT"),
            ("trade_date", "TEXT"),
            ("trade_ts_ms", "INTEGER"),
            ("availability_ts_ms", "INTEGER"),
            ("source_record_id", "TEXT"),
            ("source_url", "TEXT"),
            ("ingested_ts_ms", "INTEGER"),
            ("short_volume", "REAL"),
            ("short_exempt_volume", "REAL"),
            ("total_volume", "REAL"),
            ("market", "TEXT"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_finra_short_sale_volume_source_record_id
    ON finra_short_sale_volume(source_record_id)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_finra_short_sale_volume_symbol_availability
    ON finra_short_sale_volume(symbol, availability_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_finra_short_sale_volume_symbol_trade_date
    ON finra_short_sale_volume(symbol, trade_date DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS finra_short_interest (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        symbol TEXT,
        settlement_date TEXT,
        settlement_ts_ms INTEGER,
        dissemination_date TEXT,
        dissemination_ts_ms INTEGER,
        availability_ts_ms INTEGER,
        source_record_id TEXT,
        ingested_ts_ms INTEGER,
        short_interest_shares REAL,
        days_to_cover REAL,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "finra_short_interest",
        (
            ("id", "BIGSERIAL"),
            ("ts_ms", "INTEGER"),
            ("symbol", "TEXT"),
            ("settlement_date", "TEXT"),
            ("settlement_ts_ms", "INTEGER"),
            ("dissemination_date", "TEXT"),
            ("dissemination_ts_ms", "INTEGER"),
            ("availability_ts_ms", "INTEGER"),
            ("source_record_id", "TEXT"),
            ("ingested_ts_ms", "INTEGER"),
            ("short_interest_shares", "REAL"),
            ("days_to_cover", "REAL"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_finra_short_interest_source_record_id
    ON finra_short_interest(source_record_id)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_finra_short_interest_symbol_availability
    ON finra_short_interest(symbol, availability_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_finra_short_interest_symbol_settlement
    ON finra_short_interest(symbol, settlement_ts_ms DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS crypto_funding_rates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ts_ms INTEGER,
        symbol TEXT,
        exchange TEXT,
        perp_market TEXT,
        spot_market TEXT,
        funding_ts_ms INTEGER,
        availability_ts_ms INTEGER,
        funding_rate REAL,
        mark_price REAL,
        index_price REAL,
        spot_price REAL,
        spot_ts_ms INTEGER,
        perp_ts_ms INTEGER,
        perp_basis_pct REAL,
        source_record_id TEXT,
        ingested_ts_ms INTEGER,
        is_live INTEGER,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "crypto_funding_rates",
        (
            ("id", "BIGSERIAL"),
            ("ts_ms", "INTEGER"),
            ("symbol", "TEXT"),
            ("exchange", "TEXT"),
            ("perp_market", "TEXT"),
            ("spot_market", "TEXT"),
            ("funding_ts_ms", "INTEGER"),
            ("availability_ts_ms", "INTEGER"),
            ("funding_rate", "REAL"),
            ("mark_price", "REAL"),
            ("index_price", "REAL"),
            ("spot_price", "REAL"),
            ("spot_ts_ms", "INTEGER"),
            ("perp_ts_ms", "INTEGER"),
            ("perp_basis_pct", "REAL"),
            ("source_record_id", "TEXT"),
            ("ingested_ts_ms", "INTEGER"),
            ("is_live", "INTEGER"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_crypto_funding_rates_source_record_id
    ON crypto_funding_rates(source_record_id)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_crypto_funding_rates_symbol_availability
    ON crypto_funding_rates(symbol, availability_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_crypto_funding_rates_symbol_funding
    ON crypto_funding_rates(symbol, funding_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_crypto_funding_rates_exchange_market
    ON crypto_funding_rates(exchange, perp_market, funding_ts_ms DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS macro_series_vintages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        series_id TEXT NOT NULL,
        obs_date TEXT NOT NULL,
        obs_ts_ms INTEGER,
        vintage_date TEXT NOT NULL,
        vintage_ts_ms INTEGER,
        realtime_end TEXT,
        value REAL,
        availability_ts_ms INTEGER NOT NULL,
        source TEXT,
        ingested_ts_ms INTEGER,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "macro_series_vintages",
        (
            ("id", "BIGSERIAL"),
            ("series_id", "TEXT"),
            ("obs_date", "TEXT"),
            ("obs_ts_ms", "INTEGER"),
            ("vintage_date", "TEXT"),
            ("vintage_ts_ms", "INTEGER"),
            ("realtime_end", "TEXT"),
            ("value", "REAL"),
            ("availability_ts_ms", "INTEGER"),
            ("source", "TEXT"),
            ("ingested_ts_ms", "INTEGER"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_macro_series_vintages_series_obs_vintage
    ON macro_series_vintages(series_id, obs_date, vintage_date)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_macro_series_vintages_series_availability
    ON macro_series_vintages(series_id, availability_ts_ms DESC)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_macro_series_vintages_series_obs
    ON macro_series_vintages(series_id, obs_ts_ms DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS macro_vintage_backfill_state (
        series_id TEXT PRIMARY KEY,
        status TEXT,
        last_vintage_date TEXT,
        updated_ts_ms INTEGER,
        cursor_json JSONB,
        error TEXT
    )
    """)
    _ensure_columns(
        cur,
        "macro_vintage_backfill_state",
        (
            ("series_id", "TEXT"),
            ("status", "TEXT"),
            ("last_vintage_date", "TEXT"),
            ("updated_ts_ms", "INTEGER"),
            ("cursor_json", "JSONB"),
            ("error", "TEXT"),
        ),
    )

    cur.execute("""
    CREATE TABLE IF NOT EXISTS news_story_embeddings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id INTEGER NOT NULL,
        symbol TEXT NOT NULL,
        publish_ts_ms INTEGER,
        availability_ts_ms INTEGER NOT NULL,
        source TEXT,
        embedding_backend TEXT NOT NULL,
        model_name TEXT NOT NULL,
        dim INTEGER NOT NULL,
        vector BLOB NOT NULL,
        text_hash TEXT,
        novelty_score REAL NOT NULL DEFAULT 1.0,
        max_similarity REAL NOT NULL DEFAULT 0.0,
        stale_flag INTEGER NOT NULL DEFAULT 0,
        matched_event_id INTEGER,
        ingested_ts_ms INTEGER,
        payload_json JSONB,
        diagnostics_json JSONB
    )
    """)
    _ensure_columns(
        cur,
        "news_story_embeddings",
        (
            ("id", "BIGSERIAL"),
            ("event_id", "INTEGER"),
            ("symbol", "TEXT"),
            ("publish_ts_ms", "INTEGER"),
            ("availability_ts_ms", "INTEGER"),
            ("source", "TEXT"),
            ("embedding_backend", "TEXT"),
            ("model_name", "TEXT"),
            ("dim", "INTEGER"),
            ("vector", "BLOB"),
            ("text_hash", "TEXT"),
            ("novelty_score", "REAL"),
            ("max_similarity", "REAL"),
            ("stale_flag", "INTEGER"),
            ("matched_event_id", "INTEGER"),
            ("ingested_ts_ms", "INTEGER"),
            ("payload_json", "JSONB"),
            ("diagnostics_json", "JSONB"),
        ),
    )
    cur.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_news_story_embeddings_event_space
    ON news_story_embeddings(event_id, symbol, embedding_backend, model_name)
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_news_story_embeddings_symbol_space_avail
    ON news_story_embeddings(symbol, embedding_backend, model_name, availability_ts_ms DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS news_flow_features (
        symbol TEXT NOT NULL,
        asof_ts_ms INTEGER NOT NULL,
        bucket_ts_ms INTEGER NOT NULL,
        embedding_backend TEXT NOT NULL,
        model_name TEXT NOT NULL,
        news_novelty_max_24h REAL NOT NULL DEFAULT 0.0,
        news_stale_share_24h REAL NOT NULL DEFAULT 0.0,
        news_velocity_z REAL NOT NULL DEFAULT 0.0,
        fresh_neg_news_flag REAL NOT NULL DEFAULT 0.0,
        event_count_24h INTEGER NOT NULL DEFAULT 0,
        source_max_availability_ts_ms INTEGER,
        created_ts_ms INTEGER,
        meta_json JSONB,
        PRIMARY KEY(symbol, asof_ts_ms, embedding_backend, model_name)
    )
    """)
    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_news_flow_features_symbol_asof
    ON news_flow_features(symbol, asof_ts_ms DESC)
    """)

    try:
        _ensure_columns(
            cur,
            "news_event_features",
            (
                ("payload_json", "JSONB"),
                ("embedding_backend", "TEXT"),
                ("embedding_model_name", "TEXT"),
                ("embedding_novelty_score", "REAL"),
                ("embedding_max_similarity", "REAL"),
                ("stale_flag", "INTEGER"),
                ("novelty_computed_ts_ms", "INTEGER"),
            ),
        )
    except Exception as e:
        _warn_nonfatal("REPAIR_SCHEMA_NEWS_EVENT_FEATURE_COLUMNS_FAILED", e)
    try:
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_news_event_features_event_id
        ON news_event_features(event_id)
        """)
    except Exception as e:
        _warn_nonfatal("REPAIR_SCHEMA_NEWS_EVENT_FEATURE_EVENT_ID_INDEX_FAILED", e)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ensemble_blend_weights (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_ts INTEGER NOT NULL,
        mode TEXT NOT NULL,
        regime TEXT,
        weights_json TEXT NOT NULL,
        meta_blob BLOB,
        meta_artifact_sha256 TEXT,
        meta_artifact_alias TEXT
    )
    """)
    cur.execute("PRAGMA table_info(ensemble_blend_weights)")
    ensemble_blend_weight_cols = {str(row[1]) for row in cur.fetchall() if row and len(row) > 1}
    if "meta_artifact_sha256" not in ensemble_blend_weight_cols:
        cur.execute("ALTER TABLE ensemble_blend_weights ADD COLUMN meta_artifact_sha256 TEXT")
    if "meta_artifact_alias" not in ensemble_blend_weight_cols:
        cur.execute("ALTER TABLE ensemble_blend_weights ADD COLUMN meta_artifact_alias TEXT")

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_ensemble_blend_weights_mode_created
    ON ensemble_blend_weights(mode, regime, created_ts DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ensemble_predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT NOT NULL,
        ts INTEGER NOT NULL,
        blended_prediction REAL NOT NULL,
        family_preds_json TEXT NOT NULL,
        weights_json TEXT NOT NULL,
        agreement REAL NOT NULL
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_symbol_ts
    ON ensemble_predictions(symbol, ts DESC)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_ensemble_predictions_ts
    ON ensemble_predictions(ts DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ensemble_family_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        window_start_ts INTEGER NOT NULL,
        window_end_ts INTEGER NOT NULL,
        family TEXT NOT NULL,
        n_predictions INTEGER NOT NULL,
        realized_sharpe REAL,
        hit_rate REAL
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_ensemble_family_performance_window
    ON ensemble_family_performance(window_end_ts DESC, family)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS model_oos_predictions (
        symbol TEXT NOT NULL,
        horizon INTEGER NOT NULL,
        family TEXT NOT NULL,
        ts INTEGER NOT NULL,
        prediction REAL NOT NULL,
        target REAL NULL,
        PRIMARY KEY(symbol, horizon, family, ts)
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_model_oos_predictions_lookup
    ON model_oos_predictions(symbol, horizon, ts)
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_model_oos_predictions_family_ts
    ON model_oos_predictions(family, ts)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS ensemble_weights (
        symbol TEXT NOT NULL,
        horizon INTEGER NOT NULL,
        ts INTEGER NOT NULL,
        weights_json TEXT NOT NULL,
        intercept REAL NOT NULL DEFAULT 0,
        alpha REAL NOT NULL DEFAULT 0,
        n_train_obs INTEGER NOT NULL DEFAULT 0,
        val_metric REAL NULL,
        PRIMARY KEY(symbol, horizon, ts)
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_ensemble_weights_lookup
    ON ensemble_weights(symbol, horizon, ts DESC)
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS temporal_model_eval (
        key_type TEXT NOT NULL,
        key TEXT NOT NULL,
        horizon_s INTEGER NOT NULL,
        model_kind TEXT NOT NULL,
        ts_ms INTEGER NOT NULL,
        n_train INTEGER NOT NULL,
        n_eval INTEGER NOT NULL,
        rmse REAL NOT NULL,
        spearman REAL NOT NULL,
        directional_acc REAL NOT NULL,
        PRIMARY KEY (key_type, key, horizon_s, model_kind)
    )
    """)

    cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_temporal_model_eval_ts
    ON temporal_model_eval(ts_ms DESC)
    """)

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_orders (
            client_order_id TEXT PRIMARY KEY,
            order_uid TEXT,
            broker TEXT NOT NULL DEFAULT 'unknown',
            portfolio_orders_id INTEGER,
            source_alert_id INTEGER,
            prediction_id INTEGER,
            model_id TEXT NOT NULL DEFAULT 'baseline',
            model_version TEXT,
            symbol TEXT NOT NULL DEFAULT '',
            qty REAL NOT NULL DEFAULT 0,
            submit_ts_ms INTEGER NOT NULL DEFAULT 0,
            ref_px REAL,
            expected_px REAL,
            mid_px REAL,
            bid_px REAL,
            ask_px REAL,
            spread_bps REAL,
            broker_order_id TEXT,
            status TEXT NOT NULL DEFAULT 'submitted',
            extra_json TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            client_order_id TEXT NOT NULL,
            fill_id TEXT,
            broker TEXT,
            model_id TEXT NOT NULL DEFAULT 'baseline',
            model_version TEXT,
            symbol TEXT NOT NULL DEFAULT '',
            ts_ms INTEGER,
            submit_ts_ms INTEGER,
            fill_ts_ms INTEGER NOT NULL DEFAULT 0,
            fill_qty REAL NOT NULL DEFAULT 0,
            fill_px REAL NOT NULL DEFAULT 0,
            expected_px REAL,
            mid_px REAL,
            bid_px REAL,
            ask_px REAL,
            spread_bps REAL,
            slippage_bps REAL,
            fill_latency_ms INTEGER,
            fees REAL,
            commission REAL,
            liquidity TEXT,
            raw_json TEXT,
            extra_json TEXT
        )
        """
    )

    def _table_columns(table_name: str) -> set[str]:
        return {
            str(row[1])
            for row in cur.execute(f"PRAGMA table_info({table_name})").fetchall()
            if len(row) >= 2 and str(row[1]).strip()
        }

    execution_order_columns = _table_columns("execution_orders")
    execution_order_additions = (
        ("client_order_id", "TEXT"),
        ("broker", "TEXT NOT NULL DEFAULT 'unknown'"),
        ("portfolio_orders_id", "INTEGER"),
        ("source_alert_id", "INTEGER"),
        ("order_uid", "TEXT"),
        ("prediction_id", "INTEGER"),
        ("model_id", "TEXT NOT NULL DEFAULT 'baseline'"),
        ("model_version", "TEXT"),
        ("symbol", "TEXT NOT NULL DEFAULT ''"),
        ("qty", "REAL NOT NULL DEFAULT 0"),
        ("submit_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("ref_px", "REAL"),
        ("expected_px", "REAL"),
        ("mid_px", "REAL"),
        ("bid_px", "REAL"),
        ("ask_px", "REAL"),
        ("spread_bps", "REAL"),
        ("broker_order_id", "TEXT"),
        ("status", "TEXT NOT NULL DEFAULT 'submitted'"),
        ("extra_json", "TEXT"),
    )
    for column_name, column_ddl in execution_order_additions:
        if column_name not in execution_order_columns:
            cur.execute(f"ALTER TABLE execution_orders ADD COLUMN {column_name} {column_ddl}")

    execution_fill_columns = _table_columns("execution_fills")
    execution_fill_additions = (
        ("client_order_id", "TEXT NOT NULL DEFAULT ''"),
        ("fill_id", "TEXT"),
        ("broker", "TEXT"),
        ("model_id", "TEXT NOT NULL DEFAULT 'baseline'"),
        ("model_version", "TEXT"),
        ("symbol", "TEXT NOT NULL DEFAULT ''"),
        ("ts_ms", "INTEGER"),
        ("submit_ts_ms", "INTEGER"),
        ("fill_ts_ms", "INTEGER NOT NULL DEFAULT 0"),
        ("fill_qty", "REAL NOT NULL DEFAULT 0"),
        ("fill_px", "REAL NOT NULL DEFAULT 0"),
        ("expected_px", "REAL"),
        ("mid_px", "REAL"),
        ("bid_px", "REAL"),
        ("ask_px", "REAL"),
        ("spread_bps", "REAL"),
        ("fill_latency_ms", "INTEGER"),
        ("commission", "REAL"),
        ("liquidity", "TEXT"),
        ("raw_json", "TEXT"),
        ("extra_json", "TEXT"),
    )
    for column_name, column_ddl in execution_fill_additions:
        if column_name not in execution_fill_columns:
            cur.execute(f"ALTER TABLE execution_fills ADD COLUMN {column_name} {column_ddl}")

    cur.execute(
        """
        UPDATE execution_orders
        SET model_id = COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')
        """
    )
    cur.execute(
        """
        UPDATE execution_fills
        SET model_id = COALESCE(NULLIF(TRIM(model_id), ''), 'baseline')
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_orders_submit_ts
        ON execution_orders(submit_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_orders_source_alert
        ON execution_orders(source_alert_id, submit_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_orders_prediction_submit_ts
        ON execution_orders(prediction_id, submit_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_orders_model_submit_ts
        ON execution_orders(model_id, submit_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_orders_symbol_submit_ts
        ON execution_orders(symbol, submit_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_orders_order_uid
        ON execution_orders(order_uid)
        """
    )
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_order_idempotency_client
        ON execution_orders(client_order_id)
        """
    )

    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_ts
        ON execution_fills(fill_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_client
        ON execution_fills(client_order_id, fill_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_model_ts
        ON execution_fills(model_id, fill_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_symbol_ts
        ON execution_fills(symbol, fill_ts_ms DESC)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_execution_fills_fill_id
        ON execution_fills(fill_id)
        """
    )


MIGRATIONS = {
    1: _apply_v1,
    2: _apply_v2,
    3: _apply_v3,
    4: _apply_v4,
    5: _apply_v5,
    6: _apply_v6,
}


def _apply_migration(cur, target_version: int, now: int) -> None:
    fn = MIGRATIONS.get(int(target_version))
    if fn is None:
        raise RuntimeError(f"missing_migration_for_version:{int(target_version)}")

    _mark_schema_version(
        cur,
        int(target_version),
        int(now),
        "running",
        f"migration_started_v{int(target_version)}",
    )

    try:
        fn(cur)
        _mark_schema_version_applied(
            cur,
            int(target_version),
            int(now),
            f"migration_applied_v{int(target_version)}",
        )
    except Exception as e:
        _mark_schema_version(
            cur,
            int(target_version),
            int(now),
            "failed",
            f"migration_failed_v{int(target_version)}:{e}",
        )
        raise


def _migrate_to_target(cur, start_version: int, target_version: int, now: int) -> int:
    current_version = int(start_version)

    while current_version < int(target_version):
        next_version = current_version + 1
        _apply_migration(cur, next_version, now)
        current_version = next_version

    return int(current_version)


def run(*, include_quick_check: bool = True):
    try:
        cfg = load_runtime_config()
        db_path = str(getattr(cfg, "db_path", "") or "").strip()
    except Exception as e:
        sys.stderr.write(
            f"[repair_schema] config_load_failed:{type(e).__name__}:{e}\n"
        )
        return {"ok": False, "error": f"config_load_failed: {e}"}

    if not db_path:
        return {"ok": False, "error": "DB_PATH not set"}

    db_path_obj = Path(db_path).expanduser().resolve()
    db_path_obj.parent.mkdir(parents=True, exist_ok=True)

    try:
        from engine.runtime import storage as storage_mod

        storage_mod.init_db()
    except Exception as e:
        sys.stderr.write(
            f"[repair_schema] init_db_failed:{type(e).__name__}:{e} db_path={db_path_obj}\n"
        )
        return {
            "ok": False,
            "error": f"init_db_failed: {e}",
            "db_path": str(db_path_obj),
        }

    if not bool(getattr(storage_mod, "_SQLITE_TEST_BACKEND", False)):
        try:
            from engine.runtime.schema.migrator import apply_migrations, expected_schema_version

            applied = apply_migrations()
            storage_mod.init_db()
            validation = dict(storage_mod.get_db_validation_snapshot(include_quick_check=include_quick_check) or {})
            ok = bool(validation.get("ok"))
            return {
                "ok": ok,
                "backend": str(validation.get("backend") or "postgres"),
                "db_path": str(db_path_obj),
                "applied_migrations": list(applied or []),
                "schema_version": validation.get("schema_version"),
                "expected_schema_version": validation.get("expected_schema_version") or expected_schema_version(),
                "schema_version_ok": bool(validation.get("schema_version_ok")),
                "validation": validation,
                "error": "" if ok else str(validation.get("error") or "postgres_schema_validation_failed"),
            }
        except Exception as e:
            sys.stderr.write(
                f"[repair_schema] postgres_repair_failed:{type(e).__name__}:{e} db_path={db_path_obj}\n"
            )
            return {
                "ok": False,
                "backend": "postgres",
                "error": f"postgres_repair_failed: {e}",
                "db_path": str(db_path_obj),
            }

    from engine.runtime.storage import connect_rw_direct

    conn = connect_rw_direct(timeout_s=30.0, busy_timeout_ms=60000)
    try:
        cur = conn.cursor()
        now = int(time.time() * 1000)

        _ensure_version_tables(cur)

        current_version = _read_effective_version(cur)
        current_version = _migrate_to_target(
            cur,
            start_version=int(current_version),
            target_version=int(SCHEMA_VERSION),
            now=int(now),
        )

        _ensure_required_runtime_tables(cur)
        _mark_schema_version_applied(cur, int(SCHEMA_VERSION), now, "full_schema_verified")
        _set_runtime_meta_schema_version(cur, int(SCHEMA_VERSION), now)

        required_tables = (
            "prices",
            "alerts",
            "portfolio_state",
            "job_heartbeats",
            "trades",
            "ensemble_blend_weights",
            "ensemble_predictions",
            "ensemble_family_performance",
            "model_oos_predictions",
            "ensemble_weights",
            "insider_transactions",
            "congressional_trades",
        )
        missing_tables = [
            str(name)
            for name in required_tables
            if not cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (str(name),),
            ).fetchone()
        ]
        if missing_tables:
            conn.rollback()
            return {
                "ok": False,
                "error": f"missing_required_tables:{','.join(missing_tables)}",
                "db_path": str(db_path_obj),
                "schema_version": int(current_version),
                "expected_schema_version": int(SCHEMA_VERSION),
            }

        quick_check = "skipped" if not include_quick_check else "unknown"
        if include_quick_check:
            row = cur.execute("PRAGMA quick_check;").fetchone()
            quick_check = str(row[0] or "unknown") if row else "unknown"
            if quick_check.lower() != "ok":
                conn.rollback()
                return {
                    "ok": False,
                    "error": f"quick_check_failed: {quick_check}",
                    "db_path": str(db_path_obj),
                    "schema_version": int(current_version),
                    "expected_schema_version": int(SCHEMA_VERSION),
                }

        conn.commit()
        return {
            "ok": True,
            "db_path": str(db_path_obj),
            "schema_version": int(current_version),
            "expected_schema_version": int(SCHEMA_VERSION),
            "quick_check": quick_check,
            "quick_check_skipped": bool(not include_quick_check),
            "required_tables": list(required_tables),
        }
    except Exception as e:
        conn.rollback()
        sys.stderr.write(
            f"[repair_schema] run_failed:{type(e).__name__}:{e} db_path={db_path_obj}\n"
        )
        return {
            "ok": False,
            "error": str(e),
            "db_path": str(db_path_obj),
        }
    finally:
        conn.close()


def main() -> int:
    result = run()
    if not isinstance(result, dict):
        print({"ok": False, "error": "invalid_repair_schema_result"})
        return 2
    print(result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
