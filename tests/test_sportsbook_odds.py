import json
import math
import sqlite3

import pytest

from engine.data.sportsbook_odds import (
    SPORTSBOOK_ODDS_FEATURE_GROUP,
    SPORTSBOOK_ODDS_FEATURE_IDS,
    SPORTSBOOK_ODDS_FEATURE_PREFIX,
    approved_sportsbook_odds_mappings,
    ensure_sportsbook_odds_schema,
    evaluate_sportsbook_odds_go_gate,
    fetch_sportsbook_odds_batch,
    inventory_sportsbook_relevant_universe,
    normalize_event_odds,
    normalize_multi_outcome_probabilities,
    normalize_sportsbook_mapping,
    put_sportsbook_odds_batch,
    remove_vig,
    resolve_sportsbook_odds_snapshot,
    run_sportsbook_odds_event_study,
    run_sportsbook_odds_promotion_research,
    sportsbook_odds_provider_readiness,
    validate_sportsbook_odds_read_only_settings,
)
from engine.runtime.job_registry import ALLOWED_JOBS
from engine.strategy.feature_registry import (
    FEATURE_STAGE_SHADOW,
    assert_no_shadow_features,
    default_feature_ids,
    feature_set_tag_from_ids,
    feature_stage,
    list_groups,
)
from engine.strategy.model_feature_snapshots import build_model_feature_snapshot
from engine.strategy.promotion_guard import evaluate_sportsbook_odds_feature_promotion_gate
from services.data_source_manager import MANAGED_DAEMON_JOBS, _default_catalog


NOW = 1_700_000_000_000


def _mapping(**overrides):
    item = {
        "sport_key": "basketball_nba",
        "league": "nba",
        "event_category": "nba_regular_season",
        "market_type": "moneyline",
        "asset_symbol": "DKNG",
        "stage": "shadow",
        "allow_feature_use": True,
        "direct_trading_authority": False,
    }
    item.update(overrides)
    return item


def _approved_mapping(**overrides):
    item = _mapping(
        approval_status="approved",
        approved_for_promotion=True,
        approved_by="risk",
        approved_ts_ms=NOW - 1_000,
        approval_reason="narrow sportsbook equity event-study mapping",
        mapping_rationale="DKNG is a sportsbook equity in the explicit allowlist",
        mapping_version="unit-v1",
        owner="research",
    )
    item.update(overrides)
    return item


def _event(ts_ms=NOW, *, event_id="evt1", home_probability=None):
    outcomes = [
        {"outcome": "home", "odds": -110},
        {"outcome": "away", "odds": -110},
    ]
    if home_probability is not None:
        outcomes = [
            {"outcome": "home", "raw_implied_probability": float(home_probability)},
            {"outcome": "away", "raw_implied_probability": float(1.0 - home_probability)},
        ]
    return {
        "provider": "unitbook",
        "sport_key": "basketball_nba",
        "league": "nba",
        "event_category": "nba_regular_season",
        "provider_event_id": event_id,
        "provider_market_id": "mkt1",
        "market_type": "moneyline",
        "outcomes": outcomes,
        "source_ts_ms": int(ts_ms),
        "availability_ts_ms": int(ts_ms),
    }


def test_vig_removal_and_multi_outcome_normalization():
    assert remove_vig([0.55, 0.55]) == pytest.approx([0.5, 0.5])

    rows = normalize_multi_outcome_probabilities(
        [
            {"outcome": "home", "odds_format": "decimal", "odds": 2.0},
            {"outcome": "draw", "odds_format": "decimal", "odds": 3.5},
            {"outcome": "away", "odds_format": "decimal", "odds": 3.5},
        ]
    )
    assert sum(float(row["no_vig_probability"]) for row in rows) == pytest.approx(1.0)
    assert rows[0]["raw_implied_probability"] > rows[0]["no_vig_probability"]


def test_odds_normalization_strips_future_settlement_status():
    future_settlement = normalize_event_odds(
        {
            **_event(NOW),
            "settlement_status": "final",
            "resolution_ts_ms": NOW + 60_000,
        },
        now_ms=NOW,
    )
    assert {row["settlement_status"] for row in future_settlement} == {""}

    resolved = normalize_event_odds(
        {
            **_event(NOW + 120_000, event_id="evt2"),
            "settlement_status": "final",
            "resolution_ts_ms": NOW + 60_000,
        },
        now_ms=NOW + 120_000,
    )
    assert {row["settlement_status"] for row in resolved} == {"final"}


def test_mapping_allowlist_blocks_broad_market_credentials_and_unmapped_events():
    with pytest.raises(ValueError, match="asset_not_in_narrow_allowlist:SPY"):
        normalize_sportsbook_mapping(_mapping(asset_symbol="SPY"), now_ms=NOW)

    with pytest.raises(ValueError, match="direct_trading_authority_forbidden"):
        normalize_sportsbook_mapping(_mapping(direct_trading_authority=True), now_ms=NOW)

    with pytest.raises(ValueError, match="execution_credentials_forbidden"):
        validate_sportsbook_odds_read_only_settings({"wager": "10"}, {"api_key": "ok"})

    with pytest.raises(ValueError, match="execution_credentials_forbidden"):
        validate_sportsbook_odds_read_only_settings({"order_endpoint": "https://example.invalid"}, {"api_key": "ok"})

    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    put_sportsbook_odds_batch(con, odds=normalize_event_odds(_event(NOW), now_ms=NOW), mappings=[], now_ms=NOW)
    features, meta, available = resolve_sportsbook_odds_snapshot(con, symbol="DKNG", ts_ms=NOW)
    assert not available
    assert features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}available"] == 0.0
    assert meta["unavailable_reason"] == "asset_mapping_missing_or_no_pit_odds"


def test_mapping_approval_requires_narrow_asset_and_audit_fields():
    with pytest.raises(ValueError, match="promotion_requires_approved_status"):
        normalize_sportsbook_mapping(_mapping(approved_for_promotion=True), now_ms=NOW)

    with pytest.raises(ValueError, match="approved_mapping_requires_approved_by"):
        normalize_sportsbook_mapping(_approved_mapping(approved_by=""), now_ms=NOW)

    approved = normalize_sportsbook_mapping(_approved_mapping(), now_ms=NOW)
    assert approved["approval_status"] == "approved"
    assert approved["approved_for_promotion"] is True
    assert approved["mapping_version"] == "unit-v1"
    assert approved["direct_trading_authority"] is False

    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    con.execute(
        """
        INSERT INTO sportsbook_odds_asset_mappings (
          mapping_key, sport_key, league, event_category, market_type, asset_symbol,
          research_label, stage, enabled, allow_feature_use, direct_trading_authority,
          mapping_version, mapping_rationale, owner, approval_status, approved_by,
          approved_ts_ms, approval_reason, approved_for_promotion, source_control_ref,
          notes, created_ts_ms, updated_ts_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "bad-direct-sql",
            "basketball_nba",
            "nba",
            "nba_regular_season",
            "moneyline",
            "DKNG",
            "",
            "shadow",
            True,
            True,
            False,
            "unit-v1",
            "",
            "",
            "approved",
            "",
            NOW,
            "",
            True,
            "",
            "",
            NOW,
            NOW,
        ),
    )
    assert approved_sportsbook_odds_mappings(con, symbols=["DKNG"]) == []


def test_provider_readiness_flags_stale_settlement_and_no_vig_failures():
    rows = normalize_event_odds(_event(NOW), now_ms=NOW)
    readiness = sportsbook_odds_provider_readiness(rows, [_approved_mapping()], now_ms=NOW, require_approved_mapping=True)
    assert readiness["passed"] is True
    assert readiness["approved_mapping_count"] == 1

    stale = sportsbook_odds_provider_readiness(rows, [_approved_mapping()], now_ms=NOW + 3_600_000, stale_threshold_ms=60_000)
    assert stale["passed"] is False
    assert "stale_odds_rows" in stale["blockers"]

    bad_no_vig = [dict(rows[0])]
    readiness_bad = sportsbook_odds_provider_readiness(bad_no_vig, [_approved_mapping()], now_ms=NOW)
    assert readiness_bad["passed"] is False
    assert "no_vig_market_normalization_failed" in readiness_bad["blockers"]

    settlement = [dict(row) for row in rows]
    settlement[0]["settlement_status"] = "final"
    settlement[0]["resolution_ts_ms"] = NOW + 10_000
    readiness_settlement = sportsbook_odds_provider_readiness(settlement, [_approved_mapping()], now_ms=NOW)
    assert readiness_settlement["passed"] is False
    assert "settlement_lookahead_rows" in readiness_settlement["blockers"]


def test_historical_file_fetch_is_read_only_and_reports_readiness(tmp_path):
    missing_path = tmp_path / "missing_odds.json"
    with pytest.raises(FileNotFoundError):
        fetch_sportsbook_odds_batch(
            settings={
                "provider": "unitbook",
                "file_path": str(missing_path),
                "asset_mapping_json": json.dumps([_mapping()]),
            },
            credentials={"api_key": "read-only"},
            now_ms=NOW,
        )

    payload = [{"provider": "unitbook", **_event(NOW, home_probability=0.52)}]
    path = tmp_path / "odds.json"
    path.write_text(json.dumps({"events": payload}), encoding="utf-8")

    batch = fetch_sportsbook_odds_batch(
        settings={
            "provider": "unitbook",
            "file_path": str(path),
            "asset_mapping_json": json.dumps([_mapping()]),
        },
        credentials={"api_key": "read-only"},
        now_ms=NOW,
    )

    assert len(batch["odds"]) == 2
    assert batch["provider_state"]["read_only"] is True
    assert batch["provider_state"]["direct_trading_authority"] is False
    assert batch["provider_state"]["readiness_passed"] is True


def test_resolver_requires_explicit_mapping_and_uses_no_vig_probabilities():
    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    rows = normalize_event_odds(_event(NOW), now_ms=NOW)
    put_sportsbook_odds_batch(con, odds=rows, mappings=[_mapping()], now_ms=NOW)

    features, meta, available = resolve_sportsbook_odds_snapshot(con, symbol="DKNG", ts_ms=NOW)

    assert available
    assert features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}available"] == 1.0
    assert features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}no_vig_probability_level"] == pytest.approx(0.5)
    assert rows[0]["raw_implied_probability"] > features[f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}no_vig_probability_level"]
    assert meta["direct_trading_authority"] is False
    assert meta["requires_explicit_mapping"] is True


def test_stale_odds_and_settlement_lookahead_block_model_snapshot():
    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    put_sportsbook_odds_batch(
        con,
        odds=normalize_event_odds(_event(NOW), now_ms=NOW),
        mappings=[_mapping()],
        now_ms=NOW,
    )

    stale_ts = NOW + 61 * 60 * 1000
    snap = build_model_feature_snapshot(
        symbol="DKNG",
        ts_ms=stale_ts,
        feature_ids=list(SPORTSBOOK_ODDS_FEATURE_IDS),
        con=con,
    )

    assert snap["availability"][SPORTSBOOK_ODDS_FEATURE_GROUP] is False
    assert snap["features"][f"{SPORTSBOOK_ODDS_FEATURE_PREFIX}available"] == 0.0
    reasons = snap["pit_controls"][SPORTSBOOK_ODDS_FEATURE_GROUP]["reason_codes"]
    assert "feature_stale" in reasons


def test_research_only_feature_staging_registry_job_and_control_plane():
    assert all(feature_stage(fid) == FEATURE_STAGE_SHADOW for fid in SPORTSBOOK_ODDS_FEATURE_IDS)
    with pytest.raises(ValueError, match="shadow_features_forbidden"):
        assert_no_shadow_features(SPORTSBOOK_ODDS_FEATURE_IDS, context="live_model_serving", model_name="unit")

    defaults = default_feature_ids()
    assert not any(str(fid).startswith(SPORTSBOOK_ODDS_FEATURE_PREFIX) for fid in defaults)
    assert "sports_odds_sector_v1_shadow" in feature_set_tag_from_ids(list(SPORTSBOOK_ODDS_FEATURE_IDS))

    metadata = list_groups()[SPORTSBOOK_ODDS_FEATURE_GROUP]
    assert metadata["default_enabled"] is False
    assert metadata["direct_trading_authority"] is False
    assert metadata["research_only"] is True
    assert metadata["broad_market_default_allowed"] is False

    poll_spec = ALLOWED_JOBS["poll_sportsbook_odds"]
    backfill_spec = ALLOWED_JOBS["backfill_sportsbook_odds_event_study"]
    assert poll_spec[3]["execution"] is False
    assert poll_spec[3]["direct_trading_authority"] is False
    assert backfill_spec[3]["execution"] is False
    assert backfill_spec[3]["direct_trading_authority"] is False
    assert "poll_sportsbook_odds" in MANAGED_DAEMON_JOBS

    source = _default_catalog()["sportsbook_odds_research"]
    assert source.default_enabled is False
    assert source.source_type == "odds_provider"
    assert source.job_name == "poll_sportsbook_odds"


def test_event_study_is_research_only_and_net_after_cost():
    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    con.execute("CREATE TABLE prices (symbol TEXT NOT NULL, ts_ms INTEGER NOT NULL, price REAL NOT NULL)")
    con.executemany(
        "INSERT INTO prices(symbol, ts_ms, price) VALUES (?, ?, ?)",
        [("DKNG", NOW + 1_000, 10.0), ("DKNG", NOW + 2_000, 10.2), ("DKNG", NOW + 3_000, 10.4)],
    )
    put_sportsbook_odds_batch(
        con,
        odds=normalize_event_odds(_event(NOW, home_probability=0.50), now_ms=NOW),
        mappings=[_mapping()],
        now_ms=NOW,
    )
    put_sportsbook_odds_batch(
        con,
        odds=normalize_event_odds(_event(NOW + 1_000, home_probability=0.60), now_ms=NOW + 1_000),
        mappings=[],
        now_ms=NOW + 1_000,
    )

    result = run_sportsbook_odds_event_study(
        con,
        symbols=["DKNG"],
        start_ts_ms=NOW,
        end_ts_ms=NOW + 2_000,
        horizon_s=1,
        latency_ms=0,
        fee_bps=1.0,
        slippage_bps=5.0,
        persist=True,
        run_id="unit",
    )

    assert result["research_only"] is True
    assert result["direct_trading_authority"] is False
    assert result["no_go_for_production_features"] is True
    assert result["summaries"]
    row = con.execute("SELECT direct_trading_authority, evidence_json FROM sportsbook_odds_event_studies").fetchone()
    assert row[0] in (False, 0)
    assert "research_only" in row[1]
    assert all(math.isfinite(float(summary["mean_net_return"])) for summary in result["summaries"])


def test_promotion_gate_fails_without_approved_mapping_and_passing_evidence():
    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    put_sportsbook_odds_batch(
        con,
        odds=normalize_event_odds(_event(NOW), now_ms=NOW),
        mappings=[_mapping()],
        now_ms=NOW,
    )

    go, payload = evaluate_sportsbook_odds_go_gate(
        con,
        feature_ids=list(SPORTSBOOK_ODDS_FEATURE_IDS),
        symbols=["DKNG"],
    )
    assert go is False
    assert "approved_mapping_missing" in payload["blockers"]
    assert "passing_promotion_evidence_missing" in payload["blockers"]

    string_go, string_payload = evaluate_sportsbook_odds_go_gate(
        con,
        feature_ids=SPORTSBOOK_ODDS_FEATURE_IDS[0],
        symbols=["DKNG"],
    )
    assert string_go is False
    assert string_payload["applied"] is True
    assert "approved_mapping_missing" in string_payload["blockers"]

    guard_ok, guard_payload = evaluate_sportsbook_odds_feature_promotion_gate(
        feature_ids=SPORTSBOOK_ODDS_FEATURE_IDS[0],
        symbols=["DKNG"],
        con=con,
    )
    assert guard_ok is False
    assert guard_payload["no_go_for_production_features"] is True


def test_promotion_research_persists_go_candidate_only_after_all_checks_pass():
    con = sqlite3.connect(":memory:")
    ensure_sportsbook_odds_schema(con)
    con.execute("CREATE TABLE prices (symbol TEXT NOT NULL, ts_ms INTEGER NOT NULL, price REAL NOT NULL)")
    price_rows = [
        ("DKNG", NOW, 10.0),
        ("DKNG", NOW + 1_000, 10.1),
        ("DKNG", NOW + 2_000, 10.4),
        ("DKNG", NOW + 3_000, 10.8),
        ("DKNG", NOW + 4_000, 11.3),
        ("SPY", NOW, 100.0),
        ("SPY", NOW + 1_000, 100.2),
        ("SPY", NOW + 2_000, 100.7),
        ("SPY", NOW + 3_000, 100.9),
        ("SPY", NOW + 4_000, 101.8),
    ]
    con.executemany("INSERT INTO prices(symbol, ts_ms, price) VALUES (?, ?, ?)", price_rows)
    for idx, probability in enumerate([0.50, 0.55, 0.60, 0.65]):
        ts_ms = NOW + idx * 1_000
        rows = normalize_event_odds(_event(ts_ms, home_probability=probability), now_ms=ts_ms)
        put_sportsbook_odds_batch(
            con,
            odds=rows,
            mappings=[_approved_mapping()] if idx == 0 else [],
            now_ms=ts_ms,
        )

    result = run_sportsbook_odds_promotion_research(
        con,
        symbols=["DKNG"],
        start_ts_ms=NOW,
        end_ts_ms=NOW + 3_000,
        horizon_s=1,
        latency_ms=0,
        fee_bps=0.0,
        slippage_bps=0.0,
        train_fraction=0.34,
        min_oos_samples=1,
        min_oos_mean_net_return=-1.0,
        min_oos_hit_rate=0.0,
        max_fdr_q=1.0,
        max_abs_benchmark_corr=1.0,
        persist=True,
        run_id="unit-promotion",
    )

    assert result["no_go_for_production_features"] is False
    evidence = result["evidence"][0]
    assert evidence["passed"] is True
    assert evidence["dataset_hash"]
    assert evidence["mapping_version"] == "unit-v1"
    row = con.execute(
        "SELECT passed, direct_trading_authority, dataset_hash FROM sportsbook_odds_promotion_evidence"
    ).fetchone()
    assert row[0] in (True, 1)
    assert row[1] in (False, 0)
    assert row[2] == evidence["dataset_hash"]

    go, payload = evaluate_sportsbook_odds_go_gate(
        con,
        feature_ids=list(SPORTSBOOK_ODDS_FEATURE_IDS),
        symbols=["DKNG"],
    )
    assert go is True
    assert payload["no_go_for_production_features"] is False


def test_inventory_reports_no_go_without_sports_assets(tmp_path):
    con = sqlite3.connect(":memory:")
    missing = inventory_sportsbook_relevant_universe(con)
    assert missing["no_go_for_production_features"] is True

    wildcard_config = tmp_path / "wildcard_model_configs.json"
    wildcard_config.write_text(
        json.dumps([{"family": "unit", "instance_name": "broad", "symbol_universe": ["*"], "enabled": True}]),
        encoding="utf-8",
    )
    wildcard_only = inventory_sportsbook_relevant_universe(con, model_config_paths=[wildcard_config])
    assert wildcard_only["no_go_for_production_features"] is True
    assert wildcard_only["model_config_matched_symbols"] == []
    assert wildcard_only["model_config_inventory"]["wildcard_model_configs"] == ["unit:broad"]

    explicit_config = tmp_path / "explicit_model_configs.json"
    explicit_config.write_text(
        json.dumps(
            [
                {
                    "family": "unit",
                    "instance_name": "sportsbook_watch",
                    "symbol_universe": ["DKNG", "SPY", "*"],
                    "feature_groups": ["sports_odds_sector_v1"],
                    "enabled": True,
                    "prediction_enabled": True,
                }
            ]
        ),
        encoding="utf-8",
    )
    explicit = inventory_sportsbook_relevant_universe(sqlite3.connect(":memory:"), model_config_paths=[explicit_config])
    assert explicit["no_go_for_production_features"] is False
    assert explicit["matched_symbols"] == ["DKNG"]
    assert explicit["model_config_matched_symbols"] == ["DKNG"]
    assert explicit["model_config_inventory"]["explicit_non_narrow_symbols"] == [
        {"model_config": "unit:sportsbook_watch", "symbols": ["SPY"]}
    ]

    con.execute(
        """
        CREATE TABLE symbols (
          symbol TEXT,
          status TEXT,
          asset_class TEXT,
          score REAL,
          meta_json TEXT,
          updated_ts_ms INTEGER
        )
        """
    )
    con.execute(
        "INSERT INTO symbols(symbol, status, asset_class, score, meta_json, updated_ts_ms) VALUES (?, ?, ?, ?, ?, ?)",
        ("SPY", "ACTIVE", "EQUITY", 1.0, "{}", NOW),
    )
    assert inventory_sportsbook_relevant_universe(con)["no_go_for_production_features"] is True

    con.execute(
        "INSERT INTO symbols(symbol, status, asset_class, score, meta_json, updated_ts_ms) VALUES (?, ?, ?, ?, ?, ?)",
        ("DKNG", "WATCH", "EQUITY", 0.5, "{}", NOW),
    )
    matched = inventory_sportsbook_relevant_universe(con)
    assert matched["no_go_for_production_features"] is False
    assert "DKNG" in matched["matched_symbols"]
