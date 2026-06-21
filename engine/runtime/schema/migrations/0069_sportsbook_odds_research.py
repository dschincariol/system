"""Research-only sportsbook and betting-exchange odds snapshots."""

from __future__ import annotations


id = 69
description = "sportsbook odds research snapshots, explicit mappings, event studies, and promotion evidence"


def up(conn) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_snapshots (
          id BIGSERIAL PRIMARY KEY,
          provider TEXT NOT NULL,
          sport_key TEXT NOT NULL,
          league TEXT NOT NULL DEFAULT '',
          provider_event_id TEXT NOT NULL,
          provider_market_id TEXT NOT NULL DEFAULT '',
          event_category TEXT NOT NULL DEFAULT '',
          market_type TEXT NOT NULL,
          outcome_name TEXT NOT NULL,
          odds_format TEXT NOT NULL,
          odds_value DOUBLE PRECISION,
          raw_implied_probability DOUBLE PRECISION NOT NULL,
          no_vig_probability DOUBLE PRECISION NOT NULL,
          line DOUBLE PRECISION,
          spread DOUBLE PRECISION,
          total DOUBLE PRECISION,
          event_start_ts_ms BIGINT,
          source_ts_ms BIGINT NOT NULL,
          availability_ts_ms BIGINT NOT NULL,
          volume DOUBLE PRECISION,
          liquidity DOUBLE PRECISION,
          settlement_status TEXT NOT NULL DEFAULT '',
          settlement_ts_ms BIGINT,
          resolution_ts_ms BIGINT,
          raw_payload_hash TEXT NOT NULL,
          raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          created_ts_ms BIGINT NOT NULL,
          UNIQUE(provider, provider_event_id, provider_market_id, market_type, outcome_name, availability_ts_ms, raw_payload_hash)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_asset_mappings (
          mapping_key TEXT PRIMARY KEY,
          sport_key TEXT NOT NULL,
          league TEXT NOT NULL DEFAULT '',
          event_category TEXT NOT NULL DEFAULT '',
          market_type TEXT NOT NULL,
          asset_symbol TEXT NOT NULL DEFAULT '',
          research_label TEXT NOT NULL DEFAULT '',
          stage TEXT NOT NULL DEFAULT 'research',
          enabled BOOLEAN NOT NULL DEFAULT TRUE,
          allow_feature_use BOOLEAN NOT NULL DEFAULT FALSE,
          direct_trading_authority BOOLEAN NOT NULL DEFAULT FALSE,
          mapping_version TEXT NOT NULL DEFAULT 'v1',
          mapping_rationale TEXT NOT NULL DEFAULT '',
          owner TEXT NOT NULL DEFAULT '',
          approval_status TEXT NOT NULL DEFAULT 'research',
          approved_by TEXT NOT NULL DEFAULT '',
          approved_ts_ms BIGINT,
          approval_reason TEXT NOT NULL DEFAULT '',
          approved_for_promotion BOOLEAN NOT NULL DEFAULT FALSE,
          source_control_ref TEXT NOT NULL DEFAULT '',
          notes TEXT,
          created_ts_ms BIGINT NOT NULL,
          updated_ts_ms BIGINT NOT NULL,
          CHECK(stage IN ('research', 'shadow')),
          CHECK(approval_status IN ('research', 'pending', 'approved', 'rejected', 'disabled')),
          CHECK(direct_trading_authority = FALSE),
          CHECK(asset_symbol <> '' OR research_label <> '')
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_event_studies (
          id BIGSERIAL PRIMARY KEY,
          run_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          feature_group TEXT NOT NULL,
          mapping_key TEXT,
          symbol TEXT NOT NULL DEFAULT '',
          sport_key TEXT NOT NULL DEFAULT '',
          league TEXT NOT NULL DEFAULT '',
          market_type TEXT NOT NULL DEFAULT '',
          horizon_s BIGINT NOT NULL,
          latency_ms BIGINT NOT NULL,
          fee_bps DOUBLE PRECISION NOT NULL,
          slippage_bps DOUBLE PRECISION NOT NULL,
          sample_count BIGINT NOT NULL,
          mean_odds_move DOUBLE PRECISION NOT NULL,
          mean_forward_return DOUBLE PRECISION NOT NULL,
          mean_net_return DOUBLE PRECISION NOT NULL,
          hit_rate DOUBLE PRECISION NOT NULL,
          corr DOUBLE PRECISION,
          no_go_reason TEXT,
          direct_trading_authority BOOLEAN NOT NULL DEFAULT FALSE,
          evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          CHECK(direct_trading_authority = FALSE)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sportsbook_odds_promotion_evidence (
          evidence_key TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          ts_ms BIGINT NOT NULL,
          feature_group TEXT NOT NULL,
          mapping_key TEXT NOT NULL DEFAULT '',
          symbol TEXT NOT NULL DEFAULT '',
          mapping_version TEXT NOT NULL DEFAULT '',
          dataset_hash TEXT NOT NULL DEFAULT '',
          feature_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
          start_ts_ms BIGINT NOT NULL,
          end_ts_ms BIGINT NOT NULL,
          train_end_ts_ms BIGINT,
          test_start_ts_ms BIGINT,
          horizon_s BIGINT NOT NULL,
          latency_ms BIGINT NOT NULL,
          fee_bps DOUBLE PRECISION NOT NULL,
          slippage_bps DOUBLE PRECISION NOT NULL,
          sample_count BIGINT NOT NULL,
          oos_sample_count BIGINT NOT NULL,
          mean_net_return DOUBLE PRECISION NOT NULL,
          oos_mean_net_return DOUBLE PRECISION NOT NULL,
          hit_rate DOUBLE PRECISION NOT NULL,
          oos_hit_rate DOUBLE PRECISION NOT NULL,
          p_value DOUBLE PRECISION NOT NULL,
          fdr_q DOUBLE PRECISION NOT NULL,
          benchmark_symbol TEXT NOT NULL DEFAULT '',
          benchmark_corr DOUBLE PRECISION,
          provider_readiness_passed BOOLEAN NOT NULL DEFAULT FALSE,
          oos_passed BOOLEAN NOT NULL DEFAULT FALSE,
          net_after_cost_passed BOOLEAN NOT NULL DEFAULT FALSE,
          pit_passed BOOLEAN NOT NULL DEFAULT FALSE,
          deconfounded_passed BOOLEAN NOT NULL DEFAULT FALSE,
          production_readiness_passed BOOLEAN NOT NULL DEFAULT FALSE,
          approval_passed BOOLEAN NOT NULL DEFAULT FALSE,
          passed BOOLEAN NOT NULL DEFAULT FALSE,
          no_go_reason TEXT NOT NULL DEFAULT '',
          direct_trading_authority BOOLEAN NOT NULL DEFAULT FALSE,
          evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
          CHECK(direct_trading_authority = FALSE),
          CHECK(
            passed = FALSE OR (
              provider_readiness_passed = TRUE
              AND oos_passed = TRUE
              AND net_after_cost_passed = TRUE
              AND pit_passed = TRUE
              AND deconfounded_passed = TRUE
              AND production_readiness_passed = TRUE
              AND approval_passed = TRUE
            )
          )
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_snapshots_lookup
          ON sportsbook_odds_snapshots(sport_key, league, event_category, market_type, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_snapshots_event
          ON sportsbook_odds_snapshots(provider, provider_event_id, market_type, outcome_name, availability_ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_mappings_asset
          ON sportsbook_odds_asset_mappings(asset_symbol, enabled, allow_feature_use)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_mappings_approval
          ON sportsbook_odds_asset_mappings(approval_status, approved_for_promotion, asset_symbol)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_event_studies_run
          ON sportsbook_odds_event_studies(run_id, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_promotion_evidence_symbol
          ON sportsbook_odds_promotion_evidence(symbol, passed, ts_ms DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sportsbook_odds_promotion_evidence_mapping
          ON sportsbook_odds_promotion_evidence(mapping_key, passed, ts_ms DESC)
        """
    )
