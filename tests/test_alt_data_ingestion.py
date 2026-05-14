from __future__ import annotations

import importlib
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _reload_modules(*module_names: str):
    modules = []
    for name in module_names:
        module = importlib.import_module(name)
        modules.append(importlib.reload(module))
    return modules


def _reload_runtime_modules(*module_names: str):
    return _reload_modules("engine.runtime.config_schema", "engine.runtime.config", *module_names)


class AltDataIngestionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.prev_env = {
            key: os.environ.get(key)
            for key in (
                "DB_PATH",
                "USE_FORM4_DATA",
                "USE_CONGRESSIONAL_TRADE_DATA",
                "USE_SYMBOL_SNAPSHOT_FEATURES",
                "INGEST_FORM4_ENABLED",
                "INGEST_CONGRESSIONAL_ENABLED",
                "FORM4_BACKFILL_DAYS",
                "CONGRESSIONAL_BACKFILL_DAYS",
            )
        }
        os.environ["DB_PATH"] = str(Path(self.tmp.name) / "alt_data.db")
        os.environ["USE_SYMBOL_SNAPSHOT_FEATURES"] = "1"
        os.environ["INGEST_FORM4_ENABLED"] = "0"
        os.environ["INGEST_CONGRESSIONAL_ENABLED"] = "0"

    def tearDown(self) -> None:
        try:
            (storage,) = _reload_modules("engine.runtime.storage")
            storage.close_pooled_connections()
        except Exception:
            pass
        for key, value in self.prev_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        self.tmp.cleanup()

    def _init_storage(self):
        _, _, storage = _reload_runtime_modules("engine.runtime.storage")
        storage.init_db()
        return storage

    def test_init_db_materializes_alt_data_tables(self) -> None:
        storage = self._init_storage()
        con = storage.connect(readonly=True)
        try:
            tables = {
                str(row[0] or "")
                for row in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            }
        finally:
            con.close()

        self.assertIn("insider_transactions", tables)
        self.assertIn("congressional_trades", tables)

    def test_parse_form4_xml_normalizes_transactions(self) -> None:
        (form4_live,) = _reload_modules("engine.data.sec.form4_live")
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<ownershipDocument>
  <documentType>4</documentType>
  <periodOfReport>2026-04-10</periodOfReport>
  <issuer>
    <issuerCik>0000320193</issuerCik>
    <issuerName>Apple Inc.</issuerName>
    <issuerTradingSymbol>AAPL</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId>
      <rptOwnerCik>0001214156</rptOwnerCik>
      <rptOwnerName>Jane Insider</rptOwnerName>
    </reportingOwnerId>
    <reportingOwnerRelationship>
      <isDirector>1</isDirector>
      <isOfficer>1</isOfficer>
      <isTenPercentOwner>0</isTenPercentOwner>
      <isOther>0</isOther>
      <officerTitle>Chief Executive Officer</officerTitle>
    </reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionDate><value>2026-04-09</value></transactionDate>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>100</value></transactionShares>
        <transactionPricePerShare><value>175.50</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
      <ownershipNature>
        <directOrIndirectOwnership><value>D</value></directOrIndirectOwnership>
      </ownershipNature>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""

        rows = form4_live.parse_form4_xml(
            xml,
            filing={
                "accession": "0000320193-26-000001",
                "filed_date": "2026-04-10",
                "primary_doc_url": "https://www.sec.gov/Archives/test.xml",
            },
            filing_symbol="AAPL",
            allowed_symbols=["AAPL"],
            ingested_ts_ms=1_775_000_000_000,
        )

        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(str(row["symbol"]), "AAPL")
        self.assertEqual(str(row["transaction_type"]), "purchase")
        self.assertEqual(str(row["direction"]), "buy")
        self.assertEqual(float(row["shares"]), 100.0)
        self.assertEqual(float(row["price"]), 175.5)
        self.assertEqual(float(row["value"]), 17_550.0)
        self.assertEqual(str(row["security_type"]), "non_derivative")
        self.assertEqual(str(row["filing_accession"]), "0000320193-26-000001")
        self.assertIn("director", str(row["insider_role"]))
        self.assertIn("officer", str(row["insider_role"]))
        self.assertEqual(str(row["insider_title"]), "Chief Executive Officer")
        self.assertEqual(str(row["resolution_status"]), "resolved")
        self.assertEqual(str(row["resolution_method"]), "issuer_trading_symbol")
        self.assertTrue(str(row["entity_id"]).startswith("cik:"))

    def test_congressional_trade_normalization_uses_company_name_fallback(self) -> None:
        (congressional_trades,) = _reload_modules("engine.data.congressional_trades")
        with patch.object(
            congressional_trades,
            "infer_symbols",
            return_value={
                "symbols": ["MSFT"],
                "match_method": {"MSFT": "company_name:microsoft"},
                "match_confidence": {"MSFT": 0.91},
            },
        ):
            row = congressional_trades.normalize_congressional_trade_record(
                {
                    "representative": "Rep. Example",
                    "asset_description": "Microsoft Corporation",
                    "type": "Purchase",
                    "amount": "$1,001 - $15,000",
                    "transaction_date": "2026-04-01",
                    "disclosure_date": "2026-04-05",
                },
                source_name="house_stock_watcher",
                default_chamber="house",
                allowed_symbols=["AAPL", "MSFT"],
                ingested_ts_ms=1_775_000_100_000,
            )

        self.assertEqual(str(row["symbol"]), "MSFT")
        self.assertEqual(str(row["transaction_type"]), "purchase")
        self.assertEqual(str(row["direction"]), "buy")
        self.assertEqual(str(row["chamber"]), "house")
        self.assertEqual(float(row["amount_low"]), 1001.0)
        self.assertEqual(float(row["amount_high"]), 15000.0)
        self.assertAlmostEqual(float(row["amount_mid"]), 8000.5)
        self.assertEqual(str(row["transaction_date"]), "2026-04-01")
        self.assertEqual(str(row["disclosure_date"]), "2026-04-05")
        self.assertEqual(str(row["resolution_status"]), "resolved")
        self.assertEqual(str(row["resolution_method"]), "company_name:microsoft")
        self.assertEqual(str(row["entity_id"]), "symbol:MSFT")

    def test_persistence_upserts_are_idempotent(self) -> None:
        storage = self._init_storage()
        storage.put_insider_transaction(
            {
                "source_transaction_id": "form4:abc",
                "created_ts_ms": 1_775_000_000_000,
                "ingested_ts_ms": 1_775_000_000_000,
                "symbol": "AAPL",
                "transaction_type": "purchase",
                "direction": "buy",
                "shares": 10,
                "price": 100.0,
                "value": 1000.0,
                "transaction_date": "2026-04-01",
                "transaction_ts_ms": 1_774_000_000_000,
            }
        )
        storage.put_insider_transaction(
            {
                "source_transaction_id": "form4:abc",
                "created_ts_ms": 1_775_000_000_000,
                "ingested_ts_ms": 1_775_100_000_000,
                "symbol": "AAPL",
                "transaction_type": "purchase",
                "direction": "buy",
                "shares": 10,
                "price": 110.0,
                "value": 1100.0,
                "transaction_date": "2026-04-01",
                "transaction_ts_ms": 1_774_000_000_000,
            }
        )
        storage.put_congressional_trade(
            {
                "source_trade_id": "congress:abc",
                "created_ts_ms": 1_775_000_000_000,
                "ingested_ts_ms": 1_775_000_000_000,
                "symbol": "MSFT",
                "politician_name": "Rep. Example",
                "transaction_type": "purchase",
                "direction": "buy",
                "amount_range": "$1,001 - $15,000",
                "amount_low": 1001.0,
                "amount_high": 15000.0,
                "amount_mid": 8000.5,
                "transaction_date": "2026-04-01",
                "transaction_ts_ms": 1_774_000_000_000,
                "disclosure_date": "2026-04-05",
                "disclosure_ts_ms": 1_774_300_000_000,
            }
        )
        storage.put_congressional_trade(
            {
                "source_trade_id": "congress:abc",
                "created_ts_ms": 1_775_000_000_000,
                "ingested_ts_ms": 1_775_100_000_000,
                "symbol": "MSFT",
                "politician_name": "Rep. Example",
                "transaction_type": "sale",
                "direction": "sell",
                "amount_range": "$1,001 - $15,000",
                "amount_low": 1001.0,
                "amount_high": 15000.0,
                "amount_mid": 8000.5,
                "transaction_date": "2026-04-01",
                "transaction_ts_ms": 1_774_000_000_000,
                "disclosure_date": "2026-04-05",
                "disclosure_ts_ms": 1_774_300_000_000,
            }
        )

        con = storage.connect(readonly=True)
        try:
            insider_count = con.execute("SELECT COUNT(*) FROM insider_transactions").fetchone()
            congress_count = con.execute("SELECT COUNT(*) FROM congressional_trades").fetchone()
            insider_value = con.execute(
                "SELECT value, ingested_ts_ms FROM insider_transactions WHERE source_transaction_id=?",
                ("form4:abc",),
            ).fetchone()
            congress_direction = con.execute(
                "SELECT direction, ingested_ts_ms FROM congressional_trades WHERE source_trade_id=?",
                ("congress:abc",),
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(int(insider_count[0] or 0), 1)
        self.assertEqual(int(congress_count[0] or 0), 1)
        self.assertEqual(float(insider_value[0] or 0.0), 1100.0)
        self.assertEqual(str(congress_direction[0] or ""), "sell")

    def test_feature_snapshot_reads_persisted_alt_data_rows(self) -> None:
        os.environ["USE_FORM4_DATA"] = "1"
        os.environ["USE_CONGRESSIONAL_TRADE_DATA"] = "1"
        storage = self._init_storage()
        anchor_ts_ms = 1_776_000_000_000
        sixty_days_ms = 60 * 24 * 3600 * 1000

        storage.put_insider_transaction(
            {
                "source_transaction_id": "form4:buy1",
                "created_ts_ms": anchor_ts_ms,
                "ingested_ts_ms": anchor_ts_ms,
                "symbol": "AAPL",
                "insider_cik": "0001",
                "insider_name": "Insider One",
                "transaction_type": "purchase",
                "direction": "buy",
                "value": 10000.0,
                "transaction_ts_ms": anchor_ts_ms - (10 * 24 * 3600 * 1000),
                "transaction_date": "2026-04-01",
            }
        )
        storage.put_insider_transaction(
            {
                "source_transaction_id": "form4:sell1",
                "created_ts_ms": anchor_ts_ms,
                "ingested_ts_ms": anchor_ts_ms,
                "symbol": "AAPL",
                "insider_cik": "0001",
                "insider_name": "Insider One",
                "transaction_type": "sale",
                "direction": "sell",
                "value": 3000.0,
                "transaction_ts_ms": anchor_ts_ms - (5 * 24 * 3600 * 1000),
                "transaction_date": "2026-04-06",
            }
        )
        storage.put_insider_transaction(
            {
                "source_transaction_id": "form4:buy2",
                "created_ts_ms": anchor_ts_ms,
                "ingested_ts_ms": anchor_ts_ms,
                "symbol": "AAPL",
                "insider_cik": "0002",
                "insider_name": "Insider Two",
                "transaction_type": "purchase",
                "direction": "buy",
                "value": 7000.0,
                "transaction_ts_ms": anchor_ts_ms - sixty_days_ms,
                "transaction_date": "2026-02-10",
            }
        )

        storage.put_congressional_trade(
            {
                "source_trade_id": "congress:buy1",
                "created_ts_ms": anchor_ts_ms,
                "ingested_ts_ms": anchor_ts_ms,
                "symbol": "AAPL",
                "politician_name": "Rep. Alpha",
                "transaction_type": "purchase",
                "direction": "buy",
                "transaction_ts_ms": anchor_ts_ms - (14 * 24 * 3600 * 1000),
                "transaction_date": "2026-03-28",
                "disclosure_ts_ms": anchor_ts_ms - (8 * 24 * 3600 * 1000),
                "disclosure_date": "2026-04-03",
            }
        )
        storage.put_congressional_trade(
            {
                "source_trade_id": "congress:sell1",
                "created_ts_ms": anchor_ts_ms,
                "ingested_ts_ms": anchor_ts_ms,
                "symbol": "AAPL",
                "politician_name": "Rep. Beta",
                "transaction_type": "sale",
                "direction": "sell",
                "transaction_ts_ms": anchor_ts_ms - (12 * 24 * 3600 * 1000),
                "transaction_date": "2026-03-30",
                "disclosure_ts_ms": anchor_ts_ms - (7 * 24 * 3600 * 1000),
                "disclosure_date": "2026-04-04",
            }
        )
        storage.put_congressional_trade(
            {
                "source_trade_id": "congress:buy2",
                "created_ts_ms": anchor_ts_ms,
                "ingested_ts_ms": anchor_ts_ms,
                "symbol": "AAPL",
                "politician_name": "Sen. Gamma",
                "transaction_type": "purchase",
                "direction": "buy",
                "transaction_ts_ms": anchor_ts_ms - (4 * 24 * 3600 * 1000),
                "transaction_date": "2026-04-07",
                "disclosure_ts_ms": anchor_ts_ms - (2 * 24 * 3600 * 1000),
                "disclosure_date": "2026-04-09",
            }
        )

        _, _, feature_registry, _ = _reload_runtime_modules(
            "engine.strategy.feature_registry",
            "engine.strategy.model_feature_snapshots",
        )
        event = {
            "ts_ms": anchor_ts_ms,
            "ref_ts_ms": anchor_ts_ms,
            "source": "rss:reuters",
            "title": "AAPL update",
            "body": "Alt data snapshot test",
        }
        feature_ids = [
            "insider.buy_count_30d",
            "insider.sell_count_30d",
            "insider.net_value_30d",
            "insider.unique_insiders_90d",
            "congressional.buy_count_30d",
            "congressional.sell_count_30d",
            "congressional.net_signal_30d",
        ]
        with patch.object(feature_registry, "_schedule_feature_store_write", return_value=None):
            snapshot = feature_registry.build_feature_snapshot(
                event=event,
                symbol="AAPL",
                feature_ids=list(feature_ids),
            )

        self.assertEqual(float(snapshot["insider.buy_count_30d"]), 1.0)
        self.assertEqual(float(snapshot["insider.sell_count_30d"]), 1.0)
        self.assertEqual(float(snapshot["insider.net_value_30d"]), 7000.0)
        self.assertEqual(float(snapshot["insider.unique_insiders_90d"]), 2.0)
        self.assertEqual(float(snapshot["congressional.buy_count_30d"]), 2.0)
        self.assertEqual(float(snapshot["congressional.sell_count_30d"]), 1.0)
        self.assertEqual(float(snapshot["congressional.net_signal_30d"]), 1.0)

    def test_disabled_alt_data_features_are_filtered_out(self) -> None:
        os.environ["USE_FORM4_DATA"] = "0"
        os.environ["USE_CONGRESSIONAL_TRADE_DATA"] = "0"
        self._init_storage()
        _, _, feature_registry, _ = _reload_runtime_modules(
            "engine.strategy.feature_registry",
            "engine.strategy.model_feature_snapshots",
        )

        default_ids = feature_registry.default_feature_ids()
        self.assertFalse(any(fid.startswith("insider.") for fid in default_ids))
        self.assertFalse(any(fid.startswith("congressional.") for fid in default_ids))
        self.assertEqual(
            feature_registry.resolve_feature_ids(
                model_spec={
                    "feature_ids": [
                        "insider.buy_count_30d",
                        "congressional.net_signal_30d",
                        "base.source_credibility",
                    ]
                }
            ),
            ["base.source_credibility"],
        )

    def test_normalized_event_upsert_updates_resolution(self) -> None:
        storage = self._init_storage()
        _, _, event_normalization = _reload_runtime_modules("engine.data.event_normalization")

        unresolved = {
            "source_transaction_id": "event-upsert",
            "source": "sec_form4",
            "symbol": None,
            "entity_id": "cik:320193",
            "issuer_name": "Apple Inc.",
            "issuer_cik": "0000320193",
            "insider_name": "Jane Insider",
            "transaction_type": "purchase",
            "direction": "buy",
            "value": 10_000.0,
            "transaction_ts_ms": 1_776_000_000_000,
            "transaction_date": "2026-04-01",
            "resolution_status": "entity_resolved",
            "resolution_method": "issuer_cik",
        }
        resolved = dict(unresolved)
        resolved["symbol"] = "AAPL"
        resolved["entity_id"] = "cik:320193"
        resolved["resolution_status"] = "resolved"
        resolved["resolution_method"] = "issuer_trading_symbol"

        event_key = storage.put_normalized_event(event_normalization.normalize_insider_event(unresolved))
        self.assertGreater(int(event_key or 0), 0)
        storage.put_normalized_event(event_normalization.normalize_insider_event(resolved))

        con = storage.connect(readonly=True)
        try:
            row = con.execute(
                "SELECT COUNT(*), symbol, event_type FROM events WHERE event_key=?",
                ("form4:event-upsert",),
            ).fetchone()
        finally:
            con.close()

        self.assertEqual(int(row[0] or 0), 1)
        self.assertEqual(str(row[1] or ""), "AAPL")
        self.assertEqual(str(row[2] or ""), "insider")

    def test_source_manager_keeps_alt_jobs_disabled_by_default(self) -> None:
        self._init_storage()
        _, _, ingestion_status, data_source_manager = _reload_runtime_modules(
            "engine.runtime.ingestion_status",
            "services.data_source_manager",
        )
        manager = data_source_manager.get_manager()
        default_jobs = ingestion_status.default_ingestion_pipeline_jobs()
        desired = manager.get_desired_ingestion_jobs(default_jobs=default_jobs)

        self.assertNotIn("ingest_form4", desired)
        self.assertNotIn("ingest_congressional_trades", desired)

        manager.set_enabled("form4", True, actor="test")
        manager.set_enabled("congressional_trades", True, actor="test")
        desired_enabled = manager.get_desired_ingestion_jobs(default_jobs=default_jobs)

        self.assertIn("ingest_form4", desired_enabled)
        self.assertIn("ingest_congressional_trades", desired_enabled)


if __name__ == "__main__":
    unittest.main()
