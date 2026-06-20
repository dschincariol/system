"""CFTC Commitments of Traders ingestion and point-in-time features.

README:
- Source: CFTC Public Reporting/Socrata API at
  ``publicreporting.cftc.gov``. Defaults use Legacy Futures Only dataset
  ``6dca-aqww`` and Disaggregated Futures Only dataset ``72hh-3qpy``.
- Cadence: the supervised ingestion job polls once per day by default because
  COT is weekly.
- Availability lag: CFTC generally publishes the report Friday at 3:30 p.m.
  ET for the previous Tuesday's positions. Features join on
  ``release_ts_ms``/``availability_ts_ms``, never ``report_ts_ms``.
- Caveats: holiday schedules can delay releases. The default release-time
  encoder uses Friday 3:30 p.m. ET when the API row has no explicit release
  timestamp. COT is regime context; it is not wired as standalone alpha.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any, Dict, List, Mapping, Sequence, Tuple
from zoneinfo import ZoneInfo

import requests

from engine.runtime.storage import connect, run_write_txn

COT_FEATURE_IDS = [
    "cot_commercial_net_pctile_3y",
    "cot_noncomm_net_z",
    "cot_noncomm_extreme_flag",
    "cot_open_interest_z",
]

CFTC_PUBLIC_REPORTING_DOMAIN = os.environ.get("CFTC_PUBLIC_REPORTING_DOMAIN", "publicreporting.cftc.gov")
CFTC_DATASET_IDS = {
    "legacy": os.environ.get("CFTC_COT_LEGACY_DATASET_ID", "6dca-aqww"),
    "disaggregated": os.environ.get("CFTC_COT_DISAGG_DATASET_ID", "72hh-3qpy"),
}
CFTC_COT_REQUEST_TIMEOUT_S = float(os.environ.get("CFTC_COT_REQUEST_TIMEOUT_S", "20"))
CFTC_COT_FEATURE_LOOKBACK_DAYS = max(365, int(os.environ.get("CFTC_COT_FEATURE_LOOKBACK_DAYS", str(3 * 366))))

_UTC = timezone.utc
_EASTERN = ZoneInfo("America/New_York")
_KEY_NORMALIZER = re.compile(r"[^a-z0-9]+", re.IGNORECASE)


@dataclass(frozen=True)
class CotContractSpec:
    contract_key: str
    report_type: str
    market_name_contains: str
    symbols: Tuple[str, ...]
    topic: str = ""
    weight: float = 1.0

    @property
    def dataset_id(self) -> str:
        return str(CFTC_DATASET_IDS.get(str(self.report_type).lower(), self.report_type))


DEFAULT_COT_CONTRACT_SPECS: Tuple[CotContractSpec, ...] = (
    CotContractSpec("ES", "legacy", "E-MINI S&P 500", ("SPY", "VOO", "IVV", "VTI", "ES"), "equity_index"),
    CotContractSpec("NQ", "legacy", "NASDAQ-100", ("QQQ", "TQQQ", "SQQQ", "SMH", "NQ"), "equity_index"),
    CotContractSpec("ZN", "legacy", "10-YEAR U.S. TREASURY", ("TLT", "IEF", "ZN"), "rates"),
    CotContractSpec("CL", "disaggregated", "CRUDE OIL, LIGHT SWEET", ("USO", "XLE", "OIL", "XOM", "CVX", "CL"), "oil"),
    CotContractSpec("GC", "disaggregated", "GOLD", ("GLD", "GDX", "GC"), "gold"),
    CotContractSpec("6E", "legacy", "EURO FX", ("FXE", "6E"), "fx"),
    CotContractSpec("BTC", "legacy", "BITCOIN", ("BTC", "IBIT", "GBTC", "MSTR", "COIN"), "crypto"),
    CotContractSpec("ETH", "legacy", "ETHER", ("ETH",), "crypto"),
)


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def _clean_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def _clean_contract(value: Any) -> str:
    return str(value or "").upper().strip()


def _norm_key(key: Any) -> str:
    return _KEY_NORMALIZER.sub("", str(key or "").strip().lower())


def _field(record: Mapping[str, Any], aliases: Sequence[str], default: Any = None) -> Any:
    normalized = {_norm_key(key): value for key, value in dict(record or {}).items()}
    for alias in aliases:
        key = _norm_key(alias)
        if key in normalized and normalized[key] not in (None, ""):
            return normalized[key]
    return default


def _safe_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).replace(",", "").strip()
    if not text:
        return None
    try:
        out = float(text)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return float(out)


def _json_param(con: Any, value: Any) -> Any:
    if isinstance(value, (dict, list)) and "sqlite" in str(type(con).__module__).lower():
        return json.dumps(value, separators=(",", ":"), sort_keys=True, default=str)
    return value


def parse_date(value: Any) -> date:
    text = str(value or "").strip()
    if not text:
        raise ValueError("empty date")
    if re.fullmatch(r"\d{8}", text):
        return datetime.strptime(text, "%Y%m%d").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return datetime.strptime(text, "%Y-%m-%d").date()
    return datetime.fromisoformat(text.replace("Z", "+00:00")).date()


def date_to_ms(day: date | str) -> int:
    parsed = parse_date(day) if not isinstance(day, date) else day
    return int(datetime.combine(parsed, dt_time.min, tzinfo=_UTC).timestamp() * 1000)


def cot_release_ts_ms(report_day: date | str) -> int:
    parsed = parse_date(report_day) if not isinstance(report_day, date) else report_day
    days_until_friday = (4 - int(parsed.weekday())) % 7
    release_day = parsed + timedelta(days=days_until_friday)
    release_dt = datetime.combine(release_day, dt_time(hour=15, minute=30), tzinfo=_EASTERN)
    return int(release_dt.astimezone(_UTC).timestamp() * 1000)


def _source_record_id(*parts: Any) -> str:
    digest = hashlib.sha256("|".join(str(part or "") for part in parts).encode("utf-8")).hexdigest()[:20]
    return f"cftc_cot:{digest}"


def _parse_contract_specs_env() -> List[CotContractSpec]:
    raw = str(os.environ.get("CFTC_COT_CONTRACTS_JSON") or "").strip()
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    items = parsed if isinstance(parsed, list) else []
    out: List[CotContractSpec] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        key = _clean_contract(item.get("contract_key") or item.get("contract") or item.get("key"))
        report_type = str(item.get("report_type") or "legacy").strip().lower()
        contains = str(item.get("market_name_contains") or item.get("market") or "").strip()
        symbols = tuple(_clean_symbol(sym) for sym in list(item.get("symbols") or []) if _clean_symbol(sym))
        if key and contains and symbols:
            out.append(
                CotContractSpec(
                    key,
                    report_type,
                    contains,
                    symbols,
                    topic=str(item.get("topic") or ""),
                    weight=float(_safe_float(item.get("weight")) or 1.0),
                )
            )
    return out


def load_cot_contract_specs() -> List[CotContractSpec]:
    return list(_parse_contract_specs_env() or DEFAULT_COT_CONTRACT_SPECS)


def ensure_cot_tables(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cftc_cot_positions (
            id BIGSERIAL PRIMARY KEY,
            ts_ms BIGINT,
            report_type TEXT NOT NULL,
            contract_key TEXT NOT NULL,
            market_and_exchange_names TEXT,
            contract_market_name TEXT,
            cftc_contract_market_code TEXT,
            report_date TEXT NOT NULL,
            report_ts_ms BIGINT NOT NULL,
            release_ts_ms BIGINT NOT NULL,
            availability_ts_ms BIGINT NOT NULL,
            source_record_id TEXT NOT NULL,
            open_interest DOUBLE PRECISION,
            commercial_long DOUBLE PRECISION,
            commercial_short DOUBLE PRECISION,
            commercial_spread DOUBLE PRECISION,
            noncommercial_long DOUBLE PRECISION,
            noncommercial_short DOUBLE PRECISION,
            noncommercial_spread DOUBLE PRECISION,
            producer_merchant_long DOUBLE PRECISION,
            producer_merchant_short DOUBLE PRECISION,
            producer_merchant_spread DOUBLE PRECISION,
            swap_dealer_long DOUBLE PRECISION,
            swap_dealer_short DOUBLE PRECISION,
            swap_dealer_spread DOUBLE PRECISION,
            managed_money_long DOUBLE PRECISION,
            managed_money_short DOUBLE PRECISION,
            managed_money_spread DOUBLE PRECISION,
            other_reportable_long DOUBLE PRECISION,
            other_reportable_short DOUBLE PRECISION,
            other_reportable_spread DOUBLE PRECISION,
            nonreportable_long DOUBLE PRECISION,
            nonreportable_short DOUBLE PRECISION,
            ingested_ts_ms BIGINT,
            payload_json JSONB,
            diagnostics_json JSONB
        )
        """
    )
    con.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_cftc_cot_positions_source_record_id
          ON cftc_cot_positions(source_record_id)
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cftc_cot_positions_contract_avail
          ON cftc_cot_positions(contract_key, availability_ts_ms DESC)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cot_contract_symbol_map (
            contract_key TEXT NOT NULL,
            symbol TEXT NOT NULL,
            topic TEXT,
            weight DOUBLE PRECISION NOT NULL DEFAULT 1.0,
            active BIGINT NOT NULL DEFAULT 1,
            updated_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(contract_key, symbol)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cot_contract_symbol_map_symbol
          ON cot_contract_symbol_map(symbol, active)
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS cot_symbol_features (
            symbol TEXT NOT NULL,
            asof_ts_ms BIGINT NOT NULL,
            cot_commercial_net_pctile_3y DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            cot_noncomm_net_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            cot_noncomm_extreme_flag DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            cot_open_interest_z DOUBLE PRECISION NOT NULL DEFAULT 0.0,
            source_max_availability_ts_ms BIGINT,
            created_ts_ms BIGINT,
            meta_json JSONB,
            PRIMARY KEY(symbol, asof_ts_ms)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_cot_symbol_features_symbol_asof
          ON cot_symbol_features(symbol, asof_ts_ms DESC)
        """
    )


def seed_default_cot_mappings(con) -> int:
    ensure_cot_tables(con)
    now_ms = utc_now_ms()
    written = 0
    for spec in load_cot_contract_specs():
        for symbol in spec.symbols:
            cur = con.execute(
                """
                INSERT INTO cot_contract_symbol_map(contract_key, symbol, topic, weight, active, updated_ts_ms, meta_json)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(contract_key, symbol) DO NOTHING
                """,
                (
                    str(spec.contract_key),
                    str(symbol),
                    str(spec.topic or ""),
                    float(spec.weight or 1.0),
                    int(now_ms),
                    _json_param(con, {"source": "default_cot_contract_specs", "report_type": spec.report_type}),
                ),
            )
            written += int(getattr(cur, "rowcount", 0) or 0)
    return int(written)


def cot_target_contracts_for_symbol(con, symbol: str) -> List[Tuple[str, float]]:
    symbol_key = _clean_symbol(symbol)
    if not symbol_key:
        return []
    rows = []
    try:
        rows = con.execute(
            """
            SELECT contract_key, weight
            FROM cot_contract_symbol_map
            WHERE symbol = ?
              AND active = 1
            ORDER BY contract_key ASC
            """,
            (symbol_key,),
        ).fetchall()
    except Exception:
        rows = []
    out: List[Tuple[str, float]] = []
    for row in rows or []:
        key = _clean_contract(row[0])
        weight = float(_safe_float(row[1]) or 1.0)
        if key:
            out.append((key, weight))
    if out:
        return out
    fallback = []
    for spec in load_cot_contract_specs():
        if symbol_key in set(spec.symbols):
            fallback.append((str(spec.contract_key), float(spec.weight or 1.0)))
    return fallback


def _report_type_dataset_id(report_type: str) -> str:
    return str(CFTC_DATASET_IDS.get(str(report_type).lower(), report_type))


def _socrata_url(dataset_id: str) -> str:
    return f"https://{CFTC_PUBLIC_REPORTING_DOMAIN}/resource/{dataset_id}.json"


def _where_for_spec(spec: CotContractSpec, *, since_day: date) -> str:
    needle = str(spec.market_name_contains or "").replace("'", "''").upper()
    return (
        f"report_date_as_yyyy_mm_dd >= '{since_day.isoformat()}' "
        f"AND upper(market_and_exchange_names) like '%{needle}%'"
    )


def fetch_cot_records(
    *,
    specs: Sequence[CotContractSpec] | None = None,
    lookback_weeks: int | None = None,
    limit: int | None = None,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    rows: List[Dict[str, Any]] = []
    errors: List[str] = []
    since_day = datetime.now(tz=_EASTERN).date() - timedelta(days=7 * int(lookback_weeks or os.environ.get("CFTC_COT_LOOKBACK_WEEKS", "8")))
    row_limit = max(1, int(limit or os.environ.get("CFTC_COT_QUERY_LIMIT", "200")))
    for spec in specs or load_cot_contract_specs():
        dataset_id = _report_type_dataset_id(spec.report_type)
        try:
            response = requests.get(
                _socrata_url(dataset_id),
                params={
                    "$limit": int(row_limit),
                    "$order": "report_date_as_yyyy_mm_dd DESC",
                    "$where": _where_for_spec(spec, since_day=since_day),
                },
                timeout=float(CFTC_COT_REQUEST_TIMEOUT_S),
            )
            response.raise_for_status()
            payload = response.json()
            source_rows = payload if isinstance(payload, list) else []
            for raw in source_rows:
                normalized = normalize_cot_record(raw, spec=spec)
                if normalized is not None:
                    rows.append(normalized)
        except Exception as exc:
            errors.append(f"{spec.contract_key}:{spec.report_type}:{exc}")
    return rows, errors


def normalize_cot_record(record: Mapping[str, Any], *, spec: CotContractSpec, ingested_ts_ms: int | None = None) -> Dict[str, Any] | None:
    try:
        report_day = parse_date(_field(record, ["report_date_as_yyyy_mm_dd", "report_date", "Report_Date_as_YYYY_MM_DD"]))
    except Exception:
        return None
    release_ts = cot_release_ts_ms(report_day)
    report_type = str(spec.report_type or "legacy").lower()
    open_interest = _safe_float(_field(record, ["open_interest_all", "Open_Interest_All"]))

    producer_long = _safe_float(_field(record, ["prod_merc_positions_long", "prod_merc_positions_long_all"]))
    producer_short = _safe_float(_field(record, ["prod_merc_positions_short", "prod_merc_positions_short_all"]))
    swap_long = _safe_float(_field(record, ["swap_positions_long_all", "swap_positions_long_old"]))
    swap_short = _safe_float(_field(record, ["swap__positions_short_all", "swap_positions_short_all", "swap__positions_short_old"]))
    swap_spread = _safe_float(_field(record, ["swap__positions_spread_all", "swap_positions_spread_all"]))
    managed_long = _safe_float(_field(record, ["m_money_positions_long_all", "m_money_positions_long_old"]))
    managed_short = _safe_float(_field(record, ["m_money_positions_short_all", "m_money_positions_short_old"]))
    managed_spread = _safe_float(_field(record, ["m_money_positions_spread", "m_money_positions_spread_all"]))

    commercial_long = _safe_float(_field(record, ["comm_positions_long_all", "comm_positions_long_old"]))
    commercial_short = _safe_float(_field(record, ["comm_positions_short_all", "comm_positions_short_old"]))
    commercial_spread = _safe_float(_field(record, ["comm_positions_spread_all", "comm_positions_spread"]))
    noncomm_long = _safe_float(_field(record, ["noncomm_positions_long_all", "noncomm_positions_long_old"]))
    noncomm_short = _safe_float(_field(record, ["noncomm_positions_short_all", "noncomm_positions_short_old"]))
    noncomm_spread = _safe_float(
        _field(record, ["noncomm_postions_spread_all", "noncomm_positions_spread_all", "noncomm_positions_spread"])
    )

    if commercial_long is None and producer_long is not None:
        commercial_long = float(producer_long)
    if commercial_short is None and producer_short is not None:
        commercial_short = float(producer_short)
    if noncomm_long is None and managed_long is not None:
        noncomm_long = float(managed_long)
    if noncomm_short is None and managed_short is not None:
        noncomm_short = float(managed_short)
    if noncomm_spread is None and managed_spread is not None:
        noncomm_spread = float(managed_spread)

    contract_key = str(spec.contract_key)
    report_date = report_day.isoformat()
    return {
        "ts_ms": int(release_ts),
        "report_type": report_type,
        "contract_key": contract_key,
        "market_and_exchange_names": str(_field(record, ["market_and_exchange_names"], "") or ""),
        "contract_market_name": str(_field(record, ["contract_market_name"], "") or ""),
        "cftc_contract_market_code": str(_field(record, ["cftc_contract_market_code"], "") or ""),
        "report_date": report_date,
        "report_ts_ms": int(date_to_ms(report_day)),
        "release_ts_ms": int(release_ts),
        "availability_ts_ms": int(release_ts),
        "source_record_id": _source_record_id(report_type, contract_key, report_date),
        "open_interest": open_interest,
        "commercial_long": commercial_long,
        "commercial_short": commercial_short,
        "commercial_spread": commercial_spread,
        "noncommercial_long": noncomm_long,
        "noncommercial_short": noncomm_short,
        "noncommercial_spread": noncomm_spread,
        "producer_merchant_long": producer_long,
        "producer_merchant_short": producer_short,
        "producer_merchant_spread": _safe_float(_field(record, ["prod_merc_positions_spread", "prod_merc_positions_spread_all"])),
        "swap_dealer_long": swap_long,
        "swap_dealer_short": swap_short,
        "swap_dealer_spread": swap_spread,
        "managed_money_long": managed_long,
        "managed_money_short": managed_short,
        "managed_money_spread": managed_spread,
        "other_reportable_long": _safe_float(_field(record, ["other_rept_positions_long", "other_rept_positions_long_all"])),
        "other_reportable_short": _safe_float(_field(record, ["other_rept_positions_short", "other_rept_positions_short_all"])),
        "other_reportable_spread": _safe_float(_field(record, ["other_rept_positions_spread", "other_rept_positions_spread_all"])),
        "nonreportable_long": _safe_float(_field(record, ["nonrept_positions_long_all", "nonrept_positions_long_old"])),
        "nonreportable_short": _safe_float(_field(record, ["nonrept_positions_short_all", "nonrept_positions_short_old"])),
        "ingested_ts_ms": int(ingested_ts_ms or utc_now_ms()),
        "payload_json": dict(record or {}),
        "diagnostics_json": {
            "availability_rule": "friday_15_30_et_for_tuesday_report",
            "dataset_id": spec.dataset_id,
            "market_name_contains": str(spec.market_name_contains),
        },
    }


_COT_POSITION_COLUMNS = (
    "ts_ms",
    "report_type",
    "contract_key",
    "market_and_exchange_names",
    "contract_market_name",
    "cftc_contract_market_code",
    "report_date",
    "report_ts_ms",
    "release_ts_ms",
    "availability_ts_ms",
    "source_record_id",
    "open_interest",
    "commercial_long",
    "commercial_short",
    "commercial_spread",
    "noncommercial_long",
    "noncommercial_short",
    "noncommercial_spread",
    "producer_merchant_long",
    "producer_merchant_short",
    "producer_merchant_spread",
    "swap_dealer_long",
    "swap_dealer_short",
    "swap_dealer_spread",
    "managed_money_long",
    "managed_money_short",
    "managed_money_spread",
    "other_reportable_long",
    "other_reportable_short",
    "other_reportable_spread",
    "nonreportable_long",
    "nonreportable_short",
    "ingested_ts_ms",
    "payload_json",
    "diagnostics_json",
)


def put_cot_position(row: Mapping[str, Any], *, con) -> int:
    values = [
        _json_param(con, row.get(column)) if column in {"payload_json", "diagnostics_json"} else row.get(column)
        for column in _COT_POSITION_COLUMNS
    ]
    cur = con.execute(
        f"""
        INSERT INTO cftc_cot_positions({", ".join(_COT_POSITION_COLUMNS)})
        VALUES ({", ".join(["?"] * len(_COT_POSITION_COLUMNS))})
        ON CONFLICT(source_record_id) DO UPDATE SET
          ts_ms = excluded.ts_ms,
          report_type = excluded.report_type,
          contract_key = excluded.contract_key,
          market_and_exchange_names = excluded.market_and_exchange_names,
          contract_market_name = excluded.contract_market_name,
          cftc_contract_market_code = excluded.cftc_contract_market_code,
          report_date = excluded.report_date,
          report_ts_ms = excluded.report_ts_ms,
          release_ts_ms = excluded.release_ts_ms,
          availability_ts_ms = excluded.availability_ts_ms,
          open_interest = excluded.open_interest,
          commercial_long = excluded.commercial_long,
          commercial_short = excluded.commercial_short,
          commercial_spread = excluded.commercial_spread,
          noncommercial_long = excluded.noncommercial_long,
          noncommercial_short = excluded.noncommercial_short,
          noncommercial_spread = excluded.noncommercial_spread,
          producer_merchant_long = excluded.producer_merchant_long,
          producer_merchant_short = excluded.producer_merchant_short,
          producer_merchant_spread = excluded.producer_merchant_spread,
          swap_dealer_long = excluded.swap_dealer_long,
          swap_dealer_short = excluded.swap_dealer_short,
          swap_dealer_spread = excluded.swap_dealer_spread,
          managed_money_long = excluded.managed_money_long,
          managed_money_short = excluded.managed_money_short,
          managed_money_spread = excluded.managed_money_spread,
          other_reportable_long = excluded.other_reportable_long,
          other_reportable_short = excluded.other_reportable_short,
          other_reportable_spread = excluded.other_reportable_spread,
          nonreportable_long = excluded.nonreportable_long,
          nonreportable_short = excluded.nonreportable_short,
          ingested_ts_ms = excluded.ingested_ts_ms,
          payload_json = excluded.payload_json,
          diagnostics_json = excluded.diagnostics_json
        """,
        tuple(values),
    )
    return int(getattr(cur, "rowcount", 0) or 0)


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / max(1, len(values)))


def _sample_std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = _mean(values)
    return float(math.sqrt(max(0.0, sum((float(x) - mean) ** 2 for x in values) / max(1, len(values) - 1))))


def _zscore(value: float, history: Sequence[float]) -> float:
    std = _sample_std(history)
    if std <= 1e-12:
        return 0.0
    return float(max(-10.0, min(10.0, (float(value) - _mean(history)) / std)))


def percentile_rank(value: float, history: Sequence[float]) -> float:
    values = sorted(float(v) for v in history if math.isfinite(float(v)))
    if not values:
        return 0.5
    if len(values) == 1:
        return 0.5
    less = sum(1 for v in values if v < float(value))
    equal = sum(1 for v in values if v == float(value))
    mid_rank = float(less) + max(0.0, float(equal - 1) / 2.0)
    return float(max(0.0, min(1.0, mid_rank / float(len(values) - 1))))


def _row_to_dict(row: Any) -> Dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return {}


def _commercial_net_pct(row: Mapping[str, Any]) -> float | None:
    oi = _safe_float(row.get("open_interest"))
    long_v = _safe_float(row.get("commercial_long"))
    short_v = _safe_float(row.get("commercial_short"))
    if long_v is None or short_v is None:
        return None
    net = float(long_v - short_v)
    return float(net / oi) if oi and oi > 0.0 else net


def _noncomm_net_pct(row: Mapping[str, Any]) -> float | None:
    oi = _safe_float(row.get("open_interest"))
    long_v = _safe_float(row.get("noncommercial_long"))
    short_v = _safe_float(row.get("noncommercial_short"))
    if long_v is None or short_v is None:
        return None
    net = float(long_v - short_v)
    return float(net / oi) if oi and oi > 0.0 else net


def compute_cot_contract_features(rows: Sequence[Mapping[str, Any]], *, asof_ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in COT_FEATURE_IDS}
    usable = [
        dict(row)
        for row in rows or []
        if int((row or {}).get("availability_ts_ms") or 0) <= int(asof_ts_ms)
    ]
    usable.sort(key=lambda row: (int(row.get("report_ts_ms") or 0), int(row.get("availability_ts_ms") or 0)))
    if not usable:
        return features, {"latest_availability_ts_ms": None, "rows": 0}, False

    commercial_series = [value for row in usable if (value := _commercial_net_pct(row)) is not None]
    noncomm_series = [value for row in usable if (value := _noncomm_net_pct(row)) is not None]
    oi_series = [float(value) for row in usable if (value := _safe_float(row.get("open_interest"))) is not None]
    latest = usable[-1]
    current_comm = _commercial_net_pct(latest)
    current_noncomm = _noncomm_net_pct(latest)
    current_oi = _safe_float(latest.get("open_interest"))
    noncomm_pctile = percentile_rank(float(current_noncomm or 0.0), noncomm_series) if current_noncomm is not None else 0.5

    if current_comm is not None and commercial_series:
        features["cot_commercial_net_pctile_3y"] = percentile_rank(float(current_comm), commercial_series)
    if current_noncomm is not None and noncomm_series:
        features["cot_noncomm_net_z"] = _zscore(float(current_noncomm), noncomm_series)
        features["cot_noncomm_extreme_flag"] = 1.0 if noncomm_pctile > 0.90 or noncomm_pctile < 0.10 else 0.0
    if current_oi is not None and oi_series:
        features["cot_open_interest_z"] = _zscore(float(current_oi), oi_series)

    return (
        {str(k): float(v or 0.0) for k, v in features.items()},
        {
            "latest_availability_ts_ms": int(latest.get("availability_ts_ms") or 0) or None,
            "latest_report_date": str(latest.get("report_date") or ""),
            "latest_contract_key": str(latest.get("contract_key") or ""),
            "latest_report_type": str(latest.get("report_type") or ""),
            "rows": int(len(usable)),
            "commercial_net_pct": float(current_comm or 0.0),
            "noncomm_net_pct": float(current_noncomm or 0.0),
            "noncomm_net_pctile": float(noncomm_pctile),
        },
        True,
    )


def _load_contract_rows(con, *, contract_key: str, ts_ms: int) -> List[Dict[str, Any]]:
    window_start = int(ts_ms) - int(CFTC_COT_FEATURE_LOOKBACK_DAYS * 24 * 3600 * 1000)
    rows = con.execute(
        """
        SELECT
          report_type,
          contract_key,
          report_date,
          report_ts_ms,
          availability_ts_ms,
          open_interest,
          commercial_long,
          commercial_short,
          commercial_spread,
          noncommercial_long,
          noncommercial_short,
          noncommercial_spread
        FROM cftc_cot_positions
        WHERE contract_key = ?
          AND availability_ts_ms <= ?
          AND availability_ts_ms >= ?
        ORDER BY report_ts_ms ASC, availability_ts_ms ASC
        """,
        (_clean_contract(contract_key), int(ts_ms), int(window_start)),
    ).fetchall()
    out = [row_dict for row in rows or [] if (row_dict := _row_to_dict(row))]
    if len(out) != len(rows or []):
        out = [
            {
                "report_type": row[0],
                "contract_key": row[1],
                "report_date": row[2],
                "report_ts_ms": row[3],
                "availability_ts_ms": row[4],
                "open_interest": row[5],
                "commercial_long": row[6],
                "commercial_short": row[7],
                "commercial_spread": row[8],
                "noncommercial_long": row[9],
                "noncommercial_short": row[10],
                "noncommercial_spread": row[11],
            }
            for row in rows or []
        ]
    return out


def resolve_cot_features(con, *, symbol: str, ts_ms: int) -> Tuple[Dict[str, float], Dict[str, Any], bool]:
    features = {fid: 0.0 for fid in COT_FEATURE_IDS}
    mappings = cot_target_contracts_for_symbol(con, symbol)
    if not mappings:
        return features, {"latest_availability_ts_ms": None, "contracts": []}, False

    weighted: List[Tuple[Dict[str, float], Dict[str, Any], float]] = []
    for contract_key, weight in mappings:
        try:
            rows = _load_contract_rows(con, contract_key=contract_key, ts_ms=int(ts_ms))
        except Exception:
            rows = []
        resolved, meta, available = compute_cot_contract_features(rows, asof_ts_ms=int(ts_ms))
        if available:
            weighted.append((resolved, meta, max(0.0, float(weight or 1.0))))

    if not weighted:
        return features, {"latest_availability_ts_ms": None, "contracts": [key for key, _w in mappings]}, False

    weight_sum = sum(weight for _feat, _meta, weight in weighted) or 1.0
    for fid in COT_FEATURE_IDS:
        if fid == "cot_noncomm_extreme_flag":
            features[fid] = max(float(feat.get(fid, 0.0) or 0.0) for feat, _meta, _weight in weighted)
        else:
            features[fid] = sum(float(feat.get(fid, 0.0) or 0.0) * weight for feat, _meta, weight in weighted) / weight_sum
    latest_availability = max([int((meta or {}).get("latest_availability_ts_ms") or 0) for _feat, meta, _weight in weighted] or [0])
    latest_contracts = [str((meta or {}).get("latest_contract_key") or "") for _feat, meta, _weight in weighted]
    return (
        {str(k): float(v or 0.0) for k, v in features.items()},
        {
            "latest_availability_ts_ms": int(latest_availability) if latest_availability > 0 else None,
            "contracts": [key for key in latest_contracts if key],
            "mapped_contracts": [key for key, _weight in mappings],
            "contract_count": int(len(weighted)),
        },
        True,
    )


def materialize_cot_symbol_features(con, *, symbol: str, ts_ms: int) -> Dict[str, Any]:
    ensure_cot_tables(con)
    features, meta, available = resolve_cot_features(con, symbol=symbol, ts_ms=int(ts_ms))
    con.execute(
        """
        INSERT INTO cot_symbol_features(
          symbol, asof_ts_ms,
          cot_commercial_net_pctile_3y, cot_noncomm_net_z,
          cot_noncomm_extreme_flag, cot_open_interest_z,
          source_max_availability_ts_ms, created_ts_ms, meta_json
        ) VALUES (?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, asof_ts_ms) DO UPDATE SET
          cot_commercial_net_pctile_3y = excluded.cot_commercial_net_pctile_3y,
          cot_noncomm_net_z = excluded.cot_noncomm_net_z,
          cot_noncomm_extreme_flag = excluded.cot_noncomm_extreme_flag,
          cot_open_interest_z = excluded.cot_open_interest_z,
          source_max_availability_ts_ms = excluded.source_max_availability_ts_ms,
          created_ts_ms = excluded.created_ts_ms,
          meta_json = excluded.meta_json
        """,
        (
            _clean_symbol(symbol),
            int(ts_ms),
            float(features["cot_commercial_net_pctile_3y"]),
            float(features["cot_noncomm_net_z"]),
            float(features["cot_noncomm_extreme_flag"]),
            float(features["cot_open_interest_z"]),
            meta.get("latest_availability_ts_ms"),
            int(utc_now_ms()),
            _json_param(con, dict(meta)),
        ),
    )
    return {"symbol": _clean_symbol(symbol), "available": bool(available), "features": features, "meta": meta}


def ingest_cot_batch(*, now_ms: int | None = None) -> Dict[str, Any]:
    anchor_ms = int(now_ms or utc_now_ms())
    rows, errors = fetch_cot_records()
    con = connect()
    try:
        ensure_cot_tables(con)
        seed_default_cot_mappings(con)

        def _write(conw) -> int:
            ensure_cot_tables(conw)
            seed_default_cot_mappings(conw)
            written = 0
            for row in rows:
                written += int(put_cot_position(row, con=conw) or 0)
            return int(written)

        written = int(run_write_txn(_write, table="cftc_cot_positions", operation="ingest_cftc_cot") or 0) if rows else 0
        last_ts = max([int(row.get("availability_ts_ms") or 0) for row in rows] or [anchor_ms])
        return {
            "ok": not bool(errors),
            "rows": int(len(rows)),
            "written": int(written),
            "errors": list(errors),
            "last_ingested_ts_ms": int(last_ts),
        }
    finally:
        try:
            con.close()
        # system-audit: ignore[silent_except] connection close is best-effort cleanup.
        except Exception:
            pass
