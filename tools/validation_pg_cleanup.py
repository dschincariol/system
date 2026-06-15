"""Clean Postgres schemas created by validation isolation.

Validation runs can set ``TS_PG_SCHEMA_PER_DB_PATH=1`` so Postgres-backed
checks do not collide with the main runtime schema.  When those checks target a
shared local Timescale instance, the hashed schemas should be removed after the
validation process exits so Timescale background jobs do not accumulate.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parents[1]
HASHED_SCHEMA_RE = re.compile(r"^trading_[0-9a-f]{16}$")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_dotenv_defaults() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv(env_path, override=False)


def schema_for_db_path(db_path: str) -> str:
    resolved = os.path.abspath(str(db_path or ""))
    digest = hashlib.sha1(resolved.encode("utf-8", errors="ignore")).hexdigest()[:16]
    return f"trading_{digest}"


def _quote_ident(value: str) -> str:
    text = str(value or "")
    if not HASHED_SCHEMA_RE.match(text):
        raise ValueError(f"refusing unsafe validation schema name: {text!r}")
    return '"' + text.replace('"', '""') + '"'


def _connect():
    import psycopg
    from engine.runtime.platform import default_pg_dsn, dsn_with_pg_password

    configured = str(os.environ.get("TS_PG_DSN") or "").strip()
    parsed = urlparse(configured) if configured else None
    if configured and parsed and parsed.scheme and parsed.password:
        conninfo = configured
    else:
        conninfo = dsn_with_pg_password(configured) if configured else default_pg_dsn()
    timeout_s = max(1, int(float(os.environ.get("TS_PG_CONNECT_TIMEOUT", "2") or 2)))
    return psycopg.connect(conninfo, connect_timeout=timeout_s, autocommit=True)


def _existing_hashed_schemas(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT nspname
        FROM pg_namespace
        WHERE nspname LIKE 'trading\\_%' ESCAPE '\\'
        ORDER BY nspname
        """
    ).fetchall()
    return [str(row[0]) for row in rows or [] if HASHED_SCHEMA_RE.match(str(row[0]))]


def cleanup_validation_schemas(
    *,
    schemas: Iterable[str] | None = None,
    exclude: Iterable[str] | None = None,
    dry_run: bool = False,
) -> dict[str, object]:
    _load_dotenv_defaults()
    excluded = {str(item) for item in (exclude or ()) if str(item or "").strip()}
    dropped: list[str] = []
    skipped: list[str] = []
    with _connect() as conn:
        targets = list(schemas) if schemas is not None else _existing_hashed_schemas(conn)
        for raw_schema in targets:
            schema = str(raw_schema or "").strip()
            if not HASHED_SCHEMA_RE.match(schema):
                skipped.append(schema)
                continue
            if schema in excluded:
                skipped.append(schema)
                continue
            if not dry_run:
                conn.execute(f"DROP SCHEMA IF EXISTS {_quote_ident(schema)} CASCADE")
            dropped.append(schema)
    return {"ok": True, "dropped": dropped, "skipped": skipped, "dry_run": bool(dry_run)}


def cleanup_schema_for_db_path(db_path: str, *, dry_run: bool = False) -> dict[str, object]:
    if not str(db_path or "").strip():
        return {"ok": True, "dropped": [], "skipped": [], "dry_run": bool(dry_run)}
    return cleanup_validation_schemas(schemas=[schema_for_db_path(db_path)], dry_run=dry_run)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Drop hashed Postgres schemas created by validation isolation.")
    parser.add_argument("--db-path", action="append", default=[], help="Clean only the schema for this DB_PATH.")
    parser.add_argument("--all", action="store_true", help="Clean all hashed validation schemas.")
    parser.add_argument("--dry-run", action="store_true", help="Report targets without dropping schemas.")
    parser.add_argument("--yes", action="store_true", help="Confirm destructive cleanup.")
    args = parser.parse_args(argv)

    if not args.yes and not args.dry_run:
        print("Refusing to drop schemas without --yes or --dry-run.", file=sys.stderr)
        return 2
    if not args.all and not args.db_path:
        print("Specify --db-path or --all.", file=sys.stderr)
        return 2

    try:
        _load_dotenv_defaults()
        if args.all:
            current_db_path = str(os.environ.get("DB_PATH") or "").strip()
            exclude = [schema_for_db_path(current_db_path)] if current_db_path else []
            result = cleanup_validation_schemas(exclude=exclude, dry_run=bool(args.dry_run))
        else:
            schemas = [schema_for_db_path(path) for path in args.db_path]
            result = cleanup_validation_schemas(schemas=schemas, dry_run=bool(args.dry_run))
    except Exception as exc:
        print(f"validation schema cleanup failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1

    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
