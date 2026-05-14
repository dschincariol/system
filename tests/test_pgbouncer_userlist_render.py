from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_userlist_template_renders_scram_verifiers_without_plaintext():
    template = (ROOT / "ops/server/config/pgbouncer.userlist.txt.tmpl").read_text(encoding="utf-8")
    fixture_password = "fixture-password"
    verifier = "SCRAM-SHA-256$4096:QSXCR+Q6sek8bf92$storedkey/serverkey"
    rendered = (
        template.replace("{{ ts_ingest_scram }}", verifier)
        .replace("{{ ts_app_scram }}", verifier)
        .replace("{{ ts_reader_scram }}", verifier)
    )

    assert fixture_password not in rendered
    assert "{{" not in rendered
    assert rendered.count(verifier) == 3
    for line in rendered.splitlines():
        if line.startswith('"ts_'):
            assert '"SCRAM-SHA-256$' in line


def test_bootstrap_refreshes_userlist_from_pg_scram_hashes():
    bootstrap = (ROOT / "ops/server/bootstrap.sh").read_text(encoding="utf-8")

    assert "PGBOUNCER_USERLIST_TEMPLATE" in bootstrap
    assert "validate_scram_hash" in bootstrap
    assert "rolpassword FROM pg_authid" in bootstrap
    assert "/etc/pgbouncer/userlist.txt" in bootstrap
