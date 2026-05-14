"""Daily artifact-store verifier job."""

from __future__ import annotations

import json
import sys

from engine.artifacts.fsck import verify
from engine.artifacts.store import LocalArtifactStore


def run() -> dict[str, object]:
    store = LocalArtifactStore()
    result = verify(store, log_findings=True)
    return {
        "ok": bool(result.ok),
        "findings": [
            {
                "finding_type": finding.finding_type,
                "sha256": finding.sha256,
                "path": finding.path,
                "severity": finding.severity,
                "detail": dict(finding.detail or {}),
            }
            for finding in result.findings
        ],
    }


def main() -> int:
    out = run()
    sys.stdout.write(json.dumps(out, separators=(",", ":"), sort_keys=True) + "\n")
    return 0 if bool(out.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
