"""Compatibility launcher for ``engine.runtime.jobs.check_alerts``."""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ops._engine_job_wrapper import import_engine_module, run_engine_module

_ENGINE_MODULE = "engine.runtime.jobs.check_alerts"

if __name__ == "__main__":
    raise SystemExit(run_engine_module(_ENGINE_MODULE))

sys.modules[__name__] = import_engine_module(_ENGINE_MODULE)
