"""
FILE: app.py

Core engine module for `app`.
"""

# engine/app.py
"""
Engine entrypoint wrapper.

This repository's stable runtime entry is dashboard_server.py
(which owns HTTP + JobManager).

Keep this file so external tooling that runs `python -m engine.app`
does not break, but do not duplicate orchestration here.
"""

import os
import logging

from engine.runtime.failure_diagnostics import log_failure
from engine.runtime.logging import get_logger


LOG = get_logger("engine.app")


def main():
    try:
        from dotenv import load_dotenv
        _engine_dir = os.path.dirname(os.path.abspath(__file__))
        _project_root = os.path.abspath(os.path.join(_engine_dir, ".."))
        load_dotenv(os.path.join(_project_root, ".env"))
    except Exception as e:
        log_failure(
            LOG,
            event="engine_app_dotenv_load_failed",
            code="ENGINE_APP_DOTENV_LOAD_FAILED",
            message=str(e),
            error=e,
            level=logging.WARNING,
            component="engine.app",
            include_health=False,
            persist=True,
        )

    from dashboard_server import run_server
    run_server()


if __name__ == "__main__":
    main()
