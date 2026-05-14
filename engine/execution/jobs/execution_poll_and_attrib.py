"""
FILE: execution_poll_and_attrib.py

Job entrypoint or scheduled task for `execution_poll_and_attrib`.
"""

from engine.execution.execution_poll_and_attrib import main

if __name__ == "__main__":
    raise SystemExit(main())