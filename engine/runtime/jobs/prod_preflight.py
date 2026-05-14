"""
FILE: prod_preflight.py

Job entrypoint or scheduled task for `prod_preflight`.
"""

from engine.runtime.prod_preflight import main

if __name__ == "__main__":
    raise SystemExit(main())
