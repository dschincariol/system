"""
FILE: broker_apply_orders.py

Job entrypoint or scheduled task for `broker_apply_orders`.
"""

from engine.execution.broker_apply_orders import main

if __name__ == "__main__":
    raise SystemExit(main())
