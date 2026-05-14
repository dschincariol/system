"""
Job entrypoint for trade lifecycle tracing.
"""

import json
import os

from engine.runtime.trade_lifecycle import trace_trade_lifecycle


def main() -> int:
    source_alert_id = os.environ.get("TRACE_SOURCE_ALERT_ID")
    client_order_id = os.environ.get("TRACE_CLIENT_ORDER_ID")

    report = trace_trade_lifecycle(
        source_alert_id=(int(source_alert_id) if source_alert_id not in (None, "") else None),
        client_order_id=(str(client_order_id) if client_order_id not in (None, "") else None),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if bool(report.get("ok")) else 2


if __name__ == "__main__":
    raise SystemExit(main())
