"""Supervised Alpaca trade update stream entrypoint."""

from typing import Any

from engine.execution.broker_alpaca_rest import run_trade_updates_stream_daemon


def main(stop_event: Any = None) -> None:
    run_trade_updates_stream_daemon(stop_event=stop_event)


if __name__ == "__main__":
    main()
