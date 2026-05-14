"""
Compatibility wrapper for the legacy weather forecast ingest job.

This delegates to the symbol-aware poller so all forecast entrypoints use the
same region-to-symbol mapping and event emission path.
"""

import os

os.environ.setdefault("WEATHER_FORECAST_JOB_NAME", "compute_weather_ingest")

from engine.data.jobs.poll_weather_forecasts import main


if __name__ == "__main__":
    main()
