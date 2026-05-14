"""
Compatibility wrapper for the legacy weather alerts ingest job.

This delegates to the symbol-aware poller so all alert entrypoints use the
same region-to-symbol mapping and event emission path.
"""

import os

os.environ.setdefault("WEATHER_ALERTS_JOB_NAME", "compute_weather_alerts_ingest")

from engine.data.jobs.poll_weather_alerts import main


if __name__ == "__main__":
    main()
