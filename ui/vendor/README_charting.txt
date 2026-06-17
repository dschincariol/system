Place vendored charting runtimes here.

Canonical charting runtime:
  lightweight-charts.standalone.production.js

TradingView Lightweight Charts is the standard browser charting dependency for
financial and time-aligned UI charts in this repo. Prefer the existing lazy
loader path in ui/pro_chart_core.js instead of adding another charting runtime.

Download:
  https://unpkg.com/lightweight-charts/dist/lightweight-charts.standalone.production.js

Save as:
  ui/vendor/lightweight-charts.standalone.production.js

Do not vendor Chart.js. The old ui/vendor/chart.umd.min.js bundle was unused by
runtime HTML and JS and has been removed.

uPlot is only a future dense-time-series fallback if explicitly added for a
documented use case. It is not currently vendored.
