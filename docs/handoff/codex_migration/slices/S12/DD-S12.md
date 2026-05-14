Slice ID: S12
Goal: Move post-bind runtime boot and background-loop launch ownership out of `dashboard_server.py` into a runtime module while preserving HTTP bind/shutdown behavior.

In scope:
- engine/runtime/dashboard_runtime_boot.py
- dashboard_server.py
- tests/test_dashboard_runtime_boot.py
- tests/test_dashboard_route_contracts.py
- tests/test_runtime_graph_check.py

Out of scope:
- HTTP route-registry changes
- broker or ingestion runtime logic changes
- full service split beyond post-bind boot ownership

Required reading:
- dashboard_server.py
- engine/runtime/startup_orchestrator.py
- engine/runtime/orchestrator.py
- tests/test_dashboard_route_contracts.py

Required changes:
- No edits during DD.
- Identify the smallest safe orchestration extraction that:
  - removes nested post-bind boot ownership from `dashboard_server.run_server()`
  - keeps `dashboard_server` responsible only for HTTP bind/shutdown plus compatibility exports
  - reuses existing runtime helpers instead of rewriting startup behavior

Required verification:
- none during DD

Acceptance criteria:
- The DD output names one runtime helper module for post-bind boot ownership.
- The DD output keeps HTTP bind/shutdown in `dashboard_server.py`.

Stop and report if:
- The slice requires HTTP route changes.
- The slice requires broker/runtime behavior rewrites.

## DD Findings

- The remaining dashboard-owned orchestration lived inside nested `run_server()` helpers:
  - `_post_bind_boot_safe`
  - `_post_bind_boot`
  - background thread launch
- Existing runtime modules already own most behavior:
  - `StartupOrchestrator`
  - `RuntimeOrchestrator`
  - `JobManager`
- The clean bounded seam is one new runtime helper module that:
  - owns post-bind runtime boot
  - owns background-thread launch coordination
  - operates against the loaded `dashboard_server` module as a compatibility surface
- Focused verification should cover:
  - helper launch behavior
  - dashboard route imports still succeeding
  - runtime-graph expectations remaining unchanged
