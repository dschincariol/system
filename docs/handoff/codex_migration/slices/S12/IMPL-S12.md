Slice ID: S12
Goal: Move post-bind runtime boot and background-loop launch ownership out of `dashboard_server.py` into a runtime helper module.

In scope:
- engine/runtime/dashboard_runtime_boot.py
- dashboard_server.py
- tests/test_dashboard_runtime_boot.py
- tests/test_dashboard_route_contracts.py
- tests/test_runtime_graph_check.py

Out of scope:
- HTTP route-registry changes
- broker/runtime behavior rewrites
- full service split beyond post-bind boot ownership

Required reading:
- dashboard_server.py
- engine/runtime/dashboard_runtime_boot.py
- engine/runtime/startup_orchestrator.py
- engine/runtime/orchestrator.py

Required changes:
- Add a runtime helper module that owns:
  - post-bind runtime boot
  - post-bind failure handling
  - post-bind thread launch coordination
- Add a top-level dashboard background-thread helper.
- Replace nested dashboard runtime-boot logic with delegation into the runtime helper.
- Keep HTTP bind/shutdown in `dashboard_server.py`.
- Add focused tests for the new runtime boot seam.

Required verification:
- python -m pytest tests/test_dashboard_runtime_boot.py -q
- python -m pytest tests/test_dashboard_route_contracts.py -q
- python -m pytest tests/test_runtime_graph_check.py -q

Acceptance criteria:
- `dashboard_server.py` no longer owns the substantive post-bind runtime boot body.
- Post-bind thread launch is routed through the extracted runtime helper.
- Existing dashboard route and runtime-graph tests remain green.

Stop and report if:
- The slice requires route-registry changes.
- The slice requires runtime behavior rewrites beyond the extracted seam.

## Implementation Result

- Added `engine/runtime/dashboard_runtime_boot.py` owning:
  - `run_post_bind_boot(...)`
  - `run_post_bind_boot_safe(...)`
  - `launch_post_bind_runtime_threads(...)`
- Added a top-level `_start_background_thread(...)` helper in `dashboard_server.py`.
- Replaced the nested dashboard post-bind boot bodies with thin wrappers delegating into the runtime helper.
- Kept HTTP bind/shutdown ownership in `dashboard_server.py`.
- Added focused runtime-boot seam coverage in `tests/test_dashboard_runtime_boot.py`.

## Verification Result

- `python -m pytest tests/test_dashboard_runtime_boot.py -q`
- `python -m pytest tests/test_dashboard_route_contracts.py -q`
- `python -m pytest tests/test_runtime_graph_check.py -q`
