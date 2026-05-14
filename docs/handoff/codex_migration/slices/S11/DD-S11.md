Slice ID: S11
Goal: Make `engine.api.server` the authoritative control-plane server entrypoint while keeping `dashboard_server.py` as the compatibility surface for existing route contracts.

In scope:
- engine/api/server.py
- dashboard_server.py
- tests/test_api_server_contract.py
- tests/test_dashboard_route_contracts.py
- tests/test_runtime_graph_check.py
- tests/test_validate_repo_contract.py

Out of scope:
- route-registry rewrites
- FastAPI migration
- post-bind runtime orchestration extraction
- UI handler behavior changes

Required reading:
- engine/api/server.py
- dashboard_server.py
- tests/test_dashboard_route_contracts.py
- tests/test_runtime_graph_check.py
- tests/test_validate_repo_contract.py

Required changes:
- No edits during DD.
- Identify the smallest safe entrypoint split that:
  - moves startup ownership to `engine.api.server`
  - keeps `dashboard_server` route/helper exports stable for tests and existing imports
  - avoids route-registry or handler rewrites

Required verification:
- none during DD

Acceptance criteria:
- The DD output names the exact entrypoint handoff.
- The DD output keeps route contracts and runtime-graph behavior stable.

Stop and report if:
- The slice requires a route-registry rewrite.
- The slice requires post-bind runtime orchestration changes.

## DD Findings

- `engine/api/server.py` was only a thin shim back into `dashboard_server.run_server()`.
- Existing tests import `dashboard_server` directly for:
  - `ROUTE_SPECS`
  - `API_HANDLERS`
  - helper functions such as `_normalize_route_specs`
- The clean bounded seam is:
  - `engine.api.server.run_server()` becomes the authoritative startup entrypoint
  - `dashboard_server.py` keeps compatibility exports and delegates `run_server()`
  - no route-registry rewrite is required in `S11`
- Focused verification should cover:
  - direct `engine.api.server` contract tests
  - `dashboard_server.run_server()` delegation
  - route-contract and runtime-graph imports remaining stable
