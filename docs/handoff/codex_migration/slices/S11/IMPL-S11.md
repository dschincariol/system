Slice ID: S11
Goal: Make `engine.api.server` the authoritative control-plane server entrypoint while preserving `dashboard_server` compatibility.

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

Required changes:
- Replace the compatibility shim in `engine/api/server.py` with the authoritative control-plane entrypoint.
- Rename the concrete dashboard startup body to an internal runner.
- Make `dashboard_server.run_server()` delegate to `engine.api.server.run_server(...)`.
- Add focused tests for:
  - direct `engine.api.server` entrypoint behavior
  - `dashboard_server.run_server()` delegation

Required verification:
- python -m pytest tests/test_api_server_contract.py -q
- python -m pytest tests/test_dashboard_route_contracts.py -q
- python -m pytest tests/test_runtime_graph_check.py -q
- python -m pytest tests/test_validate_repo_contract.py -q

Acceptance criteria:
- `engine.api.server` owns startup entrypoint behavior.
- `dashboard_server` remains a compatibility surface for route contracts.
- Existing route and runtime-graph tests remain green.

Stop and report if:
- The slice requires route-registry rewrites.
- The slice requires orchestration extraction.

## Implementation Result

- Replaced `engine/api/server.py` with the authoritative control-plane server entrypoint.
- Renamed the concrete dashboard startup body to `_run_dashboard_control_plane`.
- Updated `dashboard_server.run_server()` to delegate through `engine.api.server.run_server(dashboard_module=...)`.
- Added focused tests covering:
  - the direct `engine.api.server` contract
  - compatibility delegation from `dashboard_server.run_server()`

## Verification Result

- `python -m pytest tests/test_api_server_contract.py -q`
- `python -m pytest tests/test_dashboard_route_contracts.py -q`
- `python -m pytest tests/test_runtime_graph_check.py -q`
- `python -m pytest tests/test_validate_repo_contract.py -q`
