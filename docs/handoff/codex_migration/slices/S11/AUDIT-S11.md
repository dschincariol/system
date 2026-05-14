Slice ID: S11
Goal: Independently audit the control-plane entrypoint split and verify that `engine.api.server` now owns startup while `dashboard_server` remains compatible for route-contract consumers.

In scope:
- engine/api/server.py
- dashboard_server.py
- tests/test_api_server_contract.py
- tests/test_dashboard_route_contracts.py
- tests/test_runtime_graph_check.py
- tests/test_validate_repo_contract.py
- the `S11` diff only

Out of scope:
- route-registry rewrites
- FastAPI migration
- runtime-orchestration extraction

Required reading:
- the `S11` diff
- engine/api/server.py
- dashboard_server.py
- tests/test_api_server_contract.py
- tests/test_dashboard_route_contracts.py

Required changes:
- No code changes unless the audit finds a concrete defect.
- Findings must come first.
- Explicitly check:
  - startup ownership now lives in `engine.api.server`
  - `dashboard_server.run_server()` delegates cleanly
  - route-contract and runtime-graph behavior stay unchanged

Required verification:
- python -m pytest tests/test_api_server_contract.py -q
- python -m pytest tests/test_dashboard_route_contracts.py -q
- python -m pytest tests/test_runtime_graph_check.py -q
- python -m pytest tests/test_validate_repo_contract.py -q

Acceptance criteria:
- Findings-first audit output.
- Explicit statement whether `S11` is complete or needs follow-up.

Stop and report if:
- The diff leaks into route-registry behavior.
- The diff breaks runtime-graph import expectations.

## Audit Result

- Findings: none within the approved `S11` slice.
- `engine.api.server` now owns the control-plane startup entrypoint.
- `dashboard_server.run_server()` delegates to the authoritative API entrypoint without disturbing route/helper exports.
- Route-contract, runtime-graph, and validation-contract tests remained green.

## Verification Result

- `python -m pytest tests/test_api_server_contract.py -q`
- `python -m pytest tests/test_dashboard_route_contracts.py -q`
- `python -m pytest tests/test_runtime_graph_check.py -q`
- `python -m pytest tests/test_validate_repo_contract.py -q`

## Follow-up Notes

- `S11` is complete within the approved boundary.
- The next clean step is `S12`, extracting post-bind runtime boot ownership out of `dashboard_server.py`.
