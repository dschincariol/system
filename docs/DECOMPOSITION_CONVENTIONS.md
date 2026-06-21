# Oversized Module Decomposition Convention

Last verified against code: 2026-06-21

Use this convention when incrementally decomposing oversized Python modules that
are hard to review, conflict-prone, or difficult to test in isolation.

## Rules

1. Add characterization tests before moving code. Lock public imports,
   signatures, route tables, response shape, and edge-case helper behavior that
   callers depend on.
2. Keep the original module as the compatibility facade. Existing imports such
   as `import dashboard_server` must keep working, including private names that
   existing tests or callers already inspect.
3. Move one cohesive responsibility at a time into an importable package module.
   Prefer stable boundaries such as `routing`, `handlers`, `serialization`,
   `env`, or narrowly named domain helpers.
4. Let production code delegate to the extracted module. A passing test is not
   enough; the old facade should call the new implementation so runtime behavior
   actually uses the split code.
5. Do not combine structural moves with functional changes. Keep response
   payloads, error codes, route precedence, side effects, and fallback behavior
   identical.
6. Run targeted tests for the moved responsibility and any existing contract
   tests that cover the facade before widening the refactor.

## Dashboard Pilot

The first pilot applies this pattern to `dashboard_server.py` while preserving
the same dashboard entrypoint and route globals.

Current split:

- `engine/dashboard/env.py` owns dashboard environment parsing helpers.
- `engine/dashboard/serialization.py` owns small JSON/serialization helpers.
- `engine/dashboard/db_health.py` owns DB health and schema handler
  implementation.
- `engine/dashboard/routing.py` owns fallback route metadata, route
  normalization, route filtering, and canonical route-owner validation.
- `dashboard_server.py` remains the HTTP/UI facade and continues to publish
  `_FALLBACK_ROUTE_SPECS`, `_RAW_ROUTE_SPECS`, `_normalize_route_specs`,
  `ROUTE_SPECS`, `API_HANDLERS`, and the existing handler names.

Characterization coverage starts in
`tests/test_dashboard_decomposition_contract.py` and is backed by the existing
dashboard route/UI contract tests.

## System API Slice

The next slice applies the same pattern to `engine/api/api_system.py` while
preserving `engine.api.api_system` as the public import surface.

Current split:

- `engine/api/system/route_specs.py` owns `ROUTE_SPECS_SYSTEM`.
- `engine/api/system/response.py` owns shared response helpers and readiness
  contract metadata used by the system API handlers.
- `engine/api/api_system.py` remains the compatibility facade and continues to
  publish the existing route table, handler names, helper names, and
  self-repair compatibility exports.
- `engine/api/api_self_repair.py` remains the owner of mutating self-repair and
  schema-repair handlers.

Characterization coverage starts in
`tests/test_api_system_decomposition_contract.py` and is backed by the existing
dashboard route/readiness/API system contract tests.

## Startup Entrypoint Slice

The startup slice applies the same pattern to `start_system.py` while
preserving the root executable as the public entrypoint.

Current split:

- `engine/startup/env.py` owns startup environment parsing, local `.env`
  bootstrap, local master-key file creation, and strict-runtime DB-path helper
  logic.
- `engine/startup/mode.py` owns launch target selection from argv and
  `ENGINE_MODE`.
- `engine/startup/phase.py` owns startup phase and first-failure trace mutation
  helpers.
- `engine/startup/subprocesses.py` owns import-smoke child-process command
  construction, import-smoke result shaping, and runtime-graph validator
  subprocess execution.
- `engine/startup/validation.py` owns startup-validation payload normalization,
  redaction, persistence payload assembly, and validation-gate trace payload
  construction.
- `engine/startup/dashboard.py` owns dashboard bind waiting and clean-return
  decision helpers.
- `engine/startup/shutdown.py` owns shutdown-request, signal, and bootstrap
  side-effect helper orchestration.
- `start_system.py` remains the executable compatibility facade and continues
  to publish `_env_file_has_nonempty_value`, `_append_env_line`,
  `_ensure_local_secret_file`, `_strict_runtime_requires_explicit_db_path`,
  `_ensure_local_env_file`, `_env_int`, `_env_float`, `_env_bool`,
  `_record_phase`, `_record_first_failure`, `_pick_mode_from_argv_or_env`,
  subprocess/import-smoke helpers, startup-validation helpers, dashboard
  coordination helpers, shutdown helpers, and `main`.
- `start_system.py` still owns top-level `main()` boot ordering and lifecycle
  sequencing; extracted modules provide behavior-preserving helper
  implementations behind the facade.

Characterization coverage starts in
`tests/test_start_system_decomposition_contract.py` and is backed by the
existing startup health and runtime-configuration tests.

## Follow-Up Priority

Recommended next decomposition targets:

1. `engine/api/api_system.py` - continue with read handler and readiness
   serialization slices behind the same facade.
2. `start_system.py` - future slices should be limited to additional helper
   extraction behind the facade; keep top-level `main()` boot sequencing in the
   executable entrypoint.
3. `engine/runtime/health.py` - split snapshot collection, readiness scoring,
   and serialization after adding focused health characterization tests.
4. `engine/strategy/portfolio.py` - split portfolio construction stages only
   after locking target/order equivalence with fixture-based tests.
5. `engine/execution/execution_ledger.py` - high blast radius durable execution
   state; decompose only after ledger replay/compatibility tests are in place.
6. `engine/runtime/storage_sqlite.py` - highest blast radius storage core; keep
   last and require migration/storage contract coverage before any structural
   move.
