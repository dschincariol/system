# System API Helpers

`engine/api/system/` contains cohesive implementation helpers extracted from
the large `engine/api/api_system.py` compatibility facade.

Current ownership:

- `route_specs.py` owns `ROUTE_SPECS_SYSTEM` for read-only system, health,
  readiness, telemetry, risk, and runtime diagnostic endpoints.
- `response.py` owns shared response-shaping helpers and readiness contract
  metadata used by the facade handlers.

Keep public imports on `engine.api.api_system`. New helpers here should be
called by production code through that facade unless the call site is already
inside this helper package. Mutating self-repair endpoints remain in
`engine/api/api_self_repair.py`.
