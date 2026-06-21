# Dashboard Helpers

`engine/dashboard/` contains cohesive implementation helpers extracted from
the root `dashboard_server.py` compatibility facade.

Current ownership:

- `env.py` parses dashboard environment values.
- `serialization.py` contains small JSON and fallback serialization helpers.
- `db_health.py` implements DB health and schema handler behavior.
- `routing.py` owns fallback route metadata, route normalization, route
  filtering, and canonical route-owner validation.

Keep public dashboard imports on `dashboard_server.py`. New helpers here should
be called by production code through the facade unless the call site is already
inside the extracted dashboard helper package. See
[docs/DECOMPOSITION_CONVENTIONS.md](../../docs/DECOMPOSITION_CONVENTIONS.md)
for the decomposition convention.
