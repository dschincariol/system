# Startup Helper Package

Last verified against code: 2026-06-21

`engine/startup/` contains focused helper modules used by the root
`start_system.py` executable facade.

Current split:

- `env.py` owns local `.env` bootstrap helpers, local master-key file creation,
  strict-runtime DB-path detection, and scalar environment parsing used during
  startup.
- `mode.py` owns launch-mode selection from argv and `ENGINE_MODE` for
  `safe`, `paper`, `shadow`, and `live`.
- `phase.py` owns startup phase and first-failure trace mutation helpers.
- `subprocesses.py` owns import-smoke child-process command construction,
  import-smoke result shaping, and runtime-graph validator subprocess
  execution.
- `validation.py` owns startup-validation payload normalization, redaction, and
  validation-gate trace payload construction.
- `dashboard.py` owns dashboard bind waiting and clean-return decision helpers.
- `shutdown.py` owns shutdown-request, signal, and bootstrap side-effect helper
  orchestration.
- `start_system.py` remains the public executable entrypoint and compatibility
  facade. Existing private helper names that tests or operators import continue
  to delegate to this package.

Do not move the top-level `main()` boot ordering out of `start_system.py`.
Additional startup helpers may move here only after characterization tests lock
the facade names, signatures, side effects, and failure shapes.
