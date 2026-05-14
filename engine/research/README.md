# Research Subsystem

The `engine/research/` package holds offline-only analysis helpers that stress or summarize live strategy and execution artifacts without participating in runtime control flow.

## Current Files

- [alpha_generator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\research\alpha_generator.py)
  Candidate-generation and replay-validation harness for bounded alpha discovery loops that register surviving models in the normal governance path.
- [adversarial_scenario_generator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\research\adversarial_scenario_generator.py)
  Defines deterministic what-if execution stress scenarios for governance and smoke testing.
- [model_fragility_analyzer.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\research\model_fragility_analyzer.py)
  Combines governance snapshots and execution advisories into a compact fragility summary for offline inspection.
- [symbolic_alpha_generator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\research\symbolic_alpha_generator.py)
  Generates bounded symbolic feature candidates for offline alpha discovery and persists train/serve-safe definitions.

## Discovery And Validation Flow

- [alpha_generator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\research\alpha_generator.py)
  Builds candidate model specs, trains them through the existing model families, runs replay validation, and records lifecycle outcomes.
- [symbolic_alpha_generator.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\research\symbolic_alpha_generator.py)
  Supplies deterministic symbolic features that can be promoted into the normal feature registry and candidate-spec flow.
- [../strategy/jobs/alpha_discovery_loop.py](c:\Users\dschi\Documents\GitHub\Trading-System-\engine\strategy\jobs\alpha_discovery_loop.py)
  Operational wrapper that runs the bounded discovery loop without giving research code direct execution authority.

## Maintenance Guidance

- Keep this package read-side and offline-first.
  Research helpers may query stored runtime state, but they should not mutate live execution or startup behavior.
- Reuse existing strategy and execution summaries instead of inventing parallel schemas.
  The point of this package is to stress current system outputs, not to fork the system model.
- If you add a new research harness, document the entrypoint that runs it.
  Offline scripts are easy to lose if they are not linked from a README.
