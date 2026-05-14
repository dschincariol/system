```text
Slice ID: <SXX>
Goal: <single bounded objective>

In scope:
- <paths/modules>

Out of scope:
- <paths/modules>

Required reading:
- <paths/modules/tests/docs>

Required changes:
- <exact behavior to add/fix>
- <exact interface or schema changes allowed>

Required verification:
- <pytest targets>
- <repo validation command if needed>

Acceptance criteria:
- <observable behavior>
- <specific tests green>
- <no unrelated file edits>

Stop and report if:
- <unexpected dependency>
- <touch set expands>
- <schema or contract ambiguity>
```

## Prompt Conventions

- `DD-<id>` is read-only. It must return the exact touch set, invariants, tests, and stop conditions.
- `IMPL-<id>` may only edit the approved touch set.
- `AUDIT-<id>` must review the implementation diff, rerun verification, and list findings first.
- Every prompt must restate in-scope and out-of-scope paths.
- Every implementation prompt must forbid:
  - unrelated refactors
  - placeholder code
  - silent schema changes outside the named family
  - formatting churn outside touched lines
