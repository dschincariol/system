# Codex Implementation Prompts

Ten self-contained prompts derived from the independent assessment in
`docs/Trading_System_Onboarding.docx`. Each prompt is optimized for
autonomous execution by an OpenAI Codex agent: it specifies what to read,
what to build, what acceptance looks like, and how to test.

Run them in roughly this order. Items 1, 2, and 3 are foundational and
should be merged before 4–10 to maximize statistical and operational
correctness of every downstream change.

| # | Prompt | Theme | Priority |
|---|--------|-------|----------|
| 01 | [Statistical rigor (FDR + t-stat)](01_statistical_rigor.md) | Acceptance gates | P0 |
| 02 | [Combinatorial Purged CV + Almgren-Chriss costs](02_backtest_realism.md) | Backtest realism | P0 |
| 03 | [Optuna hyperparameter optimization](03_optuna_hyperparams.md) | Tuning discipline | P0 |
| 04 | [Postgres/Timescale storage abstraction](04_storage_abstraction.md) | Data infrastructure | P1 |
| 05 | [LightGBM, XGBoost, PatchTST families](05_new_model_families.md) | Model coverage | P1 |
| 06 | [Stacked ridge ensemble](06_ensemble_blending.md) | Ensemble | P1 |
| 07 | [tsfresh + PySR feature discovery](07_feature_discovery.md) | Auto features | P1 |
| 08 | [FinBERT + LLM news features](08_finbert_nlp.md) | NLP depth | P1 |
| 09 | [Causal layer (Granger + DoWhy)](09_causal_layer.md) | Causal alpha | P2 |
| 10 | [RL portfolio manager (PPO/SAC)](10_rl_portfolio_manager.md) | Closed-loop allocator | P2 |

## How to use a prompt with Codex

1. Open the prompt file. The full body is the system prompt — paste it
   verbatim. Do not summarize or trim it.
2. Codex should run inside a clean working tree on a feature branch named
   `codex/<NN>-<short-slug>` (e.g., `codex/01-statistical-rigor`).
3. The prompt enumerates files to read before writing anything. Codex
   must do that reconnaissance pass first; it is the single biggest
   driver of output quality.
4. Each prompt ends with an explicit acceptance checklist. Codex must
   self-verify against it before declaring the task complete and must
   report any unchecked item as a known limitation.

## Conventions used in every prompt

- **Read-only files** are listed first; Codex reads but does not modify
  them. They exist to ground the implementation in current behavior.
- **Files to create or modify** are listed with one-line intent.
- **Acceptance criteria** are testable, not aspirational.
- **Test plan** specifies new test files and the exact `pytest` invocation.
- **Out of scope** is non-negotiable; anything listed there must not be
  touched even if Codex believes it would be an improvement.

## Definition of done (applies to every prompt)

- New code has unit tests; coverage on touched modules is non-decreasing.
- `pytest -q` passes on the affected test paths.
- No new linter errors (project uses default Python tooling — match the
  style of the surrounding files).
- A short `CHANGELOG`-style entry is added at the top of the relevant
  module's README or docstring describing the new behavior.
- The prompt's acceptance checklist is fully ticked, or unticked items
  are reported as deliberate carve-outs with reasoning.
