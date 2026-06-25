# Asset-Class Enablement — Verification Prompt Suite

These are **read-only acceptance audits** for Codex. Each one independently verifies that an
asset-class enablement workstream (built from the prompts in
[`../deep_dive_prompts/`](../deep_dive_prompts/)) was implemented **correctly, completely, and is
wired and functioning in the wider repo** — catching missing, broken, faked-green, or regressed
requirements. They do **not** change code; each Codex run writes an evidence-backed report to this
directory (`*_VERIFICATION_REPORT.md`).

Each audit ships in **two equivalent variants** — pick the one matching the agent you run it in. They verify
the same requirements with the same five lenses; only the execution harness differs (the **Claude** variant adds
subagent fan-out, `TodoWrite` tracking, parallel tool calls, `Read`/`Grep` inspection, and an in-session summary).

| Audits IDs | Claude Code variant | Codex variant | Source enablement prompt |
| --- | --- | --- | --- |
| OPT-01 … OPT-10 | [OPTIONS…CLAUDE](OPTIONS_ENABLEMENT_VERIFICATION_CLAUDE_PROMPTS.md) | [OPTIONS…CODEX](OPTIONS_ENABLEMENT_VERIFICATION_CODEX_PROMPTS.md) | `OPTIONS_ENABLEMENT_DEEP_DIVE_PROMPTS.md` |
| FUT-01 … FUT-10 | [FUTURES…CLAUDE](FUTURES_ENABLEMENT_VERIFICATION_CLAUDE_PROMPTS.md) | [FUTURES…CODEX](FUTURES_ENABLEMENT_VERIFICATION_CODEX_PROMPTS.md) | `FUTURES_ENABLEMENT_DEEP_DIVE_PROMPTS.md` |
| FX-00 … FX-08 | [FX…CLAUDE](FX_ENABLEMENT_VERIFICATION_CLAUDE_PROMPTS.md) | [FX…CODEX](FX_ENABLEMENT_VERIFICATION_CODEX_PROMPTS.md) | `FX_ENABLEMENT_CODEX_PROMPTS.md` |
| EQ-01 … EQ-10 | [EQUITY…CLAUDE](EQUITY_ENABLEMENT_VERIFICATION_CLAUDE_PROMPTS.md) | [EQUITY…CODEX](EQUITY_ENABLEMENT_VERIFICATION_CODEX_PROMPTS.md) | `EQUITY_ENABLEMENT_CODEX_PROMPTS.md` |
| CRYPTO-01 … CRYPTO-06 | [CRYPTO…CLAUDE](CRYPTO_ENABLEMENT_VERIFICATION_CLAUDE_PROMPTS.md) | [CRYPTO…CODEX](CRYPTO_ENABLEMENT_VERIFICATION_CODEX_PROMPTS.md) | `CRYPTO_ENABLEMENT_DEEP_DIVE_PROMPTS.md` |

## How to run

- **One asset class per run** (each prompt is self-contained). Keep the matching source enablement
  prompt open beside it — for each requirement ID, the verifier re-reads its *Definition of Done* /
  *Tests to add* / *Validation commands* and **proves** they were met. In Claude Code, the prompt's
  *How to run this audit in Claude Code* section recommends fanning the requirement IDs out across parallel
  verification subagents and synthesizing the verdicts.
- Each prompt applies **five lenses** to every requirement: runtime enforcement (file:line) · tests present
  & honest · validation commands (exact exit codes) · anti-fake-green probes · wiring & no-regression.
- Suggested order: **FX first** (it is the structural twin the futures/crypto work mirrors), then
  EQUITY (the `asset_map` classifier linchpin), then FUTURES, OPTIONS, CRYPTO. Any order works since each is
  independent.

## What these audits are built to catch

1. **Enforcement-in-tests/docs-only** — a rail that *reports* a value but never *binds* the money path
   (budget still 1.00, borrow never subtracted, greeks aggregating to zero, edge filter never gating live arming).
2. **Fake-green** — passing tests that over-mock the runtime or never drive the production path.
3. **Cross-class regressions** — non-target asset classes must stay byte-for-byte unchanged (no-touch
   `git diff` guards + golden comparisons).
4. **Mistaking intentional gates for gaps** — these features ship deliberately fail-closed / shadow-only /
   default-off / live-disabled, with some seams explicitly unowned (`broker_sim.py:2388`/`:2435`, account-ccy
   conversion, missing `crypto_instrument.py`). The audits separate *legitimate-gated* from *real-defect* in a
   GAP ledger rather than flagging disabled paths as missing.

Each report ends with a per-ID verdict table (`PASS` / `PARTIAL` / `FAKE-GREEN` / `MISSING` / `BROKEN` /
`GATED-OK`) and an overall **GO / NO-GO** with the blocking list.
