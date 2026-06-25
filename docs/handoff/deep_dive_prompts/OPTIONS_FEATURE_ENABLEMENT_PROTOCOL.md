# Options Feature Enablement Protocol

This protocol governs the shadow-only evidence check for enabling the optional
GEX/flow options features:

- `options_symbol.gex_norm_z`
- `options_symbol.gex_sign`
- `options_symbol.opt_flow_imbalance_z`

The harness does not enable live order authority, does not change broker
support, and does not flip `USE_OPTIONS_FEATURES`. It only measures whether the
registry-defined GEX/flow block has out-of-sample evidence relative to the
registry-defined base options feature block.

## Run

From the repository root:

```bash
python tools/options_feature_ablation.py \
  --out artifacts/options_feature_ablation \
  --max-rows 5000 \
  --lookback-days 180
```

Optional core covariates can be supplied without duplicating the options
registry lists:

```bash
python tools/options_feature_ablation.py \
  --core-feature price.ret_1 \
  --core-feature tech.rsi_14 \
  --out artifacts/options_feature_ablation
```

For a deterministic smoke check that does not read runtime tables:

```bash
python tools/options_feature_ablation.py \
  --synthetic \
  --min-rows 50 \
  --min-gex-coverage 0.1
```

## Read The Verdict

The JSON report contains a single machine-readable `verdict`:

- `ABSTAIN_INSUFFICIENT_DATA`: do not enable. Runtime data, GEX/flow coverage,
  usable rows, or training dependencies are insufficient.
- `ENABLE_NOT_SUPPORTED`: do not enable. Data were sufficient, but the
  with-vs-without out-of-sample delta or fold stability failed the configured
  criterion.
- `ENABLE_SUPPORTED`: the configured evidence criterion was met. This supports
  a reviewed shadow feature rollout only; it is not a profitability claim.

The default thresholds are configurable guardrails, not external constants:

- `OPTIONS_ABLATION_MIN_ROWS`
- `OPTIONS_ABLATION_MIN_RANK_IC_DELTA`
- `OPTIONS_ABLATION_MIN_STABILITY_FRACTION`
- `OPTIONS_ABLATION_MIN_GEX_COVERAGE`

## Enable

Only after a reviewed `ENABLE_SUPPORTED` report, an operator may set:

```bash
USE_OPTIONS_FEATURES=1
```

This expands the feature registry to include the three GEX/flow features. It
does not enable live options trading, broker adapters, option order routing, or
options portfolio risk controls.

## Rollback

Unset the flag or set it back to `0`:

```bash
USE_OPTIONS_FEATURES=0
```

After rollback, restart the affected training/serving processes and confirm the
registry smoke check:

```bash
python -c "from engine.strategy.feature_registry import OPTIONS_FEATURE_IDS,_OPTIONS_GEX_FLOW_FEATURE_IDS; assert all(f not in OPTIONS_FEATURE_IDS for f in _OPTIONS_GEX_FLOW_FEATURE_IDS)"
```
