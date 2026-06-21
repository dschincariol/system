/*
  FILE: ui/metric_glossary.js

  Static explanation registry for dashboard metrics. This module is read-only
  metadata and does not bind DOM behavior or alter runtime business logic.
*/

import {
  classifyMarketStressScore,
  marketStressThresholdRangeText,
} from "./market_stress_thresholds.js";

const MARKET_STRESS_RANGE_TEXT = marketStressThresholdRangeText();

function freezeEntry(key, def) {
  const persona = def && def.persona ? def.persona : {};
  return Object.freeze({
    key,
    label: def && def.label ? def.label : key,
    shortHelp: def && def.shortHelp ? def.shortHelp : "",
    fullHelp: def && def.fullHelp ? def.fullHelp : "",
    unit: def && Object.prototype.hasOwnProperty.call(def, "unit") ? def.unit : null,
    normalRange: def && Object.prototype.hasOwnProperty.call(def, "normalRange") ? def.normalRange : null,
    warningRange: def && Object.prototype.hasOwnProperty.call(def, "warningRange") ? def.warningRange : null,
    aliases: Object.freeze(Array.isArray(def && def.aliases) ? def.aliases.filter(Boolean) : []),
    persona: Object.freeze({
      fund_manager: persona.fund_manager || (def && def.shortHelp) || "",
      operations: persona.operations || (def && def.shortHelp) || "",
    }),
  });
}

const STATUS_METRICS = Object.freeze({
  system_status: freezeEntry("system_status", {
    aliases: ["system_state", "engine_state", "engineState", "systemState"],
    label: "System Status",
    shortHelp: "High-level operating state for the trading system.",
    fullHelp: "This summarizes whether the runtime is operating normally, warming up, degraded, or stopped. It is used as a compact indicator for the overall system state shown in decision and operator summary surfaces.",
    unit: "status",
    normalRange: "LIVE / RUNNING / OK",
    warningRange: "DEGRADED / WARMING_UP / BOOTING / UNKNOWN",
    persona: {
      fund_manager: "Use this as the quickest read on whether the system is in a healthy operating posture before trusting downstream metrics.",
      operations: "This is the top-line runtime state and should align with engine, readiness, and incident conditions before operators enable or resume trading.",
    },
  }),
  data_status: freezeEntry("data_status", {
    label: "Data Status",
    shortHelp: "Status of market and dashboard data flow.",
    fullHelp: "This indicates whether price and related dashboard inputs are flowing, warming up, degraded, or blocked. It is a status rollup rather than a direct market metric.",
    unit: "status",
    normalRange: "RUNNING / CONNECTED / OK / FLOWING",
    warningRange: "DEGRADED / WARMING_UP / WAITING_FOR_DASHBOARD / UNKNOWN",
    persona: {
      fund_manager: "Treat this as a trust gate for any market-sensitive display. Weak data status means the numbers may be stale even if they still render.",
      operations: "This is the operator-facing read on whether upstream market and dashboard inputs are current enough to support normal monitoring.",
    },
  }),
  health_status: freezeEntry("health_status", {
    label: "Health Status",
    shortHelp: "Overall health status shown in the runtime summary.",
    fullHelp: "This is the UI's compact health rollup for whether the dashboard can currently verify healthy system conditions. It is intentionally broader than any single subsystem check.",
    unit: "status",
    normalRange: "OK",
    warningRange: "WARMING_UP / WAITING_FOR_DASHBOARD",
    persona: {
      fund_manager: "A non-OK health state means the dashboard may be operationally incomplete even if some metrics look plausible.",
      operations: "This is the top-line health rollup used by the runtime summary to separate normal operation from warm-up and unreachable states.",
    },
  }),
  execution_status: freezeEntry("execution_status", {
    aliases: ["execution_gate", "execution_allowed", "executionEnabled"],
    label: "Execution Status",
    shortHelp: "Current execution gate state shown in dashboard control summaries.",
    fullHelp: "This indicates whether trading execution is currently allowed, blocked, disabled, or degraded based on the execution barrier and related safety state.",
    unit: "status",
    normalRange: "ALLOWED / ENABLED / LIVE",
    warningRange: "DEGRADED / DISABLED / UNKNOWN",
    persona: {
      fund_manager: "This tells you whether the system is permitted to act, which matters before reading any execution or trading outcome metric as actionable.",
      operations: "This is the operator summary of the execution barrier. A blocked or disabled state should match safety gates and incident context.",
    },
  }),
  training_status: freezeEntry("training_status", {
    aliases: ["training_mode"],
    label: "Training Status",
    shortHelp: "Status text for training availability and mode.",
    fullHelp: "This captures whether model training is allowed and what training mode the dashboard currently reports. It is operational status, not a model quality score.",
    unit: "status",
    normalRange: "ALLOWED / ENABLED / READY / LIVE",
    warningRange: "SAFE / SHADOW / OFF / WARMING_UP / UNKNOWN",
    persona: {
      fund_manager: "This shows whether the platform considers model training available, which affects how quickly fresh model updates can appear.",
      operations: "Operators should read this as a control-state indicator for training, not as evidence that a model is good or bad.",
    },
  }),
  promotion_status: freezeEntry("promotion_status", {
    aliases: ["model_status"],
    label: "Promotion Status",
    shortHelp: "Status of model promotion controls.",
    fullHelp: "This indicates whether model promotion is allowed, blocked, or explicitly turned off. It reflects control state rather than model performance.",
    unit: "status",
    normalRange: "ALLOWED",
    warningRange: "OFF",
    persona: {
      fund_manager: "This explains whether model changes can currently move into the promoted path. It should not be read as a claim that a model deserves promotion.",
      operations: "This is an operator control-state metric. OFF means manually disabled; BLOCKED means prevented by safety conditions.",
    },
  }),
  market_condition: freezeEntry("market_condition", {
    label: "Market Condition",
    shortHelp: "Runtime summary label for current market stress regime.",
    fullHelp: "This is a plain-language label derived from the market stress score. It compresses the raw stress signal into a regime name for the operator summary.",
    unit: "status",
    normalRange: "NORMAL",
    warningRange: "ELEVATED STRESS",
    persona: {
      fund_manager: "Read this as a compact regime label rather than a standalone model output.",
      operations: "This is the operator-facing name for the current market stress regime derived from the stress score.",
    },
  }),
  operating_mood: freezeEntry("operating_mood", {
    aliases: ["mood"],
    label: "Operating Mood",
    shortHelp: "Dashboard summary label for the current operating posture.",
    fullHelp: "This is a synthetic label that combines stress, readiness, health, and barrier state into a plain-language operating posture such as steady, watchful, or defensive.",
    unit: "status",
    normalRange: "STEADY",
    warningRange: "WATCHFUL / GUARDED / CAUTIOUS",
    persona: {
      fund_manager: "This is a convenience label for the system's current posture, not an investment signal.",
      operations: "Use this as a quick operator summary of combined runtime risk posture before drilling into individual checks.",
    },
  }),
  trading_mode: freezeEntry("trading_mode", {
    aliases: ["execution_mode", "executionMode"],
    label: "Trading Mode",
    shortHelp: "Trading mode reported by system status.",
    fullHelp: "This indicates whether the system is operating live, in a safe or shadow mode, or in a non-normal operating mode. It is a control-state metric.",
    unit: "status",
    normalRange: "LIVE",
    warningRange: "SAFE / SHADOW / UNKNOWN",
    persona: {
      fund_manager: "This explains whether the platform is running in a live or more guarded operating mode.",
      operations: "Operators should use this to confirm the runtime mode matches operational intent.",
    },
  }),
  execution_enabled: freezeEntry("execution_enabled", {
    label: "Execution Enabled",
    shortHelp: "Boolean flag for whether execution is enabled in system status.",
    fullHelp: "This is the raw enabled or allowed flag surfaced in the system status header. It is narrower than the overall execution status label and simply reports whether execution is enabled.",
    unit: "boolean",
    normalRange: "true",
    warningRange: "false",
    persona: {
      fund_manager: "This is the direct execution enable flag, useful when checking whether the platform can place trades at all.",
      operations: "Use this as the raw control flag behind the execution header item.",
    },
  }),
  broker_connectivity: freezeEntry("broker_connectivity", {
    aliases: ["broker_status", "brokerStatus"],
    label: "Broker Connectivity",
    shortHelp: "Connectivity status for the broker link.",
    fullHelp: "This indicates whether the broker connection is reported as connected, degraded, unknown, or disconnected. It is a transport and integration status, not a market metric.",
    unit: "status",
    normalRange: "CONNECTED / OK",
    warningRange: "DEGRADED / UNKNOWN",
    persona: {
      fund_manager: "A weak broker state reduces confidence that execution and account snapshots reflect current tradable conditions.",
      operations: "This is the broker health indicator used in the system status header and should align with account and fill visibility.",
    },
  }),
  startup_ready: freezeEntry("startup_ready", {
    label: "Startup Ready",
    shortHelp: "Overall readiness flag for the operator startup checklist.",
    fullHelp: "This indicates whether the dashboard startup checklist considers all required gates passed for normal operation.",
    unit: "boolean",
    normalRange: "true",
    warningRange: "false",
    persona: {
      fund_manager: "This is an operator readiness gate, not a trading alpha signal.",
      operations: "Operators use this as the final startup-readiness gate before enabling live operation.",
    },
  }),
  data_feed_ok: freezeEntry("data_feed_ok", {
    label: "Data Feed Ready",
    shortHelp: "Startup checklist flag for data feed readiness.",
    fullHelp: "This indicates whether the startup checklist considers the data feed requirement satisfied.",
    unit: "boolean",
    normalRange: "true",
    warningRange: "false",
    persona: {
      fund_manager: "This is a startup prerequisite showing whether the system believes it has the required data feed in place.",
      operations: "Operators should treat a false value as an unmet startup gate for safe operation.",
    },
  }),
  models_ok: freezeEntry("models_ok", {
    label: "Models Ready",
    shortHelp: "Startup checklist flag for model readiness.",
    fullHelp: "This indicates whether the startup checklist considers the required models available and ready.",
    unit: "boolean",
    normalRange: "true",
    warningRange: "false",
    persona: {
      fund_manager: "This is a startup readiness check for model availability, not a measure of model quality.",
      operations: "Operators should treat a false value as a startup gate failure for model readiness.",
    },
  }),
  risk_ok: freezeEntry("risk_ok", {
    label: "Risk Checks Ready",
    shortHelp: "Startup checklist flag for risk readiness.",
    fullHelp: "This indicates whether the startup checklist considers the risk subsystem ready for operation.",
    unit: "boolean",
    normalRange: "true",
    warningRange: "false",
    persona: {
      fund_manager: "This is an operational gate showing whether the platform believes core risk controls are ready.",
      operations: "A false value means the startup checklist still considers risk readiness incomplete.",
    },
  }),
  broker_ok: freezeEntry("broker_ok", {
    label: "Broker Ready",
    shortHelp: "Startup checklist flag for broker readiness.",
    fullHelp: "This indicates whether the startup checklist considers the broker integration ready for operation.",
    unit: "boolean",
    normalRange: "true",
    warningRange: "false",
    persona: {
      fund_manager: "This is a readiness flag for broker integration rather than a trading or performance metric.",
      operations: "Operators should read this as the startup checklist's broker gate.",
    },
  }),
});

const MARKET_STRESS_METRICS = Object.freeze({
  market_stress_score: freezeEntry("market_stress_score", {
    aliases: ["stress_score"],
    label: "Market Stress Score",
    shortHelp: "Composite cross-asset market stress gauge.",
    fullHelp: "This score combines volatility, rates, credit, and term-structure inputs into a single stress reading used by the dashboard. Higher values indicate broader market strain.",
    unit: "score",
    normalRange: MARKET_STRESS_RANGE_TEXT.normal,
    warningRange: MARKET_STRESS_RANGE_TEXT.warning,
    persona: {
      fund_manager: "This is the main regime-level market stress measure surfaced by the dashboard. Higher readings mean the environment is becoming harder for normal positioning.",
      operations: "Operators use this as the canonical stress gauge behind the market condition pill and stress banner.",
    },
  }),
  market_stress_updated_ts_ms: freezeEntry("market_stress_updated_ts_ms", {
    aliases: ["market_stress.ts_ms", "stress.ts_ms"],
    label: "Market Stress Updated Time",
    shortHelp: "Timestamp of the market-stress snapshot shown in the panel header.",
    fullHelp: "This is the update timestamp rendered next to the market stress summary. It tells the user when the currently displayed stress snapshot was produced.",
    unit: "timestamp_ms",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows how fresh the displayed market-stress snapshot is.",
      operations: "This is the timestamp shown in the market stress panel header for the current stress snapshot.",
    },
  }),
  vix: freezeEntry("vix", {
    label: "VIX",
    shortHelp: "Implied volatility level for large-cap US equities.",
    fullHelp: "VIX is the displayed options-implied volatility level used in the market stress panel. It is one component of the broader stress score.",
    unit: "index",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the raw implied-volatility input, useful for understanding whether stress is coming from equities volatility specifically.",
      operations: "This is a raw component in the market stress breakdown and should be read alongside its z-score.",
    },
  }),
  z_vix: freezeEntry("z_vix", {
    label: "VIX Z-Score",
    shortHelp: "Standardized VIX deviation used by the stress model.",
    fullHelp: "This shows how unusual the current VIX reading is relative to its comparison baseline. It is a standardized input, so magnitude matters more than the raw sign alone.",
    unit: "z-score",
    normalRange: "-1 to 1",
    warningRange: "absolute value > 1 and <= 2",
    persona: {
      fund_manager: "Use this to judge whether volatility is merely elevated or statistically unusual relative to its own history.",
      operations: "This is the normalized VIX component used in the stress model. Large absolute values mean the raw VIX level is materially out of family.",
    },
  }),
  vvix: freezeEntry("vvix", {
    label: "VVIX",
    shortHelp: "Implied volatility of VIX options.",
    fullHelp: "VVIX reflects the market's pricing of volatility in volatility itself. It appears in the stress panel as a raw component of the broader stress score.",
    unit: "index",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows how unstable the volatility regime itself appears, not just whether equity volatility is high.",
      operations: "Read this alongside its z-score to see whether volatility-of-volatility is materially elevated.",
    },
  }),
  z_vvix: freezeEntry("z_vvix", {
    label: "VVIX Z-Score",
    shortHelp: "Standardized VVIX deviation used by the stress model.",
    fullHelp: "This is the normalized VVIX component used in the market stress model. Large absolute values indicate that volatility-of-volatility is unusually far from baseline.",
    unit: "z-score",
    normalRange: "-1 to 1",
    warningRange: "absolute value > 1 and <= 2",
    persona: {
      fund_manager: "Use this to see whether the vol-of-vol backdrop is just elevated or genuinely unusual.",
      operations: "This is the normalized VVIX stress component and is more directly comparable over time than raw VVIX.",
    },
  }),
  move: freezeEntry("move", {
    label: "MOVE",
    shortHelp: "Treasury market implied-volatility level.",
    fullHelp: "MOVE is the displayed rates-volatility index in the market stress panel. It captures stress coming from the Treasury market rather than equities.",
    unit: "index",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This helps separate equity-led stress from rates-led stress in the market backdrop.",
      operations: "This is the raw rates-volatility component in the market stress breakdown.",
    },
  }),
  z_move: freezeEntry("z_move", {
    label: "MOVE Z-Score",
    shortHelp: "Standardized MOVE deviation used by the stress model.",
    fullHelp: "This is the normalized rates-volatility component of the market stress model. Large absolute values indicate materially unusual rates volatility.",
    unit: "z-score",
    normalRange: "-1 to 1",
    warningRange: "absolute value > 1 and <= 2",
    persona: {
      fund_manager: "Use this to gauge whether rates volatility is unusually disruptive rather than merely elevated.",
      operations: "This lets operators compare rates stress over time on a normalized scale.",
    },
  }),
  vix1d_over_vix: freezeEntry("vix1d_over_vix", {
    label: "VIX1D / VIX",
    shortHelp: "Very short-term volatility term-structure ratio.",
    fullHelp: "This ratio compares one-day implied volatility with the standard VIX. It is used as a short-end term-structure signal in the market stress panel.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This helps show whether very near-term volatility is spiking relative to the broader one-month volatility curve.",
      operations: "This is one of the term-structure inputs that can explain why the stress score changes even when the headline VIX level is stable.",
    },
  }),
  vix9d_over_vix: freezeEntry("vix9d_over_vix", {
    label: "VIX9D / VIX",
    shortHelp: "Short-term versus one-month volatility ratio.",
    fullHelp: "This ratio compares short-dated implied volatility with the standard VIX. It helps describe how front-end volatility is behaving relative to the one-month point.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This term-structure ratio helps show whether stress is concentrated in the front of the volatility curve.",
      operations: "This is a term-structure component used in the market stress breakdown.",
    },
  }),
  vix3m_over_vix: freezeEntry("vix3m_over_vix", {
    label: "VIX3M / VIX",
    shortHelp: "Three-month versus one-month volatility ratio.",
    fullHelp: "This ratio compares medium-horizon implied volatility with the one-month VIX. It helps describe whether the volatility curve is steepening or inverting.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This helps separate a brief volatility event from a more persistent repricing of risk.",
      operations: "This term-structure ratio is shown directly in the market stress panel and contributes context for regime changes.",
    },
  }),
  z_term: freezeEntry("z_term", {
    label: "Term Structure Z-Score",
    shortHelp: "Standardized volatility term-structure signal.",
    fullHelp: "This is the normalized term-structure component used by the market stress model. It summarizes whether the volatility curve shape is unusual relative to baseline.",
    unit: "z-score",
    normalRange: "-1 to 1",
    warningRange: "absolute value > 1 and <= 2",
    persona: {
      fund_manager: "Use this to see whether the shape of the volatility curve, not just its level, is unusually stressed.",
      operations: "This is the normalized term-structure input behind part of the composite stress score.",
    },
  }),
  credit_lqd_over_hyg: freezeEntry("credit_lqd_over_hyg", {
    label: "LQD / HYG",
    shortHelp: "Credit risk ratio used in the stress breakdown.",
    fullHelp: "This ratio compares investment-grade and high-yield credit proxies to capture widening or tightening risk tone in credit markets.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This helps identify whether credit is contributing to the current stress regime.",
      operations: "This is the credit-market component displayed in the stress breakdown.",
    },
  }),
  z_credit: freezeEntry("z_credit", {
    label: "Credit Z-Score",
    shortHelp: "Standardized credit component of the stress model.",
    fullHelp: "This is the normalized credit-market input in the market stress model. Large absolute values indicate credit conditions are unusually far from baseline.",
    unit: "z-score",
    normalRange: "-1 to 1",
    warningRange: "absolute value > 1 and <= 2",
    persona: {
      fund_manager: "Use this to judge whether credit conditions are contributing meaningfully to the system's stress reading.",
      operations: "This is the normalized credit input used by the stress model.",
    },
  }),
  rates_tlt_over_shy: freezeEntry("rates_tlt_over_shy", {
    label: "TLT / SHY",
    shortHelp: "Rates-market ratio used in the stress breakdown.",
    fullHelp: "This ratio compares long-duration and short-duration Treasury proxies to provide a compact rates-market signal used in the stress panel.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This helps show whether rates positioning and duration stress are part of the current market regime.",
      operations: "This is a raw rates component in the stress breakdown and should be interpreted with its z-score.",
    },
  }),
  z_rates: freezeEntry("z_rates", {
    label: "Rates Z-Score",
    shortHelp: "Standardized rates component of the stress model.",
    fullHelp: "This is the normalized rates-market input used in the composite market stress score. Large absolute values indicate unusually large deviation from baseline.",
    unit: "z-score",
    normalRange: "-1 to 1",
    warningRange: "absolute value > 1 and <= 2",
    persona: {
      fund_manager: "Use this to judge whether rates conditions, not just equities volatility, are driving the current stress regime.",
      operations: "This is the normalized rates input used in the market stress model.",
    },
  }),
});

const EXECUTION_METRICS = Object.freeze({
  execution_confidence_low: freezeEntry("execution_confidence_low", {
    aliases: ["conf_lo"],
    label: "Execution Confidence Low",
    shortHelp: "Lower bound of a confidence bucket in execution analytics.",
    fullHelp: "This is the lower edge of the confidence bucket shown in the execution cost table. It helps group fills by model confidence range before comparing realized cost.",
    unit: "score",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is a bucket boundary, not a performance score by itself. Read it together with fill count and mean cost.",
      operations: "This defines the lower bound of the execution analytics bucket used for the cost table.",
    },
  }),
  execution_confidence_high: freezeEntry("execution_confidence_high", {
    aliases: ["conf_hi"],
    label: "Execution Confidence High",
    shortHelp: "Upper bound of a confidence bucket in execution analytics.",
    fullHelp: "This is the upper edge of the confidence bucket shown in the execution cost table. Together with the lower bound it defines the confidence range for the fills in that row.",
    unit: "score",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This completes the confidence interval for the bucket, which helps show how cost behaved across model-confidence ranges.",
      operations: "This defines the upper bound of the execution analytics bucket used for the cost table.",
    },
  }),
  execution_fill_count: freezeEntry("execution_fill_count", {
    aliases: ["n_fills"],
    label: "Execution Fill Count",
    shortHelp: "Number of fills in a confidence bucket.",
    fullHelp: "This is the fill count for the corresponding confidence bucket in the execution metrics table. It indicates how much realized data supports that bucket's average cost.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This tells you how much sample support exists behind the displayed average cost for a bucket.",
      operations: "This is the sample size behind the bucketed execution cost figure.",
    },
  }),
  execution_mean_cost: freezeEntry("execution_mean_cost", {
    aliases: ["mean_cost", "avg_cost"],
    label: "Execution Mean Cost",
    shortHelp: "Average realized execution cost for a confidence bucket.",
    fullHelp: "This is the average realized execution cost shown for the fills in a confidence bucket. The dashboard uses it to compare cost across model-confidence ranges, not to express trading alpha.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "Use this to see whether execution got more or less costly in a given confidence band.",
      operations: "This is the canonical cost figure in the execution metrics table and the source for execution degradation checks.",
    },
  }),
});

const PORTFOLIO_METRICS = Object.freeze({
  portfolio_weight: freezeEntry("portfolio_weight", {
    label: "Portfolio Weight",
    shortHelp: "Current portfolio weight for a position.",
    fullHelp: "This is the displayed target or current portfolio weight for a symbol in the portfolio state table. It is shown as a raw decimal weight rather than a percent label in the current UI.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the current portfolio allocation weight for a symbol.",
      operations: "This is the allocation value rendered in the portfolio state table.",
    },
  }),
  portfolio_from_weight: freezeEntry("portfolio_from_weight", {
    aliases: ["from_weight"],
    label: "Order From Weight",
    shortHelp: "Starting portfolio weight for an order transition.",
    fullHelp: "This is the starting weight shown for a portfolio order before the requested change is applied.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows the allocation the order is moving away from.",
      operations: "This is the source weight shown in the portfolio orders table.",
    },
  }),
  portfolio_to_weight: freezeEntry("portfolio_to_weight", {
    aliases: ["to_weight"],
    label: "Order To Weight",
    shortHelp: "Ending portfolio weight for an order transition.",
    fullHelp: "This is the destination weight shown for a portfolio order after the requested change is applied.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows the target allocation the order is moving toward.",
      operations: "This is the destination weight shown in the portfolio orders table.",
    },
  }),
  portfolio_delta_weight: freezeEntry("portfolio_delta_weight", {
    aliases: ["delta_weight"],
    label: "Order Delta Weight",
    shortHelp: "Net portfolio weight change for an order.",
    fullHelp: "This is the displayed net change in portfolio weight for the order row. It is a decimal allocation delta rather than a currency value.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the allocation change requested by the order.",
      operations: "This is the net portfolio-weight delta shown in the orders table.",
    },
  }),
  portfolio_opened_ts_ms: freezeEntry("portfolio_opened_ts_ms", {
    aliases: ["opened_ts_ms"],
    label: "Position Opened Time",
    shortHelp: "Timestamp when the portfolio position was opened.",
    fullHelp: "This is the position open timestamp rendered in the portfolio state table.",
    unit: "timestamp_ms",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows when the current position entry began.",
      operations: "This is the raw opened timestamp for the portfolio state row.",
    },
  }),
  portfolio_updated_ts_ms: freezeEntry("portfolio_updated_ts_ms", {
    aliases: ["updated_ts_ms"],
    label: "Position Updated Time",
    shortHelp: "Timestamp of the most recent portfolio state update.",
    fullHelp: "This is the last update timestamp rendered for the portfolio state row.",
    unit: "timestamp_ms",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This tells you how recently the displayed position row was updated.",
      operations: "This is the raw last-update timestamp for the portfolio state row.",
    },
  }),
  portfolio_order_ts_ms: freezeEntry("portfolio_order_ts_ms", {
    label: "Portfolio Order Time",
    shortHelp: "Timestamp of a portfolio order row.",
    fullHelp: "This is the order timestamp rendered in the portfolio orders table.",
    unit: "timestamp_ms",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows when the portfolio order row was recorded.",
      operations: "This is the order timestamp shown in the portfolio orders table.",
    },
  }),
  broker_equity: freezeEntry("broker_equity", {
    aliases: ["account.equity"],
    label: "Broker Equity",
    shortHelp: "Broker-reported account equity.",
    fullHelp: "This is the broker snapshot's equity value rendered in the broker panel. It is an account-state value, not a backtest equity series point.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the broker-reported account equity visible in the dashboard broker snapshot.",
      operations: "This is the account equity value shown in the broker snapshot panel.",
    },
  }),
  broker_cash: freezeEntry("broker_cash", {
    aliases: ["account.cash"],
    label: "Broker Cash",
    shortHelp: "Broker-reported account cash balance.",
    fullHelp: "This is the cash value rendered in the broker snapshot panel.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the visible cash balance in the broker snapshot.",
      operations: "This is the broker-reported cash figure shown in the broker panel.",
    },
  }),
  position_quantity: freezeEntry("position_quantity", {
    label: "Position Quantity",
    shortHelp: "Broker-reported quantity for a live position.",
    fullHelp: "This is the quantity displayed for a broker position in the broker snapshot.",
    unit: "shares",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the broker-side live quantity for a position.",
      operations: "This is the position quantity shown in the broker snapshot.",
    },
  }),
  position_average_price: freezeEntry("position_average_price", {
    aliases: ["avg_px"],
    label: "Position Average Price",
    shortHelp: "Average broker-reported entry price for a position.",
    fullHelp: "This is the average price displayed for a live broker position in the broker snapshot.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the position's broker-reported average entry price.",
      operations: "This is the average price shown for a broker position.",
    },
  }),
  fill_quantity: freezeEntry("fill_quantity", {
    label: "Fill Quantity",
    shortHelp: "Quantity shown for a recent broker fill.",
    fullHelp: "This is the fill quantity rendered in the recent fills section of the broker snapshot.",
    unit: "shares",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the size of the recent fill shown in the broker snapshot.",
      operations: "This is the quantity value for a rendered broker fill row.",
    },
  }),
  fill_ts_ms: freezeEntry("fill_ts_ms", {
    aliases: ["fills.ts_ms"],
    label: "Fill Time",
    shortHelp: "Timestamp of a rendered broker fill row.",
    fullHelp: "This is the timestamp shown for a recent fill in the broker snapshot.",
    unit: "timestamp_ms",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows when the displayed broker fill occurred.",
      operations: "This is the timestamp shown for each recent fill row in the broker snapshot.",
    },
  }),
  position_size: freezeEntry("position_size", {
    label: "Position Size",
    shortHelp: "Displayed live or target position size for a row.",
    fullHelp: "This is the position size rendered in dashboard position or target tables. It is distinct from portfolio weight because it reflects units held rather than allocation share.",
    unit: "shares",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the displayed position size in units, separate from portfolio weight.",
      operations: "This is the units-held field shown in dashboard position tables.",
    },
  }),
  fill_price: freezeEntry("fill_price", {
    label: "Fill Price",
    shortHelp: "Execution price shown for a recent broker fill.",
    fullHelp: "This is the execution price rendered in the recent fills section of the broker snapshot.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the fill price visible in the broker snapshot for recent executions.",
      operations: "This is the price field shown for recent broker fills.",
    },
  }),
  equity_drift_pct: freezeEntry("equity_drift_pct", {
    aliases: ["diff_equity_pct"],
    label: "Equity Drift Percent",
    shortHelp: "Percent drift between broker and reference equity.",
    fullHelp: "This is the percent drift series shown in the equity drift panel. It is rendered as a percentage change, with the underlying value stored as a decimal fraction.",
    unit: "percent",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows how far live broker equity has drifted from the reference equity series.",
      operations: "This is the drift series plotted in the equity drift panel and should be watched for reconciliation issues.",
    },
  }),
  equity_diff_value: freezeEntry("equity_diff_value", {
    aliases: ["diff_equity"],
    label: "Equity Difference",
    shortHelp: "Absolute broker-versus-reference equity difference.",
    fullHelp: "This is the absolute equity difference reported by the reconciliation endpoint.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the raw dollar gap between broker and reference equity.",
      operations: "This is the absolute reconciliation gap used alongside the percent drift and level indicator.",
    },
  }),
  equity_diff_level: freezeEntry("equity_diff_level", {
    label: "Equity Difference Level",
    shortHelp: "Severity label for broker-backtest equity reconciliation.",
    fullHelp: "This is the reconciliation severity level shown in the equity reconciliation card. It is a categorical interpretation of the broker-versus-reference equity gap.",
    unit: "status",
    normalRange: "OK / RESOLVED",
    warningRange: "WARN / ACKED / PENDING",
    persona: {
      fund_manager: "This is the severity label for equity reconciliation, not the gap itself.",
      operations: "Operators should use this as the top-line severity state for equity reconciliation.",
    },
  }),
});

const PERFORMANCE_METRICS = Object.freeze({
  net_asset_value: freezeEntry("net_asset_value", {
    aliases: ["nav"],
    label: "Net Asset Value",
    shortHelp: "Displayed net asset value slot in dashboard telemetry.",
    fullHelp: "This is the telemetry label for portfolio net asset value. In the current dashboard path it may remain unavailable when no canonical live NAV source is present.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the portfolio NAV display slot, intended to show total portfolio value when a canonical source is available.",
      operations: "This is the telemetry-strip NAV field. If it shows unavailable, the UI does not currently have a canonical live NAV source for that path.",
    },
  }),
  total_return: freezeEntry("total_return", {
    label: "Total Return",
    shortHelp: "Backtest total return displayed in dashboard telemetry and summary.",
    fullHelp: "This is the total return figure rendered from the latest portfolio backtest summary. It is shown as a percentage based on the underlying decimal return value.",
    unit: "percent",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the backtest total return currently surfaced in the dashboard.",
      operations: "This is the canonical total return value displayed from the latest portfolio backtest summary.",
    },
  }),
  max_drawdown: freezeEntry("max_drawdown", {
    label: "Maximum Drawdown",
    shortHelp: "Worst peak-to-trough decline in the displayed backtest path.",
    fullHelp: "This is the maximum drawdown figure shown in the portfolio backtest summary and telemetry. It is rendered as a percentage from an underlying decimal drawdown value.",
    unit: "percent",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows the worst historical loss stretch in the displayed backtest path.",
      operations: "This is the canonical drawdown summary figure for the latest portfolio backtest run.",
    },
  }),
  sharpe_ratio: freezeEntry("sharpe_ratio", {
    aliases: ["sharpe"],
    label: "Sharpe Ratio",
    shortHelp: "Risk-adjusted return ratio from the displayed backtest summary.",
    fullHelp: "This is the Sharpe ratio rendered in the backtest summary and telemetry from the latest portfolio backtest run.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the dashboard's displayed risk-adjusted return ratio for the latest backtest summary.",
      operations: "This is the canonical Sharpe figure currently surfaced from the latest portfolio backtest summary.",
    },
  }),
  sortino_ratio: freezeEntry("sortino_ratio", {
    label: "Sortino Ratio",
    shortHelp: "Downside-risk-adjusted return ratio from the backtest summary.",
    fullHelp: "This is the Sortino ratio displayed in the portfolio backtest summary row.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This gives a downside-risk-adjusted return read for the displayed backtest summary.",
      operations: "This is the Sortino figure shown in the backtest summary row.",
    },
  }),
  calmar_ratio: freezeEntry("calmar_ratio", {
    label: "Calmar Ratio",
    shortHelp: "Return-to-drawdown ratio from the backtest summary.",
    fullHelp: "This is the Calmar ratio displayed in the portfolio backtest summary row.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows return relative to drawdown in the displayed backtest summary.",
      operations: "This is the Calmar figure shown in the backtest summary row.",
    },
  }),
  trade_count: freezeEntry("trade_count", {
    aliases: ["trades", "steps_used"],
    label: "Trade Count",
    shortHelp: "Displayed trade or step count in the backtest summary.",
    fullHelp: "This is the count shown as n in the backtest summary row. The current implementation sources it from the backtest metrics step count.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows how many displayed steps or trades support the backtest summary row.",
      operations: "This is the sample count surfaced in the backtest summary row.",
    },
  }),
  turnover_avg: freezeEntry("turnover_avg", {
    aliases: ["turnover"],
    label: "Average Turnover",
    shortHelp: "Average turnover shown in the backtest summary.",
    fullHelp: "This is the average turnover figure displayed as tau in the portfolio backtest summary row.",
    unit: "ratio",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows the average amount of portfolio turnover in the displayed backtest summary.",
      operations: "This is the average turnover value rendered in the backtest summary row.",
    },
  }),
});
const OPERATIONS_METRICS = Object.freeze({
  cpu_percent: freezeEntry("cpu_percent", {
    label: "CPU Usage",
    shortHelp: "Process or host CPU usage shown in telemetry.",
    fullHelp: "This is the CPU usage metric rendered in the dashboard telemetry strip. It is already expressed as a percent value rather than a decimal fraction.",
    unit: "percent_points",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is an infrastructure load metric, useful only for judging operational strain on the platform.",
      operations: "This is the telemetry-strip CPU usage value and should be read as an operational capacity indicator.",
    },
  }),
  process_rss_mb: freezeEntry("process_rss_mb", {
    label: "Memory Usage",
    shortHelp: "Resident memory footprint shown in telemetry.",
    fullHelp: "This is the process memory metric rendered in the dashboard telemetry strip, expressed in megabytes.",
    unit: "mb",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is an operational memory-usage metric, not a market or performance measure.",
      operations: "This is the telemetry-strip memory reading for process footprint.",
    },
  }),
  db_size_mb: freezeEntry("db_size_mb", {
    label: "Database Size",
    shortHelp: "Rendered database size or storage footprint metric.",
    fullHelp: "This is the database size metric shown in the dashboard telemetry strip, expressed in megabytes.",
    unit: "mb",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is infrastructure context for the dashboard rather than a trading metric.",
      operations: "This is the telemetry-strip database size figure.",
    },
  }),
  market_data_latency_ms: freezeEntry("market_data_latency_ms", {
    aliases: ["price_age_ms", "prices_age_ms", "marketDataLatencyMs"],
    label: "Market Data Latency",
    shortHelp: "Displayed age or latency of market data in milliseconds.",
    fullHelp: "This is the freshness metric shown as latency in the system status header. In the dashboard it may be normalized from market data latency or price age fields, so it should be read as data freshness rather than raw network round-trip time.",
    unit: "ms",
    normalRange: "<= 1000 ms",
    warningRange: "> 1000 ms and <= 5000 ms",
    persona: {
      fund_manager: "This is the dashboard's direct read on how fresh the market data feed appears, which affects trust in displayed market-sensitive values.",
      operations: "This is the canonical freshness or latency figure used in the system status header.",
    },
  }),
  alert_count: freezeEntry("alert_count", {
    aliases: ["alerts", "unresolved_alerts"],
    label: "Alert Count",
    shortHelp: "Count of active or unresolved alerts shown in the header.",
    fullHelp: "This is the alert-count figure rendered in the system status header. It is used as an operational load and attention signal rather than a market metric.",
    unit: "count",
    normalRange: "0",
    warningRange: "1 to 3",
    persona: {
      fund_manager: "This is the number of active alerts the system is surfacing, which can indicate a noisier or riskier operating state.",
      operations: "Operators use this as the top-line count of active alerts in the system header.",
    },
  }),
  crit_count: freezeEntry("crit_count", {
    label: "Critical Alert Count",
    shortHelp: "Count of critical alerts shown in the decision bar.",
    fullHelp: "This is the critical-alert count displayed in the decision bar. It is a categorical count of alerts currently classified as critical.",
    unit: "count",
    normalRange: "0",
    warningRange: null,
    persona: {
      fund_manager: "Any non-zero value means the dashboard is reporting critical conditions that should be understood before trusting business-as-usual operation.",
      operations: "This is the critical-alert count used in the decision bar and should match the underlying alert feed.",
    },
  }),
  warn_count: freezeEntry("warn_count", {
    label: "Warning Alert Count",
    shortHelp: "Count of warning alerts shown in the decision bar.",
    fullHelp: "This is the warning-alert count displayed in the decision bar. It is a count of alerts currently classified as warnings.",
    unit: "count",
    normalRange: "0",
    warningRange: "> 0",
    persona: {
      fund_manager: "A non-zero value means the system is tracking warning conditions even if no critical incidents are active.",
      operations: "This is the warning-alert count used in the decision bar.",
    },
  }),
  prices_ok: freezeEntry("prices_ok", {
    aliases: ["prices.ok", "health.prices.ok"],
    label: "Prices OK",
    shortHelp: "Boolean data-health flag for price freshness.",
    fullHelp: "This is the price-data health flag behind the Price Data Freshness pill. It indicates whether the dashboard currently considers price data acceptable.",
    unit: "boolean",
    normalRange: "true",
    warningRange: null,
    persona: {
      fund_manager: "This is the direct flag for whether the dashboard considers price data acceptable right now.",
      operations: "This is the raw health flag behind the price-data pill.",
    },
  }),
  labels_ok: freezeEntry("labels_ok", {
    aliases: ["labels.ok", "health.labels.ok"],
    label: "Labels OK",
    shortHelp: "Boolean health flag for label generation.",
    fullHelp: "This is the label-generation health flag behind the Label Generation Status pill.",
    unit: "boolean",
    normalRange: "true",
    warningRange: null,
    persona: {
      fund_manager: "This indicates whether the dashboard considers label generation healthy.",
      operations: "This is the raw health flag behind the label-generation pill.",
    },
  }),
  model_ok: freezeEntry("model_ok", {
    aliases: ["model.ok", "health.model.ok"],
    label: "Model OK",
    shortHelp: "Boolean health flag for the active model path.",
    fullHelp: "This is the model-health flag behind the Active Model Health pill.",
    unit: "boolean",
    normalRange: "true",
    warningRange: null,
    persona: {
      fund_manager: "This indicates whether the active model path is currently reported healthy.",
      operations: "This is the raw health flag behind the active-model pill.",
    },
  }),
  healthy_providers: freezeEntry("healthy_providers", {
    label: "Healthy Providers",
    shortHelp: "Count of healthy market-data providers.",
    fullHelp: "This is the count of healthy providers shown in the price-data health details. It helps explain whether price-data freshness is supported by active upstream sources.",
    unit: "count",
    normalRange: ">= 1",
    warningRange: null,
    persona: {
      fund_manager: "This shows how many upstream providers are currently healthy enough to support price-data freshness.",
      operations: "Operators use this as part of diagnosing market-data health and redundancy.",
    },
  }),
  fresh_rows: freezeEntry("fresh_rows", {
    label: "Fresh Rows",
    shortHelp: "Count of freshly ingested rows used in dashboard data-health displays.",
    fullHelp: "This is the fresh-row count shown in dashboard ingestion and health surfaces. It describes observed fresh input volume, not market opportunity.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This gives context on whether fresh data is actually arriving, but it does not have a universal good threshold.",
      operations: "This is the fresh-input row count used in health and ingestion displays.",
    },
  }),
  fresh_symbols: freezeEntry("fresh_symbols", {
    label: "Fresh Symbols",
    shortHelp: "Count of symbols with fresh data in ingestion details.",
    fullHelp: "This is the fresh-symbol count shown as supporting context alongside Fresh Rows in dashboard ingestion details.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This shows how many symbols contributed to the currently fresh data snapshot.",
      operations: "This is the symbol-count context shown with the Fresh Rows metric in ingestion details.",
    },
  }),
  visible_jobs_running: freezeEntry("visible_jobs_running", {
    label: "Visible Jobs",
    shortHelp: "Count of visible ingestion or runtime jobs currently running.",
    fullHelp: "This is the visible-job count rendered in dashboard ingestion details. It is an operational activity indicator rather than a trading metric.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is infrastructure context about visible background job activity.",
      operations: "This is the displayed count of visible jobs currently running in the ingestion detail card.",
    },
  }),
  labels_count: freezeEntry("labels_count", {
    aliases: ["labels.count"],
    label: "Labels Count",
    shortHelp: "Count of labels reported by the health payload.",
    fullHelp: "This is the label count shown in the Label Generation Status pill details.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is supporting context for label-generation health rather than a score of model quality.",
      operations: "This is the raw label count displayed in the label-generation health pill.",
    },
  }),
  model_support_n: freezeEntry("model_support_n", {
    aliases: ["support_n", "model.support_n"],
    label: "Model Support N",
    shortHelp: "Support count shown in the active model health pill.",
    fullHelp: "This is the support-count figure shown as n in the Active Model Health pill details.",
    unit: "count",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is supporting context for the active model health readout, not a direct quality ranking.",
      operations: "This is the support-count detail shown for the active model health pill.",
    },
  }),
  pnl_total: freezeEntry("pnl_total", {
    aliases: ["total_pnl", "day_pnl", "daily_pnl"],
    label: "Total PnL",
    shortHelp: "Displayed total profit and loss summary.",
    fullHelp: "This is the total PnL figure shown in the dashboard live PnL surfaces. Depending on the payload it may be sourced from total or day-level PnL fields already normalized by the UI.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the headline displayed PnL value currently surfaced by the dashboard.",
      operations: "This is the canonical total PnL figure after the UI's existing field normalization.",
    },
  }),
  pnl_unrealized: freezeEntry("pnl_unrealized", {
    aliases: ["unrealized_pnl", "unrealized"],
    label: "Unrealized PnL",
    shortHelp: "Displayed unrealized profit and loss.",
    fullHelp: "This is the unrealized PnL figure shown in dashboard live PnL surfaces.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the mark-to-market portion of the displayed PnL that has not yet been realized.",
      operations: "This is the unrealized PnL figure displayed by the dashboard.",
    },
  }),
  pnl_realized: freezeEntry("pnl_realized", {
    aliases: ["realized_pnl", "realized"],
    label: "Realized PnL",
    shortHelp: "Displayed realized profit and loss.",
    fullHelp: "This is the realized PnL figure shown in dashboard live PnL surfaces.",
    unit: "usd",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the portion of displayed PnL already locked in through executed trades.",
      operations: "This is the realized PnL figure surfaced in the dashboard.",
    },
  }),
});

const ALERT_METRICS = Object.freeze({
  alert_severity: freezeEntry("alert_severity", {
    aliases: ["severity"],
    label: "Alert Severity",
    shortHelp: "Severity label attached to an alert row.",
    fullHelp: "This is the severity category shown for an alert, such as INFO, WARN, or CRIT. It is a categorical prioritization signal, not a numeric measure.",
    unit: "status",
    normalRange: "INFO",
    warningRange: "WARN / HIGH",
    persona: {
      fund_manager: "This is the dashboard's priority label for the alert, useful for triage but not a market metric on its own.",
      operations: "Operators use this to prioritize incidents and alert review.",
    },
  }),
  alert_age_minutes: freezeEntry("alert_age_minutes", {
    label: "Alert Age",
    shortHelp: "Elapsed age of an alert in minutes.",
    fullHelp: "This is the elapsed time since the alert was generated, rendered in alert-related UI surfaces.",
    unit: "minutes",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This helps show how recent the alert is.",
      operations: "This is the alert age used for triage recency, expressed in minutes.",
    },
  }),
  horizon_s: freezeEntry("horizon_s", {
    label: "Alert Horizon",
    shortHelp: "Forecast or decision horizon shown with an alert.",
    fullHelp: "This is the alert horizon in seconds. It indicates the intended forward-looking window associated with the alert context.",
    unit: "seconds",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This tells you the time horizon the alert is speaking about.",
      operations: "This is the alert horizon field rendered in alert-related UI surfaces.",
    },
  }),
  expected_z: freezeEntry("expected_z", {
    aliases: ["z_score", "z"],
    label: "Expected Z",
    shortHelp: "Standardized expected move or signal magnitude shown on alerts.",
    fullHelp: "This is the standardized expected move or prediction magnitude shown in alert UI. The current dashboard already uses its absolute size to label weak, moderate, strong, or very strong alert impact.",
    unit: "z-score",
    normalRange: "absolute value < 0.8",
    warningRange: "absolute value >= 0.8 and < 2.5",
    persona: {
      fund_manager: "This is the alert's standardized move magnitude, useful for comparing alert strength across symbols or times.",
      operations: "This is the alert strength field already used by the UI to bucket impact words.",
    },
  }),
  confidence: freezeEntry("confidence", {
    aliases: ["confidence_score"],
    label: "Confidence",
    shortHelp: "Alert confidence score displayed in alerts UI.",
    fullHelp: "This is the alert confidence score shown in alert UI. The current dashboard already buckets it into low, medium, and high confidence language.",
    unit: "score",
    normalRange: ">= 0.85",
    warningRange: ">= 0.65 and < 0.85",
    persona: {
      fund_manager: "This is the alert confidence measure currently surfaced by the dashboard.",
      operations: "This is the alert confidence score already used for confidence-word labeling in the alerts UI.",
    },
  }),
  confidence_raw: freezeEntry("confidence_raw", {
    label: "Raw Confidence",
    shortHelp: "Underlying raw confidence value shown for an alert when available.",
    fullHelp: "This is the raw confidence field rendered in alert-related UI when present. It is separate from the normalized alert confidence score.",
    unit: "score",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is the unnormalized confidence figure shown alongside the alert when available.",
      operations: "This is the raw confidence field surfaced from the alert payload.",
    },
  }),
  prediction_strength: freezeEntry("prediction_strength", {
    label: "Prediction Strength",
    shortHelp: "Alert prediction-strength value shown when embedded in the payload.",
    fullHelp: "This is the prediction-strength field rendered in alert-related UI when available. It is displayed as provided by the payload and does not currently have a universal dashboard threshold.",
    unit: "score",
    normalRange: null,
    warningRange: null,
    persona: {
      fund_manager: "This is an additional alert-strength field shown when present, but the current UI does not define a universal threshold for it.",
      operations: "This is the raw prediction-strength value surfaced from the alert payload when available.",
    },
  }),
});

export const METRIC_GLOSSARY = Object.freeze({
  ...STATUS_METRICS,
  ...MARKET_STRESS_METRICS,
  ...EXECUTION_METRICS,
  ...PORTFOLIO_METRICS,
  ...PERFORMANCE_METRICS,
  ...OPERATIONS_METRICS,
  ...ALERT_METRICS,
});

const METRIC_LOOKUP = new Map();

for (const [key, def] of Object.entries(METRIC_GLOSSARY)) {
  METRIC_LOOKUP.set(normalizeLookupKey(key), def);
  for (const alias of def.aliases) {
    METRIC_LOOKUP.set(normalizeLookupKey(alias), def);
  }
}

function normalizeLookupKey(key) {
  if (key === undefined || key === null) return "";
  return String(key)
    .trim()
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .replace(/[\s./-]+/g, "_")
    .toLowerCase();
}

function asFiniteNumber(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function asUpperToken(value) {
  if (value === undefined || value === null) return "";
  return String(value).trim().toUpperCase();
}

function asBoolean(value) {
  if (typeof value === "boolean") return value;
  if (value === 1 || value === "1" || value === "true" || value === "TRUE") return true;
  if (value === 0 || value === "0" || value === "false" || value === "FALSE") return false;
  return null;
}

function rangeText(def) {
  const bits = [];
  if (def.normalRange) bits.push(`normal ${def.normalRange}`);
  if (def.warningRange) bits.push(`warning ${def.warningRange}`);
  return bits.join("; ");
}

function formatMetricValue(def, value) {
  if (value === undefined || value === null || value === "") return "unavailable";

  if (def.unit === "boolean") {
    const boolValue = asBoolean(value);
    return boolValue === null ? "unavailable" : String(boolValue);
  }

  if (def.unit === "status") {
    const token = asUpperToken(value);
    return token || "unavailable";
  }

  if (def.unit === "timestamp_ms") {
    const n = asFiniteNumber(value);
    return n && n > 0 ? new Date(n).toLocaleString() : "unavailable";
  }

  const n = asFiniteNumber(value);
  if (n === null) return String(value).trim() || "unavailable";

  switch (def.unit) {
    case "percent":
      return `${(n * 100).toFixed(2)}%`;
    case "percent_points":
      return `${n.toFixed(1)}%`;
    case "ratio":
    case "score":
    case "index":
      return n.toFixed(3);
    case "z-score":
      return n.toFixed(2);
    case "ms":
      return `${Math.round(n)} ms`;
    case "minutes":
      return `${Math.round(n)} min`;
    case "seconds":
      return `${Math.round(n)} s`;
    case "count":
      return String(Math.round(n));
    case "shares":
      return Math.abs(n) >= 100 ? n.toFixed(2) : n.toFixed(6);
    case "mb":
      return `${n.toFixed(1)} MB`;
    case "usd":
      return `$${n.toFixed(3)}`;
    default:
      return n.toString();
  }
}

function makeStatusClassifier(normalTokens, warningTokens, criticalTokens) {
  const normalSet = new Set(normalTokens || []);
  const warningSet = new Set(warningTokens || []);
  const criticalSet = new Set(criticalTokens || []);
  return (value) => {
    const token = asUpperToken(value);
    if (!token) return "unknown";
    if (normalSet.has(token)) return "normal";
    if (warningSet.has(token)) return "warning";
    if (criticalSet.has(token)) return "critical";
    return "unknown";
  };
}

function makeBooleanClassifier(falseLevel = "warning") {
  return (value) => {
    const boolValue = asBoolean(value);
    if (boolValue === null) return "unknown";
    if (boolValue) return "normal";
    return falseLevel === "critical" ? "critical" : "warning";
  };
}

function makeThresholdClassifier(normalMax, warningMax, options = {}) {
  const absolute = !!options.absolute;
  const inclusiveNormal = !!options.inclusiveNormal;
  return (value) => {
    const n = asFiniteNumber(value);
    if (n === null) return "unknown";
    const sample = absolute ? Math.abs(n) : n;
    if (inclusiveNormal ? sample <= normalMax : sample < normalMax) return "normal";
    if (sample <= warningMax) return "warning";
    return "critical";
  };
}

const CLASSIFIERS = new Map([
  [
    "system_status",
    makeStatusClassifier(
      ["LIVE", "RUNNING", "OK"],
      ["DEGRADED", "WARMING_UP", "BOOTING", "UNKNOWN"],
      ["STOPPED", "BLOCKED", "ERROR", "DOWN", "HALTED"]
    ),
  ],
  [
    "data_status",
    makeStatusClassifier(
      ["RUNNING", "CONNECTED", "OK", "FLOWING"],
      ["DEGRADED", "WARMING_UP", "WAITING_FOR_DASHBOARD", "WAITING", "UNKNOWN"],
      ["STOPPED", "BLOCKED", "ERROR", "DISCONNECTED", "DOWN"]
    ),
  ],
  [
    "health_status",
    makeStatusClassifier(
      ["OK"],
      ["WARMING_UP", "WAITING_FOR_DASHBOARD"],
      ["UNREACHABLE", "ERROR", "DOWN"]
    ),
  ],
  [
    "execution_status",
    (value) => {
      const boolValue = asBoolean(value);
      if (boolValue !== null) return boolValue ? "normal" : "critical";
      const token = asUpperToken(value);
      if (!token) return "unknown";
      if (token === "ALLOWED" || token === "ENABLED" || token === "LIVE") return "normal";
      if (token === "DEGRADED" || token === "UNKNOWN") return "warning";
      if (token === "BLOCKED" || token === "DISABLED" || token === "STOPPED" || token === "ERROR") return "critical";
      return "unknown";
    },
  ],
  [
    "training_status",
    makeStatusClassifier(
      ["ALLOWED", "ENABLED", "READY", "LIVE"],
      ["SAFE", "SHADOW", "OFF", "WARMING_UP", "UNKNOWN"],
      ["BLOCKED", "ERROR", "STOPPED"]
    ),
  ],
  [
    "promotion_status",
    makeStatusClassifier(
      ["ALLOWED"],
      ["OFF"],
      ["BLOCKED", "ERROR"]
    ),
  ],
  [
    "market_condition",
    makeStatusClassifier(
      ["NORMAL"],
      ["ELEVATED STRESS"],
      ["HIGH STRESS"]
    ),
  ],
  [
    "operating_mood",
    makeStatusClassifier(
      ["STEADY"],
      ["WATCHFUL", "GUARDED", "CAUTIOUS"],
      ["DEFENSIVE"]
    ),
  ],
  [
    "trading_mode",
    makeStatusClassifier(
      ["LIVE"],
      ["SAFE", "SHADOW", "UNKNOWN"],
      ["BLOCKED", "STOPPED", "DISABLED", "ERROR"]
    ),
  ],
  [
    "execution_enabled",
    makeBooleanClassifier("warning"),
  ],
  [
    "broker_connectivity",
    makeStatusClassifier(
      ["CONNECTED", "OK"],
      ["DEGRADED", "UNKNOWN"],
      ["DISCONNECTED", "DOWN", "ERROR"]
    ),
  ],
  ["startup_ready", makeBooleanClassifier("warning")],
  ["data_feed_ok", makeBooleanClassifier("warning")],
  ["models_ok", makeBooleanClassifier("warning")],
  ["risk_ok", makeBooleanClassifier("warning")],
  ["broker_ok", makeBooleanClassifier("warning")],
  [
    "market_stress_score",
    (value) => {
      const state = classifyMarketStressScore(value).state;
      if (state === "normal") return "normal";
      if (state === "warning") return "warning";
      if (state === "critical") return "critical";
      return "unknown";
    },
  ],
  ["z_vix", makeThresholdClassifier(1, 2, { absolute: true, inclusiveNormal: true })],
  ["z_vvix", makeThresholdClassifier(1, 2, { absolute: true, inclusiveNormal: true })],
  ["z_move", makeThresholdClassifier(1, 2, { absolute: true, inclusiveNormal: true })],
  ["z_term", makeThresholdClassifier(1, 2, { absolute: true, inclusiveNormal: true })],
  ["z_credit", makeThresholdClassifier(1, 2, { absolute: true, inclusiveNormal: true })],
  ["z_rates", makeThresholdClassifier(1, 2, { absolute: true, inclusiveNormal: true })],
  [
    "equity_diff_level",
    makeStatusClassifier(
      ["OK", "RESOLVED"],
      ["WARN", "ACKED", "PENDING"],
      ["CRIT", "CRITICAL", "ERROR"]
    ),
  ],
  [
    "market_data_latency_ms",
    makeThresholdClassifier(1000, 5000, { inclusiveNormal: true }),
  ],
  [
    "alert_count",
    (value) => {
      const n = asFiniteNumber(value);
      if (n === null) return "unknown";
      if (n <= 0) return "normal";
      if (n <= 3) return "warning";
      return "critical";
    },
  ],
  [
    "crit_count",
    (value) => {
      const n = asFiniteNumber(value);
      if (n === null) return "unknown";
      return n > 0 ? "critical" : "normal";
    },
  ],
  [
    "warn_count",
    (value) => {
      const n = asFiniteNumber(value);
      if (n === null) return "unknown";
      return n > 0 ? "warning" : "normal";
    },
  ],
  ["prices_ok", makeBooleanClassifier("critical")],
  ["labels_ok", makeBooleanClassifier("critical")],
  ["model_ok", makeBooleanClassifier("critical")],
  [
    "healthy_providers",
    (value) => {
      const n = asFiniteNumber(value);
      if (n === null) return "unknown";
      return n >= 1 ? "normal" : "critical";
    },
  ],
  [
    "alert_severity",
    makeStatusClassifier(
      ["INFO"],
      ["WARN", "HIGH"],
      ["CRIT", "CRITICAL"]
    ),
  ],
  [
    "expected_z",
    (value) => {
      const n = asFiniteNumber(value);
      if (n === null) return "unknown";
      const magnitude = Math.abs(n);
      if (magnitude < 0.8) return "normal";
      if (magnitude < 2.5) return "warning";
      return "critical";
    },
  ],
  [
    "confidence",
    (value) => {
      const n = asFiniteNumber(value);
      if (n === null) return "unknown";
      if (n >= 0.85) return "normal";
      if (n >= 0.65) return "warning";
      return "critical";
    },
  ],
]);

export function getMetricDefinition(key) {
  return METRIC_LOOKUP.get(normalizeLookupKey(key)) || null;
}

export function classifyMetricValue(key, value) {
  const def = getMetricDefinition(key);
  if (!def) return "unknown";
  const classifier = CLASSIFIERS.get(def.key);
  return classifier ? classifier(value) : "unknown";
}

export function explainMetricValue(key, value, persona = "fund_manager") {
  const def = getMetricDefinition(key);
  if (!def) return "Metric definition unavailable.";

  const audience = persona === "operations" ? "operations" : "fund_manager";
  const formatted = formatMetricValue(def, value);
  const classification = classifyMetricValue(def.key, value);
  const details = [];

  details.push(`${def.label}: ${formatted}.`);
  if (classification !== "unknown") {
    details.push(`Current classification: ${classification}.`);
  }
  details.push(def.persona[audience]);
  details.push(def.fullHelp);

  const ranges = rangeText(def);
  if (ranges) details.push(`Reference ranges: ${ranges}.`);

  return details.join(" ");
}
