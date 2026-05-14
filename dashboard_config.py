"""
FILE: dashboard_config.py

Top-level entrypoint or configuration module for `dashboard_config`.
"""

# dashboard_config.py
import os
import time

 # `dashboard_config.py` is a flat env->constant translation layer shared by the
 # dashboard and related orchestration code. Keep policy/defaults centralized here
 # rather than scattering ad hoc `os.environ.get(...)` calls across handlers.
# ------------------------------
# Restart + scheduler controls
# ------------------------------

AUTO_SIZE_POLICY = os.environ.get("AUTO_SIZE_POLICY", "0") == "1"
AUTO_SIZE_POLICY_INTERVAL_S = float(os.environ.get("AUTO_SIZE_POLICY_INTERVAL_S", "86400"))
AUTO_SIZE_POLICY_START_DELAY_S = float(os.environ.get("AUTO_SIZE_POLICY_START_DELAY_S", "20.0"))
AUTO_SIZE_POLICY_LOG = os.environ.get("AUTO_SIZE_POLICY_LOG", "1") == "1"

AUTO_PIPELINE = os.environ.get("AUTO_PIPELINE", "0") == "1"
AUTO_PIPELINE_INTERVAL_S = float(os.environ.get("AUTO_PIPELINE_INTERVAL_S", "300"))
AUTO_PIPELINE_START_DELAY_S = float(os.environ.get("AUTO_PIPELINE_START_DELAY_S", "2.0"))
AUTO_PIPELINE_LOG = os.environ.get("AUTO_PIPELINE_LOG", "1") == "1"

AUTO_CHALLENGER = os.environ.get("AUTO_CHALLENGER", "0") == "1"
AUTO_CHALLENGER_INTERVAL_S = float(os.environ.get("AUTO_CHALLENGER_INTERVAL_S", "3600"))
AUTO_CHALLENGER_START_DELAY_S = float(os.environ.get("AUTO_CHALLENGER_START_DELAY_S", "10.0"))
AUTO_CHALLENGER_LOG = os.environ.get("AUTO_CHALLENGER_LOG", "1") == "1"
AUTO_CHALLENGER_MIN_DRIFT = float(os.environ.get("AUTO_CHALLENGER_MIN_DRIFT", "0.0"))

AUTO_PIPELINE_INCLUDE_EXECUTION = (os.environ.get("AUTO_PIPELINE_INCLUDE_EXECUTION", "0") == "1")

# ------------------------------
# Health thresholds
# ------------------------------

HEALTH_PRICES_MAX_AGE_S = float(os.environ.get("HEALTH_PRICES_MAX_AGE_S", "120"))
HEALTH_EVENTS_MAX_AGE_S = float(os.environ.get("HEALTH_EVENTS_MAX_AGE_S", "600"))
HEALTH_PREDICTIONS_MAX_AGE_S = float(os.environ.get("HEALTH_PREDICTIONS_MAX_AGE_S", "600"))
HEALTH_JOBS_MAX_STALE_S = float(os.environ.get("HEALTH_JOBS_MAX_STALE_S", "180"))
HEALTH_MIN_LABELS = int(os.environ.get("HEALTH_MIN_LABELS", "10"))
HEALTH_MIN_MODEL_SUPPORT = int(os.environ.get("HEALTH_MIN_MODEL_SUPPORT", "10"))

TRAINING_RESUME_MIN_OK_STREAK = int(os.environ.get("TRAINING_RESUME_MIN_OK_STREAK", "5"))

PREFLIGHT_ENABLE = os.environ.get("PREFLIGHT_ENABLE", "1") == "1"
PREFLIGHT_BLOCK_JOBS = os.environ.get("PREFLIGHT_BLOCK_JOBS", "1") == "1"
PREFLIGHT_PRICES_MAX_AGE_S = float(os.environ.get("PREFLIGHT_PRICES_MAX_AGE_S", "300"))


# ------------------------------
# Equity drift thresholds
# ------------------------------

EQ_DIFF_WARN_PCT = float(os.environ.get("EQ_DIFF_WARN_PCT", "0.01"))
EQ_DIFF_CRIT_PCT = float(os.environ.get("EQ_DIFF_CRIT_PCT", "0.03"))
EQ_DIFF_WARN_ABS = float(os.environ.get("EQ_DIFF_WARN_ABS", "50"))
EQ_DIFF_CRIT_ABS = float(os.environ.get("EQ_DIFF_CRIT_ABS", "250"))
EQ_DIFF_ALERT_COOLDOWN_S = int(os.environ.get("EQ_DIFF_ALERT_COOLDOWN_S", "300"))

EQ_DIFF_RESOLVE_PCT = float(os.environ.get("EQ_DIFF_RESOLVE_PCT", "0.006"))
EQ_DIFF_RESOLVE_ABS = float(os.environ.get("EQ_DIFF_RESOLVE_ABS", "30"))
EQ_DIFF_RESOLVE_LOOKBACK_S = int(os.environ.get("EQ_DIFF_RESOLVE_LOOKBACK_S", "86400"))

EQ_DRIFT_SUSTAINED_WINDOW = int(os.environ.get("EQ_DRIFT_SUSTAINED_WINDOW", "5"))
EQ_DRIFT_SUSTAINED_MIN_WARN = int(os.environ.get("EQ_DRIFT_SUSTAINED_MIN_WARN", "3"))
EQ_DRIFT_SUSTAINED_MIN_CRIT = int(os.environ.get("EQ_DRIFT_SUSTAINED_MIN_CRIT", "3"))

JOB_HISTORY_MAX_ROWS = int(os.environ.get("JOB_HISTORY_MAX_ROWS", "5000"))

# ------------------------------
# Relevance stats config
# ------------------------------

ENABLE_RELEVANCE_STATS = os.environ.get("ENABLE_RELEVANCE_STATS", "1") == "1"
RELEVANCE_STATS_CACHE_TTL_S = int(os.environ.get("RELEVANCE_STATS_CACHE_TTL_S", "60"))
RELEVANCE_STATS_TIMEOUT_S = float(os.environ.get("RELEVANCE_STATS_TIMEOUT_S", "5.0"))

# ------------------------------
# Server / auth config
# ------------------------------

SERVER_SHUTDOWN_TOKEN = os.environ.get("SERVER_SHUTDOWN_TOKEN", "").strip()
DASHBOARD_API_TOKEN = os.environ.get("DASHBOARD_API_TOKEN", "").strip()

SERVER_STARTED_AT_MS = int(time.time() * 1000)

HOST = os.environ.get("DASHBOARD_HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = int(os.environ.get("DASHBOARD_PORT", "8000"))

# ------------------------------
# CRIT notifications config
# ------------------------------

EQ_CRIT_EMAIL_TO = os.environ.get("EQ_CRIT_EMAIL_TO", "")
EQ_CRIT_EMAIL_FROM = os.environ.get("EQ_CRIT_EMAIL_FROM", "alerts@localhost")
EQ_CRIT_SMTP_HOST = os.environ.get("EQ_CRIT_SMTP_HOST", "")
EQ_CRIT_SMTP_PORT = int(os.environ.get("EQ_CRIT_SMTP_PORT", "25"))

EQ_CRIT_WEBHOOK_URL = os.environ.get("EQ_CRIT_WEBHOOK_URL", "")
EQ_CRIT_WEBHOOK_TIMEOUT_S = float(os.environ.get("EQ_CRIT_WEBHOOK_TIMEOUT_S", "4.0"))

# ------------------------------
# Read-only copilot config
# ------------------------------

COPILOT_LLM_ENDPOINT = os.environ.get("COPILOT_LLM_ENDPOINT", "").strip()
COPILOT_LLM_MODEL = os.environ.get("COPILOT_LLM_MODEL", "").strip()
try:
    COPILOT_LLM_TIMEOUT_S = max(1.0, float(os.environ.get("COPILOT_LLM_TIMEOUT_S", "8.0")))
except Exception:
    COPILOT_LLM_TIMEOUT_S = 8.0
