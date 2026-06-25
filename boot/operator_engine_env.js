"use strict";

const ENGINE_ENV_PASSTHROUGH_KEYS = Object.freeze([
  "TS_PG_DSN",
  "TS_PG_PORT",
  "TS_PG_PASSWORD_FILE",
  "TIMESCALE_PASSWORD_FILE",
  "TS_PG_PASSWORD_APP_FILE",
  "TS_PG_APP_PASSWORD_FILE",
  "TS_PG_PASSWORD_INGEST_FILE",
  "TS_PG_INGEST_PASSWORD_FILE",
  "TS_PG_PASSWORD_READER_FILE",
  "TS_PG_READER_PASSWORD_FILE",
  "PGPASSWORD_FILE",
  "DATA_SOURCE_MASTER_KEY_FILE",
  "TRADING_MASTER_KEY_FILE",
  "REDIS_PASSWORD_FILE",
  "TS_REDIS_PASSWORD_FILE",
  "LIVE_CACHE_REDIS_PASSWORD_FILE",
  "DB_PATH",
  "TRADING_DATA",
  "TRADING_LOGS"
]);

const ENGINE_ENV_PASSTHROUGH_PATTERNS = Object.freeze([
  /^OBJECT_STORE_[A-Z0-9_]*_FILE$/
]);

const ENGINE_INLINE_SECRET_ENV_BLOCKLIST = Object.freeze([
  "TS_PG_PASSWORD",
  "TIMESCALE_PASSWORD",
  "TS_PG_PASSWORD_APP",
  "TS_PG_APP_PASSWORD",
  "TS_PG_PASSWORD_INGEST",
  "TS_PG_INGEST_PASSWORD",
  "TS_PG_PASSWORD_READER",
  "TS_PG_READER_PASSWORD",
  "PGPASSWORD",
  "DATA_SOURCE_MASTER_KEY",
  "TRADING_MASTER_KEY",
  "APP_MASTER_KEY",
  "REDIS_PASSWORD",
  "TS_REDIS_PASSWORD",
  "LIVE_CACHE_REDIS_PASSWORD",
  "OBJECT_STORE_ACCESS_KEY",
  "OBJECT_STORE_SECRET_KEY",
  "MINIO_ACCESS_KEY",
  "MINIO_SECRET_KEY",
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY"
]);

function isEngineEnvPassthroughKey(key) {
  const name = String(key || "").trim();
  if (!name) return false;
  if (ENGINE_ENV_PASSTHROUGH_KEYS.includes(name)) return true;
  return ENGINE_ENV_PASSTHROUGH_PATTERNS.some((pattern) => pattern.test(name));
}

function applyEnv(target, source, { passthroughOnly = false } = {}) {
  if (!source || typeof source !== "object") return target;
  for (const [rawKey, rawValue] of Object.entries(source)) {
    const key = String(rawKey || "").trim();
    if (!key || rawValue === undefined || rawValue === null) continue;
    if (passthroughOnly && !isEngineEnvPassthroughKey(key)) continue;
    target[key] = String(rawValue);
  }
  return target;
}

function pickEnginePassthroughEnv(source) {
  return applyEnv({}, source, { passthroughOnly: true });
}

function stripInlineEngineSecrets(env) {
  for (const key of ENGINE_INLINE_SECRET_ENV_BLOCKLIST) {
    delete env[key];
  }
  return env;
}

function buildEngineChildEnv(configEnv = {}, { baseEnv = process.env, extraEnv = {} } = {}) {
  const env = {};

  applyEnv(env, baseEnv);
  applyEnv(env, configEnv);

  // Re-apply the documented engine passthrough surface explicitly. This keeps
  // DSNs and secret-file pointers stable if future config normalization narrows
  // the broader environment object.
  applyEnv(env, pickEnginePassthroughEnv(baseEnv));
  applyEnv(env, pickEnginePassthroughEnv(configEnv));

  applyEnv(env, extraEnv);
  stripInlineEngineSecrets(env);
  return env;
}

module.exports = {
  ENGINE_ENV_PASSTHROUGH_KEYS,
  ENGINE_ENV_PASSTHROUGH_PATTERNS,
  ENGINE_INLINE_SECRET_ENV_BLOCKLIST,
  buildEngineChildEnv,
  isEngineEnvPassthroughKey,
  pickEnginePassthroughEnv,
  stripInlineEngineSecrets
};
