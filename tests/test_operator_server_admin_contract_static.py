from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT / "boot" / "operator_server.js"
UI_PATH = ROOT / "boot" / "operator_ui.html"


def _extract_block(text: str, marker: str, next_marker: str) -> str:
    start = text.index(marker)
    end = text.index(next_marker, start)
    return text[start:end]


def test_factory_reset_requires_server_side_confirmation():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        'app.post("/api/operator/factoryReset", (req, res) => {',
        "// --------------------------------------------\n// Python STDERR tail"
    )

    assert 'requireOperatorConfirmation(req, res, "operator.factory_reset"' in block
    assert 'emergencyStop();' in block


def test_live_start_aliases_require_confirmation():
    text = SERVER_PATH.read_text(encoding="utf-8")
    start_system_block = _extract_block(
        text,
        'app.post("/api/operator/start_system", async (req, res) => {',
        'app.post("/api/operator/restart_engine", async (req, res) => {'
    )
    guided_block = _extract_block(
        text,
        'app.post("/api/operator/guided_bootstrap", async (req, res) => {',
        "// --------------------------------------------------\n// LIVE TELEMETRY WEBSOCKET"
    )

    assert '"operator.live_start" : "operator.start"' in start_system_block
    assert "requireOperatorConfirmation(" in start_system_block
    assert '"operator.guided_bootstrap_live" : "operator.guided_bootstrap"' in guided_block
    assert "requireOperatorConfirmation(" in guided_block


def test_operator_ui_uses_structured_modal_not_native_prompts():
    text = UI_PATH.read_text(encoding="utf-8")
    fallback_match = re.search(
        r'<script id="operatorUiCrashFallback">[\s\S]*?</script>',
        text,
    )
    assert fallback_match is not None
    fallback_script = fallback_match.group(0)
    normal_ui_text = text.replace(fallback_script, "")

    assert not re.search(r"(?:window\.)?(?:confirm|prompt)\s*\(", normal_ui_text)
    assert 'window.prompt("Operator UI is degraded. Type KILL to Emergency Stop.")' in fallback_script
    assert 'window.prompt("Enter an Emergency Stop reason (10+ characters).")' in fallback_script
    assert 'import("/ui/confirmation_modal.mjs")' in text
    assert "operatorMutationConfirmation(" in text
    assert "actionId" in text
    assert "source: \"operator_console\"" in text


def test_operator_sidecar_confirmation_audit_contract_fields():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert "const OPERATOR_CONFIRMATION_REGISTRY = Object.freeze" in text
    assert "function requireOperatorConfirmation" in text
    assert "appendOperatorConfirmationAudit" in text
    for field in [
        "action_id",
        "actor",
        "source_surface",
        "reason",
        "request_id",
        "target",
        "confirmation_method",
        "confirmation_hold_ms",
        "consequence_hash",
    ]:
        assert field in text


def test_operator_server_high_impact_routes_call_confirmation_helper():
    text = SERVER_PATH.read_text(encoding="utf-8")
    required = {
        "/api/operator/config": "operator.config_write",
        "/api/operator/secrets": "operator.secrets_write",
        "/api/operator/start": "operator.start",
        "/api/operator/bootstrap": "operator.bootstrap",
        "/api/operator/stop": "operator.stop",
        "/api/operator/restart": "operator.restart",
        "/api/operator/restart_engine": "operator.restart",
        "/api/operator/emergency_stop": "operator.emergency_stop",
        "/api/operator/self_repair": "operator.self_repair",
        "/api/operator/restart_feeds": "operator.restart_feeds",
        "/api/operator/backup": "operator.backup",
        "/api/operator/update": "operator.system_update",
        "/api/operator/restart_operator": "operator.restart_operator",
        "/api/operator/factoryReset": "operator.factory_reset",
    }
    for path, action_id in required.items():
        assert path in text
        assert action_id in text


def test_operator_start_and_bootstrap_are_confirmed_bounded_routes():
    text = SERVER_PATH.read_text(encoding="utf-8")
    start_block = _extract_block(
        text,
        'app.post("/api/operator/start", wrapOperatorRoute(async (req, res) => {',
        'app.post("/api/operator/stop", (req, res) => {',
    )
    proxy_block = _extract_block(
        text,
        'app.post("/api/operator/self_repair",',
        "// UI expects the camelCase endpoint; keep snake_case as the canonical API form.",
    )

    assert '"operator.bootstrap": {' in text
    assert 'requiredToken: "BOOTSTRAP_OPERATOR"' in text
    assert "OPERATOR_START_REQUEST_TIMEOUT_MS" in start_block
    assert 'error: "start_timeout"' in start_block
    assert 'verifyHealth({ force: true, timeoutMs:' in start_block
    assert 'httpGetJson(`${base}/api/telemetry`, Math.min(5000' in start_block
    assert 'app.post("/api/operator/bootstrap",' in proxy_block
    assert 'operatorConfirmedProxyPost("/api/operator/bootstrap", "invalid_bootstrap_response", "operator.bootstrap", OPERATOR_BOOTSTRAP_REQUEST_TIMEOUT_MS)' in proxy_block


def test_operator_sidecar_snake_case_aliases_keep_camelcase_compatibility():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        "function handleClearLastErrorRoute(_req, res) {",
        'app.post("/api/operator/set_mode", (req, res) => {',
    )

    assert 'app.get(["/api/operator/bootstrap_status", "/api/operator/bootstrapStatus"]' in text
    assert 'app.get(["/api/operator/institutional_check", "/api/operator/institutionalCheck"]' in text
    assert 'app.post("/api/operator/clear_last_error", handleClearLastErrorRoute);' in block
    assert 'app.post("/api/operator/clearLastError", handleClearLastErrorRoute);' in block
    assert "clearLastError();" in block
    assert "jsonOk(res, {})" in block


def test_operator_live_controls_fail_closed_for_disable_live_execution():
    text = SERVER_PATH.read_text(encoding="utf-8")
    helper_block = _extract_block(
        text,
        "function liveExecutionEnvCleared(v) {",
        "function nowIso() {",
    )
    preflight_block = _extract_block(
        text,
        "async function getPreflight",
        "async function getReadiness()",
    )

    assert 's === "0" || s === "false" || s === "no" || s === "off"' in helper_block
    assert 'DISABLE_LIVE_EXECUTION unset' in helper_block
    assert 'DISABLE_LIVE_EXECUTION=${raw}' in helper_block
    assert "liveExecutionEnvBlocker(sanitized.DISABLE_LIVE_EXECUTION)" in preflight_block
    assert 'liveBlockers.push("DISABLE_LIVE_EXECUTION=1")' not in preflight_block


def test_proxy_only_operator_start_reconciles_before_preflight():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        'app.post("/api/operator/start", wrapOperatorRoute(async (req, res) => {',
        'app.post("/api/operator/stop", (req, res) => {',
    )

    disabled_idx = block.index("if (OPERATOR_DISABLE_INTERNAL_ENGINE_START)")
    preflight_idx = block.index("const pre = await getPreflight(mode, { forceValidation: true });")
    assert disabled_idx < preflight_idx
    assert 'reason: "OPERATOR_DISABLE_INTERNAL_ENGINE_START"' in block


def test_operator_server_caches_polled_db_routes():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert "const OPERATOR_DB_CACHE_TTL_MS = clampNumber(" in text
    assert 'readOperatorDbCache(_bootstrapCountsCache)' in text
    assert 'readOperatorDbCache(_dbSchemaCache)' in text
    assert 'writeOperatorDbCache("bootstrap_counts", payload);' in text
    assert 'writeOperatorDbCache("db_schema", payload);' in text


def test_operator_safe_feedless_degraded_health_counts_for_supervision():
    text = SERVER_PATH.read_text(encoding="utf-8")
    helper_block = _extract_block(
        text,
        "function safeModeDegradedServingHealth",
        "function rowsFromOperatorPayload",
    )
    health_block = _extract_block(
        text,
        "async function verifyHealth",
        "async function verifyDashboardReadiness",
    )

    assert 'status !== "WARMING_UP" && status !== "DEGRADED"' in helper_block
    assert "firstPriceTs" in helper_block
    assert 'value === "live" || value === "shadow"' in helper_block
    assert "const supervisionReady = bodyOk || safeDegradedServing;" in health_block
    assert "if (reachable && supervisionReady)" in health_block
    assert "safe_degraded_serving: safeDegradedServing" in health_block


def test_operator_server_caches_runtime_detection_and_uses_ingestion_owner_pid():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert "const OPERATOR_EXTERNAL_RUNTIME_CACHE_TTL_MS = clampNumber(" in text
    assert "const OPERATOR_PROCESS_SCAN_CACHE_TTL_MS = clampNumber(" in text
    assert "invalidateOperatorRuntimeCaches()" in text
    assert 'source: "operator_child"' in text
    assert 'source: "ingestion_pid_owner"' in text
    assert "commandLineLooksLikeStartSystem(ingestionOwnerCommandLine)" in text
    assert '!lowered.includes("start_ingestion.py")' in text
    assert "currentAttemptLastErrorForRuntime(ext)" in text
    assert "OPERATOR_STALE_ATTEMPT_GRACE_MS" in text
    assert "staleAttemptWithoutRuntime(ext)" in text


def test_operator_managed_mode_requires_executable_service_helper():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        "function isLinuxManagedMode() {",
        "let child = null;",
    )

    assert "fs.accessSync(SERVICE_CTL, fs.constants.X_OK);" in block
    assert "fs.existsSync(SERVICE_CTL)" not in block


def test_operator_server_uses_fast_preflight_for_refresh_snapshots():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert "const OPERATOR_PREFLIGHT_CACHE_TTL_MS = clampNumber(" in text
    assert "runProductionValidationGateCached" in text
    assert "skipValidationGate: true" in text


def test_operator_status_endpoint_stays_lightweight():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        'app.get("/api/operator/status", wrapOperatorRoute(async (req, res) => {',
        'app.get("/api/operator/bootstrap", (req, res) => {',
    )

    assert "await getReadiness()" not in block
    assert "health: null" in block


def test_operator_start_restart_and_process_faults_are_guarded():
    text = SERVER_PATH.read_text(encoding="utf-8")

    start_block = _extract_block(
        text,
        'app.post("/api/operator/start", wrapOperatorRoute(async (req, res) => {',
        'app.post("/api/operator/stop", (req, res) => {',
    )
    restart_block = _extract_block(
        text,
        'app.post("/api/operator/restart", wrapOperatorRoute(async (req, res) => {',
        'app.post("/api/operator/emergencyStop", (req, res) => {',
    )

    assert "requireOperatorConfirmation(" in start_block
    assert "OPERATOR_START_REQUEST_TIMEOUT_MS" in start_block
    assert "startEngine(mode)" in start_block
    assert "requireOperatorConfirmation(" in restart_block
    assert "startEngine(mode)" in restart_block
    assert 'process.on("unhandledRejection", handleUnhandledRejection);' in text
    assert 'process.on("uncaughtException", handleUncaughtException);' in text
    assert 'logOperatorCatch(scope, error, extra);' in text


def test_operator_lan_mode_does_not_default_to_public_bind():
    text = SERVER_PATH.read_text(encoding="utf-8")
    bind_block = _extract_block(
        text,
        "function resolveOperatorBindHost() {",
        "function resolveLanAdvertiseIp() {",
    )
    listen_block = _extract_block(
        text,
        "_httpServer = app.listen(OPERATOR_PORT, OPERATOR_BIND_HOST, () => {",
        "  const autoStart = normalizeBool(process.env.OPERATOR_AUTO_START);",
    )

    assert 'return NETWORK_MODE === "lan" ? "0.0.0.0" : "127.0.0.1";' not in bind_block
    assert 'return "127.0.0.1";' in bind_block
    assert 'if (_wildcardBind) {' in listen_block
    assert '|| NETWORK_MODE === "lan"' not in listen_block
    assert "dashboard /operator/ bridge" in text


def test_operator_mutations_require_operator_token_not_loopback():
    text = SERVER_PATH.read_text(encoding="utf-8")
    auth_block = _extract_block(
        text,
        "function operatorMutationAuthorized(req) {",
        "// --------------------------------------------------\n// Persistent State",
    )

    assert "if (isLoopbackRequest(req)) return true" not in auth_block
    assert "operatorApiTokenFromConfig()" in auth_block
    assert "operatorRequestToken(req)" in auth_block
    assert "timingSafeTokenEquals" in auth_block

    request_token_block = _extract_block(
        text,
        "function operatorRequestToken(req) {",
        "function operatorMutationAuthorized(req) {",
    )
    assert 'req.headers?.["x-operator-token"]' in request_token_block
    assert "operator_token" in request_token_block


def test_support_snapshot_proxy_declares_auth_scope_and_redaction():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert 'operatorProxyGet("/api/operator/support_snapshot", "invalid_support_snapshot_response", { redact: true, authScope: "support_snapshot" })' in text
    assert 'operator_dashboard_auth_required' in text
    assert '"DASHBOARD_API_TOKEN or DASHBOARD_API_TOKEN_FILE"' in text
    assert "dashboard_token_configured" in text


def test_operator_proxy_health_uses_health_contract_validator():
    text = SERVER_PATH.read_text(encoding="utf-8")
    proxy_block = _extract_block(
        text,
        'app.get("/api/operator/proxy/health",',
        "const ROOT = path.join(__dirname"
    )
    health_helper_block = _extract_block(
        text,
        "function operatorHealthProxyGet(path, invalidError) {",
        "const OPERATOR_BARRIER_PROXY_TIMEOUT_MS = clampNumber("
    )

    assert 'operatorHealthProxyGet("/api/health", "invalid_system_health_response")' in proxy_block
    assert 'operatorCanonicalProxyGet("/api/health", "invalid_system_health_response")' not in proxy_block
    assert "function isHealthApiShape(payload)" in text
    assert "isHealthApiShape(r.json)" in health_helper_block
    assert "operatorHealthProxyGet" in health_helper_block


def test_operator_telemetry_websocket_requires_token_and_origin_guard():
    text = SERVER_PATH.read_text(encoding="utf-8")
    ws_block = _extract_block(
        text,
        "function startTelemetryWebSocket(server){",
        "function broadcastTelemetry(type, payload){",
    )
    auth_block = _extract_block(
        text,
        "function operatorRequestToken(req) {",
        "function operatorMutationAuthorized(req) {",
    )

    assert "operatorWebSocketOriginAuthorized(info.req)" in ws_block
    assert "operatorMutationAuthorized(info.req)" in ws_block
    assert "operator_origin_forbidden" in ws_block
    assert "operator_forbidden" in ws_block
    assert "operatorRequestWebSocketProtocolToken(req)" in auth_block
    assert "operatorWebSocketTicketAuthorized(req, token)" in text
    assert "operator-ticket." in text
    assert "operator_ws" in text
    assert "exp_ms" in text
    assert "sec-websocket-protocol" in text
    assert "operator-token." in text


def test_operator_readiness_proxy_timeout_is_bounded():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        "const OPERATOR_BARRIER_PROXY_TIMEOUT_MS = clampNumber(",
        "function operatorProxyGet"
    )

    assert "process.env.OPERATOR_BARRIER_PROXY_TIMEOUT_MS || 10000" in block
    assert "60000" in block


def test_operator_sensitive_reads_require_operator_token_before_routes():
    text = SERVER_PATH.read_text(encoding="utf-8")
    middleware_idx = text.index('app.use([\n  "/api/operator",')
    summary_idx = text.index('app.get("/api/operator_summary"')
    config_idx = text.index('app.get("/api/operator/config"')

    assert middleware_idx < summary_idx
    assert middleware_idx < config_idx
    for path in [
        '"/api/operator"',
        '"/api/operator_summary"',
        '"/api/execution/barrier"',
        '"/api/system/kill_switches"',
        '"/api/broker/config"',
    ]:
        assert path in text
    assert 'if (method === "GET" || method === "HEAD" || method === "OPTIONS")' not in text
    assert "operatorRouteRequiresAuth(req)" in text
    assert "verifyClient" in text


def test_operator_safety_gate_proxy_routes_and_dashboard_get_auth():
    text = SERVER_PATH.read_text(encoding="utf-8")
    trusted_auth_block = _extract_block(
        text,
        "function trustedControlPlaneAuthHeaders(method, urlText) {",
        "const OPERATOR_CONFIRMATION_REGISTRY = Object.freeze",
    )

    assert 'if (upper === "GET" || upper === "HEAD" || upper === "OPTIONS") return {};' not in trusted_auth_block
    assert 'if (upper === "OPTIONS") return {};' in trusted_auth_block
    assert 'headers["X-API-Token"] = dashboardToken;' in trusted_auth_block
    assert 'app.get("/api/system/kill_switches",' in text
    assert 'operatorProxyGet("/api/system/kill_switches", "invalid_kill_switches_response")' in text
    assert 'app.get("/api/broker/config",' in text
    assert 'operatorProxyGet("/api/broker/config", "invalid_broker_config_response", { redact: true })' in text
    assert 'app.get("/api/operator/readiness_evidence",' in text
    assert 'operatorProxyGet("/api/operator/readiness_evidence", "invalid_readiness_evidence_response"' in text


def test_operator_generated_dashboard_token_is_file_backed():
    text = SERVER_PATH.read_text(encoding="utf-8")
    env_block = _extract_block(
        text,
        "function ensureEnvFile() {",
        "function writeEnv(obj) {",
    )

    assert 'const tokenFile = path.join(OPERATOR_DATA_DIR, "dashboard_api_token");' in env_block
    assert "writeLocalSecretFile(tokenFile, token)" in env_block
    assert "delete envNow.DASHBOARD_API_TOKEN;" in env_block
    assert "envNow.DASHBOARD_API_TOKEN_FILE = tokenFile;" in env_block


def test_operator_config_logs_and_snapshots_are_redacted():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert "OPERATOR_SENSITIVE_KEY_RE" in text
    assert "key[_\\-.]?id" in text
    assert "KEY_ID" in text
    assert "redactOperatorSensitiveText" in text
    assert 'return jsonOk(res, safeEnvForSnapshot(readEnv()));' in text
    assert "sanitized: safeEnvForSnapshot(sanitized)" in text
    assert 'operatorProxyGet("/api/operator/support_snapshot", "invalid_support_snapshot_response", { redact: true, authScope: "support_snapshot" })' in text


def test_operator_readiness_status_uses_dashboard_readiness():
    text = SERVER_PATH.read_text(encoding="utf-8")
    block = _extract_block(
        text,
        "async function getReadiness()",
        "// --------------------------------------------------\n// Logs + Snapshot",
    )

    assert "await verifyDashboardReadiness()" in block
    assert 'dashboardReadiness && dashboardReadiness.ready' in block
    assert 'const readinessStatus = ready' in block
    assert 'status: readinessStatus' in block
    assert 'engineStatus,' in block
