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

    assert not re.search(r"(?:window\.)?(?:confirm|prompt)\s*\(", text)
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
        'app.post("/api/operator/start", async (req, res) => {',
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


def test_operator_sensitive_reads_require_operator_token_before_routes():
    text = SERVER_PATH.read_text(encoding="utf-8")
    middleware_idx = text.index('app.use(["/api/operator", "/api/operator_summary", "/api/execution/barrier"]')
    summary_idx = text.index('app.get("/api/operator_summary"')
    config_idx = text.index('app.get("/api/operator/config"')

    assert middleware_idx < summary_idx
    assert middleware_idx < config_idx
    assert 'if (method === "GET" || method === "HEAD" || method === "OPTIONS")' not in text
    assert "operatorRouteRequiresAuth(req)" in text
    assert "verifyClient" in text


def test_operator_config_logs_and_snapshots_are_redacted():
    text = SERVER_PATH.read_text(encoding="utf-8")

    assert "OPERATOR_SENSITIVE_KEY_RE" in text
    assert "key[_\\-.]?id" in text
    assert "KEY_ID" in text
    assert "redactOperatorSensitiveText" in text
    assert 'return jsonOk(res, safeEnvForSnapshot(readEnv()));' in text
    assert "sanitized: safeEnvForSnapshot(sanitized)" in text
    assert 'operatorProxyGet("/api/operator/support_snapshot", "invalid_support_snapshot_response", { redact: true })' in text


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
