from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SERVER_PATH = ROOT / "boot" / "operator_server.js"


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

    assert 'confirm !== "FACTORY_RESET"' in block
    assert 'factory_reset_confirmation_required' in block


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

    assert 'confirm !== "TRADE"' in start_system_block
    assert 'LIVE_CONFIRM_REQUIRED' in start_system_block
    assert 'confirm !== "TRADE"' in guided_block
    assert 'LIVE_CONFIRM_REQUIRED' in guided_block


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
