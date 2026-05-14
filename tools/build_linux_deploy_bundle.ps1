param(
  [string]$Destination = "dist/linux-server"
)

$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = (Resolve-Path (Join-Path $scriptRoot "..")).Path
$destPath = if ([System.IO.Path]::IsPathRooted($Destination)) {
  $Destination
} else {
  Join-Path $repoRoot $Destination
}

$repoFull = [System.IO.Path]::GetFullPath($repoRoot)
$destFull = [System.IO.Path]::GetFullPath($destPath)
$distFull = [System.IO.Path]::GetFullPath((Join-Path $repoFull "dist"))

if (-not $destFull.StartsWith($distFull, [System.StringComparison]::OrdinalIgnoreCase)) {
  throw "Destination must stay under ${distFull}: ${destFull}"
}

if ($destFull -eq $repoFull) {
  throw "Destination cannot be the repository root"
}

if (Test-Path -LiteralPath $destFull) {
  Remove-Item -LiteralPath $destFull -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $destFull | Out-Null

$localOnlyPatterns = @(
  ".git/*",
  ".venv/*",
  "venv/*",
  "env/*",
  "ENV/*",
  "node_modules/*",
  "__pycache__/*",
  ".pytest_cache/*",
  ".ruff_cache/*",
  ".mypy_cache/*",
  ".claude/*",
  ".vscode/*",
  "dist/*",
  "build/*",
  "coverage/*",
  "logs/*",
  "logs-*/*",
  "tmp/*",
  "data-staging/*",
  "data-isolation/*",
  "data/operator/*",
  "data/runtime/*",
  "data/retraining/*",
  "models/*",
  "*.db",
  "*.sqlite",
  "*.sqlite3",
  "*.db-wal",
  "*.db-shm",
  "*.sqlite-wal",
  "*.sqlite-shm",
  "*.log",
  "*.log.*",
  "*.tmp",
  "*.pid",
  "*.seed",
  "*.out",
  "*.err",
  "*.lock",
  "*_trace.txt",
  "*_trace.log",
  "*_probe_trace.txt",
  "*_probe_trace.log",
  "*_hang.txt",
  "*_hang_dump.txt",
  "pyright*.json",
  "pyright*.txt",
  "ruff_repo.json",
  "ingestion_runtime-pyright.json",
  "audit_trace.txt",
  "import_probe*",
  "import_seq*",
  "import_trace*",
  "process_events_probe_trace.txt",
  "start_system_probe*_trace.log",
  "runtime_graph_rerun.log",
  "tmp_unittest_*.log",
  "unit_rerun.log"
)

function Convert-ToForwardSlash([string]$PathText) {
  return $PathText.Replace("\", "/")
}

function Test-LocalOnlyPath([string]$RelativePath) {
  $rel = Convert-ToForwardSlash $RelativePath
  foreach ($pattern in $localOnlyPatterns) {
    if ($rel -like $pattern) {
      return $true
    }
  }
  return $false
}

Push-Location $repoRoot
try {
  $files = & git ls-files -co --exclude-standard
  if ($LASTEXITCODE -ne 0) {
    throw "git ls-files failed with exit code ${LASTEXITCODE}"
  }

  $copied = 0
  $skippedMissing = 0
  $skippedLocalOnly = 0

  foreach ($relative in $files) {
    if ([string]::IsNullOrWhiteSpace($relative)) {
      continue
    }
    if (Test-LocalOnlyPath $relative) {
      $skippedLocalOnly++
      continue
    }

    $src = Join-Path $repoRoot $relative
    if (-not (Test-Path -LiteralPath $src -PathType Leaf)) {
      $skippedMissing++
      continue
    }

    $dst = Join-Path $destFull $relative
    $dstDir = Split-Path -Parent $dst
    New-Item -ItemType Directory -Force -Path $dstDir | Out-Null
    Copy-Item -LiteralPath $src -Destination $dst -Force
    $copied++
  }

  $manifest = @(
    "Trading System Linux deployment bundle",
    "Built: $((Get-Date).ToString('yyyy-MM-dd HH:mm:ss zzz'))",
    "Source: ${repoFull}",
    "Destination: ${destFull}",
    "",
    "Copied files: ${copied}",
    "Skipped local-only paths: ${skippedLocalOnly}",
    "Skipped missing tracked paths: ${skippedMissing}",
    "",
    "Policy source: deploy/PRODUCTION_FILE_MANIFEST.md",
    "Linux deploy prompt: deploy/LINUX_SERVER_CODEX_DEPLOY.md",
    "",
    "This bundle intentionally excludes secrets, virtualenvs, node_modules, logs,",
    "local databases, caches, tmp files, local model artifacts, and diagnostics."
  )
  Set-Content -LiteralPath (Join-Path $destFull "DEPLOYMENT_BUNDLE_MANIFEST.txt") -Value $manifest -Encoding ascii

  [pscustomobject]@{
    ok = $true
    destination = $destFull
    copiedFiles = $copied
    skippedLocalOnly = $skippedLocalOnly
    skippedMissing = $skippedMissing
  } | ConvertTo-Json
}
finally {
  Pop-Location
}
