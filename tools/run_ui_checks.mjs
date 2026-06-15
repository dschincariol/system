#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const NODE_RANGE = ">=20.17.0 <21";
const NPM_RANGE = ">=10.0.0 <11";
const PYTHON_RANGE = ">=3.11";
const NODE_TESTS = [
  "tests/test_command_palette_helpers.mjs",
  "tests/test_symbol_context.mjs",
  "tests/test_mobile_ops_helpers.mjs",
  "tests/test_replay_ui_helpers.mjs",
  "tests/test_table_helpers.mjs",
];
const PYTEST_UI_TESTS = [
  "tests/test_dashboard_ui_contract.py",
  "tests/test_ui_asset_refs.py",
  "tests/test_mobile_ops_surface.py",
];

function readJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

function parseVersion(raw) {
  const match = String(raw || "").match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) return null;
  return match.slice(1, 4).map((part) => Number.parseInt(part, 10));
}

function cmpVersion(a, b) {
  for (let i = 0; i < 3; i += 1) {
    if ((a[i] || 0) > (b[i] || 0)) return 1;
    if ((a[i] || 0) < (b[i] || 0)) return -1;
  }
  return 0;
}

function nodeVersionOk(version) {
  return version && cmpVersion(version, [20, 17, 0]) >= 0 && version[0] === 20;
}

function npmVersionOk(version) {
  return version && version[0] === 10;
}

function pythonVersionOk(version) {
  return version && cmpVersion(version, [3, 11, 0]) >= 0;
}

function commandName(command) {
  return process.platform === "win32" && command === "npm" ? "npm.cmd" : command;
}

function runCapture(command, args) {
  return spawnSync(commandName(command), args, {
    cwd: ROOT,
    encoding: "utf8",
    windowsHide: true,
  });
}

function formatCommand(command, args) {
  return [command, ...args].join(" ");
}

function findPython() {
  const candidates = [];
  for (const envName of ["PYTHON", "OPERATOR_PYTHON"]) {
    if (process.env[envName]) candidates.push({ command: process.env[envName], args: [], source: `$${envName}` });
  }
  if (process.platform === "win32") {
    candidates.push({ command: "py", args: ["-3.11"], source: "py -3.11" });
    candidates.push({ command: "python", args: [], source: "python" });
  } else {
    candidates.push({ command: "python", args: [], source: "python" });
    candidates.push({ command: "python3", args: [], source: "python3" });
    candidates.push({ command: "python3.11", args: [], source: "python3.11" });
  }

  for (const candidate of candidates) {
    const result = runCapture(candidate.command, [
      ...candidate.args,
      "-c",
      "import sys; print('.'.join(map(str, sys.version_info[:3])))",
    ]);
    if (result.status !== 0) continue;
    const version = parseVersion(result.stdout);
    if (pythonVersionOk(version)) {
      return { ...candidate, version: version.join(".") };
    }
  }
  return null;
}

function rel(path) {
  return relative(ROOT, path) || ".";
}

function packageDependencyInstallIssues(packageJson) {
  const missing = [];
  for (const name of Object.keys(packageJson.dependencies || {})) {
    if (!existsSync(join(ROOT, "node_modules", name, "package.json"))) {
      missing.push(name);
    }
  }
  return missing;
}

function lockfileIssues(packageJson, packageLock) {
  const issues = [];
  if (packageLock.lockfileVersion !== 3) {
    issues.push(`package-lock.json lockfileVersion is ${packageLock.lockfileVersion}; expected 3 from npm 10.`);
  }

  const rootPackage = packageLock.packages && packageLock.packages[""];
  if (!rootPackage) {
    issues.push("package-lock.json is missing the root package entry.");
    return issues;
  }

  for (const [name, version] of Object.entries(packageJson.dependencies || {})) {
    if ((rootPackage.dependencies || {})[name] !== version) {
      issues.push(`package-lock.json root dependency ${name} is stale; run npm install --package-lock-only.`);
    }
  }

  for (const [name, range] of Object.entries(packageJson.engines || {})) {
    if ((rootPackage.engines || {})[name] !== range) {
      issues.push(`package-lock.json root engine ${name} is stale; run npm install --package-lock-only.`);
    }
  }
  return issues;
}

function preflight() {
  const packageJsonPath = join(ROOT, "package.json");
  const packageLockPath = join(ROOT, "package-lock.json");
  const packageJson = readJson(packageJsonPath);
  const packageLock = existsSync(packageLockPath) ? readJson(packageLockPath) : null;
  const failures = [];

  if (packageJson.engines?.node !== NODE_RANGE) {
    failures.push(`package.json engines.node must be "${NODE_RANGE}" for the production operator runtime.`);
  }
  if (packageJson.engines?.npm !== NPM_RANGE) {
    failures.push(`package.json engines.npm must be "${NPM_RANGE}" for reproducible npm ci installs.`);
  }
  if (!existsSync(join(ROOT, ".npmrc"))) {
    failures.push(".npmrc is missing; engine-strict=true is required so npm ci fails on the wrong runtime.");
  }
  if (!packageLock) {
    failures.push("package-lock.json is missing; run npm install --package-lock-only with Node 20/npm 10 and commit it.");
  } else {
    failures.push(...lockfileIssues(packageJson, packageLock));
  }

  const nodeVersion = parseVersion(process.version);
  if (!nodeVersionOk(nodeVersion)) {
    failures.push(
      `Node.js ${NODE_RANGE} is required because production bootstrap and compose use Node 20. Found ${process.version}. Install Node.js 20 LTS, then run npm ci.`,
    );
  }

  const npmResult = runCapture("npm", ["--version"]);
  const npmVersion = npmResult.status === 0 ? parseVersion(npmResult.stdout) : null;
  if (!npmVersionOk(npmVersion)) {
    const found = npmResult.status === 0 ? npmResult.stdout.trim() : "not found";
    failures.push(`npm ${NPM_RANGE} is required. Found ${found}. Install the npm bundled with Node.js 20 LTS, then run npm ci.`);
  }

  const python = findPython();
  if (!python) {
    failures.push(`Python ${PYTHON_RANGE} is required for static UI contract checks. Install Python 3.11+ or set PYTHON=/path/to/python.`);
  }

  const missingDependencies = packageDependencyInstallIssues(packageJson);
  if (missingDependencies.length > 0) {
    failures.push(`Node dependencies are not installed from the lockfile. Missing: ${missingDependencies.join(", ")}. Run npm ci from ${rel(ROOT)}.`);
  }

  if (failures.length > 0) {
    console.error("UI validation preflight failed:");
    for (const failure of failures) {
      console.error(`- ${failure}`);
    }
    return { ok: false, python };
  }

  console.log(`UI validation environment: node ${process.version}, npm ${npmVersion.join(".")}, python ${python.version} (${python.source})`);
  return { ok: true, python };
}

function runStep(command, args) {
  console.log(`\n$ ${formatCommand(command, args)}`);
  const result = spawnSync(commandName(command), args, {
    cwd: ROOT,
    env: process.env,
    stdio: "inherit",
    windowsHide: true,
  });
  if (result.error) {
    console.error(`UI validation could not start ${command}: ${result.error.message}`);
    return 127;
  }
  if (result.status !== 0) {
    console.error(`UI validation failed while running: ${formatCommand(command, args)}`);
    return result.status || 1;
  }
  return 0;
}

function main() {
  const args = new Set(process.argv.slice(2));
  const withPytest = args.has("--pytest");
  const preflightResult = preflight();
  if (!preflightResult.ok) return 1;

  const python = preflightResult.python;
  const pythonCommand = python.command;
  const pythonArgs = python.args;
  const steps = withPytest
    ? [
        [pythonCommand, [...pythonArgs, "tools/check_local_asset_refs.py"]],
        [pythonCommand, [...pythonArgs, "-m", "pytest", ...PYTEST_UI_TESTS]],
        [process.execPath, ["--test", ...NODE_TESTS]],
      ]
    : [
        [pythonCommand, [...pythonArgs, "tools/check_local_asset_refs.py"]],
        [pythonCommand, [...pythonArgs, "tools/check_dashboard_ui_contract.py", "--node-executable", process.execPath]],
        [process.execPath, ["--test", ...NODE_TESTS]],
      ];

  for (const [command, stepArgs] of steps) {
    const status = runStep(command, stepArgs);
    if (status !== 0) return status;
  }
  console.log("\nUI validation passed.");
  return 0;
}

process.exitCode = main();
