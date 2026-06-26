import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import vm from "node:vm";

const ROOT = dirname(dirname(fileURLToPath(import.meta.url)));
const HTML = readFileSync(join(ROOT, "boot", "operator_ui.html"), "utf8");

function scriptBlocks() {
  return [...HTML.matchAll(/<script([^>]*)>([\s\S]*?)<\/script>/g)].map((match) => ({
    attrs: match[1],
    source: match[2],
    index: match.index,
  }));
}

function fallbackScript() {
  const script = scriptBlocks().find((block) => /id="operatorUiCrashFallback"/.test(block.attrs));
  assert.ok(script, "operator UI degraded fallback script is missing");
  return script;
}

function moduleScript() {
  const script = scriptBlocks().find((block) => /type="module"/.test(block.attrs));
  assert.ok(script, "operator UI module script is missing");
  return script;
}

function storage() {
  const values = new Map();
  return {
    getItem(key) {
      return values.has(key) ? values.get(key) : null;
    },
    setItem(key, value) {
      values.set(String(key), String(value));
    },
  };
}

function element(id = "") {
  const classes = new Set();
  return {
    id,
    hidden: true,
    className: "",
    textContent: "",
    innerHTML: "",
    attrs: {},
    classList: {
      add(name) {
        classes.add(name);
      },
      contains(name) {
        return classes.has(name);
      },
    },
    setAttribute(name, value) {
      this.attrs[name] = value;
    },
  };
}

function createBrowserContext() {
  const banner = element("operatorUiLoadFailureBanner");
  const detail = element("operatorUiLoadFailureDetail");
  const elements = new Map([
    [banner.id, banner],
    [detail.id, detail],
  ]);
  const listeners = new Map();
  const sessionStorage = storage();
  const localStorage = storage();
  const prompts = ["KILL", "operator incident response"];
  const alerts = [];
  const fetches = [];

  const window = {
    location: new URL("http://127.0.0.1:8000/operator/?dashboard_token=dashboard-secret"),
    sessionStorage,
    localStorage,
    onerror: null,
    prompt() {
      return prompts.shift() || "";
    },
    alert(message) {
      alerts.push(String(message));
    },
    addEventListener(name, listener) {
      if(!listeners.has(name)) listeners.set(name, []);
      listeners.get(name).push(listener);
    },
  };
  window.window = window;

  const document = {
    body: {
      prepend(node) {
        if(node.id) elements.set(node.id, node);
      },
    },
    documentElement: {
      prepend(node) {
        if(node.id) elements.set(node.id, node);
      },
    },
    createElement() {
      return element();
    },
    getElementById(id) {
      return elements.get(id) || null;
    },
  };

  const context = {
    window,
    document,
    location: window.location,
    URL,
    URLSearchParams,
    Date,
    JSON,
    Math,
    Number,
    Object,
    Promise,
    String,
    console: { error() {} },
    setTimeout(resolve) {
      resolve();
      return 1;
    },
    fetch: async (url, init) => {
      fetches.push({ url, init });
      return {
        ok: true,
        status: 202,
        text: async () => JSON.stringify({ ok: true }),
      };
    },
  };

  context.globalThis = context;
  return { context, banner, detail, listeners, fetches, alerts, sessionStorage };
}

test("operator UI installs degraded Emergency Stop before module imports", () => {
  const fallback = fallbackScript();
  const module = moduleScript();

  assert.equal(scriptBlocks().filter((block) => /type="module"/.test(block.attrs)).length, 1);
  assert.ok(fallback.index < module.index);
  assert.equal(/type\s*=/.test(fallback.attrs), false);
  assert.match(fallback.source, /window\.emergencyStopHard = degradedEmergencyStopHard/);
  assert.match(fallback.source, /window\.onerror = function/);
  assert.match(fallback.source, /addEventListener\("unhandledrejection"/);
  assert.match(HTML, /Operator UI failed to load — Emergency Stop is in degraded fallback mode/);
  assert.match(module.source, /window\.emergencyStopHard = emergencyStopHard/);
  assert.match(module.source, /__operatorUiModuleLoaded/);
});

test("degraded fallback banner renders and Emergency Stop reaches the backend contract", async () => {
  const { context, banner, detail, fetches, alerts, sessionStorage } = createBrowserContext();
  vm.runInNewContext(fallbackScript().source, context, { filename: "operator-ui-crash-fallback.js" });

  assert.equal(typeof context.window.emergencyStopHard, "function");
  assert.equal(typeof context.window.onerror, "function");
  assert.equal(sessionStorage.getItem("trading_dashboard_api_token"), "dashboard-secret");

  context.window.onerror("Failed to load module script", "/ui/state_presenter.js", 1, 1, new Error("state_presenter 404"));
  assert.equal(banner.hidden, false);
  assert.equal(banner.classList.contains("visible"), true);
  assert.equal(detail.textContent, "state_presenter 404");

  await context.window.emergencyStopHard();

  assert.equal(fetches.length, 1);
  assert.equal(fetches[0].url, "/operator/api/operator/emergency_stop");
  assert.equal(fetches[0].init.method, "POST");
  assert.equal(fetches[0].init.headers["X-API-Token"], "dashboard-secret");
  assert.equal(fetches[0].init.headers["Content-Type"], "application/json");

  const body = JSON.parse(fetches[0].init.body);
  assert.equal(body.confirmation, "KILL");
  assert.equal(body.confirmation_token, "KILL");
  assert.equal(body.confirmation_method, "typed_phrase_hold");
  assert.equal(body.confirmation_hold_ms >= 3000, true);
  assert.equal(body.consequence_ack, true);
  assert.equal(body.action_id, "operator.emergency_stop");
  assert.equal(body.actor, "operator_ui");
  assert.equal(body.source_surface, "operator_console");
  assert.equal(body.reason, "operator incident response");
  assert.equal(body.target, "global");
  assert.equal(alerts.at(-1), "Emergency Stop submitted.");
});
