import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import {
  buildTableView,
  compareTableValues,
  filterTableRows,
  sortTableRows,
} from "../ui/utils.js";

const columns = [
  { key: "symbol", accessor: (row) => row && row.symbol },
  { key: "qty", accessor: (row) => row && row.qty },
  { key: "updated", accessor: (row) => row && row.updated },
  { key: "meta", accessor: (row) => row && row.meta },
];

test("table helpers filter nested values without mutating malformed payloads", () => {
  const rows = [
    { symbol: "SPY", qty: "10", meta: { route: "broker live" } },
    null,
    { symbol: "QQQ", qty: "5", meta: ["portfolio", "intent"] },
  ];

  const filtered = filterTableRows(rows, columns, "intent");
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].symbol, "QQQ");
  assert.equal(rows.length, 3);
  assert.deepEqual(filterTableRows("bad", columns, "x"), []);
});

test("table helpers sort numbers, dates, strings, nulls, and bad rows safely", () => {
  const rows = [
    { symbol: "Beta", qty: "$1,200.50", updated: "2026-05-10T12:00:00Z" },
    { symbol: "alpha", qty: "5", updated: "2026-05-12T12:00:00Z" },
    { symbol: "Gamma", qty: "15%", updated: "2026-05-11T12:00:00Z" },
    { symbol: "", qty: null, updated: null },
    null,
  ];

  assert.deepEqual(sortTableRows(rows, columns, "qty", "asc").map((row) => row && row.symbol), [
    "alpha",
    "Gamma",
    "Beta",
    "",
    null,
  ]);
  assert.deepEqual(sortTableRows(rows, columns, "qty", "desc").map((row) => row && row.symbol), [
    "Beta",
    "Gamma",
    "alpha",
    "",
    null,
  ]);
  assert.deepEqual(sortTableRows(rows, columns, "updated", "desc").slice(0, 3).map((row) => row && row.symbol), [
    "alpha",
    "Gamma",
    "Beta",
  ]);
  assert.equal(compareTableValues("2026-05-10T00:00:00Z", "2026-05-11T00:00:00Z"), -1);
  assert.equal(compareTableValues(new Date("2026-05-10T00:00:00Z"), new Date("2026-05-11T00:00:00Z")), -1);
});

test("buildTableView returns stable counts and max-row slices", () => {
  const rows = [
    { symbol: "MSFT", qty: 2 },
    { symbol: "AAPL", qty: 3 },
    { symbol: "AMZN", qty: 4 },
  ];
  const view = buildTableView(rows, columns, {
    query: "a",
    sortKey: "symbol",
    sortDir: "asc",
    maxRows: 1,
  });

  assert.deepEqual(view.visibleRows.map((row) => row.symbol), ["AAPL"]);
  assert.equal(view.totalRows, 3);
  assert.equal(view.filteredRowsCount, 2);
  assert.equal(view.hiddenRowsCount, 1);
});

test("dashboard and terminal table wiring uses shared helpers and preserves drilldowns", () => {
  const dashboardHtml = readFileSync(new URL("../ui/dashboard.html", import.meta.url), "utf8");
  const dashboardJs = readFileSync(new URL("../ui/dashboard.js", import.meta.url), "utf8");
  const terminalHtml = readFileSync(new URL("../ui/terminal/terminal.html", import.meta.url), "utf8");
  const terminalJs = readFileSync(new URL("../ui/terminal/terminal.js", import.meta.url), "utf8");

  assert.match(dashboardJs, /buildTableView/);
  assert.match(terminalJs, /buildTableView/);

  for (const tableId of ["recentDecisions", "executionOrders", "executionFills", "suppressedTrades"]) {
    assert.match(dashboardHtml, new RegExp(`data-dashboard-table-filter="${tableId}"`));
  }
  for (const tableId of ["executionOrders", "executionFills", "suppressedTrades"]) {
    assert.match(dashboardHtml, new RegExp(`data-dashboard-table-sort="${tableId}"`));
  }
  for (const inputId of ["posFilter", "ordFilter", "fillsFilter"]) {
    assert.match(terminalHtml, new RegExp(`id="${inputId}"`));
  }
  assert.match(terminalJs, /data-terminal-table-sort/);

  assert.match(dashboardJs, /renderExecutionOrdersRows[\s\S]*decisionLookupForOrderIntent[\s\S]*_decisionLookupAttr/);
  assert.match(dashboardJs, /renderExecutionFillsRows[\s\S]*normalizeDecisionLookup[\s\S]*_decisionLookupAttr/);
  assert.match(dashboardJs, /renderSuppressedRows[\s\S]*_decisionLookupAttr/);
});
