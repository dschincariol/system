/*
  FILE: ui/telemetry_panel.js

  Dashboard telemetry-strip loader. This module reads compact runtime telemetry
  from the API and updates the lightweight CPU/RAM/DB/stress indicators without
  pulling broader dashboard rendering logic into one file.
*/

import { setMetricValueAttribute } from "./tooltip.js";

export async function loadTelemetry(fetchJSON) {

  const strip = document.getElementById("telemetryStrip");
  if (!strip) return;

  try {

    const t = await fetchJSON("/api/telemetry");
    if (!t || !t.ok) throw new Error("telemetry unavailable");

    const cpu = document.getElementById("tCpu");
    const ram = document.getElementById("tRam");
    const db  = document.getElementById("tDb");

    setMetricValueAttribute(cpu, Number(t.cpu_percent));
    setMetricValueAttribute(ram, Number(t.process_rss_mb));
    setMetricValueAttribute(db, Number(t.db_size_mb));

    if (cpu) cpu.textContent = `CPU ${Number(t.cpu_percent || 0).toFixed(1)}%`;
    if (ram) ram.textContent = `RAM ${Number(t.process_rss_mb || 0).toFixed(0)}MB`;
    if (db)  db.textContent  = `DB ${Number(t.db_size_mb || 0).toFixed(1)}MB`;

  } catch (e) {

    const cpu = document.getElementById("tCpu");
    const ram = document.getElementById("tRam");
    const db  = document.getElementById("tDb");

    setMetricValueAttribute(cpu, null);
    setMetricValueAttribute(ram, null);
    setMetricValueAttribute(db, null);

    if (cpu) cpu.textContent = "CPU —";
    if (ram) ram.textContent = "RAM —";
    if (db)  db.textContent = "DB —";
  }
}
