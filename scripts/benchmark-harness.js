#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const process = require("node:process");
const { performance } = require("node:perf_hooks");

const DEFAULT_HOST = "http://127.0.0.1:8080";
const DEFAULT_POLL_MS = 700;
const DEFAULT_TIMEOUT_MS = 600000;
const DEFAULT_TARGET_SUITE_SECONDS = 75;
const DEFAULT_CALIBRATION_FRACTION = 0.02;
const DEFAULT_MIN_CALIBRATION_UNITS = 1000;
const MIN_CALIBRATION_RUNTIME_MS = 250;
const MAX_CALIBRATION_PASSES = 5;

function usage() {
  console.log("Usage:");
  console.log(
    "  node scripts/benchmark-harness.js [--mode local|cluster|both] [--benchmarks pi,mandelbrot,...]",
  );
  console.log("      [--runs 1] [--scale 1] [--target-suite-seconds 75] [--calibrate 1]");
  console.log("      [--calibration-fraction 0.02] [--min-calibration-units 1000]");
  console.log("      [--script benchmarks/cluster_benchmark_suite.js] [--preset-file benchmarks/presets.json]");
  console.log("      [--host http://127.0.0.1:8080] [--client-token <token>] [--env-file .env]");
  console.log("      [--cluster-execution sharded|single] [--poll-ms 700] [--timeout-ms 600000]");
}

function parseArgs(argv) {
  const flags = {};
  for (let i = 0; i < argv.length; i += 1) {
    const token = argv[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2);
    const value = argv[i + 1];
    if (!value || value.startsWith("--")) {
      flags[key] = true;
      continue;
    }
    flags[key] = value;
    i += 1;
  }
  return flags;
}

function parsePositiveInt(value, fallback, name) {
  if (value === undefined) {
    return fallback;
  }
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0) {
    throw new Error(`--${name} must be a positive integer`);
  }
  return n;
}

function parsePositiveNumber(value, fallback, name) {
  if (value === undefined) {
    return fallback;
  }
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`--${name} must be a positive number`);
  }
  return n;
}

function parseBooleanFlag(value, fallback) {
  if (value === undefined) {
    return fallback;
  }
  const v = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(v)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(v)) {
    return false;
  }
  return fallback;
}

function normalizeHost(raw) {
  return String(raw || DEFAULT_HOST).replace(/\/+$/, "");
}

function getEnvValue(filePath, key) {
  const text = fs.readFileSync(path.resolve(filePath), "utf8");
  for (const line of text.split(/\r?\n/)) {
    if (!line || line.startsWith("#")) {
      continue;
    }
    const idx = line.indexOf("=");
    if (idx <= 0) {
      continue;
    }
    if (line.slice(0, idx).trim() === key) {
      return line.slice(idx + 1).trim();
    }
  }
  return "";
}

function resolveToken(flags) {
  if (flags["client-token"]) {
    return String(flags["client-token"]).trim();
  }
  if (process.env.CLIENT_API_KEY) {
    return String(process.env.CLIENT_API_KEY).trim();
  }
  if (flags["env-file"]) {
    return getEnvValue(String(flags["env-file"]), "CLIENT_API_KEY");
  }
  return "";
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestJson({ url, method = "GET", token, body }) {
  const headers = {};
  if (token) {
    headers.Authorization = `Bearer ${token}`;
  }
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  const res = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  let data = null;
  try {
    data = await res.json();
  } catch (_error) {
    data = { error: `Non-JSON response (${res.status})` };
  }

  if (!res.ok) {
    throw new Error(data.error || `${method} ${url} failed (${res.status})`);
  }

  return data;
}

async function pollJobUntilDone({ host, token, jobId, timeoutMs, pollMs }) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const job = await requestJson({
      url: `${host}/api/jobs/${jobId}`,
      method: "GET",
      token,
    });
    if (job.status === "done") {
      return { ok: true, job };
    }
    if (job.status === "failed") {
      return { ok: false, error: job.error || "Job failed", job };
    }
    await sleep(pollMs);
  }
  return { ok: false, error: `Timed out after ${timeoutMs} ms`, job: null };
}

function aggregateFromResult(result) {
  if (!result || typeof result !== "object") {
    return {};
  }
  if (result.aggregate && typeof result.aggregate === "object" && !Array.isArray(result.aggregate)) {
    return result.aggregate;
  }
  return result;
}

function numericField(result, key) {
  const agg = aggregateFromResult(result);
  const n = Number(agg[key]);
  return Number.isFinite(n) ? n : 0;
}

function safeRate(numerator, denominator) {
  if (!Number.isFinite(denominator) || denominator <= 0) {
    return 0;
  }
  return numerator / denominator;
}

function unitsPerShardFor(totalUnits, preset) {
  const baseTotal = Math.max(1, Number(preset.totalUnits) || 1);
  const basePerShard = Math.max(1, Number(preset.unitsPerShard) || 1);
  const ratio = basePerShard / baseTotal;
  const scaled = Math.max(1, Math.round(totalUnits * ratio));
  return Math.min(scaled, totalUnits);
}

async function runClusterJob({
  host,
  token,
  code,
  preset,
  totalUnits,
  unitsPerShard,
  clusterExecution,
  timeoutMs,
  pollMs,
}) {
  const args = { ...(preset.args || {}) };
  const body = {
    code,
    args,
    timeoutMs: Number(preset.timeoutMs) || 120000,
    executionModel: clusterExecution,
  };

  if (clusterExecution === "single") {
    body.args = {
      ...body.args,
      units: totalUnits,
      offset: 0,
    };
  } else {
    body.shardConfig = {
      totalUnits,
      unitsPerShard: Math.max(1, Math.min(unitsPerShard, totalUnits)),
    };
    body.reducer = preset.reducer || { type: "sum", fields: ["samples", "ops"] };
  }

  const startedAt = performance.now();
  const created = await requestJson({
    url: `${host}/api/jobs/run-js`,
    method: "POST",
    token,
    body,
  });
  const polled = await pollJobUntilDone({
    host,
    token,
    jobId: created.jobId,
    timeoutMs,
    pollMs,
  });
  const elapsedMs = performance.now() - startedAt;
  if (!polled.ok) {
    return {
      ok: false,
      error: polled.error,
      elapsedMs,
      jobId: created.jobId,
      result: null,
      executionModel: clusterExecution,
    };
  }
  return {
    ok: true,
    elapsedMs,
    jobId: created.jobId,
    result: polled.job.result,
    executionModel: polled.job.executionModel || clusterExecution,
  };
}

async function runLocalJob({ code, preset, totalUnits }) {
  const args = {
    ...(preset.args || {}),
    units: totalUnits,
    offset: 0,
  };
  const silentConsole = {
    log: () => {},
    info: () => {},
    warn: () => {},
    error: () => {},
    debug: () => {},
    trace: () => {},
  };
  const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;
  const fn = new AsyncFunction("args", "console", '"use strict";\n' + code);
  const startedAt = performance.now();
  const value = await fn(args, silentConsole);
  const elapsedMs = performance.now() - startedAt;
  return {
    ok: true,
    elapsedMs,
    result: value,
    executionModel: "single",
  };
}

function deriveNotes(benchmarkName, result) {
  const notes = [];
  if (benchmarkName === "pi") {
    const hits = numericField(result, "hits");
    const samples = numericField(result, "samples");
    if (samples > 0) {
      notes.push(`pi=${((4 * hits) / samples).toFixed(8)}`);
    }
  }
  if (benchmarkName === "mandelbrot") {
    const sumIter = numericField(result, "sumIter");
    const samples = numericField(result, "samples");
    if (samples > 0) {
      notes.push(`avgIter=${(sumIter / samples).toFixed(2)}`);
    }
  }
  if (benchmarkName === "sha256_pow") {
    const found = numericField(result, "found");
    notes.push(`found=${Math.trunc(found)}`);
  }
  if (benchmarkName === "primes") {
    const primeCount = numericField(result, "primeCount");
    notes.push(`primes=${Math.trunc(primeCount)}`);
  }
  if (benchmarkName === "suite") {
    const suiteCases = numericField(result, "suite_cases");
    notes.push(`cases=${Math.trunc(suiteCases)}`);
  }
  return notes.join("; ");
}

function pad(value, width, alignRight = false) {
  const text = String(value);
  if (text.length >= width) {
    return text;
  }
  return alignRight ? " ".repeat(width - text.length) + text : text + " ".repeat(width - text.length);
}

function fmtInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return "0";
  }
  return Math.trunc(n).toLocaleString("en-US");
}

function fmtRate(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) {
    return "0";
  }
  return n.toLocaleString("en-US", { maximumFractionDigits: 2 });
}

function renderTable(headers, rows) {
  const widths = headers.map((h) => h.length);
  for (const row of rows) {
    for (let i = 0; i < headers.length; i += 1) {
      widths[i] = Math.max(widths[i], String(row[i]).length);
    }
  }

  const horizontal = "+" + widths.map((w) => "-".repeat(w + 2)).join("+") + "+";
  const headerLine =
    "| " +
    headers
      .map((h, i) => pad(h, widths[i], i > 1))
      .join(" | ") +
    " |";
  const lines = [horizontal, headerLine, horizontal];
  for (const row of rows) {
    lines.push(
      "| " +
        row
          .map((cell, i) => pad(cell, widths[i], i > 1))
          .join(" | ") +
        " |",
    );
  }
  lines.push(horizontal);
  return lines.join("\n");
}

async function runModeSuite({
  modeName,
  benchmarkNames,
  presets,
  scriptCode,
  runs,
  baseScale,
  targetSuiteSeconds,
  calibrate,
  calibrationFraction,
  minCalibrationUnits,
  clusterExecution,
  host,
  token,
  timeoutMs,
  pollMs,
}) {
  const isCluster = modeName === "cluster";
  const rows = [];
  const benchBudgetMs = (targetSuiteSeconds * 1000) / Math.max(benchmarkNames.length, 1);
  const modeStartedAt = performance.now();

  for (const benchmarkName of benchmarkNames) {
    const preset = presets[benchmarkName];
    const baseUnits = Math.max(1, Math.trunc((Number(preset.totalUnits) || 1) * baseScale));
    let tunedUnits = baseUnits;

    if (calibrate) {
      let calibrationUnits = Math.max(
        1,
        Math.min(baseUnits, Math.max(minCalibrationUnits, Math.trunc(baseUnits * calibrationFraction))),
      );
      let calibrationElapsedMs = 0;

      for (let pass = 0; pass < MAX_CALIBRATION_PASSES; pass += 1) {
        const calibrationPerShard = unitsPerShardFor(calibrationUnits, preset);
        const calibrationRun = isCluster
          ? await runClusterJob({
              host,
              token,
              code: scriptCode,
              preset,
              totalUnits: calibrationUnits,
              unitsPerShard: calibrationPerShard,
              clusterExecution,
              timeoutMs,
              pollMs,
            })
          : await runLocalJob({
              code: scriptCode,
              preset,
              totalUnits: calibrationUnits,
            });
        if (!calibrationRun.ok) {
          throw new Error(`[${modeName}] calibration failed for ${benchmarkName}: ${calibrationRun.error}`);
        }

        calibrationElapsedMs = Math.max(1, calibrationRun.elapsedMs);
        if (calibrationElapsedMs >= MIN_CALIBRATION_RUNTIME_MS) {
          break;
        }

        const growFactor = Math.max(2, Math.ceil((MIN_CALIBRATION_RUNTIME_MS * 1.2) / calibrationElapsedMs));
        const nextUnits = Math.min(2_000_000_000, calibrationUnits * growFactor);
        if (nextUnits <= calibrationUnits) {
          break;
        }
        calibrationUnits = nextUnits;
      }

      const unitsPerMs = calibrationUnits / Math.max(1, calibrationElapsedMs);
      const recommended = Math.max(baseUnits, Math.trunc(unitsPerMs * benchBudgetMs * 1.1));
      tunedUnits = Math.max(baseUnits, Math.min(recommended, 2_000_000_000));
    }

    const tunedPerShard = unitsPerShardFor(tunedUnits, preset);
    console.log(
      `[${modeName}] ${benchmarkName} units/job=${tunedUnits} perShard=${tunedPerShard} runs=${runs}`,
    );

    let totalElapsedMs = 0;
    let totalOps = 0;
    let notes = "";
    for (let i = 0; i < runs; i += 1) {
      const run = isCluster
        ? await runClusterJob({
            host,
            token,
            code: scriptCode,
            preset,
            totalUnits: tunedUnits,
            unitsPerShard: tunedPerShard,
            clusterExecution,
            timeoutMs,
            pollMs,
          })
        : await runLocalJob({
            code: scriptCode,
            preset,
            totalUnits: tunedUnits,
          });
      if (!run.ok) {
        throw new Error(`[${modeName}] ${benchmarkName} run ${i + 1}/${runs} failed: ${run.error}`);
      }
      totalElapsedMs += run.elapsedMs;
      totalOps += numericField(run.result, "ops");
      notes = deriveNotes(benchmarkName, run.result);
      console.log(
        `[${modeName}] ${benchmarkName} run ${i + 1}/${runs}: ${run.elapsedMs.toFixed(0)} ms` +
          (notes ? ` (${notes})` : ""),
      );
    }

    const totalUnitsExecuted = tunedUnits * runs;
    const avgSecondsPerJob = safeRate(totalElapsedMs / runs, 1000);
    const jobsPerMinute = safeRate(runs * 60000, totalElapsedMs);
    const unitsPerSecond = safeRate(totalUnitsExecuted * 1000, totalElapsedMs);
    const opsPerSecond = safeRate(totalOps * 1000, totalElapsedMs);

    rows.push({
      benchmark: benchmarkName,
      runs,
      unitsPerJob: tunedUnits,
      secPerJob: avgSecondsPerJob,
      jobsPerMinute,
      unitsPerSecond,
      opsPerSecond,
      notes,
    });
  }

  const modeElapsedMs = performance.now() - modeStartedAt;
  return {
    rows,
    modeElapsedMs,
  };
}

function printModeSummary(modeName, rows, modeElapsedMs) {
  const headers = ["benchmark", "runs", "units/job", "sec/job", "jobs/min", "units/sec", "ops/sec", "notes"];
  const tableRows = rows.map((row) => [
    row.benchmark,
    row.runs,
    fmtInt(row.unitsPerJob),
    row.secPerJob.toFixed(2),
    fmtRate(row.jobsPerMinute),
    fmtRate(row.unitsPerSecond),
    fmtRate(row.opsPerSecond),
    row.notes || "-",
  ]);

  console.log(`\n=== ${modeName.toUpperCase()} SUMMARY ===`);
  console.log(renderTable(headers, tableRows));
  console.log(`Suite wall time: ${(modeElapsedMs / 1000).toFixed(2)} sec`);
}

function printComparison(localRows, clusterRows) {
  const headers = [
    "benchmark",
    "jobs/min x",
    "units/sec x",
    "ops/sec x",
    "cluster vs local units %",
    "cluster vs local ops %",
  ];
  const rows = [];
  for (const local of localRows) {
    const cluster = clusterRows.find((r) => r.benchmark === local.benchmark);
    if (!cluster) {
      continue;
    }
    const jobsX = safeRate(cluster.jobsPerMinute, local.jobsPerMinute);
    const unitsX = safeRate(cluster.unitsPerSecond, local.unitsPerSecond);
    const opsX = safeRate(cluster.opsPerSecond, local.opsPerSecond);
    rows.push([
      local.benchmark,
      jobsX.toFixed(2),
      unitsX.toFixed(2),
      opsX.toFixed(2),
      `${(unitsX * 100).toFixed(1)}%`,
      `${(opsX * 100).toFixed(1)}%`,
    ]);
  }
  if (rows.length === 0) {
    return;
  }
  console.log("\n=== CLUSTER VS LOCAL ===");
  console.log(renderTable(headers, rows));
}

async function main() {
  const flags = parseArgs(process.argv.slice(2));
  if (flags.help) {
    usage();
    process.exit(0);
  }

  const mode = String(flags.mode || "both").trim().toLowerCase();
  if (!["local", "cluster", "both"].includes(mode)) {
    throw new Error("--mode must be local|cluster|both");
  }
  const clusterExecution = String(flags["cluster-execution"] || "sharded").trim().toLowerCase();
  if (!["single", "sharded"].includes(clusterExecution)) {
    throw new Error("--cluster-execution must be single|sharded");
  }

  const runs = parsePositiveInt(flags.runs, 1, "runs");
  const baseScale = parsePositiveNumber(flags.scale, 1, "scale");
  const timeoutMs = parsePositiveInt(flags["timeout-ms"], DEFAULT_TIMEOUT_MS, "timeout-ms");
  const pollMs = parsePositiveInt(flags["poll-ms"], DEFAULT_POLL_MS, "poll-ms");
  const targetSuiteSeconds = parsePositiveNumber(
    flags["target-suite-seconds"],
    DEFAULT_TARGET_SUITE_SECONDS,
    "target-suite-seconds",
  );
  const calibrate = parseBooleanFlag(flags.calibrate, true);
  const calibrationFraction = parsePositiveNumber(
    flags["calibration-fraction"],
    DEFAULT_CALIBRATION_FRACTION,
    "calibration-fraction",
  );
  const minCalibrationUnits = parsePositiveInt(
    flags["min-calibration-units"],
    DEFAULT_MIN_CALIBRATION_UNITS,
    "min-calibration-units",
  );
  const host = normalizeHost(flags.host);
  const token = resolveToken(flags);

  const scriptPath = path.resolve(String(flags.script || "benchmarks/cluster_benchmark_suite.js"));
  const presetPath = path.resolve(String(flags["preset-file"] || "benchmarks/presets.json"));
  const scriptCode = fs.readFileSync(scriptPath, "utf8");
  const presets = JSON.parse(fs.readFileSync(presetPath, "utf8"));

  const benchmarkNames = flags.benchmarks
    ? String(flags.benchmarks)
        .split(",")
        .map((v) => v.trim())
        .filter(Boolean)
    : Object.keys(presets);

  if (benchmarkNames.length === 0) {
    throw new Error("No benchmarks selected.");
  }
  for (const benchmarkName of benchmarkNames) {
    if (!Object.prototype.hasOwnProperty.call(presets, benchmarkName)) {
      throw new Error(`Unknown benchmark '${benchmarkName}' in --benchmarks.`);
    }
  }
  if ((mode === "cluster" || mode === "both") && !token) {
    throw new Error("Cluster mode requires client token (--client-token, CLIENT_API_KEY, or --env-file).");
  }

  console.log("Benchmark Harness");
  console.log(`Script: ${scriptPath}`);
  console.log(`Presets: ${presetPath}`);
  console.log(`Benchmarks: ${benchmarkNames.join(", ")}`);
  console.log(`Runs per benchmark: ${runs}`);
  console.log(`Base scale: ${baseScale}`);
  console.log(`Target suite time (per mode): ${targetSuiteSeconds}s`);
  console.log(`Calibration: ${calibrate ? "on" : "off"} (fraction=${calibrationFraction}, minUnits=${minCalibrationUnits})`);
  if (mode !== "local") {
    console.log(`Cluster host: ${host}`);
    console.log(`Cluster execution model: ${clusterExecution}`);
  }

  let local = null;
  let cluster = null;

  if (mode === "local" || mode === "both") {
    local = await runModeSuite({
      modeName: "local",
      benchmarkNames,
      presets,
      scriptCode,
      runs,
      baseScale,
      targetSuiteSeconds,
      calibrate,
      calibrationFraction,
      minCalibrationUnits,
      clusterExecution,
      host,
      token,
      timeoutMs,
      pollMs,
    });
    printModeSummary("local", local.rows, local.modeElapsedMs);
  }

  if (mode === "cluster" || mode === "both") {
    cluster = await runModeSuite({
      modeName: "cluster",
      benchmarkNames,
      presets,
      scriptCode,
      runs,
      baseScale,
      targetSuiteSeconds,
      calibrate,
      calibrationFraction,
      minCalibrationUnits,
      clusterExecution,
      host,
      token,
      timeoutMs,
      pollMs,
    });
    printModeSummary("cluster", cluster.rows, cluster.modeElapsedMs);
  }

  if (local && cluster) {
    printComparison(local.rows, cluster.rows);
  }
}

main().catch((error) => {
  console.error(`Benchmark harness error: ${error.message}`);
  process.exit(1);
});
