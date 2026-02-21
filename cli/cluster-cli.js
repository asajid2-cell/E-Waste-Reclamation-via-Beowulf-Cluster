#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const DEFAULT_HOST = "http://127.0.0.1:8080";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_MS = 700;

function printUsage() {
  console.log("Usage:");
  console.log("  node cli/cluster-cli.js submit --a <int> --b <int> [--host <url>] [--client-token <token>] [--timeout-ms <ms>]");
  console.log("  node cli/cluster-cli.js run-js (--file <path> | --code <js>) [--args-json <json>] [--run-timeout-ms <ms>] [--timeout-ms <ms>] [--host <url>] [--client-token <token>]");
  console.log("  node cli/cluster-cli.js invite [--host <url>] [--client-token <token>] [--ttl-sec <seconds>] [--label <name>]");
}

function parseArgs(argv) {
  const [command, ...args] = argv;
  const flags = {};
  for (let i = 0; i < args.length; i += 1) {
    const token = args[i];
    if (!token.startsWith("--")) {
      continue;
    }
    const key = token.slice(2);
    const value = args[i + 1];
    if (!value || value.startsWith("--")) {
      flags[key] = true;
      continue;
    }
    flags[key] = value;
    i += 1;
  }
  return { command, flags };
}

function parseInteger(value, name) {
  if (value === undefined) {
    throw new Error(`Missing required flag --${name}`);
  }
  const n = Number(value);
  if (!Number.isInteger(n)) {
    throw new Error(`--${name} must be an integer`);
  }
  return n;
}

function parsePositiveNumber(value, name) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(`--${name} must be a positive number`);
  }
  return n;
}

function resolveClientToken(flags) {
  const token = String(flags["client-token"] || process.env.CLIENT_API_KEY || "").trim();
  if (!token) {
    throw new Error("Missing client token. Provide --client-token or set CLIENT_API_KEY env var.");
  }
  return token;
}

function normalizeHost(rawHost) {
  return String(rawHost || DEFAULT_HOST).replace(/\/+$/, "");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestJson({ url, method = "GET", token, body }) {
  const headers = {
    Authorization: `Bearer ${token}`,
  };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  let data = null;
  try {
    data = await response.json();
  } catch (_error) {
    data = { error: `Unexpected non-JSON response (${response.status})` };
  }

  if (!response.ok) {
    throw new Error(data.error || `${method} ${url} failed with status ${response.status}`);
  }

  return data;
}

function formatResult(result) {
  if (typeof result === "string") {
    return result;
  }
  return JSON.stringify(result);
}

async function waitForJob({ host, token, jobId, timeoutMs }) {
  const startedAt = Date.now();
  let lastStatus = "";

  while (Date.now() - startedAt <= timeoutMs) {
    const job = await requestJson({
      url: `${host}/api/jobs/${jobId}`,
      token,
    });

    if (job.status !== lastStatus) {
      console.log(`Status: ${job.status}`);
      lastStatus = job.status;
    }

    if (job.status === "done") {
      console.log(`Result: ${formatResult(job.result)}`);
      return 0;
    }

    if (job.status === "failed") {
      console.error(`Job failed: ${job.error || "Unknown error"}`);
      return 1;
    }

    await sleep(DEFAULT_POLL_MS);
  }

  console.error(`Timed out after ${timeoutMs} ms waiting for job completion`);
  return 1;
}

async function runSubmit(flags) {
  const host = normalizeHost(flags.host);
  const timeoutMs = flags["timeout-ms"] ? parsePositiveNumber(flags["timeout-ms"], "timeout-ms") : DEFAULT_TIMEOUT_MS;
  const token = resolveClientToken(flags);
  const a = parseInteger(flags.a, "a");
  const b = parseInteger(flags.b, "b");

  console.log(`Submitting add job to ${host}: ${a} + ${b}`);
  const jobCreated = await requestJson({
    url: `${host}/api/jobs`,
    method: "POST",
    token,
    body: { a, b },
  });

  console.log(`Job queued: ${jobCreated.jobId}`);
  return waitForJob({
    host,
    token,
    jobId: jobCreated.jobId,
    timeoutMs,
  });
}

function loadScriptCode(flags) {
  const hasFile = typeof flags.file === "string";
  const hasCode = typeof flags.code === "string";
  if (hasFile === hasCode) {
    throw new Error("Provide exactly one of --file or --code for run-js.");
  }
  if (hasCode) {
    return String(flags.code);
  }
  const absolutePath = path.resolve(String(flags.file));
  try {
    return fs.readFileSync(absolutePath, "utf8");
  } catch (error) {
    throw new Error(`Failed to read script file '${absolutePath}': ${error.message}`);
  }
}

function parseJsonArgs(flags) {
  const raw = flags["args-json"] !== undefined ? flags["args-json"] : flags.args;
  if (raw === undefined) {
    return {};
  }
  try {
    return JSON.parse(String(raw));
  } catch (_error) {
    throw new Error("--args-json must be valid JSON");
  }
}

async function runJs(flags) {
  const host = normalizeHost(flags.host);
  const timeoutMs = flags["timeout-ms"] ? parsePositiveNumber(flags["timeout-ms"], "timeout-ms") : DEFAULT_TIMEOUT_MS;
  const token = resolveClientToken(flags);
  const code = loadScriptCode(flags);
  const args = parseJsonArgs(flags);
  const runTimeoutMs = flags["run-timeout-ms"] ? parsePositiveNumber(flags["run-timeout-ms"], "run-timeout-ms") : undefined;

  console.log(`Submitting custom JS job to ${host}`);
  const body = {
    code,
    args,
  };
  if (runTimeoutMs !== undefined) {
    body.timeoutMs = Math.trunc(runTimeoutMs);
  }

  const jobCreated = await requestJson({
    url: `${host}/api/jobs/run-js`,
    method: "POST",
    token,
    body,
  });

  console.log(`Job queued: ${jobCreated.jobId}`);
  return waitForJob({
    host,
    token,
    jobId: jobCreated.jobId,
    timeoutMs,
  });
}

async function runInvite(flags) {
  const host = normalizeHost(flags.host);
  const token = resolveClientToken(flags);

  const body = {};
  if (flags["ttl-sec"] !== undefined) {
    body.ttlSec = parseInteger(flags["ttl-sec"], "ttl-sec");
  }
  if (flags.label !== undefined) {
    body.workerLabel = String(flags.label);
  }

  const invite = await requestJson({
    url: `${host}/api/invites/worker`,
    method: "POST",
    token,
    body,
  });

  console.log(`Invite URL: ${invite.inviteUrl}`);
  console.log(`Expires At: ${invite.expiresAt}`);
  console.log(`TTL Seconds: ${invite.ttlSec}`);
  return 0;
}

async function main() {
  const { command, flags } = parseArgs(process.argv.slice(2));
  if (!command || command === "--help" || flags.help) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  try {
    if (command === "submit") {
      process.exitCode = await runSubmit(flags);
      return;
    }

    if (command === "run-js" || command === "submit-js") {
      process.exitCode = await runJs(flags);
      return;
    }

    if (command === "invite") {
      process.exitCode = await runInvite(flags);
      return;
    }

    console.error(`Unknown command: ${command}`);
    printUsage();
    process.exitCode = 1;
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

main();
