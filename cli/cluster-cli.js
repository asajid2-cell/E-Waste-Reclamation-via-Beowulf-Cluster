#!/usr/bin/env node

const DEFAULT_HOST = "http://127.0.0.1:8080";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_MS = 700;

function printUsage() {
  console.log("Usage:");
  console.log("  node cli/cluster-cli.js submit --a <int> --b <int> [--host <url>] [--client-token <token>] [--timeout-ms <ms>]");
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

function resolveClientToken(flags) {
  const token = String(flags["client-token"] || process.env.CLIENT_API_KEY || "").trim();
  if (!token) {
    throw new Error("Missing client token. Provide --client-token or set CLIENT_API_KEY env var.");
  }
  return token;
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

async function runSubmit(flags) {
  const host = String(flags.host || DEFAULT_HOST).replace(/\/+$/, "");
  const timeoutMs = flags["timeout-ms"] ? Number(flags["timeout-ms"]) : DEFAULT_TIMEOUT_MS;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error("--timeout-ms must be a positive number");
  }

  const token = resolveClientToken(flags);
  const a = parseInteger(flags.a, "a");
  const b = parseInteger(flags.b, "b");

  console.log(`Submitting job to ${host}: ${a} + ${b}`);
  const jobCreated = await requestJson({
    url: `${host}/api/jobs`,
    method: "POST",
    token,
    body: { a, b },
  });

  const jobId = jobCreated.jobId;
  console.log(`Job queued: ${jobId}`);

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
      console.log(`Result: ${job.result}`);
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

async function runInvite(flags) {
  const host = String(flags.host || DEFAULT_HOST).replace(/\/+$/, "");
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
