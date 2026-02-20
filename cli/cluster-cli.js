#!/usr/bin/env node

const DEFAULT_HOST = "http://127.0.0.1:8080";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_MS = 700;

function printUsage() {
  console.log("Usage:");
  console.log("  node cli/cluster-cli.js submit --a <int> --b <int> [--host <url>] [--timeout-ms <ms>]");
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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function submitJob({ host, a, b }) {
  const response = await fetch(`${host}/api/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ a, b }),
  });

  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || "Failed to submit job");
  }

  return data.jobId;
}

async function getJobStatus(host, jobId) {
  const response = await fetch(`${host}/api/jobs/${jobId}`);
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.error || `Failed to fetch job status for ${jobId}`);
  }
  return data;
}

async function runSubmit(flags) {
  const host = String(flags.host || DEFAULT_HOST).replace(/\/+$/, "");
  const timeoutMs = flags["timeout-ms"] ? Number(flags["timeout-ms"]) : DEFAULT_TIMEOUT_MS;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    throw new Error("--timeout-ms must be a positive number");
  }

  const a = parseInteger(flags.a, "a");
  const b = parseInteger(flags.b, "b");

  console.log(`Submitting job to ${host}: ${a} + ${b}`);
  const jobId = await submitJob({ host, a, b });
  console.log(`Job queued: ${jobId}`);

  const startedAt = Date.now();
  let lastStatus = "";

  while (Date.now() - startedAt <= timeoutMs) {
    const job = await getJobStatus(host, jobId);
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

async function main() {
  const { command, flags } = parseArgs(process.argv.slice(2));
  if (!command || command === "--help" || flags.help) {
    printUsage();
    process.exitCode = 1;
    return;
  }

  if (command !== "submit") {
    console.error(`Unknown command: ${command}`);
    printUsage();
    process.exitCode = 1;
    return;
  }

  try {
    const code = await runSubmit(flags);
    process.exitCode = code;
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}

main();
