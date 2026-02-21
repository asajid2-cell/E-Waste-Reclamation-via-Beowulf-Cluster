#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");
const QRCode = require("qrcode");
const { mnemonicToToken, tokenToMnemonic } = require("../common/client-key");

const DEFAULT_HOST = "http://127.0.0.1:8080";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_POLL_MS = 700;

function printUsage() {
  console.log("Usage:");
  console.log("  node cli/cluster-cli.js submit --a <int> --b <int> [--host <url>] [--client-token <token>] [--timeout-ms <ms>]");
  console.log(
    "  node cli/cluster-cli.js run-js (--file <path> | --code <js>) [--args-json <json>] [--execution-model single|sharded] [--total-units <n>] [--units-per-shard <n>] [--reducer sum|collect|min|max] [--sum-fields <csv>] [--run-timeout-ms <ms>] [--timeout-ms <ms>] [--host <url>] [--client-token <token>]",
  );
  console.log(
    "  node cli/cluster-cli.js invite [--host <url>] [--client-token <token>] [--ttl-sec <seconds>] [--label <name>] [--qr] [--qr-file <path>]",
  );
  console.log("  node cli/cluster-cli.js token-mnemonic [--token <key>] [--env-file <.env path>]");
  console.log("  node cli/cluster-cli.js mnemonic-token --phrase \"<words>\" [--format base64url|hex]");
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

function getEnvFileValue(filePath, key) {
  const absolutePath = path.resolve(filePath);
  const content = fs.readFileSync(absolutePath, "utf8");
  for (const line of content.split(/\r?\n/)) {
    if (!line || line.startsWith("#")) {
      continue;
    }
    const idx = line.indexOf("=");
    if (idx <= 0) {
      continue;
    }
    const currentKey = line.slice(0, idx).trim();
    if (currentKey === key) {
      return line.slice(idx + 1).trim();
    }
  }
  return "";
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
  if (result && typeof result === "object" && !Array.isArray(result)) {
    const isEnvelope = Object.prototype.hasOwnProperty.call(result, "exitCode") && Object.prototype.hasOwnProperty.call(result, "output");
    if (isEnvelope) {
      const lines = [];
      lines.push(`ok=${result.ok === true ? "true" : "false"} exitCode=${result.exitCode}`);
      if (typeof result.durationMs === "number") {
        lines.push(`durationMs=${result.durationMs}`);
      }
      if (result.output && typeof result.output.text === "string" && result.output.text.length > 0) {
        lines.push("output:");
        lines.push(result.output.text);
      }
      if (result.error && typeof result.error === "object" && result.error.message) {
        lines.push(`error: ${result.error.message}`);
      }
      if (Object.prototype.hasOwnProperty.call(result, "returnValue")) {
        try {
          lines.push(`returnValue: ${JSON.stringify(result.returnValue)}`);
        } catch (_error) {
          lines.push("returnValue: [unserializable]");
        }
      }
      return lines.join("\n");
    }
  }

  if (typeof result === "string") {
    return result;
  }
  return JSON.stringify(result, null, 2);
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
      if (job.executionModel) {
        console.log(`Execution Model: ${job.executionModel}`);
      }
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

function parseCsvFields(raw) {
  return String(raw || "")
    .split(",")
    .map((v) => v.trim())
    .filter(Boolean);
}

async function runJs(flags) {
  const host = normalizeHost(flags.host);
  const timeoutMs = flags["timeout-ms"] ? parsePositiveNumber(flags["timeout-ms"], "timeout-ms") : DEFAULT_TIMEOUT_MS;
  const token = resolveClientToken(flags);
  const code = loadScriptCode(flags);
  const args = parseJsonArgs(flags);
  const runTimeoutMs = flags["run-timeout-ms"] ? parsePositiveNumber(flags["run-timeout-ms"], "run-timeout-ms") : undefined;
  const executionModel = flags["execution-model"] ? String(flags["execution-model"]).trim().toLowerCase() : "single";
  if (!["single", "sharded"].includes(executionModel)) {
    throw new Error("--execution-model must be 'single' or 'sharded'");
  }

  console.log(`Submitting custom JS job to ${host}`);
  const body = {
    code,
    args,
    executionModel,
  };
  if (runTimeoutMs !== undefined) {
    body.timeoutMs = Math.trunc(runTimeoutMs);
  }
  if (executionModel === "sharded") {
    const totalUnits = parseInteger(flags["total-units"], "total-units");
    const unitsPerShard = parseInteger(flags["units-per-shard"], "units-per-shard");
    body.shardConfig = { totalUnits, unitsPerShard };
    const reducerType = flags.reducer ? String(flags.reducer).trim().toLowerCase() : "sum";
    if (!["sum", "collect", "min", "max"].includes(reducerType)) {
      throw new Error("--reducer must be one of: sum, collect, min, max");
    }
    if (reducerType === "sum") {
      const fields = parseCsvFields(flags["sum-fields"] || "hits,samples");
      if (fields.length === 0) {
        throw new Error("--sum-fields must contain at least one field name");
      }
      body.reducer = { type: "sum", fields };
    } else if (reducerType === "collect") {
      body.reducer = { type: "collect" };
    } else {
      body.reducer = { type: reducerType, field: "value" };
    }
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
  if (invite.phraseCode) {
    console.log(`Worker Phrase: ${invite.phraseCode}`);
  }
  if (invite.phraseInviteUrl) {
    console.log(`Phrase Invite URL: ${invite.phraseInviteUrl}`);
  }
  if (invite.shortInviteUrl) {
    console.log(`Short Invite URL: ${invite.shortInviteUrl}`);
  }
  console.log(`Expires At: ${invite.expiresAt}`);
  console.log(`TTL Seconds: ${invite.ttlSec}`);
  const qrTargetUrl = invite.phraseInviteUrl || invite.shortInviteUrl || invite.inviteUrl;

  if (flags.qr) {
    const terminalQr = await QRCode.toString(qrTargetUrl, { type: "terminal", small: true });
    console.log(terminalQr);
  }

  if (flags["qr-file"] !== undefined) {
    const qrFilePath = path.resolve(String(flags["qr-file"]));
    await QRCode.toFile(qrFilePath, qrTargetUrl, {
      errorCorrectionLevel: "M",
      margin: 1,
      width: 512,
    });
    console.log(`QR PNG: ${qrFilePath}`);
  }

  return 0;
}

async function runTokenMnemonic(flags) {
  let token = "";
  if (flags.token !== undefined) {
    token = String(flags.token).trim();
  } else if (flags["env-file"] !== undefined) {
    token = getEnvFileValue(String(flags["env-file"]), "CLIENT_API_KEY");
  } else {
    token = String(flags["client-token"] || process.env.CLIENT_API_KEY || "").trim();
  }

  if (!token) {
    throw new Error("Missing token. Use --token, --env-file, --client-token, or CLIENT_API_KEY.");
  }

  const converted = tokenToMnemonic(token);
  if (!converted.ok) {
    throw new Error(`Could not convert token to mnemonic: ${converted.error}`);
  }

  console.log(`Mnemonic: ${converted.mnemonic}`);
  return 0;
}

async function runMnemonicToken(flags) {
  const phrase = String(flags.phrase || "").trim();
  if (!phrase) {
    throw new Error("Missing required flag --phrase");
  }
  const format = String(flags.format || "base64url").trim().toLowerCase();
  const converted = mnemonicToToken(phrase, format);
  if (!converted.ok) {
    throw new Error(`Could not convert mnemonic to token: ${converted.error}`);
  }
  console.log(`Token (${format}): ${converted.token}`);
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

    if (command === "token-mnemonic") {
      process.exitCode = await runTokenMnemonic(flags);
      return;
    }

    if (command === "mnemonic-token") {
      process.exitCode = await runMnemonicToken(flags);
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
