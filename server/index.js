const path = require("node:path");
const http = require("node:http");
const { URL } = require("node:url");
const { randomBytes } = require("node:crypto");

const express = require("express");
const rateLimit = require("express-rate-limit");
const helmet = require("helmet");
const QRCode = require("qrcode");
const { wordlists } = require("bip39");
const { WebSocketServer } = require("ws");

const { createStore, isValidInputNumber, MIN_VALUE, MAX_VALUE } = require("./store");
const { createDispatcher } = require("./dispatcher");
const { createAuth, resolveConfig } = require("./auth");
const { parseInviteRequestBody, parseJobCreateBody, parseRunJsCreateBody, parseWorkerMessage } = require("./validation");

const PORT = parsePositiveInt(process.env.PORT, 8080);
const HOST = process.env.HOST || "0.0.0.0";
const TRUST_PROXY = String(process.env.TRUST_PROXY || "0") === "1";
const BASE_PATH_RAW = process.env.BASE_PATH || "";

const MAX_INVALID_MESSAGES = 4;
const WS_MAX_PAYLOAD = parsePositiveInt(process.env.WS_MAX_PAYLOAD, 256 * 1024);
const WS_MAX_CONNECTIONS_PER_IP = parsePositiveInt(process.env.WS_MAX_CONNECTIONS_PER_IP, 20);
const WS_MAX_MESSAGES_PER_WINDOW = parsePositiveInt(process.env.WS_MAX_MESSAGES_PER_WINDOW, Number.MAX_SAFE_INTEGER);
const WS_MESSAGE_WINDOW_MS = parsePositiveInt(process.env.WS_MESSAGE_WINDOW_MS, 10_000);
const WS_HEARTBEAT_INTERVAL_MS = Math.max(parsePositiveInt(process.env.WS_HEARTBEAT_INTERVAL_MS, 25_000), 5_000);
const WS_HEARTBEAT_TIMEOUT_MS = Math.max(parsePositiveInt(process.env.WS_HEARTBEAT_TIMEOUT_MS, 12_000), 2_000);
const REQUEST_BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || "128kb";
const API_RATE_LIMIT_WINDOW_MS = Math.max(parsePositiveInt(process.env.API_RATE_LIMIT_WINDOW_MS, 60_000), 1000);
const API_RATE_LIMIT_MAX = Math.max(parsePositiveInt(process.env.API_RATE_LIMIT_MAX, 1200), 1);
const SHORT_INVITE_CODE_LENGTH = Math.min(Math.max(parsePositiveInt(process.env.SHORT_INVITE_CODE_LENGTH, 7), 4), 16);
const SHORT_INVITE_MAX_ACTIVE = Math.min(Math.max(parsePositiveInt(process.env.SHORT_INVITE_MAX_ACTIVE, 50000), 100), 500000);
const SHORT_INVITE_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789";
const WORKER_INVITE_PHRASE_WORDS = Math.min(Math.max(parsePositiveInt(process.env.WORKER_INVITE_PHRASE_WORDS, 8), 3), 8);
const WORKER_INVITE_PHRASE_ALPHABET_SIZE = 2048;
const SHARD_MIN_UNITS = Math.max(parsePositiveInt(process.env.SHARD_MIN_UNITS, 1), 1);
const SHARD_MAX_PER_WORKER = Math.max(parsePositiveInt(process.env.SHARD_MAX_PER_WORKER, 200), 1);
const SHARD_ABSOLUTE_MAX = Math.max(parsePositiveInt(process.env.SHARD_ABSOLUTE_MAX, Number.MAX_SAFE_INTEGER), 100);
const WORKER_MAX_CONCURRENT_JOBS = Math.max(parsePositiveInt(process.env.WORKER_MAX_CONCURRENT_JOBS, 8), 1);
const JOB_EVENT_BUFFER_SIZE = Math.max(parsePositiveInt(process.env.JOB_EVENT_BUFFER_SIZE, 4000), 100);
const JOB_EVENT_BUFFER_MAX_BYTES = Math.max(parsePositiveInt(process.env.JOB_EVENT_BUFFER_MAX_BYTES, 32 * 1024 * 1024), 1024 * 1024);

function normalizeBasePath(input) {
  if (!input || input === "/") {
    return "";
  }
  const withLeadingSlash = input.startsWith("/") ? input : `/${input}`;
  return withLeadingSlash.replace(/\/+$/, "");
}

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0) {
    return fallback;
  }
  return n;
}

function computeShardSafety(totalUnits, connectedWorkers) {
  const effectiveWorkers = Math.max(Number(connectedWorkers) || 0, 2);
  const maxShardsAllowed = Math.max(1, Math.min(SHARD_ABSOLUTE_MAX, effectiveWorkers * SHARD_MAX_PER_WORKER));
  const tunedUnitsPerShard = Math.max(SHARD_MIN_UNITS, Math.ceil(totalUnits / maxShardsAllowed));
  return {
    maxShardsAllowed,
    tunedUnitsPerShard,
    tunedTotalShards: Math.ceil(totalUnits / tunedUnitsPerShard),
  };
}

const BASE_PATH = normalizeBasePath(BASE_PATH_RAW);

function withBasePath(pathname) {
  if (!pathname.startsWith("/")) {
    throw new Error(`Expected leading slash path, got: ${pathname}`);
  }
  return `${BASE_PATH}${pathname}` || "/";
}

const API_BASE_PATH = withBasePath("/api");
const WS_WORKER_PATH = withBasePath("/ws/worker");
const CLIENT_PATH = withBasePath("/client");
const WORKER_PATH = withBasePath("/worker");
const RAYTRACE_VIEWER_PATH = withBasePath("/raytrace-viewer");
const METRICS_PATH = withBasePath("/metrics");
const SHORT_INVITE_PATH = withBasePath("/j");
const PHRASE_INVITE_PATH = withBasePath("/p");
const APP_ROOT_PATH = BASE_PATH || "/";

const config = resolveConfig();
const auth = createAuth(config);

const app = express();
const store = createStore({
  maxCustomResultBytes: config.customJobMaxResultBytes,
  maxWorkerSlots: WORKER_MAX_CONCURRENT_JOBS,
  maxJobEventBufferSize: JOB_EVENT_BUFFER_SIZE,
  maxJobEventBufferBytes: JOB_EVENT_BUFFER_MAX_BYTES,
});
const dispatcher = createDispatcher(store, auth, console);
const shortInvites = new Map();
const phraseInvites = new Map();
const phraseWordlist = wordlists.english;

function pruneExpiredShortInvites(nowMs = Date.now()) {
  for (const [code, record] of shortInvites.entries()) {
    if (!record || typeof record.expiresAtMs !== "number" || record.expiresAtMs <= nowMs) {
      shortInvites.delete(code);
    }
  }
  for (const [phraseSlug, record] of phraseInvites.entries()) {
    if (!record || typeof record.expiresAtMs !== "number" || record.expiresAtMs <= nowMs) {
      phraseInvites.delete(phraseSlug);
    }
  }
}

function generateShortCode(length) {
  const bytes = randomBytes(length);
  let out = "";
  for (let i = 0; i < length; i += 1) {
    out += SHORT_INVITE_ALPHABET[bytes[i] % SHORT_INVITE_ALPHABET.length];
  }
  return out;
}

function issueShortInvite(inviteToken, expiresAtIso) {
  const expiresAtMs = Date.parse(expiresAtIso);
  if (!Number.isFinite(expiresAtMs)) {
    return null;
  }

  pruneExpiredShortInvites();

  if (shortInvites.size >= SHORT_INVITE_MAX_ACTIVE) {
    const oldestCode = shortInvites.keys().next().value;
    if (oldestCode) {
      shortInvites.delete(oldestCode);
    }
  }

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const code = generateShortCode(SHORT_INVITE_CODE_LENGTH);
    if (!shortInvites.has(code)) {
      shortInvites.set(code, {
        token: inviteToken,
        expiresAtMs,
      });
      return code;
    }
  }

  return null;
}

function generatePhraseSlug(wordCount) {
  const bytes = randomBytes(wordCount * 2);
  const words = [];
  for (let i = 0; i < wordCount; i += 1) {
    const value = (bytes[i * 2] << 8) | bytes[i * 2 + 1];
    const index = value % WORKER_INVITE_PHRASE_ALPHABET_SIZE;
    words.push(phraseWordlist[index]);
  }
  return words.join("-");
}

function issuePhraseInvite(inviteToken, expiresAtIso) {
  const expiresAtMs = Date.parse(expiresAtIso);
  if (!Number.isFinite(expiresAtMs)) {
    return null;
  }

  pruneExpiredShortInvites();

  if (phraseInvites.size >= SHORT_INVITE_MAX_ACTIVE) {
    const oldestPhrase = phraseInvites.keys().next().value;
    if (oldestPhrase) {
      phraseInvites.delete(oldestPhrase);
    }
  }

  for (let attempt = 0; attempt < 8; attempt += 1) {
    const phraseSlug = generatePhraseSlug(WORKER_INVITE_PHRASE_WORDS);
    if (!phraseInvites.has(phraseSlug)) {
      phraseInvites.set(phraseSlug, {
        token: inviteToken,
        expiresAtMs,
      });
      return phraseSlug;
    }
  }

  return null;
}

if (TRUST_PROXY) {
  app.set("trust proxy", 1);
}

app.disable("x-powered-by");
app.use(
  helmet({
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);
app.use(express.json({ limit: REQUEST_BODY_LIMIT }));
app.use(express.static(path.join(__dirname, "..", "web"), { index: false }));
app.use((req, res, next) => {
  const startedAt = Date.now();
  res.on("finish", () => {
    if (req.path === "/health" || req.path === withBasePath("/health")) {
      return;
    }
    const elapsedMs = Date.now() - startedAt;
    const ip = req.ip || req.socket.remoteAddress || "unknown";
    console.log(`[http] ${req.method} ${req.path} status=${res.statusCode} ip=${ip} elapsed_ms=${elapsedMs}`);
  });
  next();
});

const apiLimiter = rateLimit({
  windowMs: API_RATE_LIMIT_WINDOW_MS,
  max: API_RATE_LIMIT_MAX,
  standardHeaders: true,
  legacyHeaders: false,
  skip: (req) => {
    const ip = req.ip || req.socket?.remoteAddress || "";
    return ip === "127.0.0.1" || ip === "::1" || ip === "::ffff:127.0.0.1";
  },
  message: { error: "Too many requests. Slow down." },
});

app.use(API_BASE_PATH, apiLimiter);

function requestBaseUrl(req) {
  const forwardedProto = req.headers["x-forwarded-proto"];
  if (TRUST_PROXY && typeof forwardedProto === "string" && forwardedProto.length > 0) {
    const proto = forwardedProto.split(",")[0].trim();
    return `${proto}://${req.get("host")}`;
  }
  return `${req.protocol}://${req.get("host")}`;
}

function safeJsonParse(raw) {
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch (_error) {
    return { ok: false, value: null };
  }
}

function send(ws, payload) {
  if (ws.readyState === 1) {
    ws.send(JSON.stringify(payload));
  }
}

function summarizeTask(job) {
  if (!job || !job.task || typeof job.task !== "object") {
    return null;
  }
  const task = job.task;
  if (task.op === "run_js") {
    const executionModel = job.executionModel || task.executionModel || "single";
    const summary = {
      op: "run_js",
      executionModel,
      controlState: job.controlState || "running",
      timeoutMs: task.timeoutMs,
      codeBytes: Buffer.byteLength(String(task.code || ""), "utf8"),
      hasArgs: task.args !== undefined,
    };
    if (executionModel === "sharded") {
      summary.shardConfig = job.shardConfig || task.shardConfig || null;
      summary.reducer = job.reducer || task.reducer || null;
      summary.shards = {
        total: Number.isInteger(job.totalShards) ? job.totalShards : 0,
        completed: Number.isInteger(job.completedShards) ? job.completedShards : 0,
        failed: Number.isInteger(job.failedShards) ? job.failedShards : 0,
      };
    }
    return summary;
  }
  if (task.op === "add") {
    return {
      op: "add",
      a: task.a,
      b: task.b,
    };
  }
  return task;
}

function getClientIp(req) {
  const forwardedFor = req.headers["x-forwarded-for"];
  if (TRUST_PROXY && typeof forwardedFor === "string" && forwardedFor.length > 0) {
    return forwardedFor.split(",")[0].trim();
  }
  return req.socket.remoteAddress || "unknown";
}

app.get(APP_ROOT_PATH, (_req, res) => {
  res.redirect(CLIENT_PATH);
});

if (BASE_PATH) {
  app.get("/", (_req, res) => {
    res.redirect(APP_ROOT_PATH);
  });
}

app.get(CLIENT_PATH, (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "client.html"));
});

app.get(WORKER_PATH, (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "worker.html"));
});

app.get(withBasePath("/parallel-guide"), (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "parallel-guide.html"));
});

app.get(withBasePath("/parallel-guide.html"), (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "parallel-guide.html"));
});

app.get(RAYTRACE_VIEWER_PATH, (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "raytrace-viewer.html"));
});

app.get(withBasePath("/raytrace-viewer.html"), (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "raytrace-viewer.html"));
});

app.get(METRICS_PATH, (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "metrics.html"));
});

app.get(withBasePath("/metrics.html"), (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "metrics.html"));
});

app.get(withBasePath("/j/:code"), (req, res) => {
  pruneExpiredShortInvites();
  const code = String(req.params.code || "").toLowerCase();
  const record = shortInvites.get(code);
  if (!record) {
    return res.status(404).send("Invite code not found or expired.");
  }
  if (record.expiresAtMs <= Date.now()) {
    shortInvites.delete(code);
    return res.status(410).send("Invite code expired.");
  }
  return res.redirect(`${WORKER_PATH}?invite=${encodeURIComponent(record.token)}`);
});

app.get(withBasePath("/p/:phraseSlug"), (req, res) => {
  pruneExpiredShortInvites();
  const phraseSlug = String(req.params.phraseSlug || "").toLowerCase();
  const record = phraseInvites.get(phraseSlug);
  if (!record) {
    return res.status(404).send("Invite phrase not found or expired.");
  }
  if (record.expiresAtMs <= Date.now()) {
    phraseInvites.delete(phraseSlug);
    return res.status(410).send("Invite phrase expired.");
  }
  return res.redirect(`${WORKER_PATH}?invite=${encodeURIComponent(record.token)}`);
});

app.get("/health", (_req, res) => {
  res.json({ ok: true, now: new Date().toISOString() });
});

if (BASE_PATH) {
  app.get(withBasePath("/health"), (_req, res) => {
    res.json({ ok: true, now: new Date().toISOString() });
  });
}

app.get(withBasePath("/api/auth/check"), auth.requireClientAuth, (_req, res) => {
  res.json({ ok: true });
});

app.get(withBasePath("/api/public-config"), (_req, res) => {
  res.json({
    jobSigningPublicKeySpkiB64: config.jobSigningPublicKeySpkiB64,
    signedJobTtlSec: config.signedJobTtlSec,
    customJobLimits: {
      maxCodeBytes: config.customJobMaxCodeBytes,
      maxArgsBytes: config.customJobMaxArgsBytes,
      maxResultBytes: config.customJobMaxResultBytes,
      minTimeoutMs: config.customJobMinTimeoutMs,
      maxTimeoutMs: config.customJobMaxTimeoutMs,
      defaultTimeoutMs: config.customJobDefaultTimeoutMs,
    },
  });
});

app.post(withBasePath("/api/invites/worker"), auth.requireClientAuth, async (req, res) => {
  const parsed = parseInviteRequestBody(req.body);
  if (!parsed.ok) {
    return res.status(400).json({ error: parsed.error });
  }

  const invite = auth.issueWorkerInvite(parsed.value);
  const baseUrl = requestBaseUrl(req);
  const inviteUrl = `${baseUrl}${WORKER_PATH}?invite=${encodeURIComponent(invite.token)}`;
  const shortCode = issueShortInvite(invite.token, invite.expiresAt);
  const shortInviteUrl = shortCode ? `${baseUrl}${SHORT_INVITE_PATH}/${shortCode}` : null;
  const phraseSlug = issuePhraseInvite(invite.token, invite.expiresAt);
  const phraseInviteUrl = phraseSlug ? `${baseUrl}${PHRASE_INVITE_PATH}/${phraseSlug}` : null;
  const phraseCode = phraseSlug ? phraseSlug.split("-").join(" ") : null;
  const qrTargetUrl = phraseInviteUrl || shortInviteUrl || inviteUrl;
  let inviteQrDataUrl = null;
  try {
    inviteQrDataUrl = await QRCode.toDataURL(qrTargetUrl, {
      errorCorrectionLevel: "M",
      margin: 1,
      width: 320,
    });
  } catch (error) {
    console.warn(`[invite] Failed to generate QR image: ${error.message}`);
  }

  return res.status(201).json({
    inviteToken: invite.token,
    inviteUrl,
    shortCode,
    shortInviteUrl,
    phraseSlug,
    phraseCode,
    phraseInviteUrl,
    inviteQrDataUrl,
    expiresAt: invite.expiresAt,
    ttlSec: invite.ttlSec,
  });
});

app.post(withBasePath("/api/jobs"), auth.requireClientAuth, (req, res) => {
  const parsed = parseJobCreateBody(req.body, isValidInputNumber);
  if (!parsed.ok) {
    return res.status(400).json({
      error: `${parsed.error} Fields 'a' and 'b' must be integers in [${MIN_VALUE}, ${MAX_VALUE}].`,
    });
  }

  const job = store.createJob(parsed.value);
  dispatcher.dispatch();
  return res.status(201).json({ jobId: job.jobId, status: job.status });
});

app.post(withBasePath("/api/jobs/run-js"), auth.requireClientAuth, (req, res) => {
  const parsed = parseRunJsCreateBody(req.body, {
    maxCodeBytes: config.customJobMaxCodeBytes,
    maxArgsBytes: config.customJobMaxArgsBytes,
    minTimeoutMs: config.customJobMinTimeoutMs,
    maxTimeoutMs: config.customJobMaxTimeoutMs,
    defaultTimeoutMs: config.customJobDefaultTimeoutMs,
  });
  if (!parsed.ok) {
    return res.status(400).json({ error: parsed.error });
  }

  const connectedWorkers = store.listWorkers().length;
  let createValue = parsed.value;
  const executionNotes = [];
  if (parsed.value.executionModel === "sharded" && connectedWorkers < 2) {
    const fallbackArgs = {
      ...(parsed.value.args && typeof parsed.value.args === "object" && !Array.isArray(parsed.value.args)
        ? parsed.value.args
        : {}),
    };
    if (!Object.prototype.hasOwnProperty.call(fallbackArgs, "units") && parsed.value.shardConfig) {
      fallbackArgs.units = parsed.value.shardConfig.totalUnits;
    }
    createValue = {
      ...parsed.value,
      args: fallbackArgs,
      executionModel: "single",
      shardConfig: null,
      reducer: null,
    };
    executionNotes.push("Sharded execution auto-downgraded to single because fewer than 2 workers are connected.");
  } else if (parsed.value.executionModel === "sharded" && parsed.value.shardConfig) {
    const requestedUnitsPerShard = parsed.value.shardConfig.unitsPerShard;
    const safety = computeShardSafety(parsed.value.shardConfig.totalUnits, connectedWorkers);
    const requestedTotalShards = Math.ceil(parsed.value.shardConfig.totalUnits / requestedUnitsPerShard);
    if (requestedTotalShards > safety.maxShardsAllowed) {
      executionNotes.push(
        `High shard fan-out requested (${requestedTotalShards} shards); recommended soft cap is ${safety.maxShardsAllowed} for current worker count (${connectedWorkers}).`,
      );
    }
    createValue = {
      ...parsed.value,
      shardConfig: {
        totalUnits: parsed.value.shardConfig.totalUnits,
        unitsPerShard: requestedUnitsPerShard,
        totalShards: requestedTotalShards,
      },
    };
  }

  const job = createValue.executionModel === "sharded" ? store.createShardedRunJsJob(createValue) : store.createJob(createValue);
  dispatcher.dispatch();
  return res.status(201).json({
    jobId: job.jobId,
    status: job.status,
    controlState: job.controlState || "running",
    executionModel: job.executionModel || "single",
    note: executionNotes.length > 0 ? executionNotes.join(" ") : null,
  });
});

app.post(withBasePath("/api/jobs/stop-all"), auth.requireClientAuth, (_req, res) => {
  const stopped = store.stopAllJobs("stopped_by_client");
  dispatcher.dispatch();
  return res.json(stopped);
});

app.post(withBasePath("/api/jobs/:jobId/pause"), auth.requireClientAuth, (req, res) => {
  const { jobId } = req.params;
  const paused = store.pauseShardedJob(jobId);
  if (!paused.ok) {
    return res.status(400).json({ error: paused.reason });
  }
  return res.json(paused);
});

app.post(withBasePath("/api/jobs/:jobId/resume"), auth.requireClientAuth, (req, res) => {
  const { jobId } = req.params;
  const resumed = store.resumeShardedJob(jobId);
  if (!resumed.ok) {
    return res.status(400).json({ error: resumed.reason });
  }
  dispatcher.dispatch();
  return res.json(resumed);
});

app.get(withBasePath("/api/jobs/:jobId"), auth.requireClientAuth, (req, res) => {
  const { jobId } = req.params;
  const job = store.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found." });
  }
  return res.json({
    jobId: job.jobId,
    status: job.status,
    controlState: job.controlState || "running",
    executionModel: job.executionModel || "single",
    task: summarizeTask(job),
    result: job.result,
    error: job.error,
  });
});

app.get(withBasePath("/api/jobs/:jobId/events"), auth.requireClientAuth, (req, res) => {
  const { jobId } = req.params;
  const job = store.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found." });
  }
  if ((job.executionModel || "single") !== "sharded") {
    return res.status(400).json({ error: "Live events are only available for sharded jobs." });
  }

  const afterRaw = Array.isArray(req.query.after) ? req.query.after[0] : req.query.after;
  const limitRaw = Array.isArray(req.query.limit) ? req.query.limit[0] : req.query.limit;
  const after = Number.isSafeInteger(Number(afterRaw)) && Number(afterRaw) >= 0 ? Number(afterRaw) : 0;
  const limit = Number.isSafeInteger(Number(limitRaw)) && Number(limitRaw) > 0 ? Number(limitRaw) : 64;
  const slice = store.getJobEvents(jobId, after, limit);

  return res.json({
    jobId: job.jobId,
    status: job.status,
    controlState: job.controlState || "running",
    executionModel: job.executionModel || "single",
    shardConfig: job.shardConfig || null,
    reducer: job.reducer || null,
    ...slice,
  });
});

app.get(withBasePath("/api/workers"), auth.requireClientAuth, (_req, res) => {
  const workers = store.listWorkers();
  return res.json({ count: workers.length, workers });
});

app.get(withBasePath("/api/metrics/cluster"), auth.requireClientAuth, (_req, res) => {
  return res.json({
    now: new Date().toISOString(),
    workers: store.getWorkerMetricsSnapshot(null),
    runtime: store.getRuntimeSnapshot(160),
  });
});

app.get(withBasePath("/api/worker/metrics"), (req, res) => {
  const inviteToken =
    typeof req.query.invite === "string" ? req.query.invite : Array.isArray(req.query.invite) ? req.query.invite[0] : "";
  const workerId =
    typeof req.query.workerId === "string" ? req.query.workerId : Array.isArray(req.query.workerId) ? req.query.workerId[0] : null;

  const inviteValidation = auth.verifyWorkerInvite(inviteToken || "");
  if (!inviteValidation.ok) {
    return res.status(401).json({ error: `Unauthorized worker invite: ${inviteValidation.reason}` });
  }

  const snapshot = store.getWorkerMetricsSnapshot(workerId || null);
  return res.json(snapshot);
});

const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  path: WS_WORKER_PATH,
  maxPayload: WS_MAX_PAYLOAD,
  perMessageDeflate: false,
});

const connectionsPerIp = new Map();

function incConnections(ip) {
  const next = (connectionsPerIp.get(ip) || 0) + 1;
  connectionsPerIp.set(ip, next);
  return next;
}

function decConnections(ip) {
  const next = Math.max((connectionsPerIp.get(ip) || 1) - 1, 0);
  if (next === 0) {
    connectionsPerIp.delete(ip);
  } else {
    connectionsPerIp.set(ip, next);
  }
}

wss.on("connection", (ws, req) => {
  const ip = getClientIp(req);
  const activeForIp = incConnections(ip);
  if (activeForIp > WS_MAX_CONNECTIONS_PER_IP) {
    ws.close(1013, "Too many active connections from this IP");
    decConnections(ip);
    return;
  }

  const parsedUrl = new URL(req.url, "http://localhost");
  const inviteToken = parsedUrl.searchParams.get("invite") || "";
  const inviteValidation = auth.verifyWorkerInvite(inviteToken);
  if (!inviteValidation.ok) {
    ws.close(1008, `Unauthorized worker: ${inviteValidation.reason}`);
    decConnections(ip);
    return;
  }

  ws.meta = {
    ip,
    invalidCount: 0,
    registeredWorkerId: null,
    messagesThisWindow: 0,
    awaitingPongSince: 0,
    lastPongAt: Date.now(),
  };

  const resetMessageBudgetTimer = setInterval(() => {
    if (!ws.meta) {
      return;
    }
    ws.meta.messagesThisWindow = 0;
  }, WS_MESSAGE_WINDOW_MS);

  const heartbeatTimer = setInterval(() => {
    if (!ws.meta || ws.readyState !== 1) {
      return;
    }
    const now = Date.now();
    if (ws.meta.awaitingPongSince > 0 && now - ws.meta.awaitingPongSince > WS_HEARTBEAT_TIMEOUT_MS) {
      const workerId = ws.meta.registeredWorkerId || "unknown";
      console.warn(`[worker_heartbeat_timeout] worker_id=${workerId} ip=${ip}`);
      ws.terminate();
      return;
    }
    ws.meta.awaitingPongSince = now;
    try {
      ws.ping();
      send(ws, { type: "noop", heartbeat: true, ts: now });
    } catch (error) {
      console.warn(`[worker_heartbeat_ping_failed] ip=${ip} message=${error.message}`);
    }
  }, WS_HEARTBEAT_INTERVAL_MS);

  ws.on("pong", () => {
    if (!ws.meta) {
      return;
    }
    ws.meta.awaitingPongSince = 0;
    ws.meta.lastPongAt = Date.now();
  });

  ws.on("message", (raw) => {
    ws.meta.awaitingPongSince = 0;
    ws.meta.lastPongAt = Date.now();
    ws.meta.messagesThisWindow += 1;
    if (ws.meta.messagesThisWindow > WS_MAX_MESSAGES_PER_WINDOW) {
      ws.close(1013, "Rate limit exceeded");
      return;
    }

    const parsedRaw = safeJsonParse(String(raw));
    if (!parsedRaw.ok) {
      ws.meta.invalidCount += 1;
      if (ws.meta.invalidCount >= MAX_INVALID_MESSAGES) {
        ws.close(1008, "Too many malformed messages");
      }
      return;
    }

    const parsed = parseWorkerMessage(parsedRaw.value);
    if (!parsed.ok) {
      ws.meta.invalidCount += 1;
      send(ws, { type: "error", message: parsed.error });
      if (ws.meta.invalidCount >= MAX_INVALID_MESSAGES) {
        ws.close(1008, "Too many malformed messages");
      }
      return;
    }

    if (parsed.type === "register") {
      ws.meta.registeredWorkerId = parsed.workerId;
      store.registerWorker(parsed.workerId, ws, parsed.capabilities || null, parsed.features || null);
      send(ws, { type: "ack", message: "registered", workerId: parsed.workerId });
      return;
    }

    if (!ws.meta.registeredWorkerId || !store.workerExists(ws.meta.registeredWorkerId)) {
      send(ws, { type: "error", message: "Worker must register first." });
      return;
    }

    if (parsed.workerId !== ws.meta.registeredWorkerId) {
      send(ws, { type: "error", message: "workerId mismatch." });
      return;
    }

    store.markWorkerSeen(parsed.workerId);

    if (parsed.type === "heartbeat") {
      if (parsed.capabilities) {
        store.updateWorkerCapabilities(parsed.workerId, parsed.capabilities);
      }
      send(ws, { type: "noop", heartbeatAck: true, ts: Date.now() });
      return;
    }

    if (parsed.type === "ready") {
      store.markWorkerReady(parsed.workerId);
      send(ws, { type: "noop" });
      dispatcher.dispatch();
      return;
    }

    if (parsed.type === "result") {
      const done = store.finishJob(parsed.workerId, parsed.jobId, parsed.result);
      if (!done.ok) {
        send(ws, { type: "error", message: `Result rejected: ${done.reason}` });
        return;
      }
      dispatcher.dispatch();
      return;
    }

    if (parsed.type === "error") {
      const failed = store.failJob(parsed.workerId, parsed.jobId, parsed.error);
      if (!failed.ok) {
        send(ws, { type: "error", message: `Error rejected: ${failed.reason}` });
        return;
      }
      dispatcher.dispatch();
    }
  });

  ws.on("close", (code, reasonBuffer) => {
    clearInterval(resetMessageBudgetTimer);
    clearInterval(heartbeatTimer);
    decConnections(ip);
    const reason =
      typeof reasonBuffer === "string"
        ? reasonBuffer
        : Buffer.isBuffer(reasonBuffer)
          ? reasonBuffer.toString("utf8")
          : "";
    console.log(
      `[worker_ws_close] ip=${ip} code=${code} reason=${reason ? JSON.stringify(reason) : "\"\""} worker=${
        ws.meta && ws.meta.registeredWorkerId ? ws.meta.registeredWorkerId : "unknown"
      }`,
    );

    const removed = store.removeWorkerSocket(ws);
    if (removed.workerId) {
      console.log(`[worker_disconnect] worker_id=${removed.workerId} ip=${ip}`);
      if (removed.requeuedJobId) {
        console.log(`[job_requeued] job_id=${removed.requeuedJobId} reason=worker_disconnect`);
      }
      dispatcher.dispatch();
    }
  });

  ws.on("error", (error) => {
    console.error(`[worker_socket_error] ip=${ip} message=${error.message}`);
  });
});

server.listen(PORT, HOST, () => {
  console.log(`Cluster server running at http://${HOST}:${PORT}`);
  console.log(`Client UI: http://localhost:${PORT}${CLIENT_PATH}`);
  console.log(`Worker UI: http://localhost:${PORT}${WORKER_PATH}`);
  console.log(`Short Invite Path: ${SHORT_INVITE_PATH}/<code>`);
  console.log(`Phrase Invite Path: ${PHRASE_INVITE_PATH}/<word>-<word>-<word>-<word>`);
  if (BASE_PATH) {
    console.log(`[config] BASE_PATH=${BASE_PATH}`);
  }
  if (config.env !== "production") {
    console.log(`[dev] CLIENT_API_KEY=${config.clientApiKey}`);
    console.log(`[dev] WORKER_INVITE_SECRET=${config.workerInviteSecret}`);
    console.log(`[dev] JOB_SIGNING_PUBLIC_KEY_B64=${config.jobSigningPublicKeySpkiB64}`);
  }
});
