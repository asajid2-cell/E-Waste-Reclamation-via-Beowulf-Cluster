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

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || "0.0.0.0";
const TRUST_PROXY = String(process.env.TRUST_PROXY || "0") === "1";
const BASE_PATH_RAW = process.env.BASE_PATH || "";

const MAX_INVALID_MESSAGES = 4;
const WS_MAX_PAYLOAD = Number(process.env.WS_MAX_PAYLOAD || 256 * 1024);
const WS_MAX_CONNECTIONS_PER_IP = Number(process.env.WS_MAX_CONNECTIONS_PER_IP || 20);
const WS_MAX_MESSAGES_PER_WINDOW = Number(process.env.WS_MAX_MESSAGES_PER_WINDOW || 120);
const WS_MESSAGE_WINDOW_MS = Number(process.env.WS_MESSAGE_WINDOW_MS || 10_000);
const REQUEST_BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || "128kb";
const SHORT_INVITE_CODE_LENGTH = Math.min(Math.max(Number(process.env.SHORT_INVITE_CODE_LENGTH || 7), 4), 16);
const SHORT_INVITE_MAX_ACTIVE = Math.min(Math.max(Number(process.env.SHORT_INVITE_MAX_ACTIVE || 50000), 100), 500000);
const SHORT_INVITE_ALPHABET = "abcdefghjkmnpqrstuvwxyz23456789";
const WORKER_INVITE_PHRASE_WORDS = Math.min(Math.max(Number(process.env.WORKER_INVITE_PHRASE_WORDS || 8), 3), 8);
const WORKER_INVITE_PHRASE_ALPHABET_SIZE = 2048;

function normalizeBasePath(input) {
  if (!input || input === "/") {
    return "";
  }
  const withLeadingSlash = input.startsWith("/") ? input : `/${input}`;
  return withLeadingSlash.replace(/\/+$/, "");
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
const SHORT_INVITE_PATH = withBasePath("/j");
const PHRASE_INVITE_PATH = withBasePath("/p");
const APP_ROOT_PATH = BASE_PATH || "/";

const config = resolveConfig();
const auth = createAuth(config);

const app = express();
const store = createStore({
  maxCustomResultBytes: config.customJobMaxResultBytes,
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
  windowMs: 60_000,
  max: Number(process.env.API_RATE_LIMIT_MAX || 90),
  standardHeaders: true,
  legacyHeaders: false,
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

function summarizeTask(task) {
  if (!task || typeof task !== "object") {
    return null;
  }
  if (task.op === "run_js") {
    return {
      op: "run_js",
      timeoutMs: task.timeoutMs,
      codeBytes: Buffer.byteLength(String(task.code || ""), "utf8"),
      hasArgs: task.args !== undefined,
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

  const job = store.createJob(parsed.value);
  dispatcher.dispatch();
  return res.status(201).json({ jobId: job.jobId, status: job.status });
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
    task: summarizeTask(job.task),
    result: job.result,
    error: job.error,
  });
});

app.get(withBasePath("/api/workers"), auth.requireClientAuth, (_req, res) => {
  const workers = store.listWorkers();
  return res.json({ count: workers.length, workers });
});

const server = http.createServer(app);
const wss = new WebSocketServer({
  server,
  path: WS_WORKER_PATH,
  maxPayload: WS_MAX_PAYLOAD,
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
  };

  const resetMessageBudgetTimer = setInterval(() => {
    if (!ws.meta) {
      return;
    }
    ws.meta.messagesThisWindow = 0;
  }, WS_MESSAGE_WINDOW_MS);

  ws.on("message", (raw) => {
    ws.meta.messagesThisWindow += 1;
    if (ws.meta.messagesThisWindow > WS_MAX_MESSAGES_PER_WINDOW) {
      ws.close(1008, "Rate limit exceeded");
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
      store.registerWorker(parsed.workerId, ws);
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
      send(ws, { type: "ack", jobId: parsed.jobId });
      dispatcher.dispatch();
      return;
    }

    if (parsed.type === "error") {
      const failed = store.failJob(parsed.workerId, parsed.jobId, parsed.error);
      if (!failed.ok) {
        send(ws, { type: "error", message: `Error rejected: ${failed.reason}` });
        return;
      }
      send(ws, { type: "ack", jobId: parsed.jobId });
      dispatcher.dispatch();
    }
  });

  ws.on("close", () => {
    clearInterval(resetMessageBudgetTimer);
    decConnections(ip);

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
