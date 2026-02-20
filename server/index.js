const path = require("node:path");
const http = require("node:http");
const { URL } = require("node:url");

const express = require("express");
const rateLimit = require("express-rate-limit");
const helmet = require("helmet");
const { WebSocketServer } = require("ws");

const { createStore, isValidInputNumber, MIN_VALUE, MAX_VALUE } = require("./store");
const { createDispatcher } = require("./dispatcher");
const { createAuth, resolveConfig } = require("./auth");
const { parseInviteRequestBody, parseJobCreateBody, parseWorkerMessage } = require("./validation");

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || "0.0.0.0";
const TRUST_PROXY = String(process.env.TRUST_PROXY || "0") === "1";
const BASE_PATH_RAW = process.env.BASE_PATH || "";

const MAX_INVALID_MESSAGES = 4;
const WS_MAX_PAYLOAD = Number(process.env.WS_MAX_PAYLOAD || 8 * 1024);
const WS_MAX_CONNECTIONS_PER_IP = Number(process.env.WS_MAX_CONNECTIONS_PER_IP || 20);
const WS_MAX_MESSAGES_PER_WINDOW = Number(process.env.WS_MAX_MESSAGES_PER_WINDOW || 120);
const WS_MESSAGE_WINDOW_MS = Number(process.env.WS_MESSAGE_WINDOW_MS || 10_000);
const REQUEST_BODY_LIMIT = process.env.REQUEST_BODY_LIMIT || "8kb";

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
const APP_ROOT_PATH = BASE_PATH || "/";

const config = resolveConfig();
const auth = createAuth(config);

const app = express();
const store = createStore();
const dispatcher = createDispatcher(store, console);

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

app.post(withBasePath("/api/invites/worker"), auth.requireClientAuth, (req, res) => {
  const parsed = parseInviteRequestBody(req.body);
  if (!parsed.ok) {
    return res.status(400).json({ error: parsed.error });
  }

  const invite = auth.issueWorkerInvite(parsed.value);
  const baseUrl = requestBaseUrl(req);
  const inviteUrl = `${baseUrl}${WORKER_PATH}?invite=${encodeURIComponent(invite.token)}`;

  return res.status(201).json({
    inviteToken: invite.token,
    inviteUrl,
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

app.get(withBasePath("/api/jobs/:jobId"), auth.requireClientAuth, (req, res) => {
  const { jobId } = req.params;
  const job = store.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found." });
  }
  return res.json({
    jobId: job.jobId,
    status: job.status,
    task: job.task,
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
  if (BASE_PATH) {
    console.log(`[config] BASE_PATH=${BASE_PATH}`);
  }
  if (config.env !== "production") {
    console.log(`[dev] CLIENT_API_KEY=${config.clientApiKey}`);
    console.log(`[dev] WORKER_INVITE_SECRET=${config.workerInviteSecret}`);
  }
});
