const path = require("node:path");
const http = require("node:http");

const express = require("express");
const { WebSocketServer } = require("ws");

const { createStore, isValidInputNumber, MIN_VALUE, MAX_VALUE } = require("./store");
const { createDispatcher } = require("./dispatcher");

const PORT = Number(process.env.PORT || 8080);
const MAX_INVALID_MESSAGES = 3;

const app = express();
const store = createStore();
const dispatcher = createDispatcher(store, console);

app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "web")));

app.get("/", (_req, res) => {
  res.redirect("/client");
});

app.get("/client", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "client.html"));
});

app.get("/worker", (_req, res) => {
  res.sendFile(path.join(__dirname, "..", "web", "worker.html"));
});

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

app.post("/api/jobs", (req, res) => {
  const { a, b } = req.body || {};
  if (!isValidInputNumber(a) || !isValidInputNumber(b)) {
    return res.status(400).json({
      error: `Invalid input. 'a' and 'b' must be integers in [${MIN_VALUE}, ${MAX_VALUE}].`,
    });
  }

  const job = store.createJob(a, b);
  dispatcher.dispatch();
  return res.status(201).json({ jobId: job.jobId, status: job.status });
});

app.get("/api/jobs/:jobId", (req, res) => {
  const { jobId } = req.params;
  const job = store.getJob(jobId);
  if (!job) {
    return res.status(404).json({ error: "Job not found." });
  }
  return res.json({
    jobId: job.jobId,
    status: job.status,
    result: job.result,
    error: job.error,
  });
});

app.get("/api/workers", (_req, res) => {
  const workers = store.listWorkers();
  return res.json({ count: workers.length, workers });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: "/ws/worker" });

function safeJsonParse(raw) {
  try {
    return { ok: true, value: JSON.parse(raw) };
  } catch (_error) {
    return { ok: false, value: null };
  }
}

function isValidWorkerId(workerId) {
  return (
    typeof workerId === "string" &&
    workerId.length > 0 &&
    workerId.length <= 128 &&
    /^[a-zA-Z0-9._-]+$/.test(workerId)
  );
}

function send(ws, payload) {
  ws.send(JSON.stringify(payload));
}

wss.on("connection", (ws) => {
  let invalidCount = 0;
  let registeredWorkerId = null;

  ws.on("message", (raw) => {
    const parsed = safeJsonParse(String(raw));
    if (!parsed.ok || typeof parsed.value !== "object" || parsed.value === null) {
      invalidCount += 1;
      if (invalidCount >= MAX_INVALID_MESSAGES) {
        ws.close(1008, "Too many malformed messages");
      }
      return;
    }

    const message = parsed.value;
    const { type, workerId } = message;

    if (typeof type !== "string") {
      invalidCount += 1;
      if (invalidCount >= MAX_INVALID_MESSAGES) {
        ws.close(1008, "Too many malformed messages");
      }
      return;
    }

    if (isValidWorkerId(workerId)) {
      store.markWorkerSeen(workerId);
    }

    if (type === "register") {
      if (!isValidWorkerId(workerId)) {
        send(ws, { type: "error", message: "Invalid workerId." });
        return;
      }
      registeredWorkerId = workerId;
      store.registerWorker(workerId, ws);
      send(ws, { type: "ack", message: "registered", workerId });
      return;
    }

    if (!registeredWorkerId || !store.workerExists(registeredWorkerId)) {
      send(ws, { type: "error", message: "Worker must register first." });
      return;
    }

    if (workerId !== registeredWorkerId) {
      send(ws, { type: "error", message: "workerId mismatch." });
      return;
    }

    if (type === "ready") {
      store.markWorkerReady(workerId);
      send(ws, { type: "noop" });
      dispatcher.dispatch();
      return;
    }

    if (type === "result") {
      const { jobId, result } = message;
      if (typeof jobId !== "string" || !Number.isInteger(result)) {
        send(ws, { type: "error", message: "Invalid result payload." });
        return;
      }
      const done = store.finishJob(workerId, jobId, result);
      if (!done.ok) {
        send(ws, { type: "error", message: `Result rejected: ${done.reason}` });
        return;
      }
      send(ws, { type: "ack", jobId });
      dispatcher.dispatch();
      return;
    }

    if (type === "error") {
      const { jobId, error } = message;
      if (typeof jobId !== "string") {
        send(ws, { type: "error", message: "Invalid error payload." });
        return;
      }
      const failed = store.failJob(workerId, jobId, typeof error === "string" ? error : "Worker error");
      if (!failed.ok) {
        send(ws, { type: "error", message: `Error rejected: ${failed.reason}` });
        return;
      }
      send(ws, { type: "ack", jobId });
      dispatcher.dispatch();
      return;
    }

    invalidCount += 1;
    if (invalidCount >= MAX_INVALID_MESSAGES) {
      ws.close(1008, "Too many malformed messages");
    }
  });

  ws.on("close", () => {
    const removed = store.removeWorkerSocket(ws);
    if (removed.workerId) {
      console.log(`Worker disconnected: ${removed.workerId}`);
      if (removed.requeuedJobId) {
        console.log(`Re-queued job due to disconnect: ${removed.requeuedJobId}`);
      }
      dispatcher.dispatch();
    }
  });

  ws.on("error", (error) => {
    console.error("Worker socket error:", error.message);
  });
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`Cluster MVP server running at http://0.0.0.0:${PORT}`);
  console.log(`Client UI: http://localhost:${PORT}/client`);
  console.log(`Worker UI: http://localhost:${PORT}/worker`);
});
