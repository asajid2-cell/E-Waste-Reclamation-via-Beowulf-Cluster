const { randomUUID } = require("node:crypto");

const MIN_VALUE = -1_000_000_000;
const MAX_VALUE = 1_000_000_000;

function isValidInputNumber(value) {
  return Number.isInteger(value) && value >= MIN_VALUE && value <= MAX_VALUE;
}

function createStore() {
  const jobs = new Map();
  const queue = [];
  const workers = new Map();
  const socketToWorker = new Map();
  const assignments = new Map();

  function nowIso() {
    return new Date().toISOString();
  }

  function touchJob(job) {
    job.updatedAt = nowIso();
  }

  function createJob(a, b) {
    const jobId = randomUUID();
    const job = {
      jobId,
      a,
      b,
      status: "queued",
      result: null,
      error: null,
      assignedWorkerId: null,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    jobs.set(jobId, job);
    queue.push(jobId);
    return job;
  }

  function getJob(jobId) {
    return jobs.get(jobId) || null;
  }

  function getNextQueuedJob() {
    while (queue.length > 0) {
      const jobId = queue[0];
      const job = jobs.get(jobId);
      if (!job || job.status !== "queued") {
        queue.shift();
        continue;
      }
      return job;
    }
    return null;
  }

  function dequeueQueuedJob() {
    const nextJob = getNextQueuedJob();
    if (!nextJob) {
      return null;
    }
    queue.shift();
    return nextJob;
  }

  function requeueJob(jobId, front = false) {
    const job = jobs.get(jobId);
    if (!job) {
      return;
    }
    if (job.status !== "queued") {
      job.status = "queued";
      job.error = null;
      job.result = null;
      job.assignedWorkerId = null;
      touchJob(job);
    }
    if (queue.includes(jobId)) {
      return;
    }
    if (front) {
      queue.unshift(jobId);
    } else {
      queue.push(jobId);
    }
  }

  function registerWorker(workerId, ws) {
    const existing = workers.get(workerId);
    if (existing && existing.ws && existing.ws !== ws) {
      socketToWorker.delete(existing.ws);
      try {
        existing.ws.close(1012, "Replaced by a new connection");
      } catch (_err) {
        // Ignore close errors.
      }
    }

    workers.set(workerId, {
      workerId,
      ws,
      state: "idle",
      lastSeenAt: Date.now(),
    });
    socketToWorker.set(ws, workerId);
  }

  function workerExists(workerId) {
    return workers.has(workerId);
  }

  function getWorker(workerId) {
    return workers.get(workerId) || null;
  }

  function getFirstIdleWorker() {
    for (const worker of workers.values()) {
      if (worker.state === "idle") {
        return worker;
      }
    }
    return null;
  }

  function markWorkerReady(workerId) {
    const worker = workers.get(workerId);
    if (!worker) {
      return false;
    }
    worker.state = "idle";
    worker.lastSeenAt = Date.now();
    return true;
  }

  function markWorkerSeen(workerId) {
    const worker = workers.get(workerId);
    if (!worker) {
      return false;
    }
    worker.lastSeenAt = Date.now();
    return true;
  }

  function assignJob(job, workerId) {
    const worker = workers.get(workerId);
    if (!worker || worker.state !== "idle") {
      return false;
    }
    job.status = "running";
    job.assignedWorkerId = workerId;
    touchJob(job);
    worker.state = "busy";
    worker.lastSeenAt = Date.now();
    assignments.set(job.jobId, workerId);
    return true;
  }

  function finishJob(workerId, jobId, result) {
    const assignedWorkerId = assignments.get(jobId);
    if (!assignedWorkerId || assignedWorkerId !== workerId) {
      return { ok: false, reason: "assignment_mismatch" };
    }
    const job = jobs.get(jobId);
    const worker = workers.get(workerId);
    if (!job || !worker) {
      return { ok: false, reason: "unknown_job_or_worker" };
    }

    assignments.delete(jobId);
    job.status = "done";
    job.result = result;
    job.error = null;
    job.assignedWorkerId = workerId;
    touchJob(job);

    worker.state = "idle";
    worker.lastSeenAt = Date.now();
    return { ok: true };
  }

  function failJob(workerId, jobId, error) {
    const assignedWorkerId = assignments.get(jobId);
    if (!assignedWorkerId || assignedWorkerId !== workerId) {
      return { ok: false, reason: "assignment_mismatch" };
    }
    const job = jobs.get(jobId);
    const worker = workers.get(workerId);
    if (!job || !worker) {
      return { ok: false, reason: "unknown_job_or_worker" };
    }

    assignments.delete(jobId);
    job.status = "failed";
    job.error = error;
    job.assignedWorkerId = workerId;
    touchJob(job);

    worker.state = "idle";
    worker.lastSeenAt = Date.now();
    return { ok: true };
  }

  function removeWorkerSocket(ws) {
    const workerId = socketToWorker.get(ws);
    if (!workerId) {
      return { workerId: null, requeuedJobId: null };
    }

    socketToWorker.delete(ws);
    const worker = workers.get(workerId);
    if (!worker) {
      return { workerId, requeuedJobId: null };
    }

    let requeuedJobId = null;
    for (const [jobId, assignedWorkerId] of assignments.entries()) {
      if (assignedWorkerId !== workerId) {
        continue;
      }
      assignments.delete(jobId);
      const job = jobs.get(jobId);
      if (job && job.status === "running") {
        job.status = "queued";
        job.assignedWorkerId = null;
        touchJob(job);
        requeueJob(jobId, true);
        requeuedJobId = jobId;
      }
      break;
    }

    workers.delete(workerId);
    return { workerId, requeuedJobId };
  }

  function listWorkers() {
    const now = Date.now();
    return Array.from(workers.values()).map((worker) => ({
      workerId: worker.workerId,
      state: worker.state,
      lastSeenMs: Math.max(0, now - worker.lastSeenAt),
    }));
  }

  return {
    MIN_VALUE,
    MAX_VALUE,
    isValidInputNumber,
    createJob,
    getJob,
    dequeueQueuedJob,
    requeueJob,
    registerWorker,
    workerExists,
    getWorker,
    getFirstIdleWorker,
    markWorkerReady,
    markWorkerSeen,
    assignJob,
    finishJob,
    failJob,
    removeWorkerSocket,
    listWorkers,
  };
}

module.exports = {
  createStore,
  isValidInputNumber,
  MIN_VALUE,
  MAX_VALUE,
};
