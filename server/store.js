const { randomUUID } = require("node:crypto");

const MIN_VALUE = -1_000_000_000;
const MAX_VALUE = 1_000_000_000;
const DEFAULT_MAX_CUSTOM_RESULT_BYTES = 64 * 1024;
const DEFAULT_MAX_SHARD_ATTEMPTS = 3;

function isValidInputNumber(value) {
  return Number.isInteger(value) && value >= MIN_VALUE && value <= MAX_VALUE;
}

function buildShardInjectedCode(sourceCode, shardContext) {
  const contextLiteral = JSON.stringify(shardContext);
  return `globalThis.__BRAIN__ = Object.freeze(${contextLiteral});
${sourceCode}`;
}

function initializeReducerAggregate(reducer) {
  if (!reducer || typeof reducer !== "object") {
    return {};
  }
  if (reducer.type === "collect") {
    return [];
  }
  if (reducer.type === "sum") {
    const aggregate = {};
    for (const field of reducer.fields || []) {
      aggregate[field] = 0;
    }
    return aggregate;
  }
  return { value: null };
}

function applyReducerResult(reducer, aggregate, shardResult) {
  if (!reducer || typeof reducer !== "object") {
    return;
  }

  if (reducer.type === "collect") {
    if (Array.isArray(aggregate)) {
      aggregate.push(shardResult);
    }
    return;
  }

  if (reducer.type === "sum") {
    if (!shardResult || typeof shardResult !== "object" || Array.isArray(shardResult)) {
      throw new Error("sum reducer expects shard result object.");
    }
    const envelopeReturn =
      shardResult &&
      typeof shardResult === "object" &&
      shardResult.returnValue &&
      typeof shardResult.returnValue === "object" &&
      !Array.isArray(shardResult.returnValue)
        ? shardResult.returnValue
        : null;
    for (const field of reducer.fields || []) {
      let rawValue = shardResult[field];
      if (!Number.isFinite(Number(rawValue)) && envelopeReturn) {
        rawValue = envelopeReturn[field];
      }
      const value = Number(rawValue);
      if (!Number.isFinite(value)) {
        throw new Error(`sum reducer field '${field}' must be numeric.`);
      }
      aggregate[field] = (aggregate[field] || 0) + value;
    }
    return;
  }

  if (reducer.type === "min" || reducer.type === "max") {
    const field = reducer.field;
    let rawValue = shardResult && typeof shardResult === "object" ? shardResult[field] : NaN;
    if (
      !Number.isFinite(Number(rawValue)) &&
      shardResult &&
      typeof shardResult === "object" &&
      shardResult.returnValue &&
      typeof shardResult.returnValue === "object" &&
      !Array.isArray(shardResult.returnValue)
    ) {
      rawValue = shardResult.returnValue[field];
    }
    const numeric = Number(rawValue);
    if (!Number.isFinite(numeric)) {
      throw new Error(`${reducer.type} reducer field '${field}' must be numeric.`);
    }
    if (aggregate.value === null) {
      aggregate.value = numeric;
      return;
    }
    if (reducer.type === "min" && numeric < aggregate.value) {
      aggregate.value = numeric;
      return;
    }
    if (reducer.type === "max" && numeric > aggregate.value) {
      aggregate.value = numeric;
    }
  }
}

function createStore(options = {}) {
  const maxCustomResultBytes = Number.isInteger(options.maxCustomResultBytes) && options.maxCustomResultBytes > 0
    ? options.maxCustomResultBytes
    : DEFAULT_MAX_CUSTOM_RESULT_BYTES;

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

  function createJob(task, createOptions = {}) {
    const jobId = createOptions.jobId || randomUUID();
    const job = {
      jobId,
      task,
      status: createOptions.status || "queued",
      result: null,
      error: null,
      assignedWorkerId: null,
      hidden: Boolean(createOptions.hidden),
      parentJobId: createOptions.parentJobId || null,
      executionModel: createOptions.executionModel || "single",
      attempt: 0,
      maxAttempts:
        Number.isInteger(createOptions.maxAttempts) && createOptions.maxAttempts > 0
          ? createOptions.maxAttempts
          : 1,
      shardIndex: Number.isInteger(createOptions.shardIndex) ? createOptions.shardIndex : null,
      totalShards: Number.isInteger(createOptions.totalShards) ? createOptions.totalShards : null,
      sourceCode: createOptions.sourceCode || null,
      shardContextBase: createOptions.shardContextBase || null,
      shardConfig: createOptions.shardConfig || null,
      reducer: createOptions.reducer || null,
      aggregate: createOptions.aggregate || null,
      completedShards: Number.isInteger(createOptions.completedShards) ? createOptions.completedShards : null,
      failedShards: Number.isInteger(createOptions.failedShards) ? createOptions.failedShards : null,
      childJobIds: Array.isArray(createOptions.childJobIds) ? createOptions.childJobIds : null,
      createdAt: nowIso(),
      updatedAt: nowIso(),
    };
    jobs.set(jobId, job);
    if (!createOptions.skipQueue && job.status === "queued") {
      queue.push(jobId);
    }
    return job;
  }

  function createShardedRunJsJob(task) {
    const shardConfig = task.shardConfig;
    const reducer = task.reducer;
    const totalShards = Math.ceil(shardConfig.totalUnits / shardConfig.unitsPerShard);
    const parentJob = createJob(task, {
      skipQueue: true,
      status: "running",
      executionModel: "sharded",
      maxAttempts: 1,
      totalShards,
      shardConfig,
      reducer,
      aggregate: initializeReducerAggregate(reducer),
      completedShards: 0,
      failedShards: 0,
      childJobIds: [],
    });

    let remainingUnits = shardConfig.totalUnits;
    let offset = 0;
    for (let shardIndex = 0; shardIndex < totalShards; shardIndex += 1) {
      const units = Math.min(shardConfig.unitsPerShard, remainingUnits);
      const childTask = {
        op: "run_js",
        code: "",
        args: task.args,
        timeoutMs: task.timeoutMs,
      };
      const childJob = createJob(childTask, {
        hidden: true,
        parentJobId: parentJob.jobId,
        executionModel: "sharded",
        maxAttempts: task.shardMaxAttempts || DEFAULT_MAX_SHARD_ATTEMPTS,
        shardIndex,
        totalShards,
        sourceCode: task.code,
        shardContextBase: {
          parentJobId: parentJob.jobId,
          shardId: shardIndex,
          totalShards,
          units,
          totalUnits: shardConfig.totalUnits,
          offset,
        },
      });
      parentJob.childJobIds.push(childJob.jobId);
      remainingUnits -= units;
      offset += units;
    }

    touchJob(parentJob);
    return parentJob;
  }

  function getJob(jobId) {
    const job = jobs.get(jobId) || null;
    if (!job || job.hidden) {
      return null;
    }
    return job;
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
    if (job.parentJobId && job.sourceCode && job.shardContextBase) {
      const nextAttempt = job.attempt + 1;
      const shardContext = {
        ...job.shardContextBase,
        attempt: nextAttempt,
      };
      job.task.code = buildShardInjectedCode(job.sourceCode, shardContext);
    }
    job.status = "running";
    job.assignedWorkerId = workerId;
    job.attempt += 1;
    touchJob(job);
    worker.state = "busy";
    worker.lastSeenAt = Date.now();
    assignments.set(job.jobId, workerId);
    return true;
  }

  function failParentAndStopChildren(parentJob, reason) {
    parentJob.status = "failed";
    parentJob.error = reason;
    touchJob(parentJob);

    if (!Array.isArray(parentJob.childJobIds)) {
      return;
    }
    for (const childJobId of parentJob.childJobIds) {
      const childJob = jobs.get(childJobId);
      if (!childJob) {
        continue;
      }
      if (childJob.status === "queued") {
        childJob.status = "failed";
        childJob.error = "parent_failed";
        touchJob(childJob);
      }
    }
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

    let normalizedResult = result;
    if (job.task && job.task.op === "add") {
      if (!Number.isInteger(result)) {
        return { ok: false, reason: "invalid_result_for_add" };
      }
      normalizedResult = result;
    } else if (job.task && job.task.op === "run_js") {
      let resultJson = "";
      try {
        resultJson = JSON.stringify(result);
      } catch (_error) {
        return { ok: false, reason: "custom_result_not_json" };
      }
      if (resultJson === undefined) {
        return { ok: false, reason: "custom_result_undefined" };
      }
      const resultBytes = Buffer.byteLength(resultJson, "utf8");
      if (resultBytes > maxCustomResultBytes) {
        return { ok: false, reason: "custom_result_too_large" };
      }
      normalizedResult = JSON.parse(resultJson);
    }

    assignments.delete(jobId);
    job.status = "done";
    job.result = normalizedResult;
    job.error = null;
    job.assignedWorkerId = workerId;
    touchJob(job);

    worker.state = "idle";
    worker.lastSeenAt = Date.now();

    if (job.parentJobId) {
      const parentJob = jobs.get(job.parentJobId);
      if (!parentJob) {
        return { ok: true };
      }
      if (parentJob.status === "failed" || parentJob.status === "done") {
        return { ok: true };
      }
      try {
        applyReducerResult(parentJob.reducer, parentJob.aggregate, normalizedResult);
      } catch (error) {
        failParentAndStopChildren(parentJob, `reducer_error:${error.message}`);
        return { ok: true };
      }
      parentJob.completedShards += 1;
      if (parentJob.completedShards >= parentJob.totalShards) {
        parentJob.status = "done";
        parentJob.result = {
          reducer: parentJob.reducer,
          aggregate: parentJob.aggregate,
          completedShards: parentJob.completedShards,
          totalShards: parentJob.totalShards,
        };
        parentJob.error = null;
      }
      touchJob(parentJob);
    }

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
    worker.state = "idle";
    worker.lastSeenAt = Date.now();

    if (job.parentJobId && job.status === "running" && job.attempt < job.maxAttempts) {
      job.status = "queued";
      job.error = `retryable:${error}`;
      job.assignedWorkerId = null;
      touchJob(job);
      requeueJob(jobId, true);
      return { ok: true, requeued: true };
    }

    job.status = "failed";
    job.error = error;
    job.assignedWorkerId = workerId;
    touchJob(job);

    if (job.parentJobId) {
      const parentJob = jobs.get(job.parentJobId);
      if (parentJob && parentJob.status !== "failed" && parentJob.status !== "done") {
        parentJob.failedShards += 1;
        failParentAndStopChildren(
          parentJob,
          `Shard ${job.shardIndex} failed after ${job.attempt}/${job.maxAttempts} attempts: ${error}`,
        );
      }
    }

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
    createShardedRunJsJob,
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
    maxCustomResultBytes,
  };
}

module.exports = {
  createStore,
  isValidInputNumber,
  MIN_VALUE,
  MAX_VALUE,
};
