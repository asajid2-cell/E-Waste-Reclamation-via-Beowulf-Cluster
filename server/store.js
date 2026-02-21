const { randomUUID } = require("node:crypto");

const MIN_VALUE = -1_000_000_000;
const MAX_VALUE = 1_000_000_000;
const DEFAULT_MAX_CUSTOM_RESULT_BYTES = 64 * 1024;
const DEFAULT_MAX_SHARD_ATTEMPTS = 3;
const DEFAULT_MAX_WORKER_SLOTS = 8;
const DEFAULT_JOB_EVENT_BUFFER_SIZE = 4000;

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
      if (field === "*") {
        continue;
      }
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
    const fields = Array.isArray(reducer.fields) ? reducer.fields : [];
    const wildcardMode = fields.includes("*");

    if (wildcardMode) {
      const source = envelopeReturn || shardResult;
      for (const [field, rawValue] of Object.entries(source)) {
        const value = Number(rawValue);
        if (!Number.isFinite(value)) {
          continue;
        }
        aggregate[field] = (aggregate[field] || 0) + value;
      }
      return;
    }

    for (const field of fields) {
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
  const maxWorkerSlots = Number.isInteger(options.maxWorkerSlots) && options.maxWorkerSlots > 0
    ? options.maxWorkerSlots
    : DEFAULT_MAX_WORKER_SLOTS;
  const maxJobEventBufferSize = Number.isInteger(options.maxJobEventBufferSize) && options.maxJobEventBufferSize > 0
    ? options.maxJobEventBufferSize
    : DEFAULT_JOB_EVENT_BUFFER_SIZE;

  const jobs = new Map();
  const queue = [];
  const workers = new Map();
  const workerStats = new Map();
  const socketToWorker = new Map();
  const assignments = new Map();
  const jobEventBuffers = new Map();

  function clampNumber(value, minValue, maxValue, fallback) {
    const n = Number(value);
    if (!Number.isFinite(n)) {
      return fallback;
    }
    return Math.max(minValue, Math.min(maxValue, n));
  }

  function normalizeWorkerCapabilities(capabilities) {
    if (!capabilities || typeof capabilities !== "object" || Array.isArray(capabilities)) {
      return null;
    }
    const connection =
      capabilities.connection && typeof capabilities.connection === "object" && !Array.isArray(capabilities.connection)
        ? capabilities.connection
        : {};
    const battery =
      capabilities.battery && typeof capabilities.battery === "object" && !Array.isArray(capabilities.battery)
        ? capabilities.battery
        : {};
    const normalized = {
      hardwareConcurrency: Math.trunc(clampNumber(capabilities.hardwareConcurrency, 1, 128, 1)),
      deviceMemoryGB: clampNumber(capabilities.deviceMemoryGB, 0.25, 256, 1),
      effectiveType:
        typeof connection.effectiveType === "string" && connection.effectiveType.length <= 16
          ? connection.effectiveType
          : "unknown",
      downlinkMbps: clampNumber(connection.downlinkMbps, 0, 10000, 0),
      rttMs: clampNumber(connection.rttMs, 0, 60000, 0),
      saveData: Boolean(connection.saveData),
      platform:
        typeof capabilities.platform === "string" && capabilities.platform.length <= 120
          ? capabilities.platform
          : "",
      visibility:
        typeof capabilities.visibility === "string" && capabilities.visibility.length <= 24
          ? capabilities.visibility
          : "unknown",
      batteryLevel: clampNumber(battery.level, 0, 1, 1),
      charging: battery.charging === undefined ? null : Boolean(battery.charging),
      updatedAt: nowIso(),
    };
    return normalized;
  }

  function computeStaticResourceScore(capabilities) {
    if (!capabilities) {
      return 1;
    }
    const cpuScore = clampNumber(capabilities.hardwareConcurrency, 1, 32, 1) / 4;
    const memScore = clampNumber(capabilities.deviceMemoryGB, 0.25, 32, 1) / 4;
    let networkFactor = 1;
    if (capabilities.effectiveType === "slow-2g" || capabilities.effectiveType === "2g") {
      networkFactor = 0.7;
    } else if (capabilities.effectiveType === "3g") {
      networkFactor = 0.85;
    } else if (capabilities.effectiveType === "4g") {
      networkFactor = 1.05;
    }
    const saveDataFactor = capabilities.saveData ? 0.9 : 1;
    const batteryFactor = capabilities.charging === false && capabilities.batteryLevel < 0.2 ? 0.8 : 1;
    const base = (cpuScore * 0.65 + memScore * 0.35) * networkFactor * saveDataFactor * batteryFactor;
    return clampNumber(base, 0.35, 6, 1);
  }

  function deriveWorkerSlotCapacity(capabilities) {
    if (!capabilities) {
      return 1;
    }
    const hc = Math.trunc(clampNumber(capabilities.hardwareConcurrency, 1, 128, 1));
    const memoryGb = clampNumber(capabilities.deviceMemoryGB, 0.25, 256, 1);
    const cpuBound = Math.max(1, hc > 2 ? hc - 1 : 1);
    const memBound = Math.max(1, Math.floor(memoryGb));
    let slots = Math.max(1, Math.min(cpuBound, memBound, maxWorkerSlots));
    if (capabilities.charging === false && capabilities.batteryLevel < 0.15) {
      slots = 1;
    }
    if (capabilities.saveData) {
      slots = Math.max(1, Math.min(slots, 2));
    }
    return slots;
  }

  function getWorkerAvailableSlots(worker) {
    if (!worker) {
      return 0;
    }
    const maxSlots = Number.isInteger(worker.maxSlots) && worker.maxSlots > 0 ? worker.maxSlots : 1;
    const inFlight = Number.isInteger(worker.inFlightCount) && worker.inFlightCount >= 0 ? worker.inFlightCount : 0;
    return Math.max(0, maxSlots - inFlight);
  }

  function refreshWorkerState(worker) {
    if (!worker) {
      return;
    }
    worker.state = getWorkerAvailableSlots(worker) > 0 ? "idle" : "busy";
  }

  function nowIso() {
    return new Date().toISOString();
  }

  function touchJob(job) {
    job.updatedAt = nowIso();
  }

  function normalizeResultForLiveEvent(normalizedResult) {
    if (!normalizedResult || typeof normalizedResult !== "object") {
      return normalizedResult;
    }
    if (
      Object.prototype.hasOwnProperty.call(normalizedResult, "returnValue") &&
      normalizedResult.returnValue !== undefined
    ) {
      return normalizedResult.returnValue;
    }
    return normalizedResult;
  }

  function ensureJobEventBuffer(jobId) {
    const existing = jobEventBuffers.get(jobId);
    if (existing) {
      return existing;
    }
    const created = {
      nextSeq: 1,
      events: [],
    };
    jobEventBuffers.set(jobId, created);
    return created;
  }

  function appendJobEvent(jobId, payload) {
    if (!jobId || !payload || typeof payload !== "object") {
      return null;
    }
    const buffer = ensureJobEventBuffer(jobId);
    const event = {
      seq: buffer.nextSeq,
      ts: nowIso(),
      ...payload,
    };
    buffer.nextSeq += 1;
    buffer.events.push(event);
    while (buffer.events.length > maxJobEventBufferSize) {
      buffer.events.shift();
    }
    return event;
  }

  function getJobEvents(jobId, afterSeq = 0, limit = 64) {
    const after = Number.isSafeInteger(afterSeq) && afterSeq >= 0 ? afterSeq : 0;
    const maxLimit = 256;
    const safeLimit = Number.isSafeInteger(limit) && limit > 0 ? Math.min(limit, maxLimit) : 64;
    const buffer = ensureJobEventBuffer(jobId);
    const firstSeq = buffer.events.length > 0 ? buffer.events[0].seq : buffer.nextSeq;
    const startIndex = Math.max(0, after - firstSeq + 1);
    const events = buffer.events.slice(startIndex, startIndex + safeLimit);
    const hasMore = startIndex + events.length < buffer.events.length;
    const droppedBeforeSeq = after + 1 < firstSeq ? firstSeq - (after + 1) : 0;
    return {
      events,
      hasMore,
      nextSeq: buffer.nextSeq,
      firstSeq,
      droppedBeforeSeq,
    };
  }

  function ensureWorkerStats(workerId) {
    const nowMs = Date.now();
    const existing = workerStats.get(workerId);
    if (existing) {
      return existing;
    }
    const created = {
      workerId,
      firstSeenAt: nowMs,
      lastSeenAt: nowMs,
      lastConnectedAt: 0,
      connected: false,
      reconnects: 0,
      completedJobs: 0,
      completedAddJobs: 0,
      completedRunJsJobs: 0,
      completedShards: 0,
      failedJobs: 0,
      failedShards: 0,
      contributedUnits: 0,
      totalDurationMs: 0,
      lastJobAt: 0,
      throughputUnitsPerSecEma: 0,
      throughputSamples: 0,
      parentThroughput: {},
      capabilities: null,
      resourceScore: 1,
      dispatchScore: 1,
    };
    workerStats.set(workerId, created);
    return created;
  }

  function markWorkerConnected(workerId) {
    const stats = ensureWorkerStats(workerId);
    const nowMs = Date.now();
    if (stats.lastConnectedAt > 0) {
      stats.reconnects += 1;
    }
    stats.connected = true;
    stats.lastConnectedAt = nowMs;
    stats.lastSeenAt = nowMs;
    return stats;
  }

  function updateWorkerCapabilities(workerId, capabilities) {
    const normalized = normalizeWorkerCapabilities(capabilities);
    if (!normalized) {
      return null;
    }
    const worker = workers.get(workerId);
    if (worker) {
      worker.capabilities = normalized;
      worker.staticResourceScore = computeStaticResourceScore(normalized);
      worker.maxSlots = deriveWorkerSlotCapacity(normalized);
      refreshWorkerState(worker);
    }
    const stats = ensureWorkerStats(workerId);
    stats.capabilities = normalized;
    stats.resourceScore = computeStaticResourceScore(normalized);
    return normalized;
  }

  function markWorkerDisconnected(workerId) {
    const stats = ensureWorkerStats(workerId);
    stats.connected = false;
    stats.lastSeenAt = Date.now();
    return stats;
  }

  function recordWorkerSeen(workerId) {
    const stats = ensureWorkerStats(workerId);
    stats.lastSeenAt = Date.now();
    return stats;
  }

  function recordWorkerCompletion(workerId, job, normalizedResult) {
    const stats = ensureWorkerStats(workerId);
    const nowMs = Date.now();
    stats.lastSeenAt = nowMs;
    stats.completedJobs += 1;
    stats.lastJobAt = nowMs;

    if (job && job.task && job.task.op === "add") {
      stats.completedAddJobs += 1;
    } else if (job && job.task && job.task.op === "run_js") {
      stats.completedRunJsJobs += 1;
    }

    if (job && job.parentJobId) {
      stats.completedShards += 1;
      const units = Number(job.shardContextBase && job.shardContextBase.units);
      if (Number.isFinite(units) && units > 0) {
        stats.contributedUnits += units;
      }
    }

    const durationMs = Number(
      normalizedResult && typeof normalizedResult === "object" ? normalizedResult.durationMs : Number.NaN,
    );
    if (Number.isFinite(durationMs) && durationMs >= 0) {
      stats.totalDurationMs += durationMs;
    } else if (Number.isFinite(job && job.assignedAtMs ? job.assignedAtMs : Number.NaN)) {
      stats.totalDurationMs += Math.max(0, nowMs - job.assignedAtMs);
    }

    if (job && job.parentJobId) {
      const units = Number(job.shardContextBase && job.shardContextBase.units);
      const measuredDurationMs = Number.isFinite(durationMs) && durationMs > 0 ? durationMs : Number(nowMs - (job.assignedAtMs || nowMs));
      if (Number.isFinite(units) && units > 0 && Number.isFinite(measuredDurationMs) && measuredDurationMs > 0) {
        const throughput = units / (measuredDurationMs / 1000);
        if (stats.throughputUnitsPerSecEma > 0) {
          stats.throughputUnitsPerSecEma = stats.throughputUnitsPerSecEma * 0.7 + throughput * 0.3;
        } else {
          stats.throughputUnitsPerSecEma = throughput;
        }
        stats.throughputSamples += 1;

        const parentPerf = stats.parentThroughput || {};
        const previous = parentPerf[job.parentJobId];
        if (previous && Number.isFinite(previous.ema) && previous.ema > 0) {
          previous.ema = previous.ema * 0.65 + throughput * 0.35;
          previous.updatedAt = nowMs;
        } else {
          parentPerf[job.parentJobId] = { ema: throughput, updatedAt: nowMs };
        }

        const parentEntries = Object.entries(parentPerf);
        if (parentEntries.length > 24) {
          parentEntries
            .sort((a, b) => (b[1].updatedAt || 0) - (a[1].updatedAt || 0))
            .slice(24)
            .forEach(([key]) => {
              delete parentPerf[key];
            });
        }
        stats.parentThroughput = parentPerf;
      }
    }
  }

  function recordWorkerFailure(workerId, job) {
    const stats = ensureWorkerStats(workerId);
    stats.lastSeenAt = Date.now();
    stats.failedJobs += 1;
    if (job && job.parentJobId) {
      stats.failedShards += 1;
    }
  }

  function createJob(task, createOptions = {}) {
    const jobId = createOptions.jobId || randomUUID();
    const job = {
      jobId,
      task,
      status: createOptions.status || "queued",
      controlState: createOptions.controlState || "running",
      result: null,
      error: null,
      assignedWorkerId: null,
      assignedAtMs: null,
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

  function removeFromQueue(jobId) {
    for (let i = queue.length - 1; i >= 0; i -= 1) {
      if (queue[i] === jobId) {
        queue.splice(i, 1);
      }
    }
  }

  function createShardedRunJsJob(task) {
    const shardConfig = task.shardConfig;
    const reducer = task.reducer;
    const totalShards = Math.ceil(shardConfig.totalUnits / shardConfig.unitsPerShard);
    const parentJob = createJob(task, {
      skipQueue: true,
      status: "running",
      controlState: "running",
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

    appendJobEvent(parentJob.jobId, {
      type: "job_started",
      jobId: parentJob.jobId,
      executionModel: "sharded",
      totalShards,
      shardConfig,
      reducer,
      args: task.args || {},
    });

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
    if (queue.length === 0) {
      return null;
    }

    const initialLen = queue.length;
    for (let i = 0; i < initialLen; i += 1) {
      const jobId = queue.shift();
      const job = jobs.get(jobId);
      if (!job || job.status !== "queued") {
        continue;
      }

      if (job.parentJobId) {
        const parent = jobs.get(job.parentJobId);
        if (!parent || parent.status === "failed" || parent.status === "done" || parent.controlState === "cancelled") {
          job.status = "failed";
          job.error = "parent_inactive";
          touchJob(job);
          continue;
        }
        if (parent.controlState === "paused") {
          queue.push(jobId);
          continue;
        }
      }

      return job;
    }
    return null;
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

  function cancelAssignedJob(job, reason) {
    if (!job) {
      return false;
    }
    const assignedWorkerId = assignments.get(job.jobId);
    if (!assignedWorkerId) {
      return false;
    }
    assignments.delete(job.jobId);
    const worker = workers.get(assignedWorkerId);
    if (worker) {
      worker.inFlightCount = Math.max(0, (worker.inFlightCount || 0) - 1);
      worker.lastSeenAt = Date.now();
      refreshWorkerState(worker);
    }
    job.status = "failed";
    job.error = reason || "cancelled";
    job.assignedWorkerId = null;
    job.assignedAtMs = null;
    touchJob(job);
    return true;
  }

  function pauseShardedJob(jobId) {
    const job = jobs.get(jobId);
    if (!job || job.hidden) {
      return { ok: false, reason: "job_not_found" };
    }
    if ((job.executionModel || "single") !== "sharded") {
      return { ok: false, reason: "not_sharded" };
    }
    if (job.status === "done" || job.status === "failed") {
      return { ok: false, reason: "terminal" };
    }
    job.controlState = "paused";
    touchJob(job);
    appendJobEvent(job.jobId, {
      type: "job_paused",
      jobId: job.jobId,
      completedShards: Number(job.completedShards || 0),
      totalShards: Number(job.totalShards || 0),
    });
    return { ok: true, jobId: job.jobId, controlState: job.controlState };
  }

  function resumeShardedJob(jobId) {
    const job = jobs.get(jobId);
    if (!job || job.hidden) {
      return { ok: false, reason: "job_not_found" };
    }
    if ((job.executionModel || "single") !== "sharded") {
      return { ok: false, reason: "not_sharded" };
    }
    if (job.status === "done" || job.status === "failed") {
      return { ok: false, reason: "terminal" };
    }
    job.controlState = "running";
    touchJob(job);
    appendJobEvent(job.jobId, {
      type: "job_resumed",
      jobId: job.jobId,
      completedShards: Number(job.completedShards || 0),
      totalShards: Number(job.totalShards || 0),
    });
    return { ok: true, jobId: job.jobId, controlState: job.controlState };
  }

  function cancelJob(jobId, reason = "cancelled") {
    const job = jobs.get(jobId);
    if (!job || job.hidden) {
      return { ok: false, reason: "job_not_found" };
    }
    if (job.status === "done" || job.status === "failed") {
      return { ok: true, alreadyTerminal: true };
    }

    removeFromQueue(jobId);
    cancelAssignedJob(job, reason);
    job.status = "failed";
    job.error = reason;
    job.controlState = "cancelled";
    touchJob(job);

    if ((job.executionModel || "single") === "sharded" && Array.isArray(job.childJobIds)) {
      for (const childJobId of job.childJobIds) {
        const child = jobs.get(childJobId);
        if (!child) {
          continue;
        }
        removeFromQueue(childJobId);
        cancelAssignedJob(child, "parent_cancelled");
        if (child.status !== "done" && child.status !== "failed") {
          child.status = "failed";
          child.error = "parent_cancelled";
          touchJob(child);
        }
      }
      appendJobEvent(job.jobId, {
        type: "job_cancelled",
        jobId: job.jobId,
        completedShards: Number(job.completedShards || 0),
        failedShards: Number(job.failedShards || 0),
        totalShards: Number(job.totalShards || 0),
        reason,
      });
    }

    return { ok: true };
  }

  function stopAllJobs(reason = "stopped_by_client") {
    const snapshot = Array.from(jobs.values()).filter((job) => !job.hidden);
    let stopped = 0;
    for (const job of snapshot) {
      if (job.status === "done" || job.status === "failed") {
        continue;
      }
      const stoppedResult = cancelJob(job.jobId, reason);
      if (stoppedResult.ok) {
        stopped += 1;
      }
    }
    return { ok: true, stopped };
  }

  function registerWorker(workerId, ws, capabilities = null, features = null) {
    const existing = workers.get(workerId);
    if (existing && existing.ws && existing.ws !== ws) {
      socketToWorker.delete(existing.ws);
      try {
        existing.ws.close(1012, "Replaced by a new connection");
      } catch (_err) {
        // Ignore close errors.
      }
    }

    const normalizedCapabilities = normalizeWorkerCapabilities(capabilities);
    const staticResourceScore = computeStaticResourceScore(normalizedCapabilities);
    const maxSlots = deriveWorkerSlotCapacity(normalizedCapabilities);

    workers.set(workerId, {
      workerId,
      ws,
      state: "idle",
      lastSeenAt: Date.now(),
      lastAssignedAt: 0,
      inFlightCount: 0,
      maxSlots,
      supportsAssignBatch: Boolean(features && features.assignBatch),
      capabilities: normalizedCapabilities,
      staticResourceScore,
      dispatchScore: staticResourceScore,
    });
    refreshWorkerState(workers.get(workerId));
    const stats = markWorkerConnected(workerId);
    if (normalizedCapabilities) {
      stats.capabilities = normalizedCapabilities;
    }
    stats.resourceScore = staticResourceScore;
    socketToWorker.set(ws, workerId);
  }

  function workerExists(workerId) {
    return workers.has(workerId);
  }

  function getWorker(workerId) {
    return workers.get(workerId) || null;
  }

  function getWorkerThroughputForJob(stats, job) {
    if (!stats) {
      return 0;
    }
    if (job && job.parentJobId && stats.parentThroughput && stats.parentThroughput[job.parentJobId]) {
      const perParent = Number(stats.parentThroughput[job.parentJobId].ema);
      if (Number.isFinite(perParent) && perParent > 0) {
        return perParent;
      }
    }
    const generic = Number(stats.throughputUnitsPerSecEma);
    if (Number.isFinite(generic) && generic > 0) {
      return generic;
    }
    return 0;
  }

  function getFirstIdleWorker(job = null) {
    const idleWorkers = [];
    for (const worker of workers.values()) {
      if (getWorkerAvailableSlots(worker) > 0) {
        idleWorkers.push(worker);
      }
    }
    if (idleWorkers.length === 0) {
      return null;
    }

    const nowMs = Date.now();
    const throughputs = idleWorkers
      .map((worker) => getWorkerThroughputForJob(ensureWorkerStats(worker.workerId), job))
      .filter((value) => Number.isFinite(value) && value > 0)
      .sort((a, b) => a - b);
    const medianThroughput =
      throughputs.length > 0 ? throughputs[Math.floor(throughputs.length / 2)] : 0;

    let bestWorker = null;
    let bestScore = -1;
    for (const worker of idleWorkers) {
      const stats = ensureWorkerStats(worker.workerId);
      const staticScore = Number(worker.staticResourceScore || stats.resourceScore || 1);
      const throughput = getWorkerThroughputForJob(stats, job);
      const perfFactor =
        medianThroughput > 0 && throughput > 0
          ? clampNumber(throughput / medianThroughput, 0.45, 3.2, 1)
          : 1;
      const failureRate = (stats.failedShards || 0) / ((stats.completedShards || 0) + 5);
      const reconnectRate = (stats.reconnects || 0) / ((stats.completedJobs || 0) + 10);
      const reliabilityFactor = 1 / (1 + failureRate * 2 + reconnectRate);
      const idleMs = Math.max(0, nowMs - (worker.lastAssignedAt || 0));
      const idleBoost = 1 + Math.min(idleMs / 20000, 0.35);
      const capacityBoost = 1 + Math.min(Math.max(getWorkerAvailableSlots(worker) - 1, 0) * 0.12, 0.9);
      const dispatchScore = staticScore * perfFactor * reliabilityFactor * idleBoost * capacityBoost;

      worker.dispatchScore = dispatchScore;
      stats.dispatchScore = dispatchScore;
      if (dispatchScore > bestScore) {
        bestScore = dispatchScore;
        bestWorker = worker;
      }
    }
    return bestWorker;
  }

  function markWorkerReady(workerId) {
    const worker = workers.get(workerId);
    if (!worker) {
      return false;
    }
    worker.lastSeenAt = Date.now();
    refreshWorkerState(worker);
    recordWorkerSeen(workerId);
    return true;
  }

  function markWorkerSeen(workerId) {
    const worker = workers.get(workerId);
    if (!worker) {
      return false;
    }
    worker.lastSeenAt = Date.now();
    recordWorkerSeen(workerId);
    return true;
  }

  function assignJob(job, workerId) {
    const worker = workers.get(workerId);
    if (!worker || getWorkerAvailableSlots(worker) <= 0) {
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
    job.assignedAtMs = Date.now();
    job.attempt += 1;
    touchJob(job);
    worker.inFlightCount += 1;
    worker.lastSeenAt = Date.now();
    worker.lastAssignedAt = Date.now();
    refreshWorkerState(worker);
    assignments.set(job.jobId, workerId);
    return true;
  }

  function failParentAndStopChildren(parentJob, reason) {
    const wasTerminal = parentJob.status === "failed" || parentJob.status === "done";
    parentJob.status = "failed";
    parentJob.error = reason;
    touchJob(parentJob);
    if (!wasTerminal) {
      appendJobEvent(parentJob.jobId, {
        type: "job_failed",
        jobId: parentJob.jobId,
        reason,
        completedShards: Number(parentJob.completedShards || 0),
        failedShards: Number(parentJob.failedShards || 0),
        totalShards: Number(parentJob.totalShards || 0),
      });
    }

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
    if (job.status === "failed" || job.controlState === "cancelled") {
      worker.inFlightCount = Math.max(0, (worker.inFlightCount || 0) - 1);
      worker.lastSeenAt = Date.now();
      refreshWorkerState(worker);
      return { ok: true, ignored: true };
    }
    job.status = "done";
    job.result = normalizedResult;
    job.error = null;
    job.assignedWorkerId = workerId;
    job.assignedAtMs = null;
    touchJob(job);

    worker.inFlightCount = Math.max(0, (worker.inFlightCount || 0) - 1);
    worker.lastSeenAt = Date.now();
    refreshWorkerState(worker);
    recordWorkerCompletion(workerId, job, normalizedResult);

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
      appendJobEvent(parentJob.jobId, {
        type: "shard_done",
        parentJobId: parentJob.jobId,
        childJobId: job.jobId,
        workerId,
        shardIndex: Number.isInteger(job.shardIndex) ? job.shardIndex : null,
        totalShards: Number(parentJob.totalShards || 0),
        completedShards: Number(parentJob.completedShards || 0),
        failedShards: Number(parentJob.failedShards || 0),
        shardContext: job.shardContextBase || null,
        result: normalizeResultForLiveEvent(normalizedResult),
      });
      if (parentJob.completedShards >= parentJob.totalShards) {
        const finalResult = {
          reducer: parentJob.reducer,
          aggregate: parentJob.aggregate,
          completedShards: parentJob.completedShards,
          totalShards: parentJob.totalShards,
        };
        if (
          parentJob.reducer &&
          parentJob.reducer.type === "sum" &&
          parentJob.aggregate &&
          typeof parentJob.aggregate === "object" &&
          !Array.isArray(parentJob.aggregate)
        ) {
          const hits = Number(parentJob.aggregate.hits);
          const samples = Number(parentJob.aggregate.samples);
          if (Number.isFinite(hits) && Number.isFinite(samples) && samples > 0) {
            finalResult.derived = {
              piEstimate: (4 * hits) / samples,
              hitRatio: hits / samples,
            };
          }
        }
        parentJob.status = "done";
        parentJob.result = finalResult;
        parentJob.error = null;
        appendJobEvent(parentJob.jobId, {
          type: "job_done",
          jobId: parentJob.jobId,
          completedShards: Number(parentJob.completedShards || 0),
          failedShards: Number(parentJob.failedShards || 0),
          totalShards: Number(parentJob.totalShards || 0),
          result: finalResult,
        });
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
    if (job.status === "failed" || job.controlState === "cancelled") {
      worker.inFlightCount = Math.max(0, (worker.inFlightCount || 0) - 1);
      worker.lastSeenAt = Date.now();
      refreshWorkerState(worker);
      return { ok: true, ignored: true };
    }
    worker.inFlightCount = Math.max(0, (worker.inFlightCount || 0) - 1);
    worker.lastSeenAt = Date.now();
    refreshWorkerState(worker);

    if (job.parentJobId && job.status === "running" && job.attempt < job.maxAttempts) {
      job.status = "queued";
      job.error = `retryable:${error}`;
      job.assignedWorkerId = null;
      job.assignedAtMs = null;
      touchJob(job);
      requeueJob(jobId, true);
      return { ok: true, requeued: true };
    }

    job.status = "failed";
    job.error = error;
    job.assignedWorkerId = workerId;
    job.assignedAtMs = null;
    touchJob(job);
    recordWorkerFailure(workerId, job);

    if (job.parentJobId) {
      const parentJob = jobs.get(job.parentJobId);
      if (parentJob && parentJob.status !== "failed" && parentJob.status !== "done") {
        parentJob.failedShards += 1;
        appendJobEvent(parentJob.jobId, {
          type: "shard_failed",
          parentJobId: parentJob.jobId,
          childJobId: job.jobId,
          workerId,
          shardIndex: Number.isInteger(job.shardIndex) ? job.shardIndex : null,
          totalShards: Number(parentJob.totalShards || 0),
          completedShards: Number(parentJob.completedShards || 0),
          failedShards: Number(parentJob.failedShards || 0),
          error,
        });
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
    }

    workers.delete(workerId);
    markWorkerDisconnected(workerId);
    return { workerId, requeuedJobId };
  }

  function listWorkers() {
    const now = Date.now();
    return Array.from(workers.values()).map((worker) => ({
      workerId: worker.workerId,
      state: worker.state,
      lastSeenMs: Math.max(0, now - worker.lastSeenAt),
      inFlightCount: Number(worker.inFlightCount || 0),
      maxSlots: Number(worker.maxSlots || 1),
      availableSlots: getWorkerAvailableSlots(worker),
      supportsAssignBatch: Boolean(worker.supportsAssignBatch),
      dispatchScore: Number(worker.dispatchScore || 1),
      hardwareConcurrency: worker.capabilities ? worker.capabilities.hardwareConcurrency : null,
      deviceMemoryGB: worker.capabilities ? worker.capabilities.deviceMemoryGB : null,
      effectiveType: worker.capabilities ? worker.capabilities.effectiveType : null,
    }));
  }

  function getWorkerMetricsSnapshot(targetWorkerId = null) {
    const nowMs = Date.now();
    const allWorkerIds = new Set([...workerStats.keys(), ...workers.keys()]);
    const rows = [];

    for (const workerId of allWorkerIds) {
      const stats = ensureWorkerStats(workerId);
      const connectedWorker = workers.get(workerId);
      const connected = Boolean(connectedWorker);
      const completedJobs = stats.completedJobs || 0;
      const avgDurationMs = completedJobs > 0 ? stats.totalDurationMs / completedJobs : 0;

      rows.push({
        workerId,
        connected,
        state: connectedWorker ? connectedWorker.state : "offline",
        lastSeenMs: Math.max(0, nowMs - stats.lastSeenAt),
        inFlightCount: Number(connectedWorker ? connectedWorker.inFlightCount || 0 : 0),
        maxSlots: Number(connectedWorker ? connectedWorker.maxSlots || 1 : 1),
        availableSlots: Number(connectedWorker ? getWorkerAvailableSlots(connectedWorker) : 0),
        supportsAssignBatch: Boolean(connectedWorker ? connectedWorker.supportsAssignBatch : false),
        dispatchScore: Number(
          connectedWorker && Number.isFinite(connectedWorker.dispatchScore)
            ? connectedWorker.dispatchScore
            : stats.dispatchScore || 1,
        ),
        resourceScore: Number(stats.resourceScore || 1),
        throughputUnitsPerSec: Number(stats.throughputUnitsPerSecEma || 0),
        reconnects: stats.reconnects,
        completedJobs,
        completedAddJobs: stats.completedAddJobs,
        completedRunJsJobs: stats.completedRunJsJobs,
        completedShards: stats.completedShards,
        failedJobs: stats.failedJobs,
        failedShards: stats.failedShards,
        contributedUnits: stats.contributedUnits,
        totalDurationMs: stats.totalDurationMs,
        avgDurationMs,
        capabilities: stats.capabilities || null,
        firstSeenAt: stats.firstSeenAt ? new Date(stats.firstSeenAt).toISOString() : null,
        lastJobAt: stats.lastJobAt ? new Date(stats.lastJobAt).toISOString() : null,
      });
    }

    rows.sort((a, b) => {
      if (b.completedShards !== a.completedShards) {
        return b.completedShards - a.completedShards;
      }
      if (b.contributedUnits !== a.contributedUnits) {
        return b.contributedUnits - a.contributedUnits;
      }
      return b.completedJobs - a.completedJobs;
    });

    const totals = rows.reduce(
      (acc, row) => {
        acc.workersTracked += 1;
        acc.workersConnected += row.connected ? 1 : 0;
        acc.completedJobs += row.completedJobs;
        acc.completedShards += row.completedShards;
        acc.failedJobs += row.failedJobs;
        acc.failedShards += row.failedShards;
        acc.contributedUnits += row.contributedUnits;
        acc.totalDurationMs += row.totalDurationMs;
        return acc;
      },
      {
        workersTracked: 0,
        workersConnected: 0,
        completedJobs: 0,
        completedShards: 0,
        failedJobs: 0,
        failedShards: 0,
        contributedUnits: 0,
        totalDurationMs: 0,
      },
    );

    const self = targetWorkerId ? rows.find((row) => row.workerId === targetWorkerId) || null : null;

    return {
      now: new Date(nowMs).toISOString(),
      self,
      workers: rows,
      totals,
    };
  }

  return {
    MIN_VALUE,
    MAX_VALUE,
    isValidInputNumber,
    createJob,
    createShardedRunJsJob,
    getJob,
    pauseShardedJob,
    resumeShardedJob,
    cancelJob,
    stopAllJobs,
    dequeueQueuedJob,
    requeueJob,
    registerWorker,
    updateWorkerCapabilities,
    workerExists,
    getWorker,
    getFirstIdleWorker,
    getWorkerAvailableSlots: (workerId) => getWorkerAvailableSlots(workers.get(workerId)),
    markWorkerReady,
    markWorkerSeen,
    assignJob,
    finishJob,
    failJob,
    removeWorkerSocket,
    listWorkers,
    getWorkerMetricsSnapshot,
    getJobEvents,
    maxCustomResultBytes,
  };
}

module.exports = {
  createStore,
  isValidInputNumber,
  MIN_VALUE,
  MAX_VALUE,
};
