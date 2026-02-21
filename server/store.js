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

  const jobs = new Map();
  const queue = [];
  const workers = new Map();
  const workerStats = new Map();
  const socketToWorker = new Map();
  const assignments = new Map();

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

  function nowIso() {
    return new Date().toISOString();
  }

  function touchJob(job) {
    job.updatedAt = nowIso();
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

  function registerWorker(workerId, ws, capabilities = null) {
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

    workers.set(workerId, {
      workerId,
      ws,
      state: "idle",
      lastSeenAt: Date.now(),
      lastAssignedAt: 0,
      capabilities: normalizedCapabilities,
      staticResourceScore,
      dispatchScore: staticResourceScore,
    });
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
      if (worker.state === "idle") {
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
      const dispatchScore = staticScore * perfFactor * reliabilityFactor * idleBoost;

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
    worker.state = "idle";
    worker.lastSeenAt = Date.now();
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
    job.assignedAtMs = Date.now();
    job.attempt += 1;
    touchJob(job);
    worker.state = "busy";
    worker.lastSeenAt = Date.now();
    worker.lastAssignedAt = Date.now();
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
    job.assignedAtMs = null;
    touchJob(job);

    worker.state = "idle";
    worker.lastSeenAt = Date.now();
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
    markWorkerDisconnected(workerId);
    return { workerId, requeuedJobId };
  }

  function listWorkers() {
    const now = Date.now();
    return Array.from(workers.values()).map((worker) => ({
      workerId: worker.workerId,
      state: worker.state,
      lastSeenMs: Math.max(0, now - worker.lastSeenAt),
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
    dequeueQueuedJob,
    requeueJob,
    registerWorker,
    updateWorkerCapabilities,
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
    getWorkerMetricsSnapshot,
    maxCustomResultBytes,
  };
}

module.exports = {
  createStore,
  isValidInputNumber,
  MIN_VALUE,
  MAX_VALUE,
};
