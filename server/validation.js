const MAX_JOB_LOOKUP_ID_LENGTH = 128;

function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSafeString(value, maxLength = 256) {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength;
}

function utf8ByteLength(value) {
  return Buffer.byteLength(value, "utf8");
}

function parseJobCreateBody(body, isValidInputNumber) {
  if (!isPlainObject(body)) {
    return { ok: false, error: "Body must be a JSON object." };
  }
  const { a, b } = body;
  if (!isValidInputNumber(a) || !isValidInputNumber(b)) {
    return { ok: false, error: "Fields 'a' and 'b' must be bounded integers." };
  }
  return { ok: true, value: { op: "add", a, b } };
}

function parseRunJsCreateBody(body, limits) {
  if (!isPlainObject(body)) {
    return { ok: false, error: "Body must be a JSON object." };
  }

  if (!isSafeString(body.code, limits.maxCodeBytes)) {
    return { ok: false, error: "code must be a non-empty string." };
  }
  if (utf8ByteLength(body.code) > limits.maxCodeBytes) {
    return { ok: false, error: `code exceeds max size (${limits.maxCodeBytes} bytes).` };
  }

  let args = body.args;
  if (args === undefined) {
    args = {};
  }

  let argsJson = "";
  try {
    argsJson = JSON.stringify(args);
  } catch (_error) {
    return { ok: false, error: "args must be JSON-serializable." };
  }
  if (argsJson === undefined) {
    return { ok: false, error: "args must be a valid JSON value." };
  }
  if (utf8ByteLength(argsJson) > limits.maxArgsBytes) {
    return { ok: false, error: `args exceeds max size (${limits.maxArgsBytes} bytes).` };
  }

  const timeoutMs = body.timeoutMs === undefined ? limits.defaultTimeoutMs : Number(body.timeoutMs);
  if (!Number.isSafeInteger(timeoutMs)) {
    return { ok: false, error: "timeoutMs must be a safe integer." };
  }
  if (timeoutMs < 0) {
    return { ok: false, error: "timeoutMs must be >= 0." };
  }

  const executionModelRaw = body.executionModel === undefined ? "single" : String(body.executionModel).trim().toLowerCase();
  if (!["single", "sharded"].includes(executionModelRaw)) {
    return { ok: false, error: "executionModel must be 'single' or 'sharded'." };
  }

  let shardConfig = null;
  let reducer = null;
  if (executionModelRaw === "sharded") {
    if (!isPlainObject(body.shardConfig)) {
      return { ok: false, error: "shardConfig is required for executionModel='sharded'." };
    }
    const totalUnits = Number(body.shardConfig.totalUnits);
    const unitsPerShard = Number(body.shardConfig.unitsPerShard);
    if (!Number.isInteger(totalUnits) || totalUnits <= 0) {
      return { ok: false, error: "shardConfig.totalUnits must be a positive integer." };
    }
    if (!Number.isInteger(unitsPerShard) || unitsPerShard <= 0) {
      return { ok: false, error: "shardConfig.unitsPerShard must be a positive integer." };
    }
    const totalShards = Math.ceil(totalUnits / unitsPerShard);
    shardConfig = {
      totalUnits,
      unitsPerShard,
      totalShards,
    };

    const reducerRaw = isPlainObject(body.reducer) ? body.reducer : {};
    const reducerType = String(reducerRaw.type || "sum").trim().toLowerCase();
    if (!["sum", "collect", "min", "max"].includes(reducerType)) {
      return { ok: false, error: "reducer.type must be one of: sum, collect, min, max." };
    }
    if (reducerType === "sum") {
      const fields = Array.isArray(reducerRaw.fields) ? reducerRaw.fields.map((v) => String(v).trim()).filter(Boolean) : ["hits", "samples"];
      if (fields.length === 0) {
        return { ok: false, error: "sum reducer requires a non-empty fields array." };
      }
      reducer = { type: "sum", fields };
    } else if (reducerType === "collect") {
      reducer = { type: "collect" };
    } else {
      const field = String(reducerRaw.field || "value").trim();
      if (!field) {
        return { ok: false, error: `${reducerType} reducer requires reducer.field.` };
      }
      reducer = { type: reducerType, field };
    }
  }

  return {
    ok: true,
    value: {
      op: "run_js",
      code: body.code,
      args: JSON.parse(argsJson),
      timeoutMs,
      executionModel: executionModelRaw,
      shardConfig,
      reducer,
    },
  };
}

function parseInviteRequestBody(body) {
  if (body === undefined) {
    return { ok: true, value: {} };
  }
  if (!isPlainObject(body)) {
    return { ok: false, error: "Body must be a JSON object." };
  }
  const value = {};
  if (body.ttlSec !== undefined) {
    if (!Number.isInteger(body.ttlSec) || body.ttlSec <= 0) {
      return { ok: false, error: "ttlSec must be a positive integer." };
    }
    value.ttlSec = body.ttlSec;
  }
  if (body.workerLabel !== undefined) {
    if (!isSafeString(body.workerLabel, 64)) {
      return { ok: false, error: "workerLabel must be a non-empty string up to 64 chars." };
    }
    value.workerLabel = body.workerLabel;
  }
  return { ok: true, value };
}

function isValidWorkerId(workerId) {
  return (
    typeof workerId === "string" &&
    workerId.length > 0 &&
    workerId.length <= 128 &&
    /^[a-zA-Z0-9._-]+$/.test(workerId)
  );
}

function sanitizeWorkerCapabilities(value) {
  if (value === undefined) {
    return { ok: true, value: null };
  }
  if (!isPlainObject(value)) {
    return { ok: false, error: "capabilities must be an object." };
  }

  const connectionRaw = isPlainObject(value.connection) ? value.connection : {};
  const batteryRaw = isPlainObject(value.battery) ? value.battery : {};
  const output = {};

  if (value.hardwareConcurrency !== undefined) {
    const hc = Number(value.hardwareConcurrency);
    if (!Number.isFinite(hc) || hc <= 0) {
      return { ok: false, error: "capabilities.hardwareConcurrency must be a positive number." };
    }
    output.hardwareConcurrency = hc;
  }
  if (value.deviceMemoryGB !== undefined) {
    const mem = Number(value.deviceMemoryGB);
    if (!Number.isFinite(mem) || mem <= 0) {
      return { ok: false, error: "capabilities.deviceMemoryGB must be a positive number." };
    }
    output.deviceMemoryGB = mem;
  }
  if (value.platform !== undefined) {
    if (!isSafeString(String(value.platform), 120)) {
      return { ok: false, error: "capabilities.platform must be a non-empty string up to 120 chars." };
    }
    output.platform = String(value.platform);
  }
  if (value.visibility !== undefined) {
    if (!isSafeString(String(value.visibility), 24)) {
      return { ok: false, error: "capabilities.visibility must be a non-empty string up to 24 chars." };
    }
    output.visibility = String(value.visibility);
  }

  const connection = {};
  if (connectionRaw.effectiveType !== undefined) {
    if (!isSafeString(String(connectionRaw.effectiveType), 16)) {
      return { ok: false, error: "capabilities.connection.effectiveType must be a short string." };
    }
    connection.effectiveType = String(connectionRaw.effectiveType);
  }
  if (connectionRaw.downlinkMbps !== undefined) {
    const downlink = Number(connectionRaw.downlinkMbps);
    if (!Number.isFinite(downlink) || downlink < 0) {
      return { ok: false, error: "capabilities.connection.downlinkMbps must be >= 0." };
    }
    connection.downlinkMbps = downlink;
  }
  if (connectionRaw.rttMs !== undefined) {
    const rtt = Number(connectionRaw.rttMs);
    if (!Number.isFinite(rtt) || rtt < 0) {
      return { ok: false, error: "capabilities.connection.rttMs must be >= 0." };
    }
    connection.rttMs = rtt;
  }
  if (connectionRaw.saveData !== undefined) {
    connection.saveData = Boolean(connectionRaw.saveData);
  }
  if (Object.keys(connection).length > 0) {
    output.connection = connection;
  }

  const battery = {};
  if (batteryRaw.level !== undefined) {
    const level = Number(batteryRaw.level);
    if (!Number.isFinite(level) || level < 0 || level > 1) {
      return { ok: false, error: "capabilities.battery.level must be in [0,1]." };
    }
    battery.level = level;
  }
  if (batteryRaw.charging !== undefined) {
    battery.charging = Boolean(batteryRaw.charging);
  }
  if (Object.keys(battery).length > 0) {
    output.battery = battery;
  }

  return { ok: true, value: Object.keys(output).length > 0 ? output : null };
}

function sanitizeWorkerFeatures(value) {
  if (value === undefined) {
    return { ok: true, value: null };
  }
  if (!isPlainObject(value)) {
    return { ok: false, error: "features must be an object." };
  }
  const output = {};
  if (value.assignBatch !== undefined) {
    output.assignBatch = Boolean(value.assignBatch);
  }
  return { ok: true, value: Object.keys(output).length > 0 ? output : null };
}

function parseWorkerMessage(message) {
  if (!isPlainObject(message) || !isSafeString(message.type, 32)) {
    return { ok: false, error: "Malformed message." };
  }

  const { type, workerId, jobId, result, error, capabilities, features } = message;
  switch (type) {
    case "register":
      if (!isValidWorkerId(workerId)) {
        return { ok: false, error: "Invalid workerId." };
      }
      {
        const parsedCapabilities = sanitizeWorkerCapabilities(capabilities);
        if (!parsedCapabilities.ok) {
          return { ok: false, error: parsedCapabilities.error };
        }
        const parsedFeatures = sanitizeWorkerFeatures(features);
        if (!parsedFeatures.ok) {
          return { ok: false, error: parsedFeatures.error };
        }
        return {
          ok: true,
          type,
          workerId,
          capabilities: parsedCapabilities.value,
          features: parsedFeatures.value,
        };
      }
    case "heartbeat":
      if (!isValidWorkerId(workerId)) {
        return { ok: false, error: "Invalid workerId." };
      }
      {
        const parsedCapabilities = sanitizeWorkerCapabilities(capabilities);
        if (!parsedCapabilities.ok) {
          return { ok: false, error: parsedCapabilities.error };
        }
        return { ok: true, type, workerId, capabilities: parsedCapabilities.value };
      }
    case "ready":
      if (!isValidWorkerId(workerId)) {
        return { ok: false, error: "Invalid workerId." };
      }
      return { ok: true, type, workerId };
    case "result":
      if (!isValidWorkerId(workerId)) {
        return { ok: false, error: "Invalid workerId." };
      }
      if (!isSafeString(jobId, MAX_JOB_LOOKUP_ID_LENGTH)) {
        return { ok: false, error: "Invalid result payload." };
      }
      if (!Object.prototype.hasOwnProperty.call(message, "result")) {
        return { ok: false, error: "Missing result payload." };
      }
      return { ok: true, type, workerId, jobId, result };
    case "error":
      if (!isValidWorkerId(workerId)) {
        return { ok: false, error: "Invalid workerId." };
      }
      if (!isSafeString(jobId, MAX_JOB_LOOKUP_ID_LENGTH)) {
        return { ok: false, error: "Invalid error payload." };
      }
      return {
        ok: true,
        type,
        workerId,
        jobId,
        error: typeof error === "string" && error.length <= 256 ? error : "Worker error",
      };
    default:
      return { ok: false, error: "Unsupported message type." };
  }
}

module.exports = {
  parseJobCreateBody,
  parseRunJsCreateBody,
  parseInviteRequestBody,
  parseWorkerMessage,
  isValidWorkerId,
};
