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
  if (!Number.isInteger(timeoutMs)) {
    return { ok: false, error: "timeoutMs must be an integer." };
  }
  if (timeoutMs < limits.minTimeoutMs || timeoutMs > limits.maxTimeoutMs) {
    return {
      ok: false,
      error: `timeoutMs must be in [${limits.minTimeoutMs}, ${limits.maxTimeoutMs}].`,
    };
  }

  return {
    ok: true,
    value: {
      op: "run_js",
      code: body.code,
      args: JSON.parse(argsJson),
      timeoutMs,
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

function parseWorkerMessage(message) {
  if (!isPlainObject(message) || !isSafeString(message.type, 32)) {
    return { ok: false, error: "Malformed message." };
  }

  const { type, workerId, jobId, result, error } = message;
  switch (type) {
    case "register":
      if (!isValidWorkerId(workerId)) {
        return { ok: false, error: "Invalid workerId." };
      }
      return { ok: true, type, workerId };
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
