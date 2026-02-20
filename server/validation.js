const MAX_JOB_LOOKUP_ID_LENGTH = 128;

function isPlainObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isSafeString(value, maxLength = 256) {
  return typeof value === "string" && value.length > 0 && value.length <= maxLength;
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
      if (!isSafeString(jobId, MAX_JOB_LOOKUP_ID_LENGTH) || !Number.isInteger(result)) {
        return { ok: false, error: "Invalid result payload." };
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
  parseInviteRequestBody,
  parseWorkerMessage,
  isValidWorkerId,
};
