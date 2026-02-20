const { createHmac, randomBytes, randomUUID, timingSafeEqual } = require("node:crypto");

const DEFAULT_INVITE_TTL_SEC = 3600;
const MIN_INVITE_TTL_SEC = 60;
const MAX_INVITE_TTL_SEC = 86400;

function toBase64Url(value) {
  return Buffer.from(value)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function fromBase64Url(value) {
  let base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  while (base64.length % 4 !== 0) {
    base64 += "=";
  }
  return Buffer.from(base64, "base64");
}

function safeEqualText(a, b) {
  if (typeof a !== "string" || typeof b !== "string") {
    return false;
  }
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) {
    return false;
  }
  return timingSafeEqual(bufA, bufB);
}

function buildSigner(secret) {
  return (payloadPart) => toBase64Url(createHmac("sha256", secret).update(payloadPart).digest());
}

function parsePositiveInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n <= 0) {
    return fallback;
  }
  return n;
}

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function resolveConfig() {
  const env = process.env.NODE_ENV || "development";

  let clientApiKey = process.env.CLIENT_API_KEY;
  if (!clientApiKey) {
    if (env === "production") {
      throw new Error("CLIENT_API_KEY is required in production.");
    }
    clientApiKey = `dev-client-${randomBytes(12).toString("hex")}`;
  }

  let workerInviteSecret = process.env.WORKER_INVITE_SECRET;
  if (!workerInviteSecret) {
    if (env === "production") {
      throw new Error("WORKER_INVITE_SECRET is required in production.");
    }
    workerInviteSecret = `dev-invite-${randomBytes(24).toString("hex")}`;
  }

  const inviteTtlSecRaw = parsePositiveInt(process.env.WORKER_INVITE_TTL_SEC, DEFAULT_INVITE_TTL_SEC);
  const inviteTtlSec = clamp(inviteTtlSecRaw, MIN_INVITE_TTL_SEC, MAX_INVITE_TTL_SEC);

  return {
    env,
    clientApiKey,
    workerInviteSecret,
    inviteTtlSec,
  };
}

function extractClientToken(req) {
  const authHeader = req.headers.authorization;
  if (typeof authHeader === "string" && authHeader.toLowerCase().startsWith("bearer ")) {
    return authHeader.slice(7).trim();
  }
  const xClientToken = req.headers["x-client-token"];
  if (typeof xClientToken === "string") {
    return xClientToken.trim();
  }
  return "";
}

function createAuth(config) {
  const sign = buildSigner(config.workerInviteSecret);

  function requireClientAuth(req, res, next) {
    const token = extractClientToken(req);
    if (!safeEqualText(token, config.clientApiKey)) {
      return res.status(401).json({ error: "Unauthorized client token." });
    }
    return next();
  }

  function issueWorkerInvite({ ttlSec, workerLabel } = {}) {
    const nowSec = Math.floor(Date.now() / 1000);
    const ttl = clamp(parsePositiveInt(ttlSec, config.inviteTtlSec), MIN_INVITE_TTL_SEC, MAX_INVITE_TTL_SEC);
    const payload = {
      typ: "worker_invite",
      jti: randomUUID(),
      iat: nowSec,
      exp: nowSec + ttl,
      label: typeof workerLabel === "string" && workerLabel.length > 0 ? workerLabel.slice(0, 64) : null,
    };
    const payloadPart = toBase64Url(JSON.stringify(payload));
    const signaturePart = sign(payloadPart);
    return {
      token: `v1.${payloadPart}.${signaturePart}`,
      expiresAt: new Date(payload.exp * 1000).toISOString(),
      ttlSec: ttl,
    };
  }

  function verifyWorkerInvite(token) {
    if (typeof token !== "string" || token.length < 20 || token.length > 2048) {
      return { ok: false, reason: "invalid_token_format" };
    }
    const parts = token.split(".");
    if (parts.length !== 3 || parts[0] !== "v1") {
      return { ok: false, reason: "invalid_token_format" };
    }
    const payloadPart = parts[1];
    const signaturePart = parts[2];
    const expectedSignature = sign(payloadPart);
    if (!safeEqualText(signaturePart, expectedSignature)) {
      return { ok: false, reason: "invalid_signature" };
    }

    let payload = null;
    try {
      payload = JSON.parse(fromBase64Url(payloadPart).toString("utf8"));
    } catch (_error) {
      return { ok: false, reason: "invalid_payload" };
    }

    if (!payload || payload.typ !== "worker_invite") {
      return { ok: false, reason: "invalid_payload" };
    }

    const nowSec = Math.floor(Date.now() / 1000);
    if (!Number.isInteger(payload.exp) || payload.exp < nowSec) {
      return { ok: false, reason: "expired_token" };
    }

    return { ok: true, payload };
  }

  return {
    requireClientAuth,
    issueWorkerInvite,
    verifyWorkerInvite,
  };
}

module.exports = {
  createAuth,
  resolveConfig,
  extractClientToken,
  DEFAULT_INVITE_TTL_SEC,
  MIN_INVITE_TTL_SEC,
  MAX_INVITE_TTL_SEC,
};
