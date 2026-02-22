const {
  createHmac,
  createPrivateKey,
  createPublicKey,
  createSign,
  createVerify,
  generateKeyPairSync,
  randomBytes,
  randomUUID,
  timingSafeEqual,
} = require("node:crypto");
const { mnemonicToBytes, tokenToBytes } = require("../common/client-key");

const DEFAULT_INVITE_TTL_SEC = 3600;
const MIN_INVITE_TTL_SEC = 60;
const MAX_INVITE_TTL_SEC = 86400;
const DEFAULT_SIGNED_JOB_TTL_SEC = 120;
const MIN_SIGNED_JOB_TTL_SEC = 10;
const MAX_SIGNED_JOB_TTL_SEC = 900;

const DEFAULT_CUSTOM_JOB_MAX_CODE_BYTES = 32 * 1024;
const DEFAULT_CUSTOM_JOB_MAX_ARGS_BYTES = 32 * 1024;
const DEFAULT_CUSTOM_JOB_MAX_RESULT_BYTES = 1024 * 1024;
const DEFAULT_CUSTOM_JOB_TIMEOUT_MS = 0;
const MIN_CUSTOM_JOB_TIMEOUT_MS = 0;
const MAX_CUSTOM_JOB_TIMEOUT_MS = Number.MAX_SAFE_INTEGER;

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

function safeEqualBuffer(a, b) {
  if (!Buffer.isBuffer(a) || !Buffer.isBuffer(b)) {
    return false;
  }
  if (a.length !== b.length) {
    return false;
  }
  return timingSafeEqual(a, b);
}

function buildSigner(secret) {
  return (payloadPart) => toBase64Url(createHmac("sha256", secret).update(payloadPart).digest());
}

function signTextWithRsa(privateKey, text) {
  const signer = createSign("RSA-SHA256");
  signer.update(text);
  signer.end();
  return toBase64Url(signer.sign(privateKey));
}

function verifyTextWithRsa(publicKey, text, signature) {
  const verifier = createVerify("RSA-SHA256");
  verifier.update(text);
  verifier.end();
  return verifier.verify(publicKey, signature);
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

function parseNonNegativeInt(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0) {
    return fallback;
  }
  return n;
}

function parseBoolean(value, fallback) {
  if (value === undefined || value === null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function decodeBase64(value, name) {
  try {
    return Buffer.from(value, "base64");
  } catch (_error) {
    throw new Error(`${name} is not valid base64.`);
  }
}

function parseSigningKeys({ env, privateKeyB64, publicKeyB64 }) {
  let resolvedPrivate = privateKeyB64;
  let resolvedPublic = publicKeyB64;

  if (!resolvedPrivate || !resolvedPublic) {
    if (env === "production") {
      throw new Error("JOB_SIGNING_PRIVATE_KEY_B64 and JOB_SIGNING_PUBLIC_KEY_B64 are required in production.");
    }
    const generated = generateKeyPairSync("rsa", {
      modulusLength: 2048,
      publicKeyEncoding: { type: "spki", format: "der" },
      privateKeyEncoding: { type: "pkcs8", format: "der" },
    });
    resolvedPrivate = generated.privateKey.toString("base64");
    resolvedPublic = generated.publicKey.toString("base64");
  }

  const privateKeyDer = decodeBase64(resolvedPrivate, "JOB_SIGNING_PRIVATE_KEY_B64");
  const publicKeyDer = decodeBase64(resolvedPublic, "JOB_SIGNING_PUBLIC_KEY_B64");

  const privateKey = createPrivateKey({
    key: privateKeyDer,
    type: "pkcs8",
    format: "der",
  });
  const publicKey = createPublicKey({
    key: publicKeyDer,
    type: "spki",
    format: "der",
  });

  const probePayload = `cluster-signing-probe-${randomUUID()}`;
  const probeSignature = createSign("RSA-SHA256").update(probePayload).end().sign(privateKey);
  const validPair = verifyTextWithRsa(publicKey, probePayload, probeSignature);
  if (!validPair) {
    throw new Error("JOB_SIGNING_PRIVATE_KEY_B64 and JOB_SIGNING_PUBLIC_KEY_B64 do not match.");
  }

  return {
    privateKey,
    publicKey,
    publicKeySpkiB64: resolvedPublic,
  };
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

  const signingKeys = parseSigningKeys({
    env,
    privateKeyB64: process.env.JOB_SIGNING_PRIVATE_KEY_B64 || "",
    publicKeyB64: process.env.JOB_SIGNING_PUBLIC_KEY_B64 || "",
  });

  const signedJobTtlSec = clamp(
    parsePositiveInt(process.env.SIGNED_JOB_TTL_SEC, DEFAULT_SIGNED_JOB_TTL_SEC),
    MIN_SIGNED_JOB_TTL_SEC,
    MAX_SIGNED_JOB_TTL_SEC,
  );

  const customJobMaxCodeBytes = clamp(
    parsePositiveInt(process.env.CUSTOM_JOB_MAX_CODE_BYTES, DEFAULT_CUSTOM_JOB_MAX_CODE_BYTES),
    1024,
    1024 * 1024,
  );
  const customJobMaxArgsBytes = clamp(
    parsePositiveInt(process.env.CUSTOM_JOB_MAX_ARGS_BYTES, DEFAULT_CUSTOM_JOB_MAX_ARGS_BYTES),
    1024,
    1024 * 1024,
  );
  const customJobMaxResultBytes = clamp(
    parsePositiveInt(process.env.CUSTOM_JOB_MAX_RESULT_BYTES, DEFAULT_CUSTOM_JOB_MAX_RESULT_BYTES),
    1024,
    1024 * 1024,
  );

  const customJobMinTimeoutMs = parseNonNegativeInt(process.env.CUSTOM_JOB_MIN_TIMEOUT_MS, MIN_CUSTOM_JOB_TIMEOUT_MS);
  const customJobMaxTimeoutMs = Math.max(
    customJobMinTimeoutMs,
    parseNonNegativeInt(process.env.CUSTOM_JOB_MAX_TIMEOUT_MS, MAX_CUSTOM_JOB_TIMEOUT_MS),
  );
  const customJobDefaultTimeoutMsRaw = parseNonNegativeInt(
    process.env.CUSTOM_JOB_DEFAULT_TIMEOUT_MS,
    DEFAULT_CUSTOM_JOB_TIMEOUT_MS,
  );
  const customJobDefaultTimeoutMs =
    customJobDefaultTimeoutMsRaw === 0
      ? 0
      : clamp(customJobDefaultTimeoutMsRaw, customJobMinTimeoutMs, customJobMaxTimeoutMs);

  const workerInviteRequireLatest = parseBoolean(process.env.WORKER_INVITE_REQUIRE_LATEST, true);

  return {
    env,
    clientApiKey,
    workerInviteSecret,
    inviteTtlSec,
    signedJobTtlSec,
    customJobMaxCodeBytes,
    customJobMaxArgsBytes,
    customJobMaxResultBytes,
    customJobDefaultTimeoutMs,
    customJobMinTimeoutMs,
    customJobMaxTimeoutMs,
    workerInviteRequireLatest,
    jobSigningPrivateKey: signingKeys.privateKey,
    jobSigningPublicKey: signingKeys.publicKey,
    jobSigningPublicKeySpkiB64: signingKeys.publicKeySpkiB64,
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
  const configuredClientKeyBytes = tokenToBytes(config.clientApiKey);
  let latestInviteJti = "";
  let latestInviteIssuedAtSec = 0;

  function signWorkerAssignment(job) {
    if (!job || !job.task || job.task.op !== "run_js") {
      throw new Error("signWorkerAssignment only supports run_js tasks.");
    }

    const nowMs = Date.now();
    const payload = {
      typ: "cluster_signed_job",
      v: 1,
      nonce: randomUUID(),
      iat: nowMs,
      exp: nowMs + config.signedJobTtlSec * 1000,
      jobId: job.jobId,
      task: {
        op: "run_js",
        code: job.task.code,
        args: job.task.args,
        timeoutMs: job.task.timeoutMs,
      },
    };
    const payloadText = JSON.stringify(payload);
    const signature = signTextWithRsa(config.jobSigningPrivateKey, payloadText);

    return {
      alg: "RS256",
      payload: payloadText,
      signature,
    };
  }

  function requireClientAuth(req, res, next) {
    const token = extractClientToken(req);
    if (safeEqualText(token, config.clientApiKey)) {
      return next();
    }

    if (configuredClientKeyBytes) {
      const providedBytes = tokenToBytes(token) || mnemonicToBytes(token);
      if (providedBytes && safeEqualBuffer(providedBytes, configuredClientKeyBytes)) {
        return next();
      }
    }

    return res.status(401).json({ error: "Unauthorized client token." });
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
    latestInviteJti = payload.jti;
    latestInviteIssuedAtSec = payload.iat;
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
    if (config.workerInviteRequireLatest) {
      if (!latestInviteJti) {
        return { ok: false, reason: "no_active_invite" };
      }
      if (payload.jti !== latestInviteJti) {
        return { ok: false, reason: "stale_token" };
      }
      if (!Number.isInteger(payload.iat) || payload.iat < latestInviteIssuedAtSec) {
        return { ok: false, reason: "stale_token" };
      }
    }

    return { ok: true, payload };
  }

  return {
    requireClientAuth,
    issueWorkerInvite,
    verifyWorkerInvite,
    signWorkerAssignment,
  };
}

module.exports = {
  createAuth,
  resolveConfig,
  extractClientToken,
  DEFAULT_INVITE_TTL_SEC,
  MIN_INVITE_TTL_SEC,
  MAX_INVITE_TTL_SEC,
  DEFAULT_SIGNED_JOB_TTL_SEC,
  MIN_SIGNED_JOB_TTL_SEC,
  MAX_SIGNED_JOB_TTL_SEC,
};
