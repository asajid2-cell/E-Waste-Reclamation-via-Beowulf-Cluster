const { wordlists, entropyToMnemonic, mnemonicToEntropy, validateMnemonic } = require("bip39");

const MNEMONIC_WORDLIST = wordlists.english;
const SUPPORTED_ENTROPY_BYTES = new Set([16, 20, 24, 28, 32]);

function normalizeText(value) {
  return String(value || "").trim();
}

function normalizeMnemonic(value) {
  return normalizeText(value)
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .join(" ");
}

function bytesToBase64Url(bytes) {
  return Buffer.from(bytes)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function maybeHexToBytes(value) {
  const text = normalizeText(value);
  if (!text || text.length % 2 !== 0 || !/^[0-9a-fA-F]+$/.test(text)) {
    return null;
  }
  return Buffer.from(text, "hex");
}

function maybeBase64UrlToBytes(value) {
  const text = normalizeText(value);
  if (!text || !/^[A-Za-z0-9_-]+$/.test(text)) {
    return null;
  }

  let base64 = text.replace(/-/g, "+").replace(/_/g, "/");
  if (base64.length % 4 === 1) {
    return null;
  }
  while (base64.length % 4 !== 0) {
    base64 += "=";
  }

  let bytes = null;
  try {
    bytes = Buffer.from(base64, "base64");
  } catch (_error) {
    return null;
  }

  if (bytesToBase64Url(bytes) !== text) {
    return null;
  }
  return bytes;
}

function tokenToBytes(token) {
  return maybeHexToBytes(token) || maybeBase64UrlToBytes(token);
}

function mnemonicToBytes(phrase) {
  const normalized = normalizeMnemonic(phrase);
  if (!normalized) {
    return null;
  }
  if (!validateMnemonic(normalized, MNEMONIC_WORDLIST)) {
    return null;
  }

  try {
    return Buffer.from(mnemonicToEntropy(normalized, MNEMONIC_WORDLIST), "hex");
  } catch (_error) {
    return null;
  }
}

function tokenToMnemonic(token) {
  const bytes = tokenToBytes(token);
  if (!bytes) {
    return { ok: false, error: "Token must be hex or base64url." };
  }
  if (!SUPPORTED_ENTROPY_BYTES.has(bytes.length)) {
    return { ok: false, error: "Token byte length is unsupported for mnemonic conversion." };
  }
  return { ok: true, mnemonic: entropyToMnemonic(bytes.toString("hex"), MNEMONIC_WORDLIST) };
}

function mnemonicToToken(phrase, format = "base64url") {
  const bytes = mnemonicToBytes(phrase);
  if (!bytes) {
    return { ok: false, error: "Invalid mnemonic phrase." };
  }
  if (format === "hex") {
    return { ok: true, token: bytes.toString("hex") };
  }
  if (format === "base64url") {
    return { ok: true, token: bytesToBase64Url(bytes) };
  }
  return { ok: false, error: "Unsupported format. Use hex or base64url." };
}

module.exports = {
  bytesToBase64Url,
  normalizeMnemonic,
  mnemonicToBytes,
  mnemonicToToken,
  tokenToBytes,
  tokenToMnemonic,
};
