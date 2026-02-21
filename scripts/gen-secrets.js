#!/usr/bin/env node

const { generateKeyPairSync, randomBytes } = require("node:crypto");

function makeToken(bytes = 32) {
  return randomBytes(bytes).toString("base64url");
}

console.log("CLIENT_API_KEY=" + makeToken(32));
console.log("WORKER_INVITE_SECRET=" + makeToken(48));

const keyPair = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "der" },
  privateKeyEncoding: { type: "pkcs8", format: "der" },
});

console.log("JOB_SIGNING_PRIVATE_KEY_B64=" + keyPair.privateKey.toString("base64"));
console.log("JOB_SIGNING_PUBLIC_KEY_B64=" + keyPair.publicKey.toString("base64"));
