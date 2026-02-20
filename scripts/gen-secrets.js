#!/usr/bin/env node

const { randomBytes } = require("node:crypto");

function makeToken(bytes = 32) {
  return randomBytes(bytes).toString("base64url");
}

console.log("CLIENT_API_KEY=" + makeToken(32));
console.log("WORKER_INVITE_SECRET=" + makeToken(48));
