const brain = globalThis.__BRAIN__ || null;
const units = Number(brain?.units || args.units || 1_000_000);
if (!Number.isFinite(units) || units <= 0) {
  throw new Error("Shard units must be a positive number");
}

let hits = 0;
for (let i = 0; i < units; i += 1) {
  const x = Math.random();
  const y = Math.random();
  if (x * x + y * y <= 1) {
    hits += 1;
  }
}

const shardId = Number.isInteger(brain?.shardId) ? brain.shardId : null;
const totalShards = Number.isInteger(brain?.totalShards) ? brain.totalShards : null;
const mode = shardId === null ? "single" : "sharded";
console.log(`mode=${mode} shard=${shardId ?? "single"} hits=${hits} samples=${units}`);
return { mode, shardId, totalShards, hits, samples: units };
