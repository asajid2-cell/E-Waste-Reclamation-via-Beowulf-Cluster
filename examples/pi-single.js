const samples = Number(args.samples || 10_000_000);
if (!Number.isFinite(samples) || samples <= 0) {
  throw new Error("args.samples must be a positive number");
}

let hits = 0;
for (let i = 0; i < samples; i += 1) {
  const x = Math.random();
  const y = Math.random();
  if (x * x + y * y <= 1) {
    hits += 1;
  }
}

const pi = (4 * hits) / samples;
console.log(`single-worker pi estimate=${pi}`);
return { mode: "single", hits, samples, pi };
