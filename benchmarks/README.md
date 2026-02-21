# Cluster Benchmark Suite

This folder contains a **single JS benchmark file** that works in both environments:

- local machine (single process)
- cluster worker runtime (`run_js`, single or sharded)

## Files

- `cluster_benchmark_suite.js`  
  Script body for `run_js` / local execution.  
  Supports:
  - `pi`
  - `mandelbrot`
  - `sha256_pow`
  - `mosaic`
  - `primes`
  - `matrix`
  - `nbody`
  - `sobel`
  - `sort`
  - `levenshtein`
  - `suite` (runs all tests above in one submission)
- `presets.json`  
  Default workload sizes, shard sizes, reducers, and benchmark args.

## Script Contract

The script auto-detects shard context via `globalThis.__BRAIN__` when running sharded in cluster.
If absent (local mode), it falls back to `args.units` and `args.offset`.

Returned fields are numeric so reducers can aggregate reliably.

## Recommended Reducers

- `pi`: `sum` on `hits,samples`
- `mandelbrot`: `sum` on `sumIter,escaped,samples`
- `sha256_pow`: `sum` on `hashes,found,samples`
- `mosaic`: `sum` on `score,checksum,samples`

## Running Benchmarks

Use the harness:

```bash
node scripts/benchmark-harness.js --mode local --runs 1 --scale 1
```

Cluster only:

```bash
node scripts/benchmark-harness.js --mode cluster --runs 1 --scale 1 --host https://harmonizerlabs.cc --client-token "<token>"
```

Compare local vs cluster in one run:

```bash
node scripts/benchmark-harness.js --mode both --runs 1 --scale 1 --host https://harmonizerlabs.cc --client-token "<token>"
```

Stress a stronger machine/cluster:

```bash
node scripts/benchmark-harness.js --mode both --runs 1 --scale 2 --host https://harmonizerlabs.cc --client-token "<token>"
```

The harness auto-calibrates benchmark sizes by default to target a longer run (`--target-suite-seconds`, default `75` seconds per mode), so the test is not a 5-second micro-run.

Useful knobs:

```bash
# Make suite longer (roughly 2+ minutes per mode)
node scripts/benchmark-harness.js --mode both --target-suite-seconds 150 --runs 1 --host https://harmonizerlabs.cc --client-token "<token>"

# Disable auto calibration and use raw preset sizes only
node scripts/benchmark-harness.js --mode both --calibrate 0 --runs 1 --scale 1 --host https://harmonizerlabs.cc --client-token "<token>"
```

## One-Submission Website Run

If you want one website submission (not many manual jobs):

1. Upload/paste `benchmarks/cluster_benchmark_suite.js` in the client page.
2. Set:
   - `Args JSON`: `{"benchmark":"suite"}`
   - `Reducer`: `SUM`
   - `Sum Fields`: `*`
3. Set shard totals for your target stress duration (or use `benchmarks/presets.json` `suite` values as starting point).

`*` in sum fields means server auto-sums every numeric top-level field returned by each shard.
