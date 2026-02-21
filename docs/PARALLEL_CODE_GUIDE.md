# Parallelizable Script Guide

This cluster supports two execution models for `run_js` jobs:

- `single`: one worker runs the script.
- `sharded`: the Brain splits one job into many shard jobs and runs them across multiple workers.

In the client UI, execution model is auto-resolved:

- Leave both shard fields empty -> `single`
- Fill both shard fields (`total units` + `units per shard`) -> `sharded`

## Core Rule

Write scripts so work size comes from an input value, not hardcoded constants.

Use this pattern:

```js
const units = Number(globalThis.__BRAIN__?.units || args.samples || 1_000_000);
```

When sharded, the Brain injects `globalThis.__BRAIN__` per shard.

## Good vs Bad

Bad (not parallel-friendly):

```js
let hits = 0;
for (let i = 0; i < 100_000_000; i += 1) {
  const x = Math.random();
  const y = Math.random();
  if (x * x + y * y <= 1) hits += 1;
}
return hits;
```

Good (parallel-friendly):

```js
const units = Number(globalThis.__BRAIN__?.units || args.samples || 1_000_000);
let hits = 0;
for (let i = 0; i < units; i += 1) {
  const x = Math.random();
  const y = Math.random();
  if (x * x + y * y <= 1) hits += 1;
}
return { hits, samples: units };
```

## Pi Example (Sharded)

Script:

```js
const units = Number(globalThis.__BRAIN__?.units || 10_000_000);
let hits = 0;
for (let i = 0; i < units; i += 1) {
  const x = Math.random();
  const y = Math.random();
  if (x * x + y * y <= 1) hits += 1;
}
return { hits, samples: units };
```

UI settings:

- `total units`: `100000000`
- `units per shard`: `10000000`
- reducer: `sum`
- sum fields: `hits,samples`

Final estimate after completion:

```text
pi ~= 4 * (aggregate.hits / aggregate.samples)
```

## Reducer Selection

- `sum`: add numeric fields from each shard result (counts, totals).
- `collect`: keep all shard results in an array.
- `min`: keep smallest value of one field.
- `max`: keep largest value of one field.

## Design Tips

- Keep shard return payloads small.
- Return plain JSON-safe objects.
- Use deterministic inputs if you need reproducible runs.
- Avoid side effects (network writes, global mutations) in shard code.

## Troubleshooting

- If mode says invalid in UI: set both shard fields or clear both.
- If `sum` result is wrong: ensure shard returns those numeric fields.
- If result is empty: verify your script returns a value.
