const brain = globalThis.__BRAIN__ || {};

function intOr(value, fallback) {
  const n = Number(value);
  if (!Number.isFinite(n) || !Number.isInteger(n)) {
    return fallback;
  }
  return n;
}

function clampInt(value, fallback, minValue, maxValue) {
  let n = intOr(value, fallback);
  if (n < minValue) {
    n = minValue;
  }
  if (n > maxValue) {
    n = maxValue;
  }
  return n;
}

function makeXorShift32(seed) {
  let state = (seed >>> 0) || 0x9e3779b9;
  return function nextU32() {
    state ^= state << 13;
    state ^= state >>> 17;
    state ^= state << 5;
    return state >>> 0;
  };
}

function countLeadingZeroBits(bytes) {
  let bits = 0;
  for (let i = 0; i < bytes.length; i += 1) {
    const value = bytes[i];
    if (value === 0) {
      bits += 8;
      continue;
    }
    for (let shift = 7; shift >= 0; shift -= 1) {
      if ((value & (1 << shift)) === 0) {
        bits += 1;
      } else {
        return bits;
      }
    }
  }
  return bits;
}

function getWorkConfig() {
  const fallbackUnits = clampInt(args && args.units, 250000, 1, 2_000_000_000);
  const units = clampInt(brain && brain.units, fallbackUnits, 1, 2_000_000_000);
  const fallbackOffset = intOr(args && args.offset, 0);
  const offset = Math.max(0, intOr(brain && brain.offset, fallbackOffset));
  const shardId = Number.isInteger(brain && brain.shardId) ? brain.shardId : null;
  return { units, offset, shardId };
}

function runPi(config) {
  const seed = clampInt(args && args.seed, 1337, 1, 0x7fffffff);
  const rng = makeXorShift32(seed ^ config.offset ^ config.units);
  let hits = 0;
  for (let i = 0; i < config.units; i += 1) {
    const a = rng();
    const b = rng();
    const x = a / 4294967296;
    const y = b / 4294967296;
    if (x * x + y * y <= 1) {
      hits += 1;
    }
  }
  return {
    hits,
    samples: config.units,
    ops: config.units,
  };
}

function runMandelbrot(config) {
  const width = clampInt(args && args.width, 1920, 64, 16384);
  const height = clampInt(args && args.height, 1080, 64, 16384);
  const maxIter = clampInt(args && args.maxIter, 1200, 16, 8000);
  const zoom = Math.max(Number(args && args.zoom) || 1, 0.0001);
  const centerX = Number(args && args.centerX) || -0.743643887037151;
  const centerY = Number(args && args.centerY) || 0.13182590420533;

  const scaleX = 3.5 / (width * zoom);
  const scaleY = 2.0 / (height * zoom);
  const totalPixels = Math.max(1, width * height);

  let sumIter = 0;
  let escaped = 0;

  for (let i = 0; i < config.units; i += 1) {
    const absoluteIndex = config.offset + i;
    const pixelIndex = absoluteIndex % totalPixels;
    const x = pixelIndex % width;
    const y = Math.floor(pixelIndex / width);

    const cx = centerX + (x - width / 2) * scaleX;
    const cy = centerY + (y - height / 2) * scaleY;

    let zx = 0;
    let zy = 0;
    let iter = 0;
    while (iter < maxIter) {
      const zx2 = zx * zx - zy * zy + cx;
      const zy2 = 2 * zx * zy + cy;
      zx = zx2;
      zy = zy2;
      if (zx * zx + zy * zy > 4) {
        escaped += 1;
        break;
      }
      iter += 1;
    }
    sumIter += iter;
  }

  return {
    sumIter,
    escaped,
    samples: config.units,
    ops: sumIter,
  };
}

async function runSha256Pow(config) {
  const prefix = String((args && args.prefix) || "cluster-bench");
  const targetZeroBits = clampInt(args && args.targetZeroBits, 16, 4, 28);
  const roundsPerUnit = clampInt(args && args.roundsPerUnit, 2, 1, 16);
  const encoder = new TextEncoder();
  const hasSubtle = Boolean(globalThis.crypto && globalThis.crypto.subtle && globalThis.crypto.subtle.digest);

  let found = 0;
  let bestZeroBits = 0;
  let rolling = 0;

  if (hasSubtle) {
    for (let i = 0; i < config.units; i += 1) {
      const nonce = config.offset + i;
      let payload = encoder.encode(`${prefix}:${nonce}`);
      let bytes = null;
      for (let round = 0; round < roundsPerUnit; round += 1) {
        const digest = await globalThis.crypto.subtle.digest("SHA-256", payload);
        bytes = new Uint8Array(digest);
        payload = bytes;
      }
      const zeroBits = countLeadingZeroBits(bytes);
      if (zeroBits >= targetZeroBits) {
        found += 1;
      }
      if (zeroBits > bestZeroBits) {
        bestZeroBits = zeroBits;
      }
      rolling = ((rolling * 33) ^ bytes[0] ^ (bytes[7] << 8) ^ (bytes[19] << 16)) >>> 0;
    }
  } else {
    let state = 0x811c9dc5 >>> 0;
    for (let i = 0; i < config.units; i += 1) {
      const nonce = config.offset + i;
      state ^= nonce >>> 0;
      state = Math.imul(state, 16777619) >>> 0;
      for (let round = 0; round < roundsPerUnit * 48; round += 1) {
        state ^= state << 13;
        state ^= state >>> 17;
        state ^= state << 5;
        state >>>= 0;
      }
      const pseudoZeroBits = Math.clz32(state);
      if (pseudoZeroBits >= targetZeroBits) {
        found += 1;
      }
      if (pseudoZeroBits > bestZeroBits) {
        bestZeroBits = pseudoZeroBits;
      }
      rolling = (rolling + state) >>> 0;
    }
  }

  const hashes = config.units * roundsPerUnit;
  return {
    hashes,
    found,
    bestZeroBits,
    rolling,
    samples: config.units,
    ops: hashes,
  };
}

function runMosaic(config) {
  const paletteSize = clampInt(args && args.paletteSize, 768, 16, 4096);
  const seed = clampInt(args && args.seed, 777, 1, 0x7fffffff);
  let score = 0;
  let checksum = 0;

  for (let i = 0; i < config.units; i += 1) {
    const tile = config.offset + i;
    const tr = (tile * 73 + seed * 17) & 255;
    const tg = (tile * 41 + seed * 31) & 255;
    const tb = (tile * 19 + seed * 47) & 255;

    let bestDist = 1e12;
    let bestIndex = 0;
    for (let p = 0; p < paletteSize; p += 1) {
      const pr = (p * 29 + seed * 11) & 255;
      const pg = (p * 53 + seed * 23) & 255;
      const pb = (p * 97 + seed * 37) & 255;
      const dr = tr - pr;
      const dg = tg - pg;
      const db = tb - pb;
      const dist = dr * dr + dg * dg + db * db;
      if (dist < bestDist) {
        bestDist = dist;
        bestIndex = p;
      }
    }
    score += bestDist;
    checksum = (checksum + ((bestIndex + 1) * 2654435761)) >>> 0;
  }

  return {
    score,
    checksum,
    samples: config.units,
    ops: config.units * paletteSize,
  };
}

function runPrimeCount(config) {
  const startBase = clampInt(args && args.startBase, 1000003, 2, 2_000_000_000);
  const skipEven = args && args.skipEven !== false;
  let primeCount = 0;
  let checks = 0;
  let checksum = 0;

  for (let i = 0; i < config.units; i += 1) {
    let n = startBase + config.offset + i;
    if (skipEven && (n & 1) === 0) {
      n += 1;
    }
    if (n < 2) {
      continue;
    }
    if (n === 2 || n === 3) {
      primeCount += 1;
      checksum = (checksum + n) >>> 0;
      continue;
    }
    if ((n & 1) === 0 || n % 3 === 0) {
      checks += 2;
      continue;
    }
    let isPrime = true;
    for (let d = 5; d * d <= n; d += 6) {
      checks += 2;
      if (n % d === 0 || n % (d + 2) === 0) {
        isPrime = false;
        break;
      }
    }
    if (isPrime) {
      primeCount += 1;
      checksum = (checksum + n) >>> 0;
    }
  }

  return {
    primeCount,
    checks,
    checksum,
    samples: config.units,
    ops: checks,
  };
}

function runMatrixMix(config) {
  const size = clampInt(args && args.size, 44, 8, 96);
  const roundsPerUnit = clampInt(args && args.roundsPerUnit, 1, 1, 8);
  const seed = clampInt(args && args.seed, 4242, 1, 0x7fffffff);
  const len = size * size;
  const a = new Float64Array(len);
  const b = new Float64Array(len);
  const c = new Float64Array(len);
  const rand = makeXorShift32(seed ^ config.offset);

  for (let i = 0; i < len; i += 1) {
    a[i] = (rand() & 65535) / 65535;
    b[i] = ((rand() >>> 1) & 65535) / 65535;
  }

  let checksum = 0;
  let sum = 0;
  let ops = 0;

  for (let unit = 0; unit < config.units; unit += 1) {
    for (let round = 0; round < roundsPerUnit; round += 1) {
      for (let r = 0; r < size; r += 1) {
        const rBase = r * size;
        for (let col = 0; col < size; col += 1) {
          let acc = 0;
          for (let k = 0; k < size; k += 1) {
            acc += a[rBase + k] * b[k * size + col];
          }
          c[rBase + col] = acc;
        }
      }
      for (let i = 0; i < len; i += 1) {
        b[i] = (b[i] + c[i] * 0.000001) % 1;
        sum += c[i];
      }
      ops += size * size * size * 2;
      checksum = (checksum + ((c[(unit + round) % len] * 1000003) | 0)) >>> 0;
    }
  }

  return {
    sum,
    checksum,
    samples: config.units,
    ops,
  };
}

function runNBody(config) {
  const particles = clampInt(args && args.particles, 72, 8, 256);
  const stepsPerUnit = clampInt(args && args.stepsPerUnit, 1, 1, 8);
  const dt = Number(args && args.dt) || 0.0006;
  const softening = Number(args && args.softening) || 0.001;
  const seed = clampInt(args && args.seed, 9091, 1, 0x7fffffff);
  const rand = makeXorShift32(seed ^ config.offset);

  const px = new Float64Array(particles);
  const py = new Float64Array(particles);
  const pz = new Float64Array(particles);
  const vx = new Float64Array(particles);
  const vy = new Float64Array(particles);
  const vz = new Float64Array(particles);

  for (let i = 0; i < particles; i += 1) {
    px[i] = ((rand() & 65535) / 65535 - 0.5) * 20;
    py[i] = (((rand() >>> 1) & 65535) / 65535 - 0.5) * 20;
    pz[i] = (((rand() >>> 2) & 65535) / 65535 - 0.5) * 20;
    vx[i] = (((rand() >>> 3) & 65535) / 65535 - 0.5) * 0.01;
    vy[i] = (((rand() >>> 4) & 65535) / 65535 - 0.5) * 0.01;
    vz[i] = (((rand() >>> 5) & 65535) / 65535 - 0.5) * 0.01;
  }

  let energy = 0;
  let checksum = 0;
  let ops = 0;

  for (let unit = 0; unit < config.units; unit += 1) {
    for (let step = 0; step < stepsPerUnit; step += 1) {
      for (let i = 0; i < particles; i += 1) {
        let ax = 0;
        let ay = 0;
        let az = 0;
        for (let j = 0; j < particles; j += 1) {
          if (i === j) {
            continue;
          }
          const dx = px[j] - px[i];
          const dy = py[j] - py[i];
          const dz = pz[j] - pz[i];
          const dist2 = dx * dx + dy * dy + dz * dz + softening;
          const inv = 1 / Math.sqrt(dist2);
          const inv3 = inv * inv * inv;
          ax += dx * inv3;
          ay += dy * inv3;
          az += dz * inv3;
        }
        vx[i] += ax * dt;
        vy[i] += ay * dt;
        vz[i] += az * dt;
      }
      for (let i = 0; i < particles; i += 1) {
        px[i] += vx[i] * dt;
        py[i] += vy[i] * dt;
        pz[i] += vz[i] * dt;
      }
      ops += particles * particles * 20;
    }
  }

  for (let i = 0; i < particles; i += 1) {
    energy += vx[i] * vx[i] + vy[i] * vy[i] + vz[i] * vz[i];
    checksum = (checksum + ((px[i] * 1000) | 0) + ((py[i] * 997) | 0) + ((pz[i] * 991) | 0)) >>> 0;
  }

  return {
    energy,
    checksum,
    samples: config.units,
    ops,
  };
}

function runSobel(config) {
  const width = clampInt(args && args.width, 384, 32, 1024);
  const height = clampInt(args && args.height, 384, 32, 1024);
  const seed = clampInt(args && args.seed, 5150, 1, 0x7fffffff);
  const total = width * height;
  const img = new Uint8Array(total);
  const rand = makeXorShift32(seed ^ config.offset);
  for (let i = 0; i < total; i += 1) {
    img[i] = rand() & 255;
  }

  let edgeSum = 0;
  let checksum = 0;
  let ops = 0;
  const innerW = width - 2;
  const innerH = height - 2;
  const innerTotal = Math.max(1, innerW * innerH);

  for (let i = 0; i < config.units; i += 1) {
    const p = (config.offset + i) % innerTotal;
    const x = (p % innerW) + 1;
    const y = Math.floor(p / innerW) + 1;
    const c = y * width + x;

    const gx =
      -img[c - width - 1] +
      img[c - width + 1] -
      2 * img[c - 1] +
      2 * img[c + 1] -
      img[c + width - 1] +
      img[c + width + 1];
    const gy =
      img[c - width - 1] +
      2 * img[c - width] +
      img[c - width + 1] -
      img[c + width - 1] -
      2 * img[c + width] -
      img[c + width + 1];

    const mag = Math.sqrt(gx * gx + gy * gy);
    edgeSum += mag;
    checksum = (checksum + ((mag * 31) | 0)) >>> 0;
    ops += 30;
  }

  return {
    edgeSum,
    checksum,
    samples: config.units,
    ops,
  };
}

function runSortBatch(config) {
  const length = clampInt(args && args.length, 2048, 16, 65536);
  const seed = clampInt(args && args.seed, 6060, 1, 0x7fffffff);
  const rand = makeXorShift32(seed ^ config.offset);
  let checksum = 0;

  for (let i = 0; i < config.units; i += 1) {
    const arr = new Array(length);
    for (let j = 0; j < length; j += 1) {
      arr[j] = rand();
    }
    arr.sort((a, b) => a - b);
    checksum = (checksum + ((arr[0] * 1e9) | 0) + ((arr[length - 1] * 1e9) | 0)) >>> 0;
  }

  const ops = Math.round(config.units * length * Math.log2(length));
  return {
    checksum,
    samples: config.units,
    ops,
  };
}

function runLevenshteinBatch(config) {
  const lenA = clampInt(args && args.lenA, 84, 4, 512);
  const lenB = clampInt(args && args.lenB, 84, 4, 512);
  const seed = clampInt(args && args.seed, 8080, 1, 0x7fffffff);
  const rand = makeXorShift32(seed ^ config.offset);
  const row = new Uint16Array(lenB + 1);
  let distanceSum = 0;
  let checksum = 0;

  for (let unit = 0; unit < config.units; unit += 1) {
    const a = new Uint8Array(lenA);
    const b = new Uint8Array(lenB);
    for (let i = 0; i < lenA; i += 1) {
      a[i] = 97 + (rand() % 26);
    }
    for (let j = 0; j < lenB; j += 1) {
      b[j] = 97 + (rand() % 26);
    }

    for (let j = 0; j <= lenB; j += 1) {
      row[j] = j;
    }
    for (let i = 1; i <= lenA; i += 1) {
      let prev = row[0];
      row[0] = i;
      for (let j = 1; j <= lenB; j += 1) {
        const temp = row[j];
        const cost = a[i - 1] === b[j - 1] ? 0 : 1;
        let v = row[j] + 1;
        if (row[j - 1] + 1 < v) {
          v = row[j - 1] + 1;
        }
        if (prev + cost < v) {
          v = prev + cost;
        }
        row[j] = v;
        prev = temp;
      }
    }

    const dist = row[lenB];
    distanceSum += dist;
    checksum = (checksum + ((dist + unit + 1) * 2654435761)) >>> 0;
  }

  return {
    distanceSum,
    checksum,
    samples: config.units,
    ops: config.units * lenA * lenB,
  };
}

const BENCHMARK_RUNNERS = {
  pi: runPi,
  mandelbrot: runMandelbrot,
  sha256_pow: runSha256Pow,
  mosaic: runMosaic,
  primes: runPrimeCount,
  matrix: runMatrixMix,
  nbody: runNBody,
  sobel: runSobel,
  sort: runSortBatch,
  levenshtein: runLevenshteinBatch,
};

async function runSuite(config) {
  const defaultCases = [
    "pi",
    "mandelbrot",
    "sha256_pow",
    "mosaic",
    "primes",
    "matrix",
    "nbody",
    "sobel",
    "sort",
    "levenshtein",
  ];
  const rawCases = String((args && args.suiteCases) || defaultCases.join(","))
    .split(",")
    .map((v) => v.trim().toLowerCase())
    .filter(Boolean);
  const cases = rawCases.filter((name) => name !== "suite" && BENCHMARK_RUNNERS[name]);
  if (cases.length === 0) {
    throw new Error("suite requested but no valid suiteCases were provided");
  }

  const out = {
    suite_cases: cases.length,
    suite_samples: 0,
    suite_ops: 0,
    suite_runs: 1,
  };

  const totalWeight = cases.length;
  let cursor = 0;
  for (let i = 0; i < cases.length; i += 1) {
    const name = cases[i];
    const remainingCases = cases.length - i;
    let units = Math.floor((config.units - cursor) / Math.max(remainingCases, 1));
    if (units < 1) {
      units = 1;
    }
    if (i === cases.length - 1) {
      units = Math.max(1, config.units - cursor);
    }
    const subConfig = {
      units,
      offset: config.offset + cursor,
      shardId: config.shardId,
    };
    const caseStarted = Date.now();
    const result = await BENCHMARK_RUNNERS[name](subConfig);
    const caseDuration = Date.now() - caseStarted;

    out[`${name}_runs`] = 1;
    out[`${name}_durationMs`] = caseDuration;
    out[`${name}_samples`] = Number(result.samples) || units;
    out[`${name}_ops`] = Number(result.ops) || out[`${name}_samples`];
    out.suite_samples += out[`${name}_samples`];
    out.suite_ops += out[`${name}_ops`];

    for (const [key, raw] of Object.entries(result)) {
      const value = Number(raw);
      if (!Number.isFinite(value)) {
        continue;
      }
      out[`${name}_${key}`] = value;
    }
    cursor += units;
  }

  out.ops = out.suite_ops;
  out.samples = out.suite_samples;
  return out;
}

BENCHMARK_RUNNERS.suite = runSuite;

const config = getWorkConfig();
const benchmark = String((args && args.benchmark) || "pi").trim().toLowerCase();
const runner = BENCHMARK_RUNNERS[benchmark];
if (!runner) {
  throw new Error(
    `Unknown benchmark '${benchmark}'. Use one of: ${Object.keys(BENCHMARK_RUNNERS).join(", ")}`,
  );
}

const startedAt = Date.now();
let result = await runner(config);
if (!result || typeof result !== "object") {
  result = { value: result };
}

result.benchmark = benchmark;
result.units = config.units;
result.offset = config.offset;
result.shardId = config.shardId;
result.durationMs = Date.now() - startedAt;

if (!Number.isFinite(Number(result.samples))) {
  result.samples = config.units;
}
if (!Number.isFinite(Number(result.ops))) {
  result.ops = config.units;
}

return result;
