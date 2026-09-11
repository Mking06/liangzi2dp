import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { LiangzidpHashV1 } from './liangzidp-hash-v1.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(__dirname, '..');
const wasmPath = resolve(process.env.WASM_PATH || resolve(rootDir, 'wasm', 'sha3_wasm_bg.7b9ca65ddd.wasm'));

const payload = process.argv[2]
  ? JSON.parse(process.argv[2])
  : {
      algorithm: 'LiangzidpHashV1',
      challenge: 'test',
      salt: 'salt',
      difficulty: 1,
      expireAt: Date.now() + 300000
    };

const solver = new LiangzidpHashV1();
await solver.init(wasmPath);
const started = performance.now();
const result = solver.solve(payload);
console.log(JSON.stringify({ ...result, elapsedMs: Math.round(performance.now() - started) }, null, 2));
