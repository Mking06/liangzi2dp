import assert from 'node:assert/strict';
import { once } from 'node:events';
import { resolve, dirname } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';

const __dirname = dirname(fileURLToPath(import.meta.url));
const workerUrl = new URL('../src/worker.js', import.meta.url);
const wasmPath = resolve(__dirname, '../wasm/sha3_wasm_bg.7b9ca65ddd.wasm');

async function firstMessage(worker, timeoutMs = 5000) {
  const timeout = new Promise((_, reject) => {
    setTimeout(() => reject(new Error('worker message timeout')), timeoutMs).unref();
  });
  return Promise.race([once(worker, 'message').then(([message]) => message), timeout]);
}

test('worker reports ready only after the WASM module initializes', async () => {
  const worker = new Worker(workerUrl, { workerData: { wasmPath } });
  try {
    const message = await firstMessage(worker);
    assert.equal(message.type, 'ready');
  } finally {
    await worker.terminate();
  }
});

test('ready worker solves the deterministic production vector', async () => {
  const worker = new Worker(workerUrl, { workerData: { wasmPath } });
  try {
    assert.equal((await firstMessage(worker)).type, 'ready');
    worker.postMessage({
      id: 1,
      payload: {
        algorithm: 'LiangzidpHashV1',
        challenge: '1ed5f5e350e1f351556b8a3ac4330a5ff93aa12a0c2a338dc4da82057856d5d5',
        salt: '2f2c3e3b24783a1a9649',
        difficulty: 144000,
        expireAt: 1780000000000
      }
    });
    const message = await firstMessage(worker, 30000);
    assert.equal(message.ok, true);
    assert.equal(message.result.answer, 42);
  } finally {
    await worker.terminate();
  }
});

test('worker reports initialization failure instead of ready', async () => {
  const worker = new Worker(workerUrl, {
    workerData: { wasmPath: resolve(__dirname, 'missing.wasm') }
  });
  const message = await firstMessage(worker);
  assert.equal(message.type, 'init_error');
  assert.match(message.error, /missing\.wasm|ENOENT/);
});
