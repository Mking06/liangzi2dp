import { parentPort, workerData, threadId } from 'node:worker_threads';
import { LiangzidpHashV1 } from './liangzidp-hash-v1.js';

const solver = new LiangzidpHashV1();
let ready = false;
let initError = null;
let initPromise = null;

async function ensureReady() {
  if (ready) return;
  if (initError) throw initError;
  if (!initPromise) {
    initPromise = solver.init(workerData.wasmPath).then(() => {
      ready = true;
    }).catch((error) => {
      initError = error;
      throw error;
    });
  }
  await initPromise;
}

parentPort.on('message', async (msg) => {
  const started = performance.now();
  try {
    await ensureReady();
    const result = msg.op === 'hash' ? { ok: true, hash: solver.hashV1(msg.payload.input) } : solver.solve(msg.payload);
    parentPort.postMessage({
      id: msg.id,
      ok: true,
      result: {
        ...result,
        elapsedMs: Math.round(performance.now() - started),
        worker: threadId
      }
    });
  } catch (error) {
    parentPort.postMessage({
      id: msg.id,
      ok: false,
      error: error?.stack || String(error),
      worker: threadId,
      elapsedMs: Math.round(performance.now() - started)
    });
  }
});

ensureReady().then(() => {
  parentPort.postMessage({ type: 'ready', worker: threadId });
}).catch((error) => {
  parentPort.postMessage({
    type: 'init_error',
    error: error?.stack || String(error),
    worker: threadId
  });
  setImmediate(() => process.exit(1));
});

