import { createServer } from 'node:http';
import { cpus } from 'node:os';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';

const __dirname = dirname(fileURLToPath(import.meta.url));
const rootDir = resolve(__dirname, '..');

const HOST = process.env.HOST || '127.0.0.1';
const PORT = Number(process.env.PORT || 8787);
const WASM_PATH = resolve(process.env.WASM_PATH || resolve(rootDir, 'wasm', 'sha3_wasm_bg.7b9ca65ddd.wasm'));
const WORKER_COUNT = Math.max(1, Number(process.env.POW_WORKERS || Math.max(1, cpus().length - 1)));
const TIMEOUT_MS = Math.max(1000, Number(process.env.POW_TIMEOUT_MS || 30000));
const MAX_BODY_BYTES = Math.max(1024, Number(process.env.MAX_BODY_BYTES || 64 * 1024));

class WorkerPool {
  constructor({ size, wasmPath }) {
    this.size = size;
    this.wasmPath = wasmPath;
    this.workers = [];
    this.idSeq = 0;
    this.next = 0;
    this.pending = new Map();
    this.readyCount = 0;
    this.completedCount = 0;
    this.failedCount = 0;
    this.timeoutCount = 0;
    this.restartCount = 0;
    this.initializationFailureCount = 0;

    for (let i = 0; i < size; i += 1) this.spawn(i);
  }

  spawn(slot) {
    const worker = new Worker(new URL('./worker.js', import.meta.url), {
      workerData: { wasmPath: this.wasmPath }
    });
    worker.slot = slot;
    worker.ready = false;
    worker.exited = false;
    worker.on('message', (message) => {
      if (message?.type === 'ready') {
        if (!worker.ready && this.workers[slot] === worker) {
          worker.ready = true;
          this.readyCount += 1;
        }
        return;
      }
      if (message?.type === 'init_error') {
        this.initializationFailureCount += 1;
        this.failWorker(worker, new Error(message.error || 'worker initialization failed'));
        void worker.terminate();
        return;
      }
      const item = this.pending.get(message.id);
      if (!item) return;
      this.pending.delete(message.id);
      clearTimeout(item.timer);
      if (message.ok) {
        this.completedCount += 1;
        item.resolve(message.result);
      } else {
        this.failedCount += 1;
        item.reject(new Error(message.error || 'worker failed'));
      }
    });
    worker.on('error', (error) => this.failWorker(worker, error));
    worker.on('exit', (code) => {
      worker.exited = true;
      if (worker.ready) {
        worker.ready = false;
        this.readyCount = Math.max(0, this.readyCount - 1);
      }
      if (this.workers[slot] === worker) this.workers[slot] = null;
      this.failWorker(worker, new Error(`worker exited with code ${code}`));
      this.restartCount += 1;
      setTimeout(() => this.spawn(slot), 250);
    });
    this.workers[slot] = worker;
  }

  failWorker(worker, error) {
    for (const [id, item] of this.pending.entries()) {
      if (item.worker === worker) {
        this.pending.delete(id);
        clearTimeout(item.timer);
        item.reject(error);
      }
    }
  }

  exec(payload) {
    const liveWorkers = this.workers.filter((worker) => worker && !worker.exited);
    if (liveWorkers.length === 0) throw new Error('no workers available');
    const readyWorkers = liveWorkers.filter((worker) => worker.ready);
    const candidates = readyWorkers.length ? readyWorkers : liveWorkers;
    const id = ++this.idSeq;
    const worker = candidates[this.next++ % candidates.length];

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        this.timeoutCount += 1;
        reject(new Error(`POW solve timeout after ${TIMEOUT_MS}ms`));
      }, TIMEOUT_MS);

      this.pending.set(id, { resolve, reject, timer, worker });
      worker.postMessage({ id, payload, op: payload.__op });
    });
  }

  stats() {
    return {
      workers: this.workers.filter((worker) => worker && !worker.exited).length,
      readyWorkers: this.readyCount,
      pending: this.pending.size,
      completed: this.completedCount,
      failed: this.failedCount,
      timeouts: this.timeoutCount,
      restarts: this.restartCount,
      initializationFailures: this.initializationFailureCount,
      wasmPath: this.wasmPath,
      timeoutMs: TIMEOUT_MS
    };
  }
}

const pool = new WorkerPool({ size: WORKER_COUNT, wasmPath: WASM_PATH });

function sendJson(res, status, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(body),
    'cache-control': 'no-store'
  });
  res.end(body);
}

async function readJson(req) {
  return new Promise((resolveRead, rejectRead) => {
    let size = 0;
    const chunks = [];
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > MAX_BODY_BYTES) {
        req.destroy(new Error(`request body too large, max ${MAX_BODY_BYTES} bytes`));
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => {
      try {
        const text = Buffer.concat(chunks).toString('utf8');
        resolveRead(text ? JSON.parse(text) : {});
      } catch (error) {
        rejectRead(error);
      }
    });
    req.on('error', rejectRead);
  });
}

const server = createServer(async (req, res) => {
  try {
    const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);

    if (req.method === 'GET' && url.pathname === '/health') {
      const stats = pool.stats();
      const healthy = stats.readyWorkers > 0;
      sendJson(res, healthy ? 200 : 503, { ok: healthy, ...stats });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/solve') {
      const payload = await readJson(req);
      const started = performance.now();
      const result = await pool.exec(payload);
      sendJson(res, result.ok ? 200 : 422, {
        ...result,
        totalElapsedMs: Math.round(performance.now() - started)
      });
      return;
    }

    if (req.method === 'POST' && url.pathname === '/hash') {
      const payload = await readJson(req);
      if (typeof payload.input !== 'string') throw new Error('payload.input must be a string');
      const result = await pool.exec({ __op: 'hash', input: payload.input });
      sendJson(res, 200, result);
      return;
    }
    sendJson(res, 404, { ok: false, error: 'not found' });
  } catch (error) {
    sendJson(res, 400, { ok: false, error: error?.message || String(error) });
  }
});

server.listen(PORT, HOST, () => {
  console.log(JSON.stringify({
    ok: true,
    service: 'liangzidp-pow-service',
    listen: `http://${HOST}:${PORT}`,
    workers: WORKER_COUNT,
    wasmPath: WASM_PATH,
    timeoutMs: TIMEOUT_MS
  }));
});

