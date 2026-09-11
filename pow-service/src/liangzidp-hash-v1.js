import { readFile } from 'node:fs/promises';

export class LiangzidpHashV1 {
  constructor() {
    this.exports = null;
    this.cachedUint8Memory = null;
    this.textEncoder = new TextEncoder();
    this.textDecoder = new TextDecoder('utf-8', { ignoreBOM: true, fatal: true });
    this.wasmVectorLen = 0;
  }

  async init(wasmPath) {
    const wasmBuffer = await readFile(wasmPath);
    const { instance } = await WebAssembly.instantiate(wasmBuffer, { wbg: {} });
    this.exports = instance.exports;
    this.cachedUint8Memory = null;

    for (const name of [
      'memory',
      '__wbindgen_add_to_stack_pointer',
      '__wbindgen_export_0',
      '__wbindgen_export_1',
      'wasm_solve',
      'wasm_liangzidp_hash_v1',
      '__wbindgen_export_2'
    ]) {
      if (!(name in this.exports)) {
        throw new Error(`WASM export missing: ${name}`);
      }
    }
  }

  u8() {
    if (!this.cachedUint8Memory || this.cachedUint8Memory.byteLength === 0) {
      this.cachedUint8Memory = new Uint8Array(this.exports.memory.buffer);
    }
    return this.cachedUint8Memory;
  }

  passStringToWasm0(text, malloc, realloc) {
    text = String(text);

    if (!realloc) {
      const encoded = this.textEncoder.encode(text);
      const ptr = malloc(encoded.length, 1) >>> 0;
      this.u8().subarray(ptr, ptr + encoded.length).set(encoded);
      this.wasmVectorLen = encoded.length;
      return ptr;
    }

    const originalLength = text.length;
    let ptr = malloc(originalLength, 1) >>> 0;
    const mem = this.u8();

    let offset = 0;
    for (; offset < originalLength; offset += 1) {
      const code = text.charCodeAt(offset);
      if (code > 0x7f) break;
      mem[ptr + offset] = code;
    }

    if (offset !== originalLength) {
      if (offset !== 0) text = text.slice(offset);
      ptr = realloc(ptr, originalLength, offset + text.length * 3, 1) >>> 0;
      const view = this.u8().subarray(ptr + offset, ptr + offset + text.length * 3);
      const result = this.textEncoder.encodeInto(text, view);
      offset += result.written;
      ptr = realloc(ptr, offset + text.length * 3, offset, 1) >>> 0;
    }

    this.wasmVectorLen = offset;
    return ptr;
  }

  getStringFromWasm0(ptr, len) {
    ptr = ptr >>> 0;
    return this.textDecoder.decode(this.u8().subarray(ptr, ptr + len));
  }

  hashV1(input) {
    if (!this.exports) throw new Error('WASM not initialized');

    const retptr = this.exports.__wbindgen_add_to_stack_pointer(-16) >>> 0;
    let outPtr = 0;
    let outLen = 0;

    try {
      const ptr0 = this.passStringToWasm0(
        input,
        this.exports.__wbindgen_export_0,
        this.exports.__wbindgen_export_1
      );
      const len0 = this.wasmVectorLen;
      this.exports.wasm_liangzidp_hash_v1(retptr, ptr0, len0);

      const view = new DataView(this.exports.memory.buffer);
      outPtr = view.getInt32(retptr + 0, true);
      outLen = view.getInt32(retptr + 4, true);
      return this.getStringFromWasm0(outPtr, outLen);
    } finally {
      this.exports.__wbindgen_add_to_stack_pointer(16);
      if (outPtr && outLen) this.exports.__wbindgen_export_2(outPtr, outLen, 1);
    }
  }
  solve(payload) {
    if (!this.exports) throw new Error('WASM not initialized');

    const algorithm = payload.algorithm ?? 'LiangzidpHashV1';
    if (algorithm !== 'LiangzidpHashV1') {
      throw new Error(`Unsupported algorithm: ${algorithm}`);
    }

    const challenge = payload.challenge;
    const salt = payload.salt;
    const difficulty = Number(payload.difficulty);
    const expireAt = payload.expireAt;

    if (typeof challenge !== 'string' || challenge.length === 0) {
      throw new Error('payload.challenge must be a non-empty string');
    }
    if (typeof salt !== 'string' || salt.length === 0) {
      throw new Error('payload.salt must be a non-empty string');
    }
    if (!Number.isFinite(difficulty)) {
      throw new Error('payload.difficulty must be a finite number');
    }
    if (expireAt === undefined || expireAt === null || String(expireAt).length === 0) {
      throw new Error('payload.expireAt is required');
    }

    const prefix = `${salt}_${expireAt}_`;
    const malloc = this.exports.__wbindgen_export_0;
    const realloc = this.exports.__wbindgen_export_1;
    const retptr = this.exports.__wbindgen_add_to_stack_pointer(-16) >>> 0;

    try {
      const ptr0 = this.passStringToWasm0(challenge, malloc, realloc);
      const len0 = this.wasmVectorLen;
      const ptr1 = this.passStringToWasm0(prefix, malloc, realloc);
      const len1 = this.wasmVectorLen;

      this.exports.wasm_solve(retptr, ptr0, len0, ptr1, len1, difficulty);

      const view = new DataView(this.exports.memory.buffer);
      const status = view.getInt32(retptr + 0, true);
      const rawAnswer = view.getFloat64(retptr + 8, true);

      if (status === 0 || !Number.isFinite(rawAnswer)) {
        return { ok: false, answer: null, status, rawAnswer };
      }

      return {
        ok: true,
        answer: Math.trunc(rawAnswer),
        status,
        rawAnswer
      };
    } finally {
      this.exports.__wbindgen_add_to_stack_pointer(16);
    }
  }
}

