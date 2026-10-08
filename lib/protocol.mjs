import { createHash, createHmac } from 'node:crypto';
import { gzip, gunzip, createGzip } from 'node:zlib';
import { Readable } from 'node:stream';
import { promisify } from 'node:util';

export const zip = promisify(gzip);
export const unzip = promisify(gunzip);
export const TYPE = 'application/vnd.dsh-request-transport.v1';
export const MAX_BODY = 32 * 1024 * 1024;
export const MAX_META = 32 * 1024;
export const hash = data => createHash('sha256').update(data).digest('hex');
export const digestKey = (key, fields) => createHmac('sha256', key).update(JSON.stringify(fields)).digest('hex');
export const isHash = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);

export class ProtocolError extends Error {
  constructor(code, status = 400) { super(code); this.code = code; this.status = status; }
}
export function assert(condition, code, status) {
  if (!condition) throw new ProtocolError(code, status);
}
export function endpoint(base, target) {
  const b = new URL(base), t = new URL(target);
  const prefix = b.pathname.replace(/\/$/, '');
  const nativeBeta = t.pathname === `${prefix}/messages` && t.search === '?beta=true';
  return !t.username && !t.password && !t.hash && (!t.search || nativeBeta) && t.origin === b.origin &&
    ['chat/completions', 'responses', 'messages'].some(p => t.pathname === `${prefix}/${p}`);
}
export function safeURL(value) {
  const u = new URL(value);
  assert(!u.username && !u.password && !u.hash && !u.search, 'invalid_url');
  assert(u.protocol === 'https:' || (u.protocol === 'http:' && ['localhost', '127.0.0.1', '[::1]'].includes(u.hostname)), 'https_required');
  return u.href;
}
export function pack(meta, payload) {
  const header = Buffer.from(JSON.stringify({ v: 1, ...meta }));
  assert(header.length <= MAX_META, 'metadata_too_large', 413);
  const size = Buffer.alloc(4); size.writeUInt32BE(header.length);
  return Buffer.concat([size, header, payload]);
}
// Feed the existing payload buffer directly into zlib; avoid copying a full frame.
export async function zipFrame(meta, payload) {
  const compressor = createGzip({ level: 6 });
  const source = Readable.from([pack(meta, Buffer.alloc(0)), payload]);
  source.on('error', error => compressor.destroy(error));
  source.pipe(compressor);
  const parts = [];
  for await (const part of compressor) parts.push(part);
  return Buffer.concat(parts);
}
export function unpack(frame) {
  assert(frame.length >= 4, 'invalid_frame');
  const size = frame.readUInt32BE(0);
  assert(size <= MAX_META && size + 4 <= frame.length, 'invalid_metadata');
  let meta;
  try { meta = JSON.parse(frame.subarray(4, 4 + size).toString()); } catch { throw new ProtocolError('invalid_metadata'); }
  assert(meta && meta.v === 1 && typeof meta === 'object', 'unsupported_version');
  return { meta, payload: frame.subarray(4 + size) };
}
export function difference(base, body) {
  let prefix = 0, suffix = 0;
  while (prefix < Math.min(base.length, body.length) && base[prefix] === body[prefix]) prefix++;
  while (suffix < Math.min(base.length, body.length) - prefix && base[base.length - suffix - 1] === body[body.length - suffix - 1]) suffix++;
  return { prefix, suffix, payload: body.subarray(prefix, body.length - suffix) };
}
export function reconstruct(meta, payload, base, maxBody = MAX_BODY) {
  assert(isHash(meta.target), 'invalid_target');
  assert(Number.isSafeInteger(meta.length) && meta.length >= 0 && meta.length <= maxBody, 'body_too_large', 413);
  let body;
  if (meta.kind === 'full') body = payload;
  else {
    assert(meta.kind === 'delta' && isHash(meta.base), 'invalid_delta');
    assert(base, 'cache_miss', 409);
    const { prefix, suffix } = meta;
    assert(Number.isSafeInteger(prefix) && Number.isSafeInteger(suffix) && prefix >= 0 && suffix >= 0 && prefix + suffix <= base.length, 'invalid_range');
    assert(prefix + payload.length + suffix === meta.length, 'invalid_length');
    body = Buffer.concat([base.subarray(0, prefix), payload, base.subarray(base.length - suffix)], meta.length);
  }
  assert(body.length === meta.length && hash(body) === meta.target, 'body_integrity_failed');
  return body;
}

// Fixed expiry bounds retained sensitive data even when a session is repeatedly read.
export class MemoryCache {
  constructor({ maxBytes = 128 * 1024 * 1024, maxEntries = 128, ttlMs = 3600000, now = Date.now } = {}) {
    this.entries = new Map(); this.bytes = 0;
    Object.assign(this, { maxBytes, maxEntries, ttlMs, now });
  }
  delete(key) { const e = this.entries.get(key); if (e) this.bytes -= e.body.length; this.entries.delete(key); }
  sweep() { for (const [key, entry] of this.entries) if (entry.expires <= this.now()) this.delete(key); }
  get(key) {
    this.sweep(); const entry = this.entries.get(key);
    if (entry) { this.entries.delete(key); this.entries.set(key, entry); }
    return entry?.body;
  }
  set(key, body) {
    this.sweep();
    if (!this.maxEntries || body.length > this.maxBytes) return false;
    this.delete(key);
    while (this.entries.size >= this.maxEntries || this.bytes + body.length > this.maxBytes) this.delete(this.entries.keys().next().value);
    this.entries.set(key, { body: Buffer.from(body), expires: this.now() + this.ttlMs });
    this.bytes += body.length; return true;
  }
  clear() { this.entries.clear(); this.bytes = 0; }
}
