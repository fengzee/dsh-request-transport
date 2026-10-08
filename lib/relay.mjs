import http from 'node:http';
import { randomBytes, timingSafeEqual } from 'node:crypto';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { TYPE, MAX_BODY, MAX_META, MemoryCache, ProtocolError, assert, digestKey, hash, isHash, endpoint, safeURL, unpack, unzip, reconstruct } from './protocol.mjs';

const hop = new Set(['host', 'connection', 'keep-alive', 'proxy-authorization', 'proxy-authenticate', 'te', 'trailer', 'transfer-encoding', 'upgrade', 'content-length', 'content-encoding', 'forwarded']);
function cleanHeaders(source) {
  const headers = new Headers(source);
  const nominated = (headers.get('connection') ?? '').split(',').map(v => v.trim().toLowerCase());
  for (const key of [...headers.keys()]) {
    if (hop.has(key) || nominated.includes(key) || key.startsWith('x-forwarded-') || key.startsWith('x-dsh-transport-')) headers.delete(key);
  }
  return headers;
}
async function readBounded(request, limit) {
  let length = 0; const parts = [];
  for await (const part of request) {
    length += part.length; assert(length <= limit, 'wire_body_too_large', 413); parts.push(part);
  }
  return Buffer.concat(parts);
}

export function createRelay({ upstream, token, cache = new MemoryCache(), maxBodyBytes = MAX_BODY, maxConcurrent = 2, uploadTimeoutMs = 120000, upstreamTimeoutMs = 1800000, fetch: upstreamFetch = globalThis.fetch, onMetric = () => {} }) {
  const upstreamURL = safeURL(upstream);
  assert(typeof token === 'string' && token.length >= 32, 'transport_token_missing_or_short');
  const tokenHash = Buffer.from(hash(token), 'hex'), secret = randomBytes(32);
  let active = 0;
  const notify = metric => { try { onMetric(Object.freeze(metric)); } catch { /* Never change request semantics. */ } };
  const server = http.createServer(async (req, res) => {
    res.setHeader('cache-control', 'no-store');
    const fail = (status, code, retry = false) => {
      res.setHeader('x-dsh-transport-version', '1');
      if (retry) res.setHeader('x-dsh-transport-retry', 'full');
      res.writeHead(status, { 'content-type': 'application/json', connection: 'close' });
      res.end(JSON.stringify({ error: { code, message: code } }));
    };
    if (req.url === '/healthz' && req.method === 'GET') { res.end('ok'); return; }
    if (req.url !== '/dsh-transport/v1' || req.method !== 'POST') { fail(404, 'not_found'); return; }
    const received = req.headers.authorization;
    if (typeof received !== 'string' || !received.startsWith('Bearer ') || !timingSafeEqual(Buffer.from(hash(received.slice(7)), 'hex'), tokenHash)) { fail(401, 'unauthorized'); return; }
    if (active >= maxConcurrent) { fail(429, 'relay_busy'); return; }
    active++;
    const controller = new AbortController();
    const abort = () => controller.abort();
    res.on('close', abort);
    const uploadTimer = setTimeout(() => req.destroy(), uploadTimeoutMs);
    uploadTimer.unref();
    let upstreamTimer;
    try {
      assert(req.headers['content-type'] === TYPE, 'unsupported_content_type', 415);
      assert(req.headers['content-encoding'] === 'gzip', 'gzip_required', 415);
      const wire = await readBounded(req, maxBodyBytes + MAX_META + 65536);
      clearTimeout(uploadTimer);
      let decoded;
      try { decoded = await unzip(wire, { maxOutputLength: maxBodyBytes + MAX_META + 4 }); }
      catch { throw new ProtocolError('invalid_or_oversize_gzip', 413); }
      const { meta, payload } = unpack(decoded);
      assert(typeof meta.url === 'string' && endpoint(upstreamURL, meta.url), 'upstream_not_allowed', 403);
      assert(typeof meta.provider === 'string' && meta.provider.length <= 256 && typeof meta.model === 'string' && meta.model.length <= 256, 'invalid_scope');
      assert(meta.session === null || isHash(meta.session), 'invalid_session');
      assert(meta.headers && typeof meta.headers === 'object' && !Array.isArray(meta.headers) && Object.values(meta.headers).every(v => typeof v === 'string'), 'invalid_headers');
      // Bind all original headers (including every credential and tenant header).
      const namespace = digestKey(secret, [hash(token), meta.url, meta.provider, meta.model, meta.session, Object.entries(meta.headers).sort()]);
      const base = meta.session && isHash(meta.base) ? cache.get(`${namespace}:${meta.base}`) : undefined;
      const body = reconstruct(meta, payload, base, maxBodyBytes);
      // Requests without a DSH session are compressed but never cached.
      const saved = meta.session !== null && cache.set(`${namespace}:${meta.target}`, body);
      const headers = cleanHeaders(meta.headers);
      headers.set('content-type', 'application/json');
      headers.set('accept-encoding', 'identity');
      upstreamTimer = setTimeout(abort, upstreamTimeoutMs); upstreamTimer.unref();
      const response = await upstreamFetch(meta.url, { method: 'POST', headers, body, signal: controller.signal, redirect: 'manual' });
      // Never redirect a credential-bearing POST or let upstream forge a retry marker.
      if (response.status >= 300 && response.status < 400) {
        await response.body?.cancel();
        throw new ProtocolError('upstream_redirect_refused', 502);
      }
      const responseHeaders = cleanHeaders(response.headers);
      responseHeaders.set('cache-control', 'no-store');
      responseHeaders.set('x-accel-buffering', 'no');
      responseHeaders.set('x-dsh-transport-version', '1');
      responseHeaders.set('x-dsh-transport-mode', meta.kind);
      if (saved) responseHeaders.set('x-dsh-transport-ack', meta.target);
      res.writeHead(response.status, Object.fromEntries(responseHeaders));
      res.flushHeaders();
      notify({ kind: meta.kind, uploadedBytes: wire.length, originalBytes: body.length, reusedBytes: body.length - payload.length, newBytes: payload.length, frameBytes: decoded.length, resync: meta.resync === true, status: response.status });
      if (response.body) await pipeline(Readable.fromWeb(response.body), res); else res.end();
    } catch (error) {
      if (res.headersSent) res.destroy();
      else if (!res.destroyed) {
        const known = error instanceof ProtocolError;
        fail(known ? error.status : 502, known ? error.code : 'relay_error', known && error.code === 'cache_miss');
      }
    } finally {
      clearTimeout(uploadTimer); clearTimeout(upstreamTimer); res.off('close', abort); active--;
    }
  });
  server.requestTimeout = uploadTimeoutMs;
  server.headersTimeout = Math.min(uploadTimeoutMs, 30000);
  const sweep = setInterval(() => cache.sweep(), Math.min(cache.ttlMs, 30000)); sweep.unref();
  server.on('close', () => { clearInterval(sweep); cache.clear(); });
  return { server, cache };
}
