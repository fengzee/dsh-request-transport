import { randomBytes } from 'node:crypto';
import { TYPE, MAX_BODY, MemoryCache, hash, digestKey, difference, endpoint, safeURL, zipFrame, assert } from './protocol.mjs';

const signed = headers => [...headers.keys()].some(k => /^(signature|signature-input|digest|content-md5|x-amz-)/i.test(k)) || /^AWS4-/i.test(headers.get('authorization') ?? '');

export function createTransport({ fetch: originalFetch = globalThis.fetch, routes = [], maxBodyBytes = MAX_BODY, cache = new MemoryCache(), onMetric = () => {} } = {}) {
  const secret = randomBytes(32);
  const compiled = routes.map(r => ({ ...r, upstream: safeURL(r.upstream), relay: safeURL(r.relay) }));
  const notify = metric => { try { onMetric(Object.freeze(metric)); } catch { /* Metrics cannot replay or break a model request. */ } };
  async function transport(input, init, scope) {
    const url = input instanceof Request ? input.url : String(input);
    const route = scope && compiled.find(r => r.provider === scope.provider && endpoint(r.upstream, url));
    if (!route) return originalFetch(input, init);
    const method = init?.method ?? (input instanceof Request ? input.method : 'GET');
    const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
    if (method.toUpperCase() !== 'POST' || signed(headers)) return originalFetch(input, init);
    assert(!headers.has('content-encoding') || headers.get('content-encoding') === 'identity', 'disable_other_request_compression_plugins');
    const token = route.auth === 'api-key' ? (headers.get('authorization')?.replace(/^Bearer /, '') || headers.get('x-api-key')) : process.env[route.tokenEnv];
    assert(typeof token === 'string' && token.length >= (route.auth === 'api-key' ? 1 : 32), 'transport_token_missing_or_short');
    const request = new Request(input, init);
    assert(!request.bodyUsed, 'body_already_used');
    const chunks = []; let length = 0;
    if (request.body) {
      for await (const part of request.body) {
        length += part.byteLength;
        assert(length <= maxBodyBytes, 'body_too_large', 413);
        chunks.push(Buffer.from(part));
      }
    }
    const body = Buffer.concat(chunks);
    const requestHeaders = Object.fromEntries(headers);
    const session = scope.sessionId ? hash(String(scope.sessionId)) : null;
    const meta = { url, provider: scope.provider, model: scope.model ?? '', session, headers: requestHeaders, target: hash(body), length: body.length };
    // Header values bind API keys and tenants without retaining credentials in map keys.
    const key = digestKey(secret, [route.relay, token, url, meta.provider, meta.model, session, [...headers].sort()]);
    const base = session && route.delta !== false ? cache.get(key) : undefined;
    const full = await zipFrame({ ...meta, kind: 'full' }, body);
    let wire = full, kind = 'full';
    if (base) {
      const diff = difference(base, body);
      const candidate = await zipFrame({ ...meta, kind: 'delta', base: hash(base), prefix: diff.prefix, suffix: diff.suffix }, diff.payload);
      if (candidate.length + 64 < full.length) { wire = candidate; kind = 'delta'; }
    }
    const send = data => originalFetch(route.relay, {
      method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-type': TYPE, 'content-encoding': 'gzip' },
      body: data, signal: request.signal, redirect: 'error',
    });
    let response = await send(wire), uploaded = wire.length, resync = false;
    // This response is emitted only before any upstream dispatch. Never replay an ambiguous failure.
    if (kind === 'delta' && response.status === 409 && response.headers.get('x-dsh-transport-retry') === 'full' && response.headers.get('x-dsh-transport-version') === '1') {
      await response.body?.cancel();
      cache.delete(key);
      const recovery = await zipFrame({ ...meta, kind: 'full', resync: true }, body);
      response = await send(recovery); uploaded += recovery.length; resync = true;
    }
    if (session && route.delta !== false && response.headers.get('x-dsh-transport-ack') === meta.target) cache.set(key, body);
    notify({ kind, originalBytes: body.length, uploadedBytes: uploaded, resync, status: response.status });
    return response;
  }
  return { fetch: transport, dispose: () => cache.clear(), cache };
}
