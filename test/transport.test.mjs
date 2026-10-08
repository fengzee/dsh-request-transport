import { test } from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import { randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { createRelay } from '../lib/relay.mjs';
import { createTransport } from '../lib/client.mjs';
import { TYPE, MemoryCache, hash, difference, reconstruct, pack, zip } from '../lib/protocol.mjs';

const token = 'test-only-token-that-is-at-least-32-bytes';
process.env.DSH_TRANSPORT_TEST_TOKEN = token;
const scope = { provider: 'test', model: 'model', sessionId: 'parent' };
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return `http://127.0.0.1:${server.address().port}`; };
const close = server => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
const fixture = suffix => JSON.stringify({ model: 'model', messages: [{ role: 'user', content: fixture.history }, { role: 'user', content: suffix }], tools: [{ name: 'example' }], stream: true });
fixture.history = randomBytes(100000).toString('base64');
async function setup(t, options = {}) {
  const received = [], metrics = [];
  const upstream = http.createServer(async (req, res) => {
    const parts = []; for await (const part of req) parts.push(part);
    received.push({ body: Buffer.concat(parts).toString(), headers: req.headers, path: req.url });
    res.writeHead(options.upstreamStatus ?? 200, { 'content-type': 'text/event-stream', 'x-dsh-transport-retry': 'full', 'x-dsh-transport-version': '1' });
    res.write('data: {"text":"hello"}\n\n');
    setTimeout(() => res.end('data: [DONE]\n\n'), 5);
  });
  const origin = await listen(upstream);
  const relay = createRelay({ upstream: `${origin}/v1`, token, ...options });
  const relayURL = `${await listen(relay.server)}/dsh-transport/v1`;
  const client = createTransport({ routes: [{ provider: 'test', upstream: `${origin}/v1`, relay: relayURL, tokenEnv: 'DSH_TRANSPORT_TEST_TOKEN' }], onMetric: m => metrics.push(m) });
  t.after(async () => { client.dispose(); await close(relay.server); await close(upstream); });
  const send = async (body, s = scope, key = 'upstream-key', path = 'chat/completions') => {
    const response = await client.fetch(`${origin}/v1/${path}`, { method: 'POST', body, headers: { authorization: `Bearer ${key}`, 'content-type': 'application/json' } }, s);
    return { response, text: await response.text() };
  };
  return { received, metrics, relay, client, send, origin, relayURL };
}

test('full gzip then delta preserve exact bytes and stream for three API paths', async t => {
  const f = await setup(t);
  for (const path of ['chat/completions', 'responses', 'messages']) {
    const first = fixture('first'), second = fixture('第二次 😀');
    assert.equal((await f.send(first, scope, 'key', path)).response.status, 200);
    assert.match((await f.send(second, scope, 'key', path)).text, /\[DONE\]/);
    assert.equal(f.received.at(-2).body, first); assert.equal(f.received.at(-1).body, second);
    assert.equal(f.metrics.at(-2).kind, 'full'); assert.equal(f.metrics.at(-1).kind, 'delta');
    assert.ok(f.metrics.at(-1).uploadedBytes < 1500);
    assert.equal(f.received.at(-1).headers['content-encoding'], undefined);
    assert.equal(f.received.at(-1).headers.authorization, 'Bearer key');
  }
});

test('parent, siblings, model, credential and endpoint are independent', async t => {
  const f = await setup(t);
  await f.send(fixture('first'));
  for (const s of [{ ...scope, sessionId: 'child-a' }, { ...scope, sessionId: 'child-b' }, { ...scope, model: 'other' }]) {
    await f.send(fixture('changed'), s); assert.equal(f.metrics.at(-1).kind, 'full');
  }
  await f.send(fixture('changed'), scope, 'rotated-key'); assert.equal(f.metrics.at(-1).kind, 'full');
  await f.send(fixture('changed'), scope, 'upstream-key', 'responses'); assert.equal(f.metrics.at(-1).kind, 'full');
  await f.send(fixture('changed')); assert.equal(f.metrics.at(-1).kind, 'delta');
});

test('missing session never caches, compaction/edit restore exact bytes', async t => {
  const f = await setup(t);
  await f.send(fixture('first'), { ...scope, sessionId: undefined });
  await f.send(fixture('second'), { ...scope, sessionId: undefined });
  assert.deepEqual(f.metrics.map(m => m.kind), ['full', 'full']); assert.equal(f.relay.cache.bytes, 0);
  for (const body of [fixture('first'), '{"messages":[],"model":"model"}', fixture('edited')]) { await f.send(body); assert.equal(f.received.at(-1).body, body); }
});

test('cache eviction resends full once and invokes upstream exactly once', async t => {
  const f = await setup(t);
  await f.send(fixture('first')); f.relay.cache.clear();
  await f.send(fixture('second'));
  assert.equal(f.metrics.at(-1).resync, true); assert.equal(f.received.length, 2);
  assert.equal(f.received.at(-1).body, fixture('second'));
});

test('same-session concurrent and branched calls remain immutable', async t => {
  const f = await setup(t); await f.send(fixture('seed'));
  const bodies = Array.from({ length: 8 }, (_, i) => fixture(`parallel-${i}`));
  await Promise.all(bodies.map(body => f.send(body)));
  assert.deepEqual(f.received.slice(1).map(r => r.body).sort(), [...bodies].sort());
  await f.send(fixture('continued')); assert.equal(f.received.at(-1).body, fixture('continued'));
});

test('upstream errors and forged cache-miss markers do not trigger retry', async t => {
  const f = await setup(t, { upstreamStatus: 409 });
  await f.send(fixture('first')); const second = await f.send(fixture('second'));
  assert.equal(second.response.status, 409); assert.equal(second.response.headers.get('x-dsh-transport-retry'), null);
  assert.equal(f.metrics.at(-1).resync, false); assert.equal(f.received.length, 2);
});

test('authentication, route allowlist, integrity, bounds reject before upstream', async t => {
  const f = await setup(t, { maxBodyBytes: 2048 });
  const meta = { url: `${f.origin}/v1/chat/completions`, provider: 'test', model: 'model', session: hash('parent'), headers: { authorization: 'Bearer key' }, kind: 'full', target: hash('{}'), length: 2 };
  const post = (m, body, auth = token) => fetch(f.relayURL, { method: 'POST', headers: { authorization: `Bearer ${auth}`, 'content-type': TYPE, 'content-encoding': 'gzip' }, body });
  let response = await post(meta, Buffer.from('broken'), 'wrong'); assert.equal(response.status, 401); await response.text();
  response = await post(meta, await zip(pack({ ...meta, url: 'https://untrusted.invalid/v1/chat/completions' }, Buffer.from('{}')))); assert.equal(response.status, 403); await response.text();
  response = await post(meta, await zip(pack(meta, Buffer.from('[]')))); assert.equal(response.status, 400); await response.text();
  response = await post(meta, await zip(Buffer.alloc(100000))); assert.equal(response.status, 413); await response.text();
  assert.equal(f.received.length, 0);
});

test('TTL, LRU and byte limits release cached bodies', () => {
  let now = 0; const cache = new MemoryCache({ maxBytes: 5, maxEntries: 2, ttlMs: 10, now: () => now });
  cache.set('a', Buffer.from('abc')); cache.set('b', Buffer.from('de')); cache.get('a');
  cache.set('c', Buffer.from('f')); assert.equal(cache.get('b'), undefined); assert.equal(cache.bytes, 4);
  assert.equal(cache.set('huge', Buffer.alloc(6)), false);
  now = 11; cache.sweep(); assert.equal(cache.bytes, 0);
});

test('binary difference handles unicode, empty bodies, deletion and random edits', () => {
  for (let i = 0; i < 100; i++) {
    const base = randomBytes(i * 13), body = Buffer.concat([base.subarray(0, i), Buffer.from('中文😀'), base.subarray(i + 3)]);
    const d = difference(base, body);
    assert.deepEqual(reconstruct({ kind: 'delta', base: hash(base), target: hash(body), length: body.length, ...d }, d.payload, base), body);
  }
});

test('network failure is never replayed by transport', async () => {
  let calls = 0;
  const client = createTransport({ routes: [{ provider: 'test', upstream: 'https://example.invalid/v1', relay: 'https://relay.invalid/dsh-transport/v1', tokenEnv: 'DSH_TRANSPORT_TEST_TOKEN' }], fetch: async () => { calls++; throw new Error('connection lost'); } });
  await assert.rejects(client.fetch('https://example.invalid/v1/messages', { method: 'POST', body: '{}' }, scope));
  assert.equal(calls, 1);
});
