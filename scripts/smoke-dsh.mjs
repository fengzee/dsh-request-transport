// Uses the installed, unmodified DSH runtime and real pi-ai HTTP adapter against a local mock.
import { createRequire } from 'node:module';
import { existsSync } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import http from 'node:http';
import { once } from 'node:events';
import { randomBytes } from 'node:crypto';
import assert from 'node:assert/strict';
import { createRelay } from '../lib/relay.mjs';
import * as plugin from '../lib/plugin.mjs';

const requireDSH = createRequire(resolve(process.env.DSH_APP ?? '.', 'package.json'));
const load = name => import(pathToFileURL(requireDSH.resolve(name)).href);
const version = requireDSH('@deepseek-ai/dsh-llm/package.json').version;
assert.equal(version, '0.2.0-rc.2', 'This smoke test targets exactly DSH 0.2.0-rc.2');
const { Context } = await load('@deepseek-ai/cordis');
const { LlmRuntime, createUserMessage } = await load('@deepseek-ai/dsh-llm');
const { PiAiAdapter } = await load('@deepseek-ai/dsh-llm-pi-ai');
const piRoot = requireDSH.resolve.paths('@earendil-works/pi-ai').map(p => resolve(p, '@earendil-works/pi-ai')).find(p => existsSync(resolve(p, 'package.json')));
const { openAICompletionsApi } = await import(pathToFileURL(resolve(piRoot, 'dist/api/openai-completions.lazy.js')).href);
const listen = async server => { server.listen(0, '127.0.0.1'); await once(server, 'listening'); return `http://127.0.0.1:${server.address().port}`; };
const close = server => new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
const bodies = [], metrics = [];
const upstream = http.createServer(async (req, res) => {
  const parts = []; for await (const part of req) parts.push(part);
  bodies.push(JSON.parse(Buffer.concat(parts).toString()));
  res.writeHead(200, { 'content-type': 'text/event-stream' });
  for (const chunk of [
    { id: 'mock-response', object: 'chat.completion.chunk', model: 'transport-smoke', choices: [{ index: 0, delta: { role: 'assistant', content: 'ok' }, finish_reason: null }] },
    { id: 'mock-response', object: 'chat.completion.chunk', model: 'transport-smoke', choices: [{ index: 0, delta: {}, finish_reason: 'stop' }], usage: { prompt_tokens: 10, completion_tokens: 1, total_tokens: 11 } },
  ]) res.write(`data: ${JSON.stringify(chunk)}\n\n`);
  res.end('data: [DONE]\n\n');
});
const origin = await listen(upstream);
const token = randomBytes(32).toString('hex');
process.env.DSH_TRANSPORT_SMOKE_TOKEN = token;
const relay = createRelay({ upstream: `${origin}/v1`, token, onMetric: m => metrics.push(m) });
const relayURL = `${await listen(relay.server)}/dsh-transport/v1`;
const ctx = new Context();
const originalFetch = globalThis.fetch;
try {
  await ctx.plugin(LlmRuntime).await();
  const fork = ctx.plugin(plugin, {
    routes: [{ provider: 'transport-smoke', upstream: `${origin}/v1`, relay: relayURL, tokenEnv: 'DSH_TRANSPORT_SMOKE_TOKEN' }],
  });
  await fork.await();
  const api = openAICompletionsApi();
  const model = { id: 'transport-smoke', name: 'Transport smoke', api: 'openai-completions', provider: 'transport-smoke', baseUrl: `${origin}/v1`, input: ['text'], reasoning: false, contextWindow: 262144, maxTokens: 100, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 } };
  const provider = {
    id: 'transport-smoke', name: 'Transport smoke', baseUrl: `${origin}/v1`, getModels: () => [model],
    auth: { apiKey: { name: 'mock', resolve: async ({ credential }) => ({ auth: { apiKey: credential?.key }, source: 'mock' }) } },
    stream: (...args) => api.stream(...args), streamSimple: (...args) => api.streamSimple(...args),
  };
  const profiles = new Map([['transport-smoke', { provider: 'transport-smoke', displayName: 'Smoke', piProvider: provider, modelErrors: new Map(), configuredMaxTokens: new Map(), streamIdleTimeoutMs: 10000 }]]);
  ctx.llm.registerAdapter(['transport-smoke'], new PiAiAdapter({ profiles: () => profiles, resolveApiKey: async () => 'mock-key' }));
  const history = randomBytes(100000).toString('base64');
  const run = async (sessionId, last) => {
    const options = Object.freeze({ provider: 'transport-smoke', model: 'transport-smoke', sessionId, messages: [createUserMessage({ content: [{ type: 'text', text: history }], source: { kind: 'user' } }), createUserMessage({ content: [{ type: 'text', text: last }], source: { kind: 'user' } })] });
    const chunks = []; for await (const chunk of ctx.llm.stream(options)) chunks.push(chunk);
    const finish = chunks.find(c => c.type === 'finish');
    assert.equal(finish?.reason?.kind, 'stop', JSON.stringify(finish));
  };
  await run('parent', 'first'); await run('parent', 'second');
  await Promise.all([run('child-a', 'a-first'), run('child-b', 'b-first')]);
  await Promise.all([run('child-a', 'a-second'), run('child-b', 'b-second')]);
  relay.cache.clear(); await run('parent', 'after-eviction');
  assert.deepEqual(metrics.map(m => m.kind).slice(0, 2), ['full', 'delta']);
  assert.equal(metrics.filter(m => m.kind === 'delta').length, 3);
  assert.equal(bodies.length, 7);
  assert.deepEqual(bodies.slice(2, 4).map(b => b.messages.at(-1).content).sort(), ['a-first', 'b-first']);
  assert.deepEqual(bodies.slice(4, 6).map(b => b.messages.at(-1).content).sort(), ['a-second', 'b-second']);
  await fork.dispose();
  assert.equal(globalThis.fetch, originalFetch);
  console.log(JSON.stringify({ dsh: version, modelRequests: bodies.length, modes: metrics.map(m => m.kind), parentDeltaBytes: metrics[1].uploadedBytes, originalBytes: metrics[1].originalBytes, pluginDisposed: true }));
} finally {
  await ctx.fiber?.dispose(); delete process.env.DSH_TRANSPORT_SMOKE_TOKEN;
  await close(relay.server); await close(upstream);
}
