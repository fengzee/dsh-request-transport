import { AsyncLocalStorage } from 'node:async_hooks';
import z from '@deepseek-ai/schemastery';
import { createTransport } from './client.mjs';
import { MemoryCache, MAX_BODY } from './protocol.mjs';

export const name = 'request-transport';
export const Config = z.object({
  routes: z.array(z.object({
    provider: z.string(), upstream: z.string(), relay: z.string(),
    auth: z.union([z.const('api-key'), z.const('relay-token')]).default('relay-token'),
    tokenEnv: z.string().default('DSH_TRANSPORT_TOKEN'), delta: z.boolean().default(true),
  })).default([]),
  maxBodyBytes: z.number().min(1024).max(MAX_BODY).default(MAX_BODY),
  cacheBytes: z.number().min(0).max(512 * 1024 * 1024).default(128 * 1024 * 1024),
  cacheEntries: z.number().min(0).max(1024).default(128),
  cacheTtlMs: z.number().min(1000).max(3600000).default(3600000),
});

export function apply(ctx, config) {
  const scope = new AsyncLocalStorage();
  ctx.effect(() => {
    const previous = globalThis.fetch;
    const transport = createTransport({
      fetch: previous, ...config,
      cache: new MemoryCache({ maxBytes: config.cacheBytes, maxEntries: config.cacheEntries, ttlMs: config.cacheTtlMs }),
    });
    let active = true;
    const wrapped = (input, init) => active ? transport.fetch(input, init, scope.getStore()) : previous(input, init);
    globalThis.fetch = wrapped;
    // Periodically release expired bodies even if there are no more model calls.
    const timer = setInterval(() => transport.cache.sweep(), Math.min(config.cacheTtlMs ?? 3600000, 30000));
    timer.unref();
    return () => {
      active = false; clearInterval(timer); transport.dispose();
      if (globalThis.fetch === wrapped) globalThis.fetch = previous;
    };
  });
  ctx.on('llm/stream', (options, next) => {
    const identity = { provider: options.provider, model: options.model, sessionId: options.sessionId };
    const inner = scope.run(identity, next);
    return {
      [Symbol.asyncIterator]() {
        const iterator = scope.run(identity, () => inner[Symbol.asyncIterator]());
        return {
          next: (...args) => scope.run(identity, () => iterator.next(...args)),
          return: value => scope.run(identity, () => iterator.return?.(value) ?? Promise.resolve({ done: true, value })),
          throw: error => scope.run(identity, () => iterator.throw ? iterator.throw(error) : Promise.reject(error)),
        };
      },
    };
  });
}
