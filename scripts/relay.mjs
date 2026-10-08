#!/usr/bin/env node
import { createRelay } from '../lib/relay.mjs';
import { MemoryCache } from '../lib/protocol.mjs';

function integer(name, fallback, min, max) {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isSafeInteger(value) || value < min || value > max) throw new Error(`Invalid ${name}`);
  return value;
}
try {
  const { server } = createRelay({
    upstream: process.env.UPSTREAM_BASE_URL,
    token: process.env.DSH_TRANSPORT_TOKEN,
    cache: new MemoryCache({
      maxBytes: integer('CACHE_BYTES', 134217728, 0, 1073741824),
      maxEntries: integer('CACHE_ENTRIES', 128, 0, 4096),
      ttlMs: integer('CACHE_TTL_MS', 3600000, 1000, 3600000),
    }),
    maxBodyBytes: integer('MAX_BODY_BYTES', 33554432, 1024, 33554432),
    maxConcurrent: integer('MAX_CONCURRENT', 2, 1, 128),
  });
  server.listen(integer('PORT', 8788, 1, 65535), process.env.HOST ?? '127.0.0.1', () => {
    console.log('DSH transport relay ready');
  });
  for (const signal of ['SIGTERM', 'SIGINT']) process.on(signal, () => {
    server.close(() => process.exit(0));
    setTimeout(() => { server.closeAllConnections(); process.exit(0); }, 10000).unref();
  });
} catch {
  console.error('Invalid relay configuration. Check UPSTREAM_BASE_URL, DSH_TRANSPORT_TOKEN and numeric limits.');
  process.exitCode = 1;
}
