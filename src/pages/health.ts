import type { APIRoute } from 'astro';
import { inflight, eventLoopLag, cacheWarmed, cacheWarmedAt, getCacheStats, getRollingMetrics } from '../middleware';
import { getQueryCacheSize } from '../lib/db';
import { dbMeta } from '../lib/d1-adapter';

export const prerender = false;
const startTime = Date.now();

export const GET: APIRoute = async ({ locals }) => {
  const env = (locals as any).runtime?.env || {};
  const dbResults: Record<string, boolean> = {};
  for (const [key, db] of Object.entries(env)) {
    if (db && typeof (db as any).prepare === 'function') {
      try {
        const row = await (db as any).prepare('SELECT 1 AS ok').first();
        dbResults[key] = row?.ok === 1;
      } catch { dbResults[key] = false; }
    }
  }

  const allDbOk = Object.keys(dbResults).length > 0 && Object.values(dbResults).every(v => v);
  const mem = process.memoryUsage();
  const cache = getCacheStats();
  const demand = getRollingMetrics();

  return new Response(JSON.stringify({
    status: allDbOk ? 'ok' : 'degraded',
    uptime: Math.round((Date.now() - startTime) / 1000),
    memMB: Math.round(mem.rss / 1048576),
    heapMB: Math.round(mem.heapUsed / 1048576),
    lagMs: Math.round(eventLoopLag * 100) / 100,
    inflight,
    dbs: dbResults,
    cache: { warmed: cacheWarmed, warmedAt: cacheWarmedAt, ...cache, query: getQueryCacheSize() },
    demand: { ...demand, queueDepth: inflight },
    db: { mmapMB: Math.round(dbMeta.mmapSize / 1048576), fileMB: Math.round(dbMeta.fileSizeBytes / 1048576) },
  }), {
    status: allDbOk ? 200 : 503,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
  });
};
