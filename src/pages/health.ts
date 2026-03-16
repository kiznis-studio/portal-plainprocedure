import type { APIRoute } from 'astro';
import { readFileSync } from 'node:fs';
import { inflight, eventLoopLag, cacheWarmed, cacheWarmedAt, getCacheStats, getRollingMetrics } from '../middleware';
import { getQueryCacheSize } from '../lib/db';
import { dbMeta } from '../lib/d1-adapter';

export const prerender = false;
const startTime = Date.now();

// Read cgroup memory for live usage
function getContainerMemory() {
  try {
    const maxRaw = readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim();
    const current = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
    const limitBytes = maxRaw === 'max' ? null : parseInt(maxRaw, 10);
    const limitMB = limitBytes ? Math.round(limitBytes / 1048576) : null;
    const currentMB = Math.round(current / 1048576);
    const headroomMB = limitMB ? limitMB - currentMB : null;
    const usagePct = limitMB ? Math.round(currentMB / limitMB * 1000) / 1000 : null;
    return { limitMB, currentMB, headroomMB, usagePct };
  } catch {
    return { limitMB: null, currentMB: null, headroomMB: null, usagePct: null };
  }
}

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
  const container = getContainerMemory();

  // Memory warnings
  const warnings: string[] = [];
  if (container.usagePct !== null && container.usagePct > 0.85) {
    warnings.push(`memory_pressure: ${Math.round(container.usagePct * 100)}% used — consider increasing container limit`);
  }
  if (container.headroomMB !== null && container.headroomMB < 50) {
    warnings.push(`low_headroom: only ${container.headroomMB}MB free — OOM risk`);
  }

  return new Response(JSON.stringify({
    status: allDbOk ? 'ok' : 'degraded',
    uptime: Math.round((Date.now() - startTime) / 1000),
    process: {
      rssMB: Math.round(mem.rss / 1048576),
      heapUsedMB: Math.round(mem.heapUsed / 1048576),
      heapTotalMB: Math.round(mem.heapTotal / 1048576),
      externalMB: Math.round(mem.external / 1048576),
    },
    container,
    lagMs: Math.round(eventLoopLag * 100) / 100,
    inflight,
    dbs: dbResults,
    cache: { warmed: cacheWarmed, warmedAt: cacheWarmedAt, ...cache, query: getQueryCacheSize() },
    demand: { ...demand, queueDepth: inflight },
    db: {
      mmapMB: Math.round(dbMeta.mmapSize / 1048576),
      fileMB: Math.round(dbMeta.fileSizeBytes / 1048576),
      cacheMB: Math.round((dbMeta.cacheSizeKB || 0) / 1024),
    },
    warnings,
  }), {
    status: allDbOk ? 200 : 503,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
  });
};
