import type { APIRoute } from 'astro';
import { readFileSync } from 'node:fs';
import { inflight, eventLoopLag, cacheWarmed, cacheWarmedAt, getRollingMetrics, getAdaptiveCacheStats } from '../middleware';
import { getQueryCacheSize } from '../lib/db';
import { dbMeta } from '../lib/d1-adapter';

export const prerender = false;
const startTime = Date.now();

// Read cgroup memory for live usage (subtract reclaimable page cache).
// memory.current includes file page cache from SQLite reads, which the kernel
// reclaims freely under pressure. Use working_set = current - inactive_file
// (same formula as docker stats / Kubernetes memory metrics).
function getContainerMemory() {
  try {
    const maxRaw = readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim();
    const current = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
    let inactiveFile = 0;
    try {
      const stat = readFileSync('/sys/fs/cgroup/memory.stat', 'utf-8');
      const m = stat.match(/^inactive_file\s+(\d+)/m);
      if (m) inactiveFile = parseInt(m[1], 10);
    } catch {}
    const workingSet = current - inactiveFile;
    const limitBytes = maxRaw === 'max' ? null : parseInt(maxRaw, 10);
    const limitMB = limitBytes ? Math.round(limitBytes / 1048576) : null;
    const currentMB = Math.round(workingSet / 1048576);
    const rawMB = Math.round(current / 1048576);
    const headroomMB = limitMB ? limitMB - currentMB : null;
    const usagePct = limitMB ? Math.round(currentMB / limitMB * 1000) / 1000 : null;
    return { limitMB, currentMB, rawMB, headroomMB, usagePct };
  } catch {
    return { limitMB: null, currentMB: null, rawMB: null, headroomMB: null, usagePct: null };
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
  const demand = getRollingMetrics();
  const container = getContainerMemory();
  const adaptive = getAdaptiveCacheStats();

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
    cache: {
      warmed: cacheWarmed,
      warmedAt: cacheWarmedAt,
      query: getQueryCacheSize(),
      adaptive,
    },
    demand: { ...demand, queueDepth: inflight },
    db: {
      mmapMB: Math.round(dbMeta.mmapSize / 1048576),
      fileMB: Math.round(dbMeta.fileSizeBytes / 1048576),
      cacheMB: Math.round((dbMeta.cacheSizeKB || 0) / 1024),
    },
    // TRM scaling hints — container's self-assessment
    scaling: {
      ready: cacheWarmed,
      memoryPressure: container.usagePct,
    },
    warnings,
  }), {
    // Always return 200 — the JSON body has status: 'ok'|'degraded' for detailed state.
    // Docker HEALTHCHECK, cluster readiness probe, and Kuma all expect 200.
    // Returning 503 prevents worker registration → cascading failure.
    status: 200,
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
  });
};
