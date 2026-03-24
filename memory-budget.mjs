// memory-budget.mjs
// Holistic container memory budget — auto-calculates allocations for every
// component from the cgroup v2 memory limit. No manual CACHE_ENTRIES tuning.
//
// Budget model (v2 — no response cache):
//   CONTAINER_LIMIT (cgroup)
//   ├── HEADROOM (10%) — GC spikes, kernel buffers, safety margin
//   ├── WARMER (128MB) — background warming process (≥512MB containers only)
//   ├── PRIMARY PROCESS — Node.js base + IPC broker
//   ├── WORKER 0..N — Node.js + Astro + SQLite (page cache + mmap)
//   ├── EXPLICIT QUERY CACHE (60%) — cached() queries, warm on startup
//   └── ADAPTIVE QUERY CACHE (40%) — auto-discovered hot queries via IPC

import { readFileSync, statSync } from 'node:fs';

// ─── Constants (measured baselines) ───
const PRIMARY_BASE_MB = 80;       // Node.js runtime + cluster proxy + mgmt server
const WORKER_BASE_MB = 80;        // Node.js + Astro SSR framework per worker
const HEADROOM_PCT = 0.10;        // 10% safety margin
const SQLITE_SHARE_PCT = 0.25;    // 25% of usable for all SQLite combined
const MMAP_SHARE_OF_SQLITE = 0.70;  // 70% of SQLite budget goes to mmap
const CACHE_SHARE_OF_SQLITE = 0.30; // 30% goes to page cache
const EXPLICIT_QCACHE_SHARE = 0.60; // 60% of remaining for cached() queries
const ADAPTIVE_QCACHE_SHARE = 0.40; // 40% of remaining for auto-discovered queries
const AVG_QUERY_ENTRY_BYTES = 8192; // 8KB avg query cache entry

// ─── Read container memory limit from cgroup v2 ───
export function readCgroupMemoryLimit() {
  try {
    const raw = readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim();
    if (raw === 'max') return null; // no limit (bare metal / no cgroup)
    const bytes = parseInt(raw, 10);
    return isNaN(bytes) ? null : bytes;
  } catch {
    return null; // cgroup v2 not available
  }
}

// ─── Read current memory usage from cgroup v2 ───
export function readCgroupMemoryCurrent() {
  try {
    const raw = readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim();
    return parseInt(raw, 10) || 0;
  } catch {
    return 0;
  }
}

// ─── Get DB file size ───
export function getDbFileSizeMB(dbPath) {
  try { return statSync(dbPath).size / (1024 * 1024); } catch { return 0; }
}

// ─── Core budget calculation ───
export function calculateBudget(options = {}) {
  const {
    workersMax = parseInt(process.env.WORKERS_MAX || '4', 10),
    dbPath = process.env.DATABASE_PATH || '/data/portal.db',
  } = options;

  const cgroupLimit = readCgroupMemoryLimit();
  const dbSizeMB = getDbFileSizeMB(dbPath);

  // Fallback: no cgroup limit (local dev, bare metal) → conservative 512MB assumption
  const limitMB = cgroupLimit ? cgroupLimit / (1024 * 1024) : 512;

  const headroomMB = limitMB * HEADROOM_PCT;
  const warmerHeapMB = limitMB >= 512 ? 128 : 0; // Skip warmer for tiny containers
  const usableMB = limitMB - headroomMB - warmerHeapMB;

  // ─── SQLite budgets (scaled to container, shared across all workers) ───
  const sqliteTotalMB = usableMB * SQLITE_SHARE_PCT;
  const perWorkerSqliteMB = sqliteTotalMB / Math.max(workersMax, 1);

  // mmap: capped at 256MB and DB file size
  const mmapMB = Math.min(
    Math.floor(perWorkerSqliteMB * MMAP_SHARE_OF_SQLITE),
    256,
    dbSizeMB || 256, // don't exceed file size
  );

  // page cache: capped at 64MB (negative KB for SQLite PRAGMA)
  const pcacheMB = Math.min(
    Math.floor(perWorkerSqliteMB * CACHE_SHARE_OF_SQLITE),
    64,
  );
  const pcacheKB = Math.max(pcacheMB * 1024, 1024); // minimum 1MB

  // ─── Worker capacity ───
  const perWorkerTotalMB = WORKER_BASE_MB + mmapMB + pcacheMB;

  // Check if configured WORKERS_MAX fits
  let effectiveWorkersMax = workersMax;
  const minCacheMB = usableMB * 0.10; // must have at least 10% for caches

  while (effectiveWorkersMax > 1) {
    const workersCost = effectiveWorkersMax * perWorkerTotalMB;
    const remaining = usableMB - PRIMARY_BASE_MB - workersCost;
    if (remaining >= minCacheMB) break;
    effectiveWorkersMax--;
  }

  // Hard cap: large DBs cause OOM with multiple workers due to mmap contention
  if (dbSizeMB > 1000 && effectiveWorkersMax > 2) {
    effectiveWorkersMax = 2;
  }

  // Recalculate per-worker SQLite with actual worker count (if reduced, each worker gets more)
  let finalMmapMB = mmapMB;
  let finalPcacheMB = pcacheMB;
  let finalPcacheKB = pcacheKB;
  if (effectiveWorkersMax < workersMax) {
    const finalPerWorkerSqliteMB = sqliteTotalMB / Math.max(effectiveWorkersMax, 1);
    finalMmapMB = Math.min(Math.floor(finalPerWorkerSqliteMB * MMAP_SHARE_OF_SQLITE), 256, dbSizeMB || 256);
    finalPcacheMB = Math.min(Math.floor(finalPerWorkerSqliteMB * CACHE_SHARE_OF_SQLITE), 64);
    finalPcacheKB = Math.max(finalPcacheMB * 1024, 1024);
  }

  // Final worker cost (recalculated with actual SQLite budgets)
  const finalPerWorkerTotalMB = WORKER_BASE_MB + finalMmapMB + finalPcacheMB;
  const workersCostMB = effectiveWorkersMax * finalPerWorkerTotalMB;

  // ─── Cache budgets (what's left after processes + SQLite) ───
  // No response cache — CF edge handles page caching
  const remainingMB = Math.max(usableMB - PRIMARY_BASE_MB - workersCostMB, minCacheMB);

  const explicitCacheMB = remainingMB * EXPLICIT_QCACHE_SHARE;
  const adaptiveCacheMB = remainingMB * ADAPTIVE_QCACHE_SHARE;

  const explicitCacheMax = Math.max(
    500,
    Math.min(10000, Math.floor(explicitCacheMB * 1024 * 1024 / AVG_QUERY_ENTRY_BYTES)),
  );

  const adaptiveCacheMax = Math.max(
    100,
    Math.min(5000, Math.floor(adaptiveCacheMB * 1024 * 1024 / AVG_QUERY_ENTRY_BYTES)),
  );

  const budget = {
    containerLimitMB: Math.round(limitMB),
    headroomMB: Math.round(headroomMB),
    warmerHeapMB,
    usableMB: Math.round(usableMB),
    dbSizeMB: Math.round(dbSizeMB),

    // Process budgets
    primaryBaseMB: PRIMARY_BASE_MB,
    workerBaseMB: WORKER_BASE_MB,
    effectiveWorkersMax,
    configuredWorkersMax: workersMax,
    workersReduced: effectiveWorkersMax < workersMax,

    // SQLite budgets (per worker — recalculated if workers reduced)
    sqliteMmapBytes: finalMmapMB * 1024 * 1024,
    sqliteMmapMB: finalMmapMB,
    sqliteCacheKB: finalPcacheKB,
    sqliteCacheMB: finalPcacheMB,

    // Query cache budgets (no response cache — CF edge handles pages)
    explicitCacheMax,
    explicitCacheMB: Math.round(explicitCacheMB),
    adaptiveCacheMax,
    adaptiveCacheMB: Math.round(adaptiveCacheMB),
  };

  return budget;
}

// ─── Format budget as startup log ───
export function logBudget(budget) {
  const lines = [
    `[memory-budget] Container: ${budget.containerLimitMB}MB | Usable: ${budget.usableMB}MB (after ${HEADROOM_PCT * 100}% headroom` +
      (budget.warmerHeapMB > 0 ? ` + ${budget.warmerHeapMB}MB warmer` : '') + ')',
    `[memory-budget] Workers: ${budget.effectiveWorkersMax} max` +
      (budget.workersReduced ? ` (reduced from ${budget.configuredWorkersMax} — not enough memory)` : ''),
    `[memory-budget] SQLite/worker: mmap=${budget.sqliteMmapMB}MB, pcache=${budget.sqliteCacheMB}MB`,
    `[memory-budget] Explicit query cache: ${budget.explicitCacheMax} max entries (~${budget.explicitCacheMB}MB)`,
    `[memory-budget] Adaptive query cache: ${budget.adaptiveCacheMax} max entries (~${budget.adaptiveCacheMB}MB)`,
    budget.warmerHeapMB > 0
      ? `[memory-budget] Warmer: ${budget.warmerHeapMB}MB reserved (separate process)`
      : `[memory-budget] Warmer: disabled (container <512MB)`,
    `[memory-budget] DB: ${budget.dbSizeMB}MB`,
  ];
  for (const line of lines) console.log(line);
}
