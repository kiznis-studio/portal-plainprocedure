// D1-compatible adapter wrapping better-sqlite3
// Exposes the same API as Cloudflare D1 so all existing db.ts functions work unchanged
// D1: db.prepare(sql).bind(...params).first<T>() / .all<T>() / .run()
// better-sqlite3: db.prepare(sql).get(...params) / .all(...params) / .run(...params)
// Key difference: D1 uses numbered params (?1, ?2), better-sqlite3 only works with unnamed (?)
//
// IMPORTANT: Methods are SYNCHRONOUS (return plain values, not Promises) despite the D1
// interface declaring Promise returns. better-sqlite3 is synchronous and wrapping in
// async breaks the `.all<T>().results` chain pattern used by 70+ sitemaps. Callers
// using `await` still work fine (await on a non-Promise resolves immediately).

import Database from 'better-sqlite3';
import { copyFileSync, existsSync, statSync } from 'node:fs';
import { basename, join } from 'node:path';

interface D1Result<T = unknown> {
  results: T[];
  success: boolean;
  meta: Record<string, unknown>;
}

interface D1PreparedStatement {
  bind(...params: unknown[]): {
    first<T = unknown>(): Promise<T | null>;
    all<T = unknown>(): Promise<D1Result<T>>;
    run(): Promise<{ success: boolean; meta: Record<string, unknown> }>;
  };
  first<T = unknown>(): Promise<T | null>;
  all<T = unknown>(): Promise<D1Result<T>>;
  run(): Promise<{ success: boolean; meta: Record<string, unknown> }>;
}

export interface D1Database {
  prepare(sql: string): D1PreparedStatement;
}

// Convert D1 numbered params (?1, ?2) to unnamed (?) for better-sqlite3,
// and return a mapping so bind args can be expanded for reused params.
// E.g. "WHERE a = ?1 OR b = ?1 LIMIT ?2" → sql "WHERE a = ? OR b = ? LIMIT ?"
//      indices [1, 1, 2] → bind(x, y) expands to [x, x, y]
function normalizeParams(sql: string): { sql: string; indices: number[] } {
  const indices: number[] = [];
  const normalized = sql.replace(/\?(\d+)/g, (_m, num) => {
    indices.push(parseInt(num, 10));
    return '?';
  });
  return { sql: normalized, indices };
}

function expandParams(params: unknown[], indices: number[]): unknown[] {
  if (indices.length === 0) return params; // plain ? params — no expansion
  return indices.map(i => params[i - 1]); // ?1 → params[0], ?2 → params[1], etc.
}

// Exported metadata for health endpoint
export const dbMeta = { mmapSize: 0, fileSizeBytes: 0, cacheSizeKB: 0 };

// ─── Query stats for adaptive cache ───
interface QueryStat {
  calls: number;
  totalMs: number;
  paramVariants: Set<string>;
}

const queryStats = new Map<string, QueryStat>();
let statsReportInterval: ReturnType<typeof setInterval> | null = null;

// Slow query threshold — queries above this are reported to primary for Sentry alerting
const SLOW_QUERY_MS = 1000;

function trackQuery(fp: string, ph: string, ms: number) {
  const stat = queryStats.get(fp);
  if (stat) {
    stat.calls++;
    stat.totalMs += ms;
    stat.paramVariants.add(ph);
  } else {
    queryStats.set(fp, { calls: 1, totalMs: ms, paramVariants: new Set([ph]) });
  }

  // Report slow queries to primary for Sentry alerting
  if (ms > SLOW_QUERY_MS && process.send) {
    process.send({
      type: 'slow-query',
      fingerprint: fp.substring(0, 100),
      params: ph.substring(0, 50),
      ms: Math.round(ms),
    });
  }
}

function startStatsReporter() {
  if (statsReportInterval || !process.send) return;
  statsReportInterval = setInterval(() => {
    if (queryStats.size === 0) return;
    const batch: Array<{ fingerprint: string; calls: number; totalMs: number; avgMs: number; paramVariants: number }> = [];
    for (const [fp, stat] of queryStats) {
      batch.push({
        fingerprint: fp,
        calls: stat.calls,
        totalMs: Math.round(stat.totalMs),
        avgMs: Math.round(stat.totalMs / stat.calls * 10) / 10,
        paramVariants: stat.paramVariants.size,
      });
    }
    process.send!({ type: 'query-stats-batch', stats: batch });
    queryStats.clear();
  }, 10_000);
  statsReportInterval.unref();
}

// Fingerprint: normalize SQL by collapsing whitespace (params are already ? placeholders)
function makeFingerprint(sql: string): string {
  return sql.replace(/\s+/g, ' ').trim();
}

function makeParamsHash(params: unknown[]): string {
  if (params.length === 0) return '_';
  return params.map(p => String(p)).join('|');
}

// ─── Adaptive cache client ───
const promotedFingerprints = new Set<string>();
const pendingIpcCallbacks = new Map<string, { resolve: (v: unknown) => void; timer: ReturnType<typeof setTimeout> }>();

// Listen for messages from primary
if (process.send) {
  process.on('message', (msg: any) => {
    if (msg?.type === 'adaptive-promoted') {
      promotedFingerprints.clear();
      for (const fp of msg.fingerprints) promotedFingerprints.add(fp);
    }
    if (msg?.type === 'qcache-result') {
      const pending = pendingIpcCallbacks.get(msg.key);
      if (pending) {
        clearTimeout(pending.timer);
        pendingIpcCallbacks.delete(msg.key);
        pending.resolve(msg.hit ? msg.value : null);
      }
    }
  });
}

function adaptiveGet(key: string): Promise<unknown | null> {
  return new Promise((resolve) => {
    if (!process.send) { resolve(null); return; }
    const timer = setTimeout(() => {
      pendingIpcCallbacks.delete(key);
      resolve(null); // timeout — fall through to DB
    }, 50);
    pendingIpcCallbacks.set(key, { resolve, timer });
    process.send!({ type: 'qcache-get', key });
  });
}

function adaptiveSet(key: string, value: unknown) {
  process.send?.({ type: 'qcache-set', key, value });
}

export function getAdaptiveCacheStats() {
  return { promotedCount: promotedFingerprints.size };
}

// Auto-tune SQLite pragmas based on memory budget.
function applyPragmas(db: InstanceType<typeof Database>, dbPath: string) {
  let fileSize = 0;
  try { fileSize = statSync(dbPath).size; } catch { /* use defaults */ }

  const fileSizeMB = fileSize / (1024 * 1024);

  // cache_size: from budget or fallback to DB-size tiers
  let cacheSizeKB: number;
  if (process.env.SQLITE_CACHE_KB) {
    cacheSizeKB = parseInt(process.env.SQLITE_CACHE_KB, 10);
  } else if (fileSizeMB > 500) { cacheSizeKB = 65536; }
  else if (fileSizeMB > 100) { cacheSizeKB = 32768; }
  else if (fileSizeMB > 10) { cacheSizeKB = 16384; }
  else { cacheSizeKB = 4096; }

  // mmap_size: from budget or fallback to capped file size
  let mmapSize: number;
  if (process.env.SQLITE_MMAP_BYTES) {
    mmapSize = parseInt(process.env.SQLITE_MMAP_BYTES, 10);
  } else {
    const MMAP_CAP = 256 * 1024 * 1024;
    mmapSize = Math.min(Math.max(fileSize, 16 * 1024 * 1024), MMAP_CAP);
  }

  try {
    db.pragma(`cache_size = -${cacheSizeKB}`);
    db.pragma(`mmap_size = ${mmapSize}`);
    db.pragma('temp_store = MEMORY');
  } catch { /* non-critical */ }

  dbMeta.mmapSize = mmapSize;
  dbMeta.fileSizeBytes = fileSize;
  dbMeta.cacheSizeKB = cacheSizeKB;
}

// Self-heal WAL mode databases on read-only mounts.
function openDatabase(dbPath: string): InstanceType<typeof Database> {
  try {
    const db = new Database(dbPath, { fileMustExist: true });
    db.prepare('SELECT 1').get();
    // Read-only data — DELETE mode is correct (no WAL overhead on static DBs)
    db.pragma('query_only = ON');
    applyPragmas(db, dbPath);
    return db;
  } catch (err: any) {
    if (!err?.message?.includes('readonly database')) throw err;

    // WAL mode on :ro mount — self-heal by copying to /tmp
    const tmpPath = join('/tmp', `d1-heal-${basename(dbPath)}`);
    console.warn(`[d1-adapter] WAL mode detected on ${dbPath} — copying to ${tmpPath} and fixing`);
    copyFileSync(dbPath, tmpPath);
    if (existsSync(dbPath + '-wal')) copyFileSync(dbPath + '-wal', tmpPath + '-wal');
    if (existsSync(dbPath + '-shm')) copyFileSync(dbPath + '-shm', tmpPath + '-shm');

    const fixDb = new Database(tmpPath);
    fixDb.pragma('journal_mode = DELETE');
    fixDb.close();

    const db = new Database(tmpPath, { readonly: true });
    applyPragmas(db, dbPath);
    console.warn(`[d1-adapter] Self-healed: ${dbPath} → ${tmpPath} (journal_mode=DELETE)`);
    return db;
  }
}

export function createD1Adapter(dbPath: string): D1Database {
  const db = openDatabase(dbPath);

  // Prepared statement cache — avoids recompiling SQL on every call
  const stmtCache = new Map<string, ReturnType<typeof db.prepare>>();
  function getStmt(sql: string): ReturnType<typeof db.prepare> {
    let s = stmtCache.get(sql);
    if (!s) { s = db.prepare(sql); stmtCache.set(sql, s); }
    return s;
  }

  // Start reporting query stats to primary (cluster mode)
  startStatsReporter();

  return {
    prepare(sql: string): D1PreparedStatement {
      const { sql: normalized, indices } = normalizeParams(sql);
      const stmt = getStmt(normalized);
      const fp = makeFingerprint(normalized);

      function makeBindResult(params: unknown[]) {
        params = expandParams(params, indices);
        const ph = makeParamsHash(params);

        return {
          first<T = unknown>(): T | null {
            const start = performance.now();
            const row = stmt.get(...params);
            const ms = performance.now() - start;
            trackQuery(fp, ph, ms);
            // Fire-and-forget: populate adaptive cache in background
            if (promotedFingerprints.has(fp) && ms > 3) {
              adaptiveSet(`adaptive:${fp}:${ph}`, (row as T) ?? null);
            }
            return (row as T) ?? null;
          },
          all<T = unknown>(): D1Result<T> {
            const start = performance.now();
            const rows = stmt.all(...params);
            const ms = performance.now() - start;
            trackQuery(fp, ph, ms);
            const result: D1Result<T> = { results: rows as T[], success: true, meta: {} };
            if (promotedFingerprints.has(fp) && ms > 3) {
              adaptiveSet(`adaptive:${fp}:${ph}`, result);
            }
            return result;
          },
          run() {
            stmt.run(...params);
            return { success: true, meta: {} };
          },
        };
      }

      return {
        bind(...params: unknown[]) {
          return makeBindResult(params);
        },
        // Unbound versions (no params)
        first<T = unknown>(): T | null {
          const start = performance.now();
          const row = stmt.get();
          const ms = performance.now() - start;
          trackQuery(fp, '_', ms);
          if (promotedFingerprints.has(fp) && ms > 3) {
            adaptiveSet(`adaptive:${fp}:_`, (row as T) ?? null);
          }
          return (row as T) ?? null;
        },
        all<T = unknown>(): D1Result<T> {
          const start = performance.now();
          const rows = stmt.all();
          const ms = performance.now() - start;
          trackQuery(fp, '_', ms);
          const result: D1Result<T> = { results: rows as T[], success: true, meta: {} };
          if (promotedFingerprints.has(fp) && ms > 3) {
            adaptiveSet(`adaptive:${fp}:_`, result);
          }
          return result;
        },
        run() {
          stmt.run();
          return { success: true, meta: {} };
        },
      };
    },
  };
}
