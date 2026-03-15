// D1-compatible adapter wrapping better-sqlite3
// Exposes the same API as Cloudflare D1 so all existing db.ts functions work unchanged
// D1: db.prepare(sql).bind(...params).first<T>() / .all<T>() / .run()
// better-sqlite3: db.prepare(sql).get(...params) / .all(...params) / .run(...params)
// Key difference: D1 uses numbered params (?1, ?2), better-sqlite3 only works with unnamed (?)

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

// Convert D1 numbered params (?1, ?2) to unnamed (?) for better-sqlite3
function normalizeParams(sql: string): string {
  return sql.replace(/\?(\d+)/g, '?');
}

// Exported metadata for health endpoint
export const dbMeta = { mmapSize: 0, fileSizeBytes: 0 };

// Auto-tune SQLite pragmas based on DB file size.
// mmap_size scales to full file size (virtual address space is free on 64-bit).
// cache_size uses tiers (it's real RAM allocation).
function applyPragmas(db: InstanceType<typeof Database>, dbPath: string) {
  let fileSize = 0;
  try { fileSize = statSync(dbPath).size; } catch { /* use defaults */ }

  // cache_size: tiered by file size (negative = KB)
  const fileSizeMB = fileSize / (1024 * 1024);
  let cacheSize: number;
  if (fileSizeMB > 500) cacheSize = -65536;       // 64MB
  else if (fileSizeMB > 100) cacheSize = -32768;   // 32MB
  else if (fileSizeMB > 10) cacheSize = -16384;    // 16MB
  else cacheSize = -4096;                           // 4MB

  // mmap_size: capped to fit within Docker container memory limits
  // Cap at 256MB — larger DBs still benefit from OS page cache outside mmap
  const MMAP_CAP = 256 * 1024 * 1024;
  const mmapSize = Math.min(Math.max(fileSize, 16 * 1024 * 1024), MMAP_CAP);

  try {
    db.pragma(`cache_size = ${cacheSize}`);
    db.pragma(`mmap_size = ${mmapSize}`);
    db.pragma('temp_store = MEMORY');
  } catch { /* non-critical */ }

  dbMeta.mmapSize = mmapSize;
  dbMeta.fileSizeBytes = fileSize;
}

// Self-heal WAL mode databases on read-only mounts.
// WAL mode requires writing a WAL file even for reads, which fails on :ro mounts.
// Fix: copy to /tmp, convert to DELETE journal mode, use the copy.
function openDatabase(dbPath: string): InstanceType<typeof Database> {
  try {
    const db = new Database(dbPath, { fileMustExist: true });
    // Try a simple prepare to verify the DB is usable
    db.prepare('SELECT 1').get();
    // Enable WAL mode for better concurrent read performance
    try { db.pragma('journal_mode = WAL'); } catch { /* expected on truly read-only mounts */ }
    db.pragma('query_only = ON'); // Safety: reject all SQL writes

    // Performance pragmas — auto-tuned to DB file size (session-level, not persisted)
    applyPragmas(db, dbPath);

    return db;
  } catch (err: any) {
    if (!err?.message?.includes('readonly database')) throw err;

    // WAL mode on :ro mount — self-heal by copying to /tmp
    const tmpPath = join('/tmp', `d1-heal-${basename(dbPath)}`);
    console.warn(`[d1-adapter] WAL mode detected on ${dbPath} — copying to ${tmpPath} and fixing`);
    copyFileSync(dbPath, tmpPath);
    // Also copy WAL/SHM files if they exist alongside the DB
    if (existsSync(dbPath + '-wal')) copyFileSync(dbPath + '-wal', tmpPath + '-wal');
    if (existsSync(dbPath + '-shm')) copyFileSync(dbPath + '-shm', tmpPath + '-shm');

    // Open writable copy and convert to DELETE mode
    const fixDb = new Database(tmpPath);
    fixDb.pragma('journal_mode = DELETE');
    fixDb.close();

    // Now open as readonly with auto-tuned performance pragmas
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

  return {
    prepare(sql: string): D1PreparedStatement {
      const normalized = normalizeParams(sql);
      const stmt = getStmt(normalized);

      function makeBindResult(params: unknown[]) {
        return {
          async first<T = unknown>(): Promise<T | null> {
            const row = stmt.get(...params);
            return (row as T) ?? null;
          },
          async all<T = unknown>(): Promise<D1Result<T>> {
            const rows = stmt.all(...params);
            return { results: rows as T[], success: true, meta: {} };
          },
          async run() {
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
        async first<T = unknown>(): Promise<T | null> {
          const row = stmt.get();
          return (row as T) ?? null;
        },
        async all<T = unknown>(): Promise<D1Result<T>> {
          const rows = stmt.all();
          return { results: rows as T[], success: true, meta: {} };
        },
        async run() {
          stmt.run();
          return { success: true, meta: {} };
        },
      };
    },
  };
}
