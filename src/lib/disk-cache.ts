/**
 * T19: Disk Query Cache
 *
 * Persists hot query results to /tmp/query-cache/*.json files.
 * Survives process restarts within the same container (not container recreates).
 * Drop-in enhancement for the cached() + warmQueryCache pattern.
 *
 * Usage in db.ts:
 *   import { persistToDisk, loadFromDisk, warmFromDisk } from './disk-cache';
 *
 *   // In cached(): add disk fallback between Map miss and compute
 *   const fromDisk = loadFromDisk<T>(key);
 *   if (fromDisk !== null) { queryCache.set(key, fromDisk); return fromDisk; }
 *
 *   // In warmQueryCache(): pre-load from disk + persist after warming
 *   const diskLoaded = warmFromDisk(queryCache);
 *   // ... existing warming ...
 *   persistToDisk(queryCache);
 */

import { readFileSync, writeFileSync, mkdirSync, readdirSync } from 'fs';

const CACHE_DIR = '/tmp/query-cache';
try { mkdirSync(CACHE_DIR, { recursive: true }); } catch {}

function keyToPath(key: string): string {
  return `${CACHE_DIR}/${encodeURIComponent(key)}.json`;
}

/**
 * Write cache entries to disk as JSON files.
 * Call after warmQueryCache() completes.
 * @param keys - specific keys to persist (default: all)
 * @returns number of entries written
 */
export function persistToDisk(queryCache: Map<string, any>, keys?: string[]): number {
  let written = 0;
  const targets = keys ?? Array.from(queryCache.keys());
  for (const key of targets) {
    if (!queryCache.has(key)) continue;
    try {
      writeFileSync(keyToPath(key), JSON.stringify(queryCache.get(key)));
      written++;
    } catch {}
  }
  return written;
}

/**
 * Read a single entry from disk cache.
 * @returns parsed value or null if not found/corrupt
 */
export function loadFromDisk<T = any>(key: string): T | null {
  try {
    return JSON.parse(readFileSync(keyToPath(key), 'utf-8'));
  } catch {
    return null;
  }
}

/**
 * Bulk-load all disk cache files into the query cache Map.
 * Call at the start of warmQueryCache() for instant pre-population.
 * @returns number of entries loaded
 */
export function warmFromDisk(queryCache: Map<string, any>): number {
  let loaded = 0;
  try {
    for (const file of readdirSync(CACHE_DIR)) {
      if (!file.endsWith('.json')) continue;
      const key = decodeURIComponent(file.slice(0, -5));
      try {
        queryCache.set(key, JSON.parse(readFileSync(`${CACHE_DIR}/${file}`, 'utf-8')));
        loaded++;
      } catch {}
    }
  } catch {}
  return loaded;
}
