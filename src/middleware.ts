// Unified middleware — handles both single-DB and multi-DB portals automatically.
// Single-DB: set DATABASE_PATH=/data/portal.db → discovers as { DB: adapter }
// Multi-DB: set DATABASE_PATH + DATABASE_RENT_PATH etc → discovers all
import { defineMiddleware } from 'astro:middleware';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import http from 'node:http';
import * as Sentry from '@sentry/astro';
import { createD1Adapter, getAdaptiveCacheStats } from './lib/d1-adapter';
import type { D1Database } from './lib/d1-adapter';

// Try to import warmQueryCache — portals without it skip in-process warming
let _warmQueryCache: ((arg: any) => Promise<void>) | null = null;
try {
  const dbModule = await import('./lib/db');
  if (typeof dbModule.warmQueryCache === 'function') _warmQueryCache = dbModule.warmQueryCache;
} catch { /* db.ts may not export warmQueryCache */ }

// --- Sitemap disk cache ---
const SITEMAP_CACHE_DIR = '/tmp/sitemap-cache';
try { mkdirSync(SITEMAP_CACHE_DIR, { recursive: true }); } catch {}

function sitemapCachePath(urlPath: string): string {
  return `${SITEMAP_CACHE_DIR}/${encodeURIComponent(urlPath)}.xml`;
}

function getSitemapFromDisk(urlPath: string): string | null {
  try { return readFileSync(sitemapCachePath(urlPath), 'utf-8'); } catch { return null; }
}

function saveSitemapToDisk(urlPath: string, body: string): void {
  try { writeFileSync(sitemapCachePath(urlPath), body, 'utf-8'); } catch {}
}

function isSitemapPath(p: string): boolean {
  return (p.includes('sitemap') || p === '/robots.txt') && (p.endsWith('.xml') || p === '/robots.txt');
}

// Working-set memory: memory.current minus reclaimable page cache (inactive_file).
// Raw memory.current includes file page cache from SQLite reads, which inflates
// usage to 90%+ on large-DB portals while actual app memory is 30-40%.
function containerMemoryPct(): number {
  try {
    const max = parseInt(readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim());
    const cur = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
    let inactiveFile = 0;
    try {
      const stat = readFileSync('/sys/fs/cgroup/memory.stat', 'utf-8');
      const m = stat.match(/^inactive_file\s+(\d+)/m);
      if (m) inactiveFile = parseInt(m[1], 10);
    } catch {}
    const workingSet = cur - inactiveFile;
    return max > 0 ? workingSet / max : 0;
  } catch { return 0; }
}

// --- DB auto-discovery ---
// Discovers databases from env vars. Single-DB portals get { DB: adapter }.
// Multi-DB portals get { DB: primary, DB_RENT: rent, ... }.
const dbInstances: Record<string, ReturnType<typeof createD1Adapter> | null> = {};

function discoverDatabases(): Record<string, string> {
  const paths: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) {
    if (!value) continue;
    if (key === 'DATABASE_PATH') { paths['DB'] = value; }
    else if (key.startsWith('DATABASE_') && key.endsWith('_PATH')) {
      paths['DB_' + key.slice(9, -5)] = value;
    } else if (key.startsWith('DB_') && key.endsWith('_PATH')) {
      paths[key.slice(0, -5)] = value;
    }
  }
  // Fallback: if no DATABASE_PATH env var, check default mount point
  if (!paths['DB'] && existsSync('/data/portal.db')) {
    paths['DB'] = '/data/portal.db';
  }
  return paths;
}

const DB_PATHS = discoverDatabases();
const IS_MULTI_DB = Object.keys(DB_PATHS).length > 1;

function getDb(key: string): ReturnType<typeof createD1Adapter> | null {
  if (key in dbInstances) return dbInstances[key];
  const path = DB_PATHS[key];
  if (!path || !existsSync(path)) { dbInstances[key] = null; return null; }
  dbInstances[key] = createD1Adapter(path);
  return dbInstances[key];
}

// Null-safe DB stub — returns empty results for all queries.
// Prevents sitemap crashes when the real DB is unavailable (startup, missing file).
// Synchronous (matching the adapter) so .all().results works.
const NULL_DB: D1Database = {
  prepare: () => {
    const bound = {
      first: () => null,
      all: () => ({ results: [] as unknown[], success: true, meta: {} }),
      run: () => ({ success: true, meta: {} }),
    };
    return { ...bound, bind: () => bound };
  },
};

function getAllDbs(): Record<string, D1Database> {
  const env: Record<string, D1Database> = {};
  for (const key of Object.keys(DB_PATHS)) {
    env[key] = getDb(key) || NULL_DB;
  }
  // Ensure DB key always exists (single-DB portals expect env.DB)
  if (!env['DB']) env['DB'] = NULL_DB;
  return env;
}

if (IS_MULTI_DB) {
  console.log(`[middleware] Multi-DB: ${Object.keys(DB_PATHS).length} databases: ${Object.keys(DB_PATHS).join(', ')}`);
}

// --- Inflight counter ---
let inflight = 0;

// --- Event loop lag (sampled every 2s) ---
let eventLoopLag = 0;
const lagInterval = setInterval(() => {
  const s = performance.now();
  setImmediate(() => { eventLoopLag = performance.now() - s; });
}, 2000);
lagInterval.unref();

// --- Rolling demand metrics (15s window) ---
let reqCount = 0;
let latencySum = 0;
let windowStart = Date.now();

function recordRequest(latencyMs: number) { reqCount++; latencySum += latencyMs; }

function getRollingMetrics() {
  const now = Date.now();
  const elapsed = (now - windowStart) / 1000;
  const rate = elapsed > 0 ? Math.round(reqCount / elapsed * 100) / 100 : 0;
  const avg = reqCount > 0 ? Math.round(latencySum / reqCount) : 0;
  if (now - windowStart > 15000) { reqCount = 0; latencySum = 0; windowStart = now; }
  return { requestRate: rate, avgLatency: avg };
}

// --- Background query cache warming ---
// Only worker 0 (CACHE_WARM_WORKER=1) runs in-process warming.
// Calls warmQueryCache from db.ts — handles both single-DB and multi-DB signatures.
let cacheWarmed = false;
let cacheWarmedAt: string | null = null;
const IS_WARM_WORKER = process.env.CACHE_WARM_WORKER !== '0';

function startBackgroundWarming(): void {
  if (!IS_WARM_WORKER) { cacheWarmed = true; cacheWarmedAt = new Date().toISOString(); return; }

  // Multi-DB portals: warm in-process (warmQueryCache(env) with all DBs)
  // Single-DB portals: skip in-process warming — warmer.mjs handles it via HTTP page fetches
  // which trigger cached() functions naturally. In-process warming on single-DB portals
  // risks signature mismatch (db vs env argument).
  if (!IS_MULTI_DB || !_warmQueryCache) {
    cacheWarmed = true;
    cacheWarmedAt = new Date().toISOString();
    return;
  }

  const env = getAllDbs();
  if (Object.keys(env).length === 0) { cacheWarmed = true; return; }

  (async () => {
    try {
      await _warmQueryCache!(env);
      cacheWarmedAt = new Date().toISOString();
    } catch (err) {
      console.error('[cache] Warming failed:', err);
    }
    cacheWarmed = true;
  })();
}
startBackgroundWarming();

// --- Sitemap background warming (self-fetch via HTTP) ---
let sitemapsWarmed = false;

function warmSitemaps(): void {
  if (!IS_WARM_WORKER) return;
  const port = parseInt(process.env.PORT || '4321');

  function selfFetch(urlPath: string): Promise<string> {
    return new Promise((resolve) => {
      const req = http.get({ hostname: '127.0.0.1', port, path: urlPath, timeout: 30000 }, (res) => {
        let body = '';
        res.on('data', (c: Buffer) => body += c);
        res.on('end', () => resolve(body));
      });
      req.on('error', () => resolve(''));
      req.on('timeout', () => { req.destroy(); resolve(''); });
    });
  }

  const checkInterval = setInterval(async () => {
    if (!cacheWarmed) return; // wait for query cache to warm first
    clearInterval(checkInterval);
    try {
      const indexXml = await selfFetch('/sitemap-index.xml');
      if (!indexXml.includes('<sitemapindex') && !indexXml.includes('<urlset')) {
        const fallback = await selfFetch('/sitemap.xml');
        if (fallback.includes('<urlset')) saveSitemapToDisk('/sitemap.xml', fallback);
        sitemapsWarmed = true;
        return;
      }
      saveSitemapToDisk('/sitemap-index.xml', indexXml);
      const locs = [...indexXml.matchAll(/<loc>(.*?)<\/loc>/g)].map(m => {
        try { return new URL(m[1]).pathname; } catch { return null; }
      }).filter(Boolean) as string[];
      let warmed = 1;
      for (const loc of locs) {
        const memPct = containerMemoryPct();
        if (memPct > 0.85) await new Promise(r => setTimeout(r, 30000));
        else if (memPct > 0.70) await new Promise(r => setTimeout(r, 5000));
        if (getSitemapFromDisk(loc)) { warmed++; continue; }
        const xml = await selfFetch(loc);
        if (xml && xml.length > 50) { saveSitemapToDisk(loc, xml); warmed++; }
        await new Promise(r => setTimeout(r, 2000));
      }
      console.log(`[sitemap-cache] Warmed ${warmed} sitemaps to disk`);
    } catch (err) {
      console.error('[sitemap-cache] Warming failed:', (err as Error).message);
    }
    sitemapsWarmed = true;
  }, 2000);
  checkInterval.unref();
}
warmSitemaps();

export { inflight, eventLoopLag, cacheWarmed, cacheWarmedAt, getRollingMetrics, getAdaptiveCacheStats };

// --- Sentry error context ---
// Gathers container + portal info so every Sentry issue has enough context to debug immediately.
function getSentryContext(path: string, method: string, elapsed?: number) {
  const memPct = containerMemoryPct();
  let limitMB = 0;
  let currentMB = 0;
  let rawMB = 0;
  try {
    limitMB = Math.round(parseInt(readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim()) / 1024 / 1024);
    const rawBytes = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
    rawMB = Math.round(rawBytes / 1024 / 1024);
    let inactiveFile = 0;
    try {
      const stat = readFileSync('/sys/fs/cgroup/memory.stat', 'utf-8');
      const m = stat.match(/^inactive_file\s+(\d+)/m);
      if (m) inactiveFile = parseInt(m[1], 10);
    } catch {}
    currentMB = Math.round((rawBytes - inactiveFile) / 1024 / 1024);
  } catch {}
  return {
    extra: {
      path,
      method,
      fullUrl: undefined as string | undefined,
      elapsed: elapsed ? Math.round(elapsed) : undefined,
      memoryPct: Math.round(memPct * 100),
      memoryMB: `${currentMB}/${limitMB}`,
      memoryRawMB: rawMB,
      heapUsedMB: Math.round(process.memoryUsage().heapUsed / 1024 / 1024),
      rssMB: Math.round(process.memoryUsage().rss / 1024 / 1024),
      inflight,
      eventLoopLagMs: Math.round(eventLoopLag * 100) / 100,
      nodeVersion: process.version,
      databases: Object.keys(DB_PATHS).join(', '),
      cacheWarmed,
    },
    tags: {
      route: path.split('/').slice(0, 3).join('/'),
      method,
    },
  };
}

// --- Edge TTL ---
// Portal data is static between DB updates. All pages get long edge TTL by default.
// Only pages with query parameters (search, filters) get shorter TTL since the
// result depends on user input and there are too many variants to cache effectively.
function getEdgeTtl(url: URL): number {
  if (url.search) return 3600;   // 1h — has query params (search, filter, pagination)
  return 86400;                   // 24h — static data page
}

export const onRequest = defineMiddleware(async (context, next) => {
  const path = context.url.pathname;
  (context.locals as any).runtime = { env: getAllDbs() };

  if (path === '/health') return next();
  if (path.charCodeAt(1) === 95) return next();
  if (path.startsWith('/fav')) return next();

  if (context.request.method === 'GET') {
    // L0: Sitemap disk cache
    if (isSitemapPath(path)) {
      const diskCached = getSitemapFromDisk(path);
      if (diskCached) {
        const ct = path === '/robots.txt' ? 'text/plain' : 'application/xml';
        return new Response(diskCached, {
          headers: { 'Content-Type': ct, 'Cache-Control': 'public, max-age=300, s-maxage=86400', 'X-Cache': 'DISK' },
        });
      }
    }

    // Render — no in-memory response cache (CF edge handles page caching)
    inflight++;
    const start = performance.now();
    try {
      const response = await next();
      const elapsed = performance.now() - start;
      recordRequest(elapsed);
      if (elapsed > 500) console.warn(`[slow] ${path} ${Math.round(elapsed)}ms lag=${Math.round(eventLoopLag)}ms`);

      // Capture server errors (5xx) — Astro returns a Response even on render failures,
      // so @sentry/astro middleware may not see them as exceptions. Explicitly report.
      if (response.status >= 500) {
        const ctx = getSentryContext(path, 'GET', elapsed);
        (ctx.extra as any).status = response.status;
        (ctx.extra as any).fullUrl = context.url.href;
        Sentry.captureMessage(`Server error ${response.status} on ${path}`, {
          level: 'error',
          ...ctx,
        });
      }

      if (response.status === 200) {
        const ct = response.headers.get('content-type') || '';
        if (ct.includes('text/html') || ct.includes('xml')) {
          const ttl = ct.includes('xml') ? 86400 : getEdgeTtl(context.url);
          const cc = `public, max-age=300, s-maxage=${ttl}`;

          if (isSitemapPath(path)) {
            const body = await response.text();
            if (body.length > 50) saveSitemapToDisk(path, body);
            return new Response(body, { headers: { 'Content-Type': ct, 'Cache-Control': cc, 'X-Cache': 'MISS' } });
          }

          return new Response(response.body, {
            headers: { 'Content-Type': ct, 'Cache-Control': cc },
          });
        }
      }
      return response;
    } catch (err) {
      // Catch rendering exceptions (OG routes, API routes, page render failures)
      // that Astro's router would otherwise swallow before @sentry/astro sees them.
      const ctx = getSentryContext(path, 'GET');
      (ctx.extra as any).fullUrl = context.url.href;
      (ctx.extra as any).userAgent = context.request.headers.get('user-agent') || 'unknown';
      Sentry.captureException(err, ctx);
      throw err;
    } finally {
      inflight--;
    }
  }

  // Non-GET requests — also capture errors
  try {
    return await next();
  } catch (err) {
    const ctx = getSentryContext(path, context.request.method);
    (ctx.extra as any).fullUrl = context.url.href;
    Sentry.captureException(err, ctx);
    throw err;
  }
});
