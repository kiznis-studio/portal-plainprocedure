import { defineMiddleware } from 'astro:middleware';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { createD1Adapter, getAdaptiveCacheStats } from './lib/d1-adapter';

// --- Sitemap disk cache ---
// Sitemaps are immutable between deploys (data doesn't change during container lifetime).
// Generate once → store on disk → serve instantly. Container restart clears /tmp naturally.
const SITEMAP_CACHE_DIR = '/tmp/sitemap-cache';
try { mkdirSync(SITEMAP_CACHE_DIR, { recursive: true }); } catch {}

function sitemapCachePath(urlPath: string): string {
  return `${SITEMAP_CACHE_DIR}/${encodeURIComponent(urlPath)}.xml`;
}

function getSitemapFromDisk(urlPath: string): string | null {
  const fp = sitemapCachePath(urlPath);
  try { return readFileSync(fp, 'utf-8'); } catch { return null; }
}

function saveSitemapToDisk(urlPath: string, body: string): void {
  try { writeFileSync(sitemapCachePath(urlPath), body, 'utf-8'); } catch {}
}

function isSitemapPath(p: string): boolean {
  return (p.includes('sitemap') || p === '/robots.txt') && (p.endsWith('.xml') || p === '/robots.txt');
}

// --- DB initialization ---
const DATABASE_PATH = process.env.DATABASE_PATH || '/data/portal.db';
let db: ReturnType<typeof createD1Adapter> | null = null;
function getDb() {
  if (!db) {
    if (!existsSync(DATABASE_PATH)) return null as any;
    db = createD1Adapter(DATABASE_PATH);
  }
  return db;
}

// --- Inflight counter (metrics for /health + TRM) ---
let inflight = 0;

// --- Event loop lag (sampled every 2s) ---
let eventLoopLag = 0;
const lagInterval = setInterval(() => {
  const s = performance.now();
  setImmediate(() => { eventLoopLag = performance.now() - s; });
}, 2000);
lagInterval.unref();

// --- Rolling demand metrics (15s window, counter-based) ---
let reqCount = 0;
let latencySum = 0;
let windowStart = Date.now();

function recordRequest(latencyMs: number) {
  reqCount++;
  latencySum += latencyMs;
}

function getRollingMetrics() {
  const now = Date.now();
  const elapsed = (now - windowStart) / 1000;
  const rate = elapsed > 0 ? Math.round(reqCount / elapsed * 100) / 100 : 0;
  const avg = reqCount > 0 ? Math.round(latencySum / reqCount) : 0;
  // Reset window every 15s
  if (now - windowStart > 15000) {
    reqCount = 0;
    latencySum = 0;
    windowStart = now;
  }
  return { requestRate: rate, avgLatency: avg };
}

// Workers are always ready — warming is handled by the warmer process
let cacheWarmed = true;
let cacheWarmedAt: string | null = new Date().toISOString();

export { inflight, eventLoopLag, cacheWarmed, cacheWarmedAt, getRollingMetrics, getAdaptiveCacheStats };

// --- Edge TTL: fast startsWith checks instead of regex ---
function getEdgeTtl(p: string): number {
  const c = p.charCodeAt(1); // first char after '/'
  // Detail pages: 24h (86400s)
  if (c === 112 || c === 101 || c === 102 || c === 100 || c === 98 || c === 97 ||
      c === 108 || c === 111 || c === 106 || c === 122) {
    return 86400;
  }
  if (p.startsWith('/s') || p.startsWith('/c') || p.startsWith('/m')) return 86400;
  if (p.startsWith('/ranking') || p.startsWith('/guide')) return 21600;
  return 3600;
}

export const onRequest = defineMiddleware(async (context, next) => {
  const path = context.url.pathname;
  (context.locals as any).runtime = { env: { DB: getDb() } };

  // Fast-path: health endpoint — always available
  if (path === '/health') return next();

  // Fast-path: static assets + cluster management
  if (path.charCodeAt(1) === 95) return next(); // starts with '/_'
  if (path.startsWith('/fav')) return next();

  if (context.request.method === 'GET') {
    // L0: Sitemap disk cache — sitemaps are immutable between deploys
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
      if (elapsed > 500) {
        console.warn(`[slow] ${path} ${Math.round(elapsed)}ms lag=${Math.round(eventLoopLag)}ms`);
      }

      if (response.status === 200) {
        const ct = response.headers.get('content-type') || '';
        if (ct.includes('text/html') || ct.includes('xml')) {
          const ttl = ct.includes('xml') ? 86400 : getEdgeTtl(path);
          const cc = `public, max-age=300, s-maxage=${ttl}`;

          // Sitemaps: persist to disk for future requests
          if (isSitemapPath(path)) {
            const body = await response.text();
            if (body.length > 50) saveSitemapToDisk(path, body);
            return new Response(body, { headers: { 'Content-Type': ct, 'Cache-Control': cc, 'X-Cache': 'MISS' } });
          }

          // HTML: set edge cache headers only — no in-memory cache
          return new Response(response.body, {
            headers: { 'Content-Type': ct, 'Cache-Control': cc },
          });
        }
      }
      return response;
    } finally {
      inflight--;
    }
  }

  return next();
});
