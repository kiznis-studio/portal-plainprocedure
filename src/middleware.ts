import { defineMiddleware } from 'astro:middleware';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { gzipSync, gunzipSync } from 'node:zlib';
import { createD1Adapter } from './lib/d1-adapter';

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


// Container memory awareness — reads cgroup v2 metrics (all workers combined)
function containerMemoryPct(): number {
  try {
    const max = parseInt(readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim());
    const cur = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
    return max > 0 ? cur / max : 0;
  } catch { return 0; }
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

// Workers are always ready — warming is handled by the warmer process (KIZ-319)
let cacheWarmed = true;
let cacheWarmedAt: string | null = new Date().toISOString();

// --- Compressed LRU response cache (disabled in cluster mode — primary handles caching) ---
interface CacheEntry { compressed: Buffer; contentType: string; cacheControl: string; hits: number; }
const WORKER_CACHE_ENABLED = process.env.WORKER_RESPONSE_CACHE !== '0';
const responseCache = new Map<string, CacheEntry>();
const MAX_CACHE = WORKER_CACHE_ENABLED ? parseInt(process.env.CACHE_ENTRIES || '5000', 10) : 0;
let totalHits = 0;
let totalMisses = 0;

function getCached(key: string): Response | null {
  if (!WORKER_CACHE_ENABLED) return null;
  const entry = responseCache.get(key);
  if (!entry) { totalMisses++; return null; }
  responseCache.delete(key);
  entry.hits++;
  responseCache.set(key, entry);
  totalHits++;
  return new Response(gunzipSync(entry.compressed), {
    headers: { 'Content-Type': entry.contentType, 'Cache-Control': entry.cacheControl, 'X-Cache': 'HIT' },
  });
}

function setCache(key: string, body: string, contentType: string, cacheControl: string) {
  if (!WORKER_CACHE_ENABLED) return;
  if (!body || body.length < 50 || body.charCodeAt(0) !== 60) return;
  if (responseCache.has(key)) responseCache.delete(key);
  if (responseCache.size >= MAX_CACHE) {
    const firstKey = responseCache.keys().next().value;
    if (firstKey) responseCache.delete(firstKey);
  }
  responseCache.set(key, { compressed: gzipSync(body, { level: 1 }), contentType, cacheControl, hits: 0 });
}

function getCacheStats() {
  const top: Array<{ url: string; hits: number }> = [];
  for (const [k, v] of responseCache) top.push({ url: k, hits: v.hits });
  top.sort((a, b) => b.hits - a.hits);
  const total = totalHits + totalMisses;
  return {
    enabled: WORKER_CACHE_ENABLED,
    size: responseCache.size, maxSize: MAX_CACHE,
    totalHits, totalMisses,
    hitRate: total > 0 ? Math.round(totalHits / total * 1000) / 1000 : 0,
    top10: top.slice(0, 10),
  };
}

export { inflight, eventLoopLag, cacheWarmed, cacheWarmedAt, getCacheStats, getRollingMetrics };

// --- Edge TTL: fast startsWith checks instead of regex ---
function getEdgeTtl(p: string): number {
  const c = p.charCodeAt(1); // first char after '/'
  // Detail pages: 24h (86400s)
  // Most start with p(rovider), e(mployer), s(chool/tate), c(ity/ounty/hapter), m(etro),
  // f(acility), d(rug), b(reed), a(irport), l(ender), o(ccupation), j(ob), z(ip)
  if (c === 112 || c === 101 || c === 102 || c === 100 || c === 98 || c === 97 ||
      c === 108 || c === 111 || c === 106 || c === 122) {
    // These are single-char fast checks for common detail page prefixes
    // p=112 e=101 f=102 d=100 b=98 a=97 l=108 o=111 j=106 z=122
    return 86400;
  }
  if (p.startsWith('/s') || p.startsWith('/c') || p.startsWith('/m')) return 86400;
  if (p.startsWith('/ranking') || p.startsWith('/guide')) return 21600;
  return 3600;
}

export const onRequest = defineMiddleware(async (context, next) => {
  const path = context.url.pathname;
  (context.locals as any).runtime = { env: { DB: getDb() } };

  // Fast-path: health endpoint — always available, even during warming
  if (path === '/health') return next();

  // Fast-path: static assets + cluster management
  if (path.charCodeAt(1) === 95) return next(); // starts with '/_' (_astro, _cluster)
  if (path.startsWith('/fav')) return next();

  // No blocking — requests always proceed. Cache hits use cache, misses go to DB.

  if (context.request.method === 'GET') {
    const cacheKey = path + context.url.search;

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

    // L1: In-memory LRU cache
    const cached = getCached(cacheKey);
    if (cached) return cached;

    // Render
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
          const body = await response.text();
          const cc = `public, max-age=300, s-maxage=${ttl}`;
          // Sitemaps: disk-only (don't consume LRU memory — preserves page cache)
          if (isSitemapPath(path) && body.length > 50) {
            saveSitemapToDisk(path, body);
            return new Response(body, { headers: { 'Content-Type': ct, 'Cache-Control': cc, 'X-Cache': 'MISS' } });
          }
          setCache(cacheKey, body, ct, cc);
          return new Response(body, {
            headers: { 'Content-Type': ct, 'Cache-Control': cc, 'X-Cache': 'MISS' },
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
