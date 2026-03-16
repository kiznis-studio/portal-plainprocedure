import { defineMiddleware } from 'astro:middleware';
import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'node:fs';
import { gzipSync, gunzipSync } from 'node:zlib';
import http from 'node:http';
import { createD1Adapter } from './lib/d1-adapter';
import { warmQueryCache } from './lib/db';

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

let sitemapsWarmed = false;

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

// --- Background batched cache warming ---
// Only the first cluster worker (CACHE_WARM_WORKER=1) runs proactive warming.
// Additional workers populate cache lazily from real traffic — no duplicate CPU work.
// Warming runs in small batches with pauses so CPU stays low and requests are never blocked.
let cacheWarmed = false;
let cacheWarmedAt: string | null = null;

const WARM_BATCH = parseInt(process.env.CACHE_WARM_BATCH || '10', 10);
const WARM_PAUSE = parseInt(process.env.CACHE_WARM_PAUSE || '500', 10);
const IS_WARM_WORKER = process.env.CACHE_WARM_WORKER !== '0';

function startBackgroundWarming(): void {
  if (!IS_WARM_WORKER) {
    cacheWarmed = true;
    return;
  }
  const database = getDb();
  if (!database) { cacheWarmed = true; return; }
  (async () => {
    try {
      await warmQueryCache(database, WARM_BATCH, WARM_PAUSE);
      cacheWarmedAt = new Date().toISOString();
    } catch (err) {
      console.error('[cache] Warming failed:', err);
    }
    cacheWarmed = true;
  })();
}
startBackgroundWarming();

// --- Sitemap background warming ---
// After main cache warms, self-fetch all sitemaps to populate disk cache.
// Sequential, low priority — does not block request serving.
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

  // Wait for main cache warming + a few seconds for the server to settle
  const checkInterval = setInterval(async () => {
    if (!cacheWarmed) return;
    clearInterval(checkInterval);

    try {
      const indexXml = await selfFetch('/sitemap-index.xml');
      if (!indexXml.includes('<sitemapindex') && !indexXml.includes('<urlset')) {
        // No sitemap index — try /sitemap.xml
        const fallback = await selfFetch('/sitemap.xml');
        if (fallback.includes('<urlset')) {
          saveSitemapToDisk('/sitemap.xml', fallback);
          console.log('[sitemap-cache] Warmed 1 sitemap (sitemap.xml)');
        }
        sitemapsWarmed = true;
        return;
      }

      saveSitemapToDisk('/sitemap-index.xml', indexXml);

      // Parse child sitemap URLs
      const locs = [...indexXml.matchAll(/<loc>(.*?)<\/loc>/g)].map(m => {
        try { return new URL(m[1]).pathname; } catch { return null; }
      }).filter(Boolean) as string[];

      let warmed = 1; // index already cached
      for (const loc of locs) {
        // Memory-aware throttle: gentle slowdown under pressure, never stops
        const memPct = containerMemoryPct();
        if (memPct > 0.80) {
          await new Promise(r => setTimeout(r, 30000)); // 30s — very gentle
        } else if (memPct > 0.65) {
          await new Promise(r => setTimeout(r, 5000));  // 5s — moderate
        }
        if (getSitemapFromDisk(loc)) { warmed++; continue; } // already cached
        const xml = await selfFetch(loc);
        if (xml && xml.length > 50) {
          saveSitemapToDisk(loc, xml);
          warmed++;
        }
        // Throttle: 2s pause between sitemaps to limit CPU/memory pressure
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
