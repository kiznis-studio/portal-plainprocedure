/**
 * warmer.mjs — Background cache warming process
 *
 * Spawned by primary via child_process.fork() with capped heap (--max-old-space-size=256).
 * Warms HTML pages via HTTP pre-fetch. Warms sitemaps directly from SQLite
 * with mmap disabled + posix_fadvise page cache eviction to prevent memory bloat.
 *
 * Reports detailed results to primary via IPC:
 *   warm-complete: { pagesWarmed, sitemapsWarmed, sitemapsTotal, failedSitemaps }
 *   warm-failed: { error, pagesWarmed, sitemapsWarmed }
 *   warm-timeout: { pagesWarmed, sitemapsWarmed, sitemapsTotal, memoryPct }
 *
 * Primary raises Sentry alerts for failures/timeouts.
 *
 * Env vars:
 *   DATABASE_PATH       — path to SQLite DB file (default: /data/portal.db)
 *   SITE_URL / SITE     — production URL for sitemap <loc> tags
 *   SITEMAP_BATCH_SIZE  — sitemaps per batch before memory check (default: 10)
 *   SITEMAP_MEMORY_CEILING — stop warming above this cgroup % (default: 0.85)
 *   WARMER_START_DELAY  — ms to wait before starting (default: 10000)
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import http from 'node:http';

const PRIMARY_PORT = parseInt(process.env.PRIMARY_PORT || '4321', 10);
const SITEMAP_CACHE_DIR = '/tmp/sitemap-cache';
const DB_PATH = process.env.DATABASE_PATH || '/data/portal.db';
const SITE_URL = (process.env.SITE_URL || process.env.SITE || 'https://example.com').replace(/\/$/, '');
let pagesWarmed = 0;
let sitemapsWarmed = 0;
let sitemapsTotal = 0;
const failedSitemaps = [];

try { mkdirSync(SITEMAP_CACHE_DIR, { recursive: true }); } catch {}

// ─── Memory awareness ───
function containerMemoryPct() {
  try {
    const max = parseInt(readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim());
    const cur = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
    return max > 0 ? cur / max : 0;
  } catch { return 0; }
}

function shouldPause() {
  const pct = containerMemoryPct();
  if (pct > 0.90) return 30000;
  if (pct > 0.85) return 5000;
  return 0;
}

/**
 * Evict DB file pages from OS page cache (Option B).
 * Even with mmap_size=0, read() syscalls populate OS page cache.
 * posix_fadvise(DONTNEED) tells the kernel those pages can be reclaimed.
 */
function evictDbPageCache() {
  try {
    execFileSync('python3', ['-c',
      `import os; fd=os.open('${DB_PATH}', os.O_RDONLY); os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED); os.close(fd)`
    ], { timeout: 5000, stdio: 'ignore' });
  } catch {
    // python3 may not be available — non-fatal
  }
}

// ─── Page pre-fetch warming ───
async function warmPages() {
  console.log('[warmer] Starting page pre-fetch warming...');

  await fetchWithRetry('/');
  pagesWarmed++;

  for (const page of ['/states', '/rankings', '/search', '/guides']) {
    const pause = shouldPause();
    if (pause > 0) {
      console.log(`[warmer] Memory ${Math.round(containerMemoryPct() * 100)}%, pausing ${pause / 1000}s`);
      await sleep(pause);
    }
    await fetchWithRetry(page);
    pagesWarmed++;
    await sleep(1000);
  }

  // Sitemaps: try direct DB first (zero page cache impact), fall back to HTTP
  await warmSitemapsDirect();

  const allSitemapsOk = failedSitemaps.length === 0 && sitemapsWarmed > 0;

  console.log(`[warmer] Complete: ${pagesWarmed} pages, ${sitemapsWarmed}/${sitemapsTotal} sitemaps${failedSitemaps.length > 0 ? ` (${failedSitemaps.length} FAILED)` : ''}`);

  process.send?.({
    type: 'warm-complete',
    pagesWarmed,
    sitemapsWarmed,
    sitemapsTotal,
    failedSitemaps: failedSitemaps.slice(0, 10),
    allSitemapsOk,
  });
}

// ─── Direct DB sitemap generation (Option A: mmap_size=0 + Option B: fadvise) ───
async function warmSitemapsDirect() {
  if (!existsSync(DB_PATH)) {
    return warmSitemapsHttp();
  }

  let Database;
  try {
    Database = (await import('better-sqlite3')).default;
  } catch {
    console.log('[warmer] better-sqlite3 not available — HTTP fallback');
    return warmSitemapsHttp();
  }

  let db;
  try {
    // Option A: Separate read-only connection with mmap DISABLED.
    // Prevents sitemap generation from bloating OS page cache (cgroup memory).
    db = new Database(DB_PATH, { readonly: true, fileMustExist: true });
    db.pragma('mmap_size=0');
    db.pragma('cache_size=-5120');   // 5MB bounded internal cache
    db.pragma('journal_mode=DELETE');

    // Auto-detect sitemap_pages table
    const hasSitemapPages = db.prepare(
      "SELECT 1 FROM sqlite_master WHERE type='table' AND name='sitemap_pages'"
    ).get();

    if (!hasSitemapPages) {
      db.close();
      console.log('[warmer] No sitemap_pages table — HTTP fallback');
      return warmSitemapsHttp();
    }

    // Discover boundary column name (start_npi, start_key, start_slug, etc.)
    const cols = db.prepare("PRAGMA table_info(sitemap_pages)").all();
    const boundaryCol = cols.find(c => c.name.startsWith('start_'))?.name;
    if (!boundaryCol) {
      db.close();
      console.log('[warmer] No start_* column in sitemap_pages — HTTP fallback');
      return warmSitemapsHttp();
    }

    // Discover which table the boundary references by checking the main entity table
    // Convention: sitemap_pages boundary comes from the largest user table
    const tables = db.prepare(
      "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '%_stats' AND name NOT LIKE 'sitemap_%' AND name NOT LIKE '%_cache' ORDER BY name"
    ).all().map(t => t.name);

    // Find the entity table + slug column by inspecting sitemap page file naming
    // First, generate the index XML via HTTP to discover child sitemap URLs
    const indexXml = await fetchPage('/sitemap-index.xml');
    if (!indexXml || !indexXml.includes('<sitemapindex')) {
      db.close();
      return warmSitemapsHttp();
    }

    saveSitemap('/sitemap-index.xml', indexXml);

    // Parse child sitemap URLs to identify paginated entity sitemaps
    const locs = [...indexXml.matchAll(/<loc>(.*?)<\/loc>/g)]
      .map(m => { try { return new URL(m[1]).pathname; } catch { return null; } })
      .filter(Boolean);

    sitemapsTotal = locs.length + 1;
    sitemapsWarmed = 1;

    // Identify paginated sitemaps (pattern: /sitemap-NAME-N.xml where N is a number)
    const paginatedPattern = /^\/sitemap-(.+)-(\d+)\.xml$/;
    const paginatedGroups = new Map(); // prefix → [page numbers]
    const nonPaginated = [];

    for (const loc of locs) {
      const match = loc.match(paginatedPattern);
      if (match) {
        const prefix = match[1];
        const page = parseInt(match[2]);
        if (!paginatedGroups.has(prefix)) paginatedGroups.set(prefix, []);
        paginatedGroups.get(prefix).push({ page, path: loc });
      } else {
        nonPaginated.push(loc);
      }
    }

    // Generate paginated sitemaps directly from DB
    const BATCH_SIZE = parseInt(process.env.SITEMAP_BATCH_SIZE || '10', 10);
    const MEMORY_CEILING = parseFloat(process.env.SITEMAP_MEMORY_CEILING || '0.85');
    const pageCountRow = db.prepare('SELECT MAX(page) as max_page FROM sitemap_pages').get();
    const maxPage = pageCountRow?.max_page ?? 0;

    if (maxPage > 0 && paginatedGroups.size > 0) {
      // Find the entity table: look for a table with a 'slug' column
      const entityTable = tables.find(t => {
        const info = db.prepare(`PRAGMA table_info(${t})`).all();
        return info.some(c => c.name === 'slug');
      });

      if (entityTable) {
        const getBoundary = db.prepare(`SELECT ${boundaryCol} FROM sitemap_pages WHERE page = ?`);
        const orderCol = boundaryCol.replace('start_', '');
        const getSlugs = db.prepare(`SELECT slug FROM ${entityTable} WHERE ${orderCol} >= ? ORDER BY ${orderCol} LIMIT 50000`);

        // Detect the entity URL prefix from the first paginated sitemap's name
        // e.g., sitemap-providers-1.xml → entity prefix is likely /provider/
        // This is a heuristic — the Astro page handler defines the actual prefix
        const firstPrefix = [...paginatedGroups.keys()][0];
        // Convention: sitemap prefix is plural, URL is singular (providers → /provider/)
        const entityPath = '/' + firstPrefix.replace(/s$/, '') + '/';

        const memBefore = Math.round(containerMemoryPct() * 100);
        console.log(`[warmer] Direct DB: ${maxPage} paginated sitemaps (table=${entityTable}, boundary=${boundaryCol}, mmap=off, memory=${memBefore}%)`);

        for (let page = 1; page <= maxPage; page++) {
          const cachePath = `/sitemap-${firstPrefix}-${page}.xml`;
          if (existsSitemap(cachePath)) { sitemapsWarmed++; continue; }

          const boundary = getBoundary.get(page);
          if (!boundary) { failedSitemaps.push(cachePath); continue; }

          const slugs = getSlugs.all(boundary[boundaryCol]).map(r => r.slug);
          if (slugs.length === 0) { failedSitemaps.push(cachePath); continue; }

          const xml = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
            ...slugs.map(s => `  <url><loc>${SITE_URL}${entityPath}${s}</loc></url>`),
            '</urlset>',
          ].join('\n');

          saveSitemap(cachePath, xml);
          sitemapsWarmed++;

          // After each batch: evict DB pages from OS page cache (Option B)
          if (page % BATCH_SIZE === 0) {
            evictDbPageCache();
            const memPct = containerMemoryPct();
            if (memPct > MEMORY_CEILING) {
              console.log(`[warmer] Memory ${Math.round(memPct * 100)}% > ceiling — pausing`);
              await sleep(5000);
              evictDbPageCache();
              if (containerMemoryPct() > 0.90) {
                console.log(`[warmer] Memory still critical — stopping paginated sitemaps`);
                break;
              }
            }
          }
        }

        evictDbPageCache(); // final cleanup
        const memAfter = Math.round(containerMemoryPct() * 100);
        console.log(`[warmer] Paginated sitemaps done (memory: ${memBefore}%→${memAfter}%)`);
      }
    }

    db.close();

    // Warm non-paginated sitemaps via HTTP (small, negligible memory)
    for (const loc of nonPaginated) {
      if (existsSitemap(loc)) { sitemapsWarmed++; continue; }
      const xml = await fetchPage(loc);
      if (xml && xml.length > 50) { saveSitemap(loc, xml); sitemapsWarmed++; }
      else { failedSitemaps.push(loc); }
      await sleep(500);
    }

  } catch (err) {
    if (db) try { db.close(); } catch {}
    failedSitemaps.push(`direct-gen: ${err.message}`);
    console.error('[warmer] Direct sitemap generation failed:', err.message);
    return warmSitemapsHttp();
  }
}

// ─── HTTP fallback (portals without better-sqlite3 or sitemap_pages) ───
async function warmSitemapsHttp() {
  try {
    const indexXml = await fetchPage('/sitemap-index.xml');
    if (!indexXml || !indexXml.includes('<sitemapindex')) {
      const fallback = await fetchPage('/sitemap.xml');
      if (fallback && fallback.includes('<urlset')) {
        saveSitemap('/sitemap.xml', fallback);
        sitemapsWarmed = 1;
        sitemapsTotal = 1;
      } else {
        failedSitemaps.push('/sitemap-index.xml (not found)');
      }
      return;
    }

    if (!existsSitemap('/sitemap-index.xml')) saveSitemap('/sitemap-index.xml', indexXml);
    const locs = [...indexXml.matchAll(/<loc>(.*?)<\/loc>/g)]
      .map(m => { try { return new URL(m[1]).pathname; } catch { return null; } })
      .filter(Boolean);

    if (!sitemapsTotal) sitemapsTotal = locs.length + 1;
    if (!sitemapsWarmed) sitemapsWarmed = 1;

    for (let i = 0; i < locs.length; i++) {
      const loc = locs[i];
      const pause = shouldPause();
      if (pause > 0) await sleep(pause);
      if (existsSitemap(loc)) { sitemapsWarmed++; continue; }

      const xml = await fetchPage(loc);
      if (xml && xml.length > 50) { saveSitemap(loc, xml); sitemapsWarmed++; }
      else { failedSitemaps.push(loc); }
      await sleep(2000);

      if (i > 0 && i % 5 === 0) evictDbPageCache();
    }
  } catch (err) {
    failedSitemaps.push(`http: ${err.message}`);
  }
}

// ─── Helpers ───
function fetchPage(urlPath) {
  return new Promise((resolve) => {
    const req = http.get(
      { hostname: '127.0.0.1', port: PRIMARY_PORT, path: urlPath, timeout: 30000 },
      (res) => {
        let body = '';
        res.on('data', (c) => body += c);
        res.on('end', () => resolve(body));
      }
    );
    req.on('error', () => resolve(''));
    req.on('timeout', () => { req.destroy(); resolve(''); });
  });
}

async function fetchWithRetry(urlPath, retries = 2) {
  for (let i = 0; i <= retries; i++) {
    const result = await fetchPage(urlPath);
    if (result && result.length > 50) return result;
    if (i < retries) await sleep(2000);
  }
  return '';
}

function saveSitemap(urlPath, body) {
  try {
    writeFileSync(`${SITEMAP_CACHE_DIR}/${encodeURIComponent(urlPath)}.xml`, body, 'utf-8');
  } catch {}
}

function existsSitemap(urlPath) {
  try {
    return existsSync(`${SITEMAP_CACHE_DIR}/${encodeURIComponent(urlPath)}.xml`);
  } catch { return false; }
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

// ─── Main ───
const startDelay = parseInt(process.env.WARMER_START_DELAY || '10000', 10);
console.log(`[warmer] PID ${process.pid} starting in ${startDelay / 1000}s (waiting for workers)`);

setTimeout(async () => {
  try {
    await warmPages();
  } catch (err) {
    console.error('[warmer] Fatal error:', err.message);
    process.send?.({
      type: 'warm-failed',
      error: err.message,
      pagesWarmed,
      sitemapsWarmed,
    });
  }
  process.exit(0);
}, startDelay);

// Safety timeout
setTimeout(() => {
  const memPct = Math.round(containerMemoryPct() * 100);
  console.log(`[warmer] Timeout (10 min) — warmed ${pagesWarmed} pages, ${sitemapsWarmed}/${sitemapsTotal} sitemaps (memory: ${memPct}%)`);
  process.send?.({
    type: 'warm-timeout',
    pagesWarmed,
    sitemapsWarmed,
    sitemapsTotal,
    failedSitemaps: failedSitemaps.slice(0, 10),
    memoryPct: memPct,
  });
  process.exit(0);
}, 600000);
