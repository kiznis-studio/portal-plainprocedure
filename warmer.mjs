/**
 * warmer.mjs — Background cache warming process
 *
 * Spawned by primary via child_process.fork() with capped heap (--max-old-space-size=256).
 * Warms caches by pre-fetching pages through the primary HTTP server.
 *
 * Reports detailed results to primary via IPC:
 *   warm-complete: { pagesWarmed, sitemapsWarmed, sitemapsTotal, failedSitemaps }
 *   warm-failed: { error, pagesWarmed, sitemapsWarmed }
 *   warm-timeout: { pagesWarmed, sitemapsWarmed, sitemapsTotal, memoryPct }
 *
 * Primary raises Sentry alerts for failures/timeouts.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import http from 'node:http';

const PRIMARY_PORT = parseInt(process.env.PRIMARY_PORT || '4321', 10);
const SITEMAP_CACHE_DIR = '/tmp/sitemap-cache';
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
  if (pct > 0.90) return 30000; // 30s — critical, near OOM
  if (pct > 0.85) return 5000;  // 5s — elevated
  return 0;                     // below 85% — proceed
}

// ─── Page pre-fetch warming ───
async function warmPages() {
  console.log('[warmer] Starting page pre-fetch warming...');

  // Priority 1: Homepage
  await fetchWithRetry('/');
  pagesWarmed++;

  // Priority 2: Listing pages
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

  // Priority 3: Sitemaps
  await warmSitemaps();

  const allSitemapsOk = failedSitemaps.length === 0 && sitemapsWarmed > 0;

  console.log(`[warmer] Complete: ${pagesWarmed} pages, ${sitemapsWarmed}/${sitemapsTotal} sitemaps${failedSitemaps.length > 0 ? ` (${failedSitemaps.length} FAILED)` : ''}`);

  process.send?.({
    type: 'warm-complete',
    pagesWarmed,
    sitemapsWarmed,
    sitemapsTotal,
    failedSitemaps: failedSitemaps.slice(0, 10), // cap to avoid huge IPC message
    allSitemapsOk,
  });
}

// ─── Sitemap warming ───
async function warmSitemaps() {
  try {
    const indexXml = await fetchPage('/sitemap-index.xml');
    if (!indexXml || !indexXml.includes('<sitemapindex')) {
      // Try /sitemap.xml
      const fallback = await fetchPage('/sitemap.xml');
      if (fallback && fallback.includes('<urlset')) {
        saveSitemap('/sitemap.xml', fallback);
        sitemapsWarmed = 1;
        sitemapsTotal = 1;
        console.log('[warmer] Warmed 1 sitemap (single file)');
      } else {
        // No sitemap at all — this is a problem
        failedSitemaps.push('/sitemap-index.xml (not found)');
        console.error('[warmer] No sitemap found at /sitemap-index.xml or /sitemap.xml');
      }
      return;
    }

    saveSitemap('/sitemap-index.xml', indexXml);

    // Parse child sitemap URLs
    const locs = [...indexXml.matchAll(/<loc>(.*?)<\/loc>/g)]
      .map(m => { try { return new URL(m[1]).pathname; } catch { return null; } })
      .filter(Boolean);

    sitemapsTotal = locs.length + 1; // +1 for the index itself
    let warmed = 1;

    for (const loc of locs) {
      const pause = shouldPause();
      if (pause > 0) {
        console.log(`[warmer] Memory ${Math.round(containerMemoryPct() * 100)}%, pausing ${pause / 1000}s`);
        await sleep(pause);
      }
      if (existsSitemap(loc)) { warmed++; continue; }

      const xml = await fetchPage(loc);
      if (xml && xml.length > 50) {
        saveSitemap(loc, xml);
        warmed++;
      } else {
        failedSitemaps.push(loc);
        console.warn(`[warmer] Sitemap FAILED: ${loc} (${xml ? xml.length + ' bytes' : 'empty/error'})`);
      }
      await sleep(2000);
    }

    sitemapsWarmed = warmed;
    console.log(`[warmer] Warmed ${warmed}/${sitemapsTotal} sitemaps to disk${failedSitemaps.length > 0 ? ` — ${failedSitemaps.length} FAILED` : ''}`);
  } catch (err) {
    failedSitemaps.push(`exception: ${err.message}`);
    console.error('[warmer] Sitemap warming failed:', err.message);
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

// Safety timeout — report what was achieved before exit
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
