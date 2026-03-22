/**
 * warmer.mjs — Background cache warming process (KIZ-319)
 *
 * Spawned by primary via child_process.fork() with capped heap (--max-old-space-size=256).
 * Warms caches by pre-fetching pages through the primary HTTP server, which routes to
 * workers. Workers compute queries → disk-cache.ts persists results → all workers benefit.
 *
 * This process is EXPENDABLE — if it crashes, workers keep serving (cold cache).
 * The primary auto-restarts it after a cooldown.
 */

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import http from 'node:http';

const PRIMARY_PORT = parseInt(process.env.PRIMARY_PORT || '4321', 10);
const SITEMAP_CACHE_DIR = '/tmp/sitemap-cache';

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
  if (pct > 0.85) return 30000; // 30s pause — critical
  if (pct > 0.70) return 5000;  // 5s pause — caution
  return 0;
}

// ─── Page pre-fetch warming ───
async function warmPages() {
  console.log('[warmer] Starting page pre-fetch warming...');

  // Priority 1: Homepage (most important, warms global queries)
  await fetchWithRetry('/');

  // Priority 2: Listing pages (warm state/category queries)
  for (const page of ['/states', '/rankings', '/search', '/guides']) {
    const pause = shouldPause();
    if (pause > 0) {
      console.log(`[warmer] Memory ${Math.round(containerMemoryPct() * 100)}%, pausing ${pause / 1000}s`);
      await sleep(pause);
    }
    await fetchWithRetry(page);
    await sleep(1000); // 1s between pages
  }

  // Priority 3: Sitemaps (crawlers need these)
  await warmSitemaps();

  console.log('[warmer] Warming complete');
  process.send?.({ type: 'warm-complete' });
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
        console.log('[warmer] Warmed 1 sitemap');
      }
      return;
    }

    saveSitemap('/sitemap-index.xml', indexXml);

    // Parse child sitemap URLs
    const locs = [...indexXml.matchAll(/<loc>(.*?)<\/loc>/g)]
      .map(m => { try { return new URL(m[1]).pathname; } catch { return null; } })
      .filter(Boolean);

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
      }
      await sleep(2000); // 2s between sitemaps
    }
    console.log(`[warmer] Warmed ${warmed} sitemaps to disk`);
  } catch (err) {
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
// Wait for workers to be ready before warming
const startDelay = parseInt(process.env.WARMER_START_DELAY || '10000', 10);
console.log(`[warmer] PID ${process.pid} starting in ${startDelay / 1000}s (waiting for workers)`);

setTimeout(async () => {
  try {
    await warmPages();
  } catch (err) {
    console.error('[warmer] Fatal error:', err.message);
    process.send?.({ type: 'warm-failed', error: err.message });
  }
  // Exit cleanly after warming — primary will not restart unless crash (non-zero exit)
  process.exit(0);
}, startDelay);

// Safety timeout — kill if warming takes too long
setTimeout(() => {
  console.log('[warmer] Timeout reached (10 min), exiting');
  process.exit(0);
}, 600000);
