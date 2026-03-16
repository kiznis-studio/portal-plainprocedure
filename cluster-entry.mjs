/**
 * Cluster entry point with shared response cache at primary.
 *
 * Architecture:
 *   Caddy → Primary :4321 (shared response cache)
 *            ├── Cache HIT → serve directly (~0.5ms, no worker)
 *            └── Cache MISS → proxy to worker → cache → serve
 *                 ├── Worker 0 :14321 (render + query cache warming)
 *                 ├── Worker 1 :14322 (render only)
 *                 └── Worker N :1432N (render only)
 *
 * Benefits vs per-worker cache:
 *   - ONE response cache not N duplicates
 *   - Cache hits never reach workers — zero worker CPU for cached pages
 *   - New workers scale instantly (no cold cache)
 *   - Only worker 0 warms query cache — no duplicate DB work
 */

import cluster from 'node:cluster';
import http from 'node:http';
import { writeFileSync } from 'node:fs';
import { gzipSync, gunzipSync } from 'node:zlib';

const MIN_WORKERS = 1;
const MAX_WORKERS = parseInt(process.env.WORKERS_MAX || '4', 10);
const EXTERNAL_PORT = parseInt(process.env.PORT || '4321', 10);
const INTERNAL_BASE_PORT = EXTERNAL_PORT + 10000; // workers: 14321, 14322, ...
const HOST = process.env.HOST || '0.0.0.0';
let targetWorkers = parseInt(process.env.WORKERS || '1', 10);

if (cluster.isPrimary) {
  // V8 serialization for IPC — natively handles Map, Set, Date, RegExp, ArrayBuffer.
  // Must be called before fork() and only in primary process.
  cluster.setupPrimary({ serialization: 'advanced' });

  // ─── Shared response cache (owned by primary) ───
  const MAX_CACHE = parseInt(process.env.CACHE_ENTRIES || '5000', 10);
  const responseCache = new Map(); // key → { compressed, contentType, cacheControl, hits }
  let totalHits = 0;
  let totalMisses = 0;

  function getCached(key) {
    const entry = responseCache.get(key);
    if (!entry) { totalMisses++; return null; }
    responseCache.delete(key);
    entry.hits++;
    responseCache.set(key, entry);
    totalHits++;
    return entry;
  }

  function setCache(key, compressed, contentType, cacheControl) {
    if (responseCache.has(key)) responseCache.delete(key);
    if (responseCache.size >= MAX_CACHE) {
      const firstKey = responseCache.keys().next().value;
      if (firstKey) responseCache.delete(firstKey);
    }
    responseCache.set(key, { compressed, contentType, cacheControl, hits: 0 });
  }

  // ─── Worker management ───
  const workerPorts = []; // active worker ports for round-robin
  const gracefullyShuttingDown = new Set();
  let nextWorker = 0;
  let workerIndex = 0;

  function forkWorker(isFirst) {
    const port = INTERNAL_BASE_PORT + workerIndex++;
    const w = cluster.fork({
      CACHE_WARM_WORKER: isFirst ? '1' : '0',
      WORKER_INTERNAL: '1',
      PORT: String(port),
      HOST: '127.0.0.1',
    });
    w._assignedPort = port;
    w.on('message', msg => handleWorkerMessage(w, msg));
    // Poll worker health until it responds — avoids race condition (ECONNRESET)
    const readyCheck = setInterval(() => {
      if (w.isDead()) { clearInterval(readyCheck); return; }
      const probe = http.get(`http://127.0.0.1:${port}/health`, (res) => {
        if (res.statusCode === 200 && !workerPorts.includes(port)) {
          workerPorts.push(port);
          console.log(`[cluster] Worker ${w.process.pid} ready on :${port}`);
        }
        res.resume();
        clearInterval(readyCheck);
      });
      probe.on('error', () => {}); // not ready yet — retry on next interval
      probe.setTimeout(1000, () => probe.destroy());
    }, 500);
    return w;
  }

  // ─── Shared query cache (broker for worker IPC) ───
  // Worker 0 warms queries → sends results to primary via IPC.
  // Other workers ask primary → get instant hits. Zero duplicate DB work.
  const sharedQueryCache = new Map(); // key → value

  function handleWorkerMessage(worker, msg) {
    if (!msg || !msg.type) return;

    if (msg.type === 'qcache-set') {
      // Worker computed a value — store in shared cache
      sharedQueryCache.set(msg.key, msg.value);
    }

    if (msg.type === 'qcache-get') {
      // Worker asking for a cached value
      const hit = sharedQueryCache.has(msg.key);
      worker.send({
        type: 'qcache-result',
        key: msg.key,
        hit,
        value: hit ? sharedQueryCache.get(msg.key) : null,
      });
    }

    if (msg.type === 'listening') {
      workerPorts.push(worker._assignedPort);
      console.log(`[cluster] Worker ${worker.process.pid} ready on :${worker._assignedPort}`);
    }
  }

  // ─── Crash throttle — prevent rapid-fire restarts under memory pressure ───
  const recentCrashes = [];
  const CRASH_WINDOW = 60_000;  // 60s sliding window
  const MAX_CRASHES = 5;        // max crashes before backing off
  const BACKOFF_MS = 30_000;    // 30s cooldown before next restart attempt
  let backoffTimer = null;

  console.log(`[cluster] Primary ${process.pid} starting ${targetWorkers} workers`);
  for (let i = 0; i < targetWorkers; i++) {
    forkWorker(i === 0);
  }

  cluster.on('exit', (worker, code, signal) => {
    const idx = workerPorts.indexOf(worker._assignedPort);
    if (idx !== -1) workerPorts.splice(idx, 1);

    if (gracefullyShuttingDown.has(worker.id)) {
      gracefullyShuttingDown.delete(worker.id);
      console.log(`[cluster] Worker ${worker.process.pid} shut down gracefully`);
      return;
    }

    // Track crash frequency
    const now = Date.now();
    recentCrashes.push(now);
    while (recentCrashes.length > 0 && now - recentCrashes[0] > CRASH_WINDOW) {
      recentCrashes.shift();
    }

    const reason = signal || `code ${code}`;
    if (recentCrashes.length >= MAX_CRASHES) {
      console.warn(`[cluster] Worker ${worker.process.pid} crashed (${reason}) — ${recentCrashes.length} crashes in 60s, backing off ${BACKOFF_MS / 1000}s`);
      if (!backoffTimer) {
        backoffTimer = setTimeout(() => {
          backoffTimer = null;
          if (Object.keys(cluster.workers).length < targetWorkers) {
            console.log(`[cluster] Backoff expired, restarting worker`);
            forkWorker(false);
          }
        }, BACKOFF_MS);
      }
      return;
    }

    console.warn(`[cluster] Worker ${worker.process.pid} crashed (${reason}), restarting`);
    if (Object.keys(cluster.workers).length < targetWorkers) {
      forkWorker(false);
    }
  });

  // ─── Edge TTL (matches middleware.ts) ───
  function getEdgeTtl(p) {
    const c = p.charCodeAt(1);
    if (c === 112 || c === 101 || c === 102 || c === 100 || c === 98 || c === 97 ||
        c === 108 || c === 111 || c === 106 || c === 122) return 86400;
    if (p.startsWith('/s') || p.startsWith('/c') || p.startsWith('/m')) return 86400;
    if (p.startsWith('/ranking') || p.startsWith('/guide')) return 21600;
    return 3600;
  }

  // ─── Proxy to worker ───
  function proxyToWorker(req, res) {
    if (workerPorts.length === 0) {
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      res.end('No workers available');
      return;
    }
    const port = workerPorts[nextWorker++ % workerPorts.length];
    const proxyReq = http.request(
      { hostname: '127.0.0.1', port, path: req.url, method: req.method, headers: req.headers },
      (proxyRes) => {
        const ct = proxyRes.headers['content-type'] || '';
        const status = proxyRes.statusCode;
        const cacheable = req.method === 'GET' && status === 200 &&
                          (ct.includes('text/html') || ct.includes('xml'));

        if (cacheable) {
          // Buffer response to cache it
          const chunks = [];
          proxyRes.on('data', c => chunks.push(c));
          proxyRes.on('end', () => {
            const body = Buffer.concat(chunks);
            if (body.length > 50 && body[0] === 60) { // starts with '<'
              const path = req.url.split('?')[0];
              const ttl = ct.includes('xml') ? 86400 : getEdgeTtl(path);
              const cc = `public, max-age=300, s-maxage=${ttl}`;
              setCache(req.url, gzipSync(body, { level: 1 }), ct, cc);
              res.writeHead(200, { 'Content-Type': ct, 'Cache-Control': cc, 'X-Cache': 'MISS' });
            } else {
              res.writeHead(status, proxyRes.headers);
            }
            res.end(body);
          });
        } else {
          // Non-cacheable — stream directly
          res.writeHead(status, proxyRes.headers);
          proxyRes.pipe(res);
        }
      }
    );
    proxyReq.on('error', (err) => {
      console.error(`[cluster] Proxy error to :${port}: ${err.message}`);
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'text/plain' });
        res.end('Bad Gateway');
      }
    });
    req.pipe(proxyReq);
  }

  // ─── Primary HTTP server — caching proxy ───
  http.createServer((req, res) => {
    const path = req.url.split('?')[0];

    // Health: primary answers directly when no workers (keeps Docker healthcheck alive)
    if (path === '/health') {
      if (workerPorts.length === 0) {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'degraded', workers: 0, cache: responseCache.size }));
        return;
      }
      proxyToWorker(req, res);
      return;
    }

    // Always proxy: static assets, cluster mgmt, non-GET
    if (path.startsWith('/_') || path.startsWith('/fav')) {
      proxyToWorker(req, res);
      return;
    }

    // GET: check shared cache first
    if (req.method === 'GET') {
      const entry = getCached(req.url);
      if (entry) {
        res.writeHead(200, {
          'Content-Type': entry.contentType,
          'Cache-Control': entry.cacheControl,
          'X-Cache': 'HIT',
        });
        res.end(gunzipSync(entry.compressed));
        return;
      }
    }

    proxyToWorker(req, res);
  }).listen(EXTERNAL_PORT, HOST, () => {
    console.log(`[cluster] Caching proxy on :${EXTERNAL_PORT} (cache max=${MAX_CACHE})`);
  });

  // ─── Management API ───
  const mgmtPort = parseInt(process.env.MGMT_PORT || '4322', 10);
  const TRM_SECRET = process.env.TRM_SECRET || '';

  http.createServer((req, res) => {
    if (TRM_SECRET && req.headers['x-trm-secret'] !== TRM_SECRET) {
      res.writeHead(403);
      res.end('Forbidden');
      return;
    }
    const url = new URL(req.url, `http://localhost:${mgmtPort}`);

    if (req.method === 'GET' && url.pathname === '/_cluster/status') {
      const total = totalHits + totalMisses;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        workers: Object.keys(cluster.workers).length,
        targetWorkers,
        minWorkers: MIN_WORKERS,
        maxWorkers: MAX_WORKERS,
        pids: Object.values(cluster.workers).map(w => w.process.pid),
        workerPorts,
        responseCache: {
          size: responseCache.size,
          maxSize: MAX_CACHE,
          totalHits,
          totalMisses,
          hitRate: total > 0 ? Math.round(totalHits / total * 1000) / 1000 : 0,
        },
        queryCache: {
          size: sharedQueryCache.size,
        },
      }));
      return;
    }

    if (req.method === 'POST' && url.pathname === '/_cluster/scale') {
      let body = '';
      req.on('data', c => body += c);
      req.on('end', () => {
        try {
          const { workers: desired } = JSON.parse(body);
          const clamped = Math.max(MIN_WORKERS, Math.min(MAX_WORKERS, desired));
          const current = Object.keys(cluster.workers).length;

          if (clamped > current) {
            for (let i = 0; i < clamped - current; i++) forkWorker(false);
            console.log(`[cluster] Scaling UP ${current} -> ${clamped}`);
          } else if (clamped < current) {
            const workers = Object.values(cluster.workers);
            const toKill = workers.slice(-(current - clamped));
            for (const w of toKill) {
              gracefullyShuttingDown.add(w.id);
              const idx = workerPorts.indexOf(w._assignedPort);
              if (idx !== -1) workerPorts.splice(idx, 1);
              w.send('graceful-shutdown');
              setTimeout(() => { if (!w.isDead()) w.kill(); }, 10000);
            }
            console.log(`[cluster] Scaling DOWN ${current} -> ${clamped}`);
          }
          targetWorkers = clamped;
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ previous: current, target: clamped }));
        } catch (e) {
          res.writeHead(400);
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }

    res.writeHead(404);
    res.end('Not found');
  }).listen(mgmtPort, '127.0.0.1');

  console.log(`[cluster] Management endpoint on :${mgmtPort}`);

} else {
  // ─── Worker process ───

  // Make workers preferred OOM targets so the primary (cache + proxy) survives.
  // Positive values = more likely to be killed. Default is 0, max 1000.
  // Writing positive values works without CAP_SYS_RESOURCE.
  try { writeFileSync('/proc/self/oom_score_adj', '500'); } catch {}

  process.on('message', msg => {
    if (msg === 'graceful-shutdown') {
      console.log(`[cluster] Worker ${process.pid} shutting down gracefully`);
      setTimeout(() => process.exit(0), 5000);
    }
  });

  // Import Astro SSR — listens on PORT (set to internal port by primary)
  await import('./dist/server/entry.mjs');
}
