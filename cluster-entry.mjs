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
import { fork } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { gzipSync, gunzipSync } from 'node:zlib';

const MIN_WORKERS = 1;
const CONFIGURED_MAX_WORKERS = parseInt(process.env.WORKERS_MAX || '4', 10);
const EXTERNAL_PORT = parseInt(process.env.PORT || '4321', 10);
const INTERNAL_BASE_PORT = EXTERNAL_PORT + 10000; // workers: 14321, 14322, ...
const HOST = process.env.HOST || '0.0.0.0';
let targetWorkers = parseInt(process.env.WORKERS || '1', 10);

if (cluster.isPrimary) {
  // V8 serialization for IPC — natively handles Map, Set, Date, RegExp, ArrayBuffer.
  // Must be called before fork() and only in primary process.
  cluster.setupPrimary({ serialization: 'advanced' });

  // ─── Sentry integration (crash reporting from primary) ───
  let Sentry = null;
  const SENTRY_DSN = process.env.SENTRY_DSN || '';
  if (SENTRY_DSN) {
    try {
      Sentry = await import('@sentry/node');
      Sentry.init({
        dsn: SENTRY_DSN,
        environment: 'production',
        serverName: process.env.PORTAL_NAME || 'unknown-portal',
        autoSessionTracking: false,
        tracesSampleRate: 0,
      });
      console.log('[sentry] Initialized for crash reporting');
    } catch (e) {
      console.warn('[sentry] Failed to initialize:', e.message);
    }
  }

  // Rate-limit Sentry events to avoid flooding (max 1 per event type per 60s)
  const sentryRateLimit = new Map(); // eventType → lastSentTimestamp
  const SENTRY_RATE_LIMIT_MS = 60000;

  function reportToSentry(eventType, extra = {}) {
    if (!Sentry) return;
    const now = Date.now();
    const lastSent = sentryRateLimit.get(eventType) || 0;
    if (now - lastSent < SENTRY_RATE_LIMIT_MS) return;
    sentryRateLimit.set(eventType, now);

    Sentry.captureMessage(`[cluster] ${eventType}`, {
      level: eventType.includes('crash') || eventType.includes('degraded') || eventType.includes('50') ? 'error' : 'warning',
      tags: { portal: process.env.PORTAL_NAME || 'unknown', component: 'cluster' },
      extra,
    });
  }

  // ─── Memory budget — auto-calculates all allocations from cgroup limit ───
  const { calculateBudget, logBudget } = await import('./memory-budget.mjs');
  const budget = calculateBudget();
  logBudget(budget);

  const MAX_WORKERS = budget.effectiveWorkersMax;
  if (MAX_WORKERS < CONFIGURED_MAX_WORKERS) {
    console.warn(`[cluster] WORKERS_MAX reduced ${CONFIGURED_MAX_WORKERS} → ${MAX_WORKERS} (memory budget)`);
  }

  // ─── Shared response cache (owned by primary) ───
  const MAX_CACHE = budget.responseCacheEntries;
  if (process.env.CACHE_ENTRIES) {
    console.warn(`[cluster] CACHE_ENTRIES env var ignored — using budget: ${MAX_CACHE} entries`);
  }
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
      // Memory budget → workers
      WORKER_RESPONSE_CACHE: '0',  // Primary handles all caching
      SQLITE_CACHE_KB: String(budget.sqliteCacheKB),
      SQLITE_MMAP_BYTES: String(budget.sqliteMmapBytes),
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
  // Bounded query cache with LRU eviction (budget-allocated)
  const QUERY_CACHE_MAX = budget.queryCacheMax;
  const sharedQueryCache = new Map(); // key → value

  function setQueryCache(key, value) {
    if (sharedQueryCache.has(key)) sharedQueryCache.delete(key);
    if (sharedQueryCache.size >= QUERY_CACHE_MAX) {
      const firstKey = sharedQueryCache.keys().next().value;
      if (firstKey !== undefined) sharedQueryCache.delete(firstKey);
    }
    sharedQueryCache.set(key, value);
  }

  function handleWorkerMessage(worker, msg) {
    if (!msg || !msg.type) return;

    if (msg.type === 'qcache-set') {
      // Worker computed a value — store in shared cache
      setQueryCache(msg.key, msg.value);
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

  targetWorkers = Math.min(targetWorkers, MAX_WORKERS);
  console.log(`[cluster] Primary ${process.pid} starting ${targetWorkers} workers (max=${MAX_WORKERS})`);
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
    reportToSentry('worker_crash', {
      pid: worker.process.pid,
      signal,
      code,
      crashCount: recentCrashes.length,
      workersRemaining: Object.keys(cluster.workers).length,
    });

    if (Object.keys(cluster.workers).length === 0) {
      reportToSentry('portal_degraded', {
        reason: 'All workers crashed',
        crashCount: recentCrashes.length,
      });
    }

    if (Object.keys(cluster.workers).length < targetWorkers) {
      forkWorker(false);
    }
  });

  // ─── Background warmer (separate process, capped heap) ───
  let warmerProcess = null;
  let warmerCooldown = false;
  let warmerDone = false; // Set to true after warmer completes — slow response alerts suppressed until then
  const WARMER_COOLDOWN_MS = 60000; // 1 min cooldown between warmer restarts

  function spawnWarmer() {
    if (warmerCooldown || warmerProcess) return;

    // Wait for at least 1 worker to be ready
    if (workerPorts.length === 0) {
      setTimeout(spawnWarmer, 2000);
      return;
    }

    const warmerHeap = Math.min(256, budget.warmerHeapMB || 128);
    warmerProcess = fork('./warmer.mjs', [], {
      execArgv: [`--max-old-space-size=${warmerHeap}`],
      env: {
        ...process.env,
        PRIMARY_PORT: String(EXTERNAL_PORT),
        WARMER_START_DELAY: '5000',
      },
      stdio: ['ignore', 'inherit', 'inherit', 'ipc'],
    });

    warmerProcess.on('message', (msg) => {
      if (msg.type === 'warm-complete') {
        console.log('[warmer] Warming completed successfully');
        warmerDone = true;
      }
      if (msg.type === 'warm-failed') {
        console.error(`[warmer] Failed: ${msg.error}`);
      }
    });

    warmerProcess.on('exit', (code) => {
      warmerProcess = null;
      if (code !== 0 && code !== null) {
        console.warn(`[warmer] Crashed (code ${code}), cooldown ${WARMER_COOLDOWN_MS / 1000}s`);
        reportToSentry('warmer_crash', { exitCode: code });
        warmerCooldown = true;
        setTimeout(() => { warmerCooldown = false; }, WARMER_COOLDOWN_MS);
      }
    });

    console.log(`[warmer] Spawned PID ${warmerProcess.pid} (${warmerHeap}MB heap cap)`);
  }

  // Start warmer after a brief delay (let workers init first)
  // Skip warmer for tiny containers — budget.warmerHeapMB=0 means container <512MB
  if (budget.warmerHeapMB > 0) {
    setTimeout(spawnWarmer, 5000);
  } else {
    console.log('[warmer] Disabled — container too small for background warming');
    warmerDone = true; // No warmup phase — slow alerts active immediately
  }

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
      reportToSentry('proxy_503', { url: req.url, reason: 'no_workers' });
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      res.end('No workers available');
      return;
    }
    const port = workerPorts[nextWorker++ % workerPorts.length];
    const proxyStart = Date.now();
    const proxyReq = http.request(
      { hostname: '127.0.0.1', port, path: req.url, method: req.method, headers: req.headers },
      (proxyRes) => {
        const ct = proxyRes.headers['content-type'] || '';
        const status = proxyRes.statusCode;

        // Report 5xx errors from workers to Sentry
        if (status >= 500) {
          reportToSentry(`worker_${status}`, { url: req.url, workerPort: port });
        }

        // Report slow responses (>3s) to Sentry — only after warmer completes
        // Cold pages during warmup are expected and would flood Sentry on every deploy
        const elapsed = Date.now() - proxyStart;
        if (warmerDone && elapsed > 3000) {
          reportToSentry('slow_response', { url: req.url, elapsedMs: elapsed, workerPort: port });
        }

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
      reportToSentry('proxy_502', { url: req.url, workerPort: port, error: err.message });
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

    // /ping: comprehensive external health check (for Kuma monitoring)
    if (path === '/ping') {
      const checks = {
        workers: workerPorts.length > 0 ? 'ok' : 'no_workers',
        cache: responseCache.size > 0 ? `ok (${responseCache.size} entries)` : 'cold',
        queryCache: sharedQueryCache.size > 0 ? `ok (${sharedQueryCache.size} entries)` : 'cold',
        warmer: warmerProcess && !warmerProcess.killed ? 'running' : (warmerDone ? 'done' : 'idle'),
      };

      try {
        const max = parseInt(readFileSync('/sys/fs/cgroup/memory.max', 'utf-8').trim());
        const cur = parseInt(readFileSync('/sys/fs/cgroup/memory.current', 'utf-8').trim());
        const pct = Math.round(cur / max * 100);
        checks.memory = pct < 90 ? `ok (${pct}% of ${Math.round(max / 1048576)}MB)` : `warning (${pct}%)`;
      } catch { checks.memory = 'unknown'; }

      const allOk = checks.workers === 'ok' && !String(checks.memory).startsWith('warning');
      const status = allOk ? 'healthy' : 'degraded';

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status,
        checks,
        uptime: Math.round(process.uptime()),
        timestamp: new Date().toISOString(),
      }));
      return;
    }

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
    console.log(`[cluster] Caching proxy on :${EXTERNAL_PORT} (cache=${MAX_CACHE} entries, qcache=${QUERY_CACHE_MAX} max)`);
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
          maxSize: QUERY_CACHE_MAX,
        },
        budget,
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
