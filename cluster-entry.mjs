/**
 * Cluster entry point — proxy + query cache broker (v2).
 *
 * Architecture:
 *   Caddy → Primary :4321 (proxy + query cache broker)
 *            └── Proxy to worker (retry on failure, round-robin)
 *                 ├── Worker 0 :14321 (render + query cache warming)
 *                 ├── Worker 1 :14322 (render only)
 *                 └── Worker N :1432N (render only)
 *
 * Caching layers:
 *   - CF edge: HTML page cache (300s browser, 86400s edge)
 *   - Sitemap disk: /tmp/sitemap-cache (workers)
 *   - Explicit query cache: cached() in db.ts, warm on startup (IPC → primary)
 *   - Adaptive query cache: auto-discovered hot queries (IPC → primary)
 *   - No in-memory response cache — CF edge handles pages
 */

import cluster from 'node:cluster';
import http from 'node:http';
import { fork } from 'node:child_process';
import { existsSync, readFileSync, statSync, writeFileSync } from 'node:fs';

// Derive portal name: PORTAL_NAME env > hostname (container_name = portal-slug) > 'unknown'
import os from 'node:os';
const PORTAL_NAME = process.env.PORTAL_NAME || os.hostname().replace(/^portal-/, '') || 'unknown';

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
        serverName: PORTAL_NAME,
        autoSessionTracking: false,
        tracesSampleRate: 0,
      });
      console.log('[sentry] Initialized for crash reporting');
    } catch (e) {
      console.warn('[sentry] Failed to initialize:', e.message);
    }
  }

  // ─── Sentry alerting thresholds ───
  const SENTRY_SLOW_MS = parseInt(process.env.SENTRY_SLOW_MS || '2000', 10);
  const PROXY_TIMEOUT_MS = parseInt(process.env.PROXY_TIMEOUT_MS || '15000', 10);

  // Rate-limit Sentry events with count aggregation.
  // Instead of dropping repeated events, we count them and include the count
  // in the next event that fires. This preserves severity information —
  // "14 proxy_502 in 60s" is very different from "1 proxy_502".
  const sentryRateLimit = new Map(); // eventType → { lastSent, suppressedCount }
  const SENTRY_RATE_LIMIT_MS = 60000;

  // Suppress transient deploy noise during container startup (proxy_502/503, slow queries).
  // These fire during image swap and cache warming — not actionable.
  const STARTUP_GRACE_MS = parseInt(process.env.SENTRY_STARTUP_GRACE_MS || '120000', 10);
  const processStartTime = Date.now();
  const STARTUP_SUPPRESSED = new Set(['proxy_502', 'proxy_503', 'slow_query', 'worker_timeout']);

  function reportToSentry(eventType, extra = {}) {
    if (!Sentry) return;
    const now = Date.now();

    // During startup grace period, suppress known deploy-transient events
    if (now - processStartTime < STARTUP_GRACE_MS && STARTUP_SUPPRESSED.has(eventType)) {
      return;
    }
    const state = sentryRateLimit.get(eventType) || { lastSent: 0, suppressedCount: 0 };

    if (now - state.lastSent < SENTRY_RATE_LIMIT_MS) {
      // Within rate limit window — count but don't send yet
      state.suppressedCount++;
      sentryRateLimit.set(eventType, state);
      return;
    }

    // Include suppressed count from previous window if any
    if (state.suppressedCount > 0) {
      extra.suppressedInLastWindow = state.suppressedCount;
      extra.totalOccurrences = state.suppressedCount + 1;
    }

    sentryRateLimit.set(eventType, { lastSent: now, suppressedCount: 0 });

    const isError = eventType.includes('crash') || eventType.includes('degraded') ||
                    eventType.includes('50') || eventType.includes('timeout') ||
                    eventType === 'crash_throttle';
    Sentry.captureMessage(`[cluster] ${eventType}`, {
      level: isError ? 'error' : 'warning',
      tags: { portal: PORTAL_NAME, component: 'cluster' },
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

  // ─── No response cache — CF edge handles page caching ───
  // Response cache removed in v2. Memory freed for query caches + workers.

  // ─── Worker management ───
  const workerPorts = []; // active worker ports for round-robin
  const gracefullyShuttingDown = new Set();
  let nextWorker = 0;
  let workerIndex = 0;
  let primaryWorkerPort = null; // Worker 0 — owns warm query cache, never blacklisted

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
    w._isPrimary = isFirst; // Worker 0 = primary (warm query cache owner)
    if (isFirst) primaryWorkerPort = port;
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
  const QUERY_CACHE_MAX = budget.explicitCacheMax + budget.adaptiveCacheMax;
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

    // Note: worker readiness is handled by the health-check polling in forkWorker().
    // The 'listening' message is NOT used for port registration to avoid race conditions
    // where requests route to a half-started worker before /health responds.

    if (msg.type === 'query-stats-batch') {
      mergeQueryStats(msg.stats);
    }

    if (msg.type === 'slow-query') {
      reportToSentry('slow_query', {
        fingerprint: msg.fingerprint,
        params: msg.params,
        ms: msg.ms,
        workerPid: worker.process.pid,
      });
    }
  }

  // ─── Adaptive cache: query stats aggregation + promotion ───
  const ADAPTIVE_CACHE_MAX = budget.adaptiveCacheMax;
  const PROMOTION_INTERVAL = 30_000; // analyze every 30s
  const MIN_QUERY_MS = 3;            // don't cache sub-3ms queries (IPC overhead)
  const MAX_PARAM_VARIANTS = 500;    // skip per-entity queries

  const globalQueryStats = new Map(); // fingerprint → { calls, totalMs, avgMs, paramVariants }
  let promotedFingerprints = new Set();

  function mergeQueryStats(batch) {
    for (const stat of batch) {
      const existing = globalQueryStats.get(stat.fingerprint);
      if (existing) {
        existing.calls += stat.calls;
        existing.totalMs += stat.totalMs;
        existing.avgMs = existing.totalMs / existing.calls;
        existing.paramVariants = Math.max(existing.paramVariants, stat.paramVariants);
      } else {
        globalQueryStats.set(stat.fingerprint, { ...stat });
      }
    }
  }

  function runPromotion() {
    const candidates = [];
    for (const [fp, stat] of globalQueryStats) {
      if (stat.avgMs < MIN_QUERY_MS) continue;
      if (stat.paramVariants > MAX_PARAM_VARIANTS) continue;
      const value = stat.calls * stat.avgMs; // total CPU savings
      candidates.push({ fingerprint: fp, value, avgMs: stat.avgMs, calls: stat.calls, paramVariants: stat.paramVariants });
    }

    candidates.sort((a, b) => b.value - a.value);

    const newPromoted = new Set();
    let entries = 0;
    for (const c of candidates) {
      if (entries >= ADAPTIVE_CACHE_MAX) break;
      newPromoted.add(c.fingerprint);
      entries += Math.min(c.paramVariants || 10, 100);
    }

    // Only broadcast if promotion set changed
    const changed = newPromoted.size !== promotedFingerprints.size ||
      [...newPromoted].some(fp => !promotedFingerprints.has(fp));

    if (changed) {
      promotedFingerprints = newPromoted;
      for (const worker of Object.values(cluster.workers)) {
        if (worker && !worker.isDead()) {
          try { worker.send({ type: 'adaptive-promoted', fingerprints: [...newPromoted] }); } catch {}
        }
      }

      // Evict demoted entries from shared cache
      for (const key of sharedQueryCache.keys()) {
        if (!key.startsWith('adaptive:')) continue;
        const parts = key.split(':');
        const fp = parts.slice(1, -1).join(':');
        if (!newPromoted.has(fp)) sharedQueryCache.delete(key);
      }

      if (newPromoted.size > 0) {
        const top3 = candidates.slice(0, 3).map(c =>
          `${c.fingerprint.substring(0, 40)}(${c.calls}x${Math.round(c.avgMs)}ms)`
        ).join(', ');
        console.log(`[adaptive] Promoted ${newPromoted.size} queries. Top: ${top3}`);
      }
    }

    // Decay stats for next window (halve to let new patterns emerge)
    for (const [fp, stat] of globalQueryStats) {
      stat.calls = Math.floor(stat.calls / 2);
      stat.totalMs = stat.totalMs / 2;
      if (stat.calls === 0) globalQueryStats.delete(fp);
    }
  }

  const promotionTimer = setInterval(runPromotion, PROMOTION_INTERVAL);
  promotionTimer.unref();

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
      reportToSentry('crash_throttle', {
        crashCount: recentCrashes.length,
        window: '60s',
        backoffMs: BACKOFF_MS,
        workersRemaining: Object.keys(cluster.workers).length,
      });
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
        warmerDone = true;
        if (msg.allSitemapsOk) {
          console.log(`[warmer] Complete: ${msg.pagesWarmed} pages, ${msg.sitemapsWarmed}/${msg.sitemapsTotal} sitemaps`);
        } else {
          // Sitemaps partially failed — alert via Sentry
          const failedCount = msg.failedSitemaps?.length || 0;
          console.warn(`[warmer] Complete with ${failedCount} sitemap failures: ${msg.sitemapsWarmed}/${msg.sitemapsTotal} OK`);
          if (failedCount > 0) {
            reportToSentry('sitemap_warming_partial', {
              pagesWarmed: msg.pagesWarmed,
              sitemapsWarmed: msg.sitemapsWarmed,
              sitemapsTotal: msg.sitemapsTotal,
              failedSitemaps: msg.failedSitemaps,
              culprit: msg.failedSitemaps[0],
            });
          }
        }
      }
      if (msg.type === 'warm-failed') {
        console.error(`[warmer] Failed: ${msg.error}`);
        reportToSentry('warmer_failed', {
          error: msg.error,
          pagesWarmed: msg.pagesWarmed || 0,
          sitemapsWarmed: msg.sitemapsWarmed || 0,
        });
      }
      if (msg.type === 'warm-timeout') {
        warmerDone = true; // Allow slow alerts even though warmup didn't complete
        console.error(`[warmer] TIMEOUT: ${msg.pagesWarmed} pages, ${msg.sitemapsWarmed}/${msg.sitemapsTotal} sitemaps, memory ${msg.memoryPct}%`);
        reportToSentry('warmer_timeout', {
          pagesWarmed: msg.pagesWarmed,
          sitemapsWarmed: msg.sitemapsWarmed,
          sitemapsTotal: msg.sitemapsTotal,
          failedSitemaps: msg.failedSitemaps,
          memoryPct: msg.memoryPct,
          culprit: msg.memoryPct > 85
            ? `Memory pressure (${msg.memoryPct}%) blocked warming — optimize DB working set`
            : `Warming took >10 min — check slow sitemap generation`,
        });
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

  // Edge TTL removed — middleware.ts sets Cache-Control headers, no primary-side caching

  // ─── Proxy to worker with retry failover ───
  // On connection error (ECONNRESET, ECONNREFUSED), immediately retry on the next
  // healthy worker instead of returning 502. Only fails to client when ALL workers
  // have been tried or the request times out. Timeouts are never retried (the query
  // is genuinely slow, retrying won't help).
  const MAX_RETRIES = 2; // max retry attempts (total attempts = 1 + MAX_RETRIES)

  // Track recently-failed ports to avoid routing to dying workers.
  // Entries auto-expire after 5s — enough for the cluster exit handler to clean up.
  const failedPorts = new Map(); // port → timestamp
  const FAILED_PORT_TTL = 5000;

  function getHealthyPort() {
    const now = Date.now();
    // Clean expired entries
    for (const [p, t] of failedPorts) {
      if (now - t > FAILED_PORT_TTL) failedPorts.delete(p);
    }
    // Find a port not in the failed set
    for (let i = 0; i < workerPorts.length; i++) {
      const port = workerPorts[nextWorker++ % workerPorts.length];
      if (!failedPorts.has(port)) return port;
    }
    // All ports failed recently — try the least-recently-failed one
    return workerPorts.length > 0 ? workerPorts[nextWorker++ % workerPorts.length] : null;
  }

  function proxyToWorker(req, res, attempt = 0) {
    const port = getHealthyPort();
    if (!port) {
      reportToSentry('proxy_503', { url: req.url, reason: 'no_healthy_workers', attempt });
      res.writeHead(503, { 'Content-Type': 'text/plain' });
      res.end('No workers available');
      return;
    }
    const proxyStart = attempt === 0 ? Date.now() : (req._proxyStart || Date.now());
    if (attempt === 0) req._proxyStart = proxyStart;
    let timedOut = false;
    const proxyReq = http.request(
      { hostname: '127.0.0.1', port, path: req.url, method: req.method, headers: req.headers },
      (proxyRes) => {
        const ct = proxyRes.headers['content-type'] || '';
        const status = proxyRes.statusCode;

        // Report 5xx from workers to Sentry
        if (status >= 500) {
          reportToSentry(`worker_${status}`, { url: req.url, workerPort: port });
        }

        // Report slow responses — only after warmer completes
        const elapsed = Date.now() - proxyStart;
        if (warmerDone && elapsed > SENTRY_SLOW_MS) {
          reportToSentry('slow_response', { url: req.url, elapsedMs: elapsed, workerPort: port, thresholdMs: SENTRY_SLOW_MS });
        }

        // Stream response directly — no in-memory page cache
        // CF edge + browser handle page-level caching via Cache-Control headers
        res.writeHead(status, proxyRes.headers);
        proxyRes.pipe(res);
      }
    );

    // Timeout — never retried (the query is genuinely slow)
    proxyReq.setTimeout(PROXY_TIMEOUT_MS, () => {
      timedOut = true;
      proxyReq.destroy();
      const elapsed = Date.now() - proxyStart;
      console.error(`[cluster] Proxy timeout (${PROXY_TIMEOUT_MS}ms) to :${port} for ${req.url}`);
      reportToSentry('proxy_timeout', { url: req.url, workerPort: port, elapsedMs: elapsed, timeoutMs: PROXY_TIMEOUT_MS });
      if (!res.headersSent) {
        res.writeHead(504, { 'Content-Type': 'text/plain' });
        res.end('Gateway Timeout');
      }
    });

    proxyReq.on('error', (err) => {
      if (timedOut) return;

      // Mark this port as temporarily failed — skip it for 5s.
      // Never blacklist the primary worker (Worker 0) — it owns the warm query cache
      // and is the last line of defense. Better to retry on it than have no workers.
      if (port !== primaryWorkerPort) {
        failedPorts.set(port, Date.now());
      }

      // Retry on next worker if attempts remain and headers haven't been sent
      if (attempt < MAX_RETRIES && !res.headersSent && workerPorts.length > 1) {
        console.warn(`[cluster] Retry ${attempt + 1}/${MAX_RETRIES}: :${port} failed (${err.message}), trying next worker for ${req.url}`);
        proxyToWorker(req, res, attempt + 1);
        return;
      }

      console.error(`[cluster] Proxy error to :${port}: ${err.message} (attempt ${attempt + 1}, no more retries)`);
      reportToSentry('proxy_502', { url: req.url, workerPort: port, error: err.message, attempt: attempt + 1 });
      if (!res.headersSent) {
        res.writeHead(502, { 'Content-Type': 'text/plain' });
        res.end('Bad Gateway');
      }
    });
    req.pipe(proxyReq);
  }

  // ─── Primary HTTP server — caching proxy ───
  http.createServer((req, res) => {
   try {
    const path = req.url.split('?')[0];

    // /ping: comprehensive external health check (for Kuma monitoring)
    if (path === '/ping') {
      const dbPath = process.env.DATABASE_PATH || '/data/portal.db';
      const checks = {
        workers: workerPorts.length > 0 ? 'ok' : 'no_workers',
        workerCount: Object.keys(cluster.workers).length,
        queryCache: `${sharedQueryCache.size} entries`,
        adaptiveCache: { promoted: promotedFingerprints.size, statsTracked: globalQueryStats.size },
        warmer: warmerProcess && !warmerProcess.killed ? 'running' : (warmerDone ? 'done' : 'idle'),
      };

      // DB check — file exists, readable, size
      try {
        if (existsSync(dbPath)) {
          const dbStat = statSync(dbPath);
          checks.db = `ok (${Math.round(dbStat.size / 1048576)}MB)`;
        } else {
          checks.db = 'missing';
        }
      } catch (e) { checks.db = `error (${e.message})`; }

      // Memory check — cgroup v2 (subtract reclaimable page cache)
      // memory.current includes file page cache from SQLite reads, which the kernel
      // reclaims freely under pressure. Use working_set = current - inactive_file
      // (same formula as docker stats / Kubernetes memory metrics).
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
        const pct = Math.round(workingSet / max * 100);
        checks.memory = `${pct}% of ${Math.round(max / 1048576)}MB (working set)`;
        checks.memoryRaw = `${Math.round(cur / max * 100)}% (incl. page cache)`;
        checks.memoryOk = pct < 90;
      } catch { checks.memory = 'unknown'; checks.memoryOk = true; }

      const allOk = checks.workers === 'ok' && checks.memoryOk !== false && !String(checks.db).startsWith('error') && checks.db !== 'missing';
      const status = allOk ? 'healthy' : 'degraded';

      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status,
        portal: PORTAL_NAME,
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
        res.end(JSON.stringify({
          status: 'degraded', workers: 0,
          scaling: { ready: false, stable: recentCrashes.length < 3 },
        }));
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

    // All requests → proxy to workers (no in-memory page cache)
    proxyToWorker(req, res);
   } catch (err) {
    console.error(`[cluster] Primary handler error: ${err.message}`);
    reportToSentry('primary_error', { url: req.url, error: err.message, stack: err.stack });
    if (!res.headersSent) {
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Internal Server Error');
    }
   }
  }).listen(EXTERNAL_PORT, HOST, () => {
    console.log(`[cluster] Proxy on :${EXTERNAL_PORT} (qcache=${QUERY_CACHE_MAX} max, adaptive=${ADAPTIVE_CACHE_MAX} max)`);
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
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        workers: Object.keys(cluster.workers).length,
        targetWorkers,
        minWorkers: MIN_WORKERS,
        maxWorkers: MAX_WORKERS,
        pids: Object.values(cluster.workers).map(w => w.process.pid),
        workerPorts,
        queryCache: {
          explicit: sharedQueryCache.size,
          adaptivePromoted: promotedFingerprints.size,
          statsTracked: globalQueryStats.size,
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
