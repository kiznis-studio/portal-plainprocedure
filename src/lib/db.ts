import type { D1Database } from './d1-adapter';
import type {
  State,
  Hospital,
  Procedure,
  ProcedureCategory,
  ProcedureStatePrice,
  ProcedureStatePriceWithState,
  Provider,
  ProviderProcedureWithDetails,
  Stat,
  ProcedurePricing,
  StateProcedurePricingRow,
  ProviderProcedurePricingRow,
} from './types';
import { persistToDisk, loadFromDisk, warmFromDisk } from './disk-cache';

// --- Query-level cache ---
const queryCache = new Map<string, unknown>();

export function getQueryCacheSize(): number {
  return queryCache.size;
}

function cached<T>(key: string, fn: () => Promise<T>): Promise<T> {
  const hit = queryCache.get(key);
  if (hit !== undefined) return Promise.resolve(hit as T);
  return fn().then(v => { queryCache.set(key, v); return v; });
}

// --- Stats ---
export function getStats(db: D1Database): Promise<Stat[]> {
  return cached('stats', async () => {
    const r = await db.prepare('SELECT key, value FROM stats').all<Stat>();
    return r.results;
  });
}

export async function getStat(db: D1Database, key: string): Promise<string | null> {
  const r = await db.prepare('SELECT value FROM stats WHERE key = ?1').bind(key).first<{ value: string }>();
  return r?.value ?? null;
}

// --- States ---
export function getAllStates(db: D1Database): Promise<State[]> {
  return cached('states:all', async () => {
    const r = await db.prepare('SELECT * FROM states ORDER BY name COLLATE NOCASE').all<State>();
    return r.results;
  });
}

export async function getStateByAbbr(db: D1Database, abbr: string): Promise<State | null> {
  const r = await db.prepare('SELECT * FROM states WHERE abbr = ?1').bind(abbr).first<State>();
  return r;
}

// --- Procedures ---
export function getAllProcedures(db: D1Database): Promise<Procedure[]> {
  return cached('procedures:all', async () => {
    const r = await db.prepare(
      'SELECT * FROM procedures ORDER BY national_total_services DESC'
    ).all<Procedure>();
    return r.results;
  });
}

export async function getProcedureBySlug(db: D1Database, slug: string): Promise<Procedure | null> {
  const r = await db.prepare('SELECT * FROM procedures WHERE slug = ?1').bind(slug).first<Procedure>();
  return r;
}

export async function getProceduresByCategory(db: D1Database, category: string): Promise<Procedure[]> {
  const r = await db.prepare(
    'SELECT * FROM procedures WHERE category = ?1 ORDER BY national_total_services DESC'
  ).bind(category).all<Procedure>();
  return r.results;
}

export function getTopProcedures(db: D1Database, limit: number = 10): Promise<Procedure[]> {
  return cached('proc:top', async () => {
    const r = await db.prepare(
      'SELECT * FROM procedures ORDER BY national_total_services DESC LIMIT 50'
    ).all<Procedure>();
    return r.results;
  }).then(rows => rows.slice(0, limit));
}

export function getMostExpensiveProcedures(db: D1Database, limit: number = 10): Promise<Procedure[]> {
  return cached('proc:expensive', async () => {
    const r = await db.prepare(
      `SELECT * FROM procedures
       WHERE national_avg_medicare_payment IS NOT NULL AND national_avg_medicare_payment > 0
       ORDER BY national_avg_medicare_payment DESC LIMIT 50`
    ).all<Procedure>();
    return r.results;
  }).then(rows => rows.slice(0, limit));
}

export function getHighestMarkupProcedures(db: D1Database, limit: number = 10): Promise<Procedure[]> {
  return cached('proc:markup', async () => {
    const r = await db.prepare(
      `SELECT *, ROUND(national_avg_submitted_charge / NULLIF(national_avg_medicare_payment, 0), 1) as markup_ratio
       FROM procedures
       WHERE national_avg_medicare_payment > 10 AND national_avg_submitted_charge > 0
         AND national_total_services > 1000
       ORDER BY markup_ratio DESC LIMIT 50`
    ).all<Procedure>();
    return r.results;
  }).then(rows => rows.slice(0, limit));
}

export function getWidestPriceRange(db: D1Database, limit: number = 10): Promise<Procedure[]> {
  return cached('proc:range', async () => {
    const r = await db.prepare(
      `SELECT *, (price_range_high - price_range_low) as price_spread
       FROM procedures
       WHERE price_range_low IS NOT NULL AND price_range_high IS NOT NULL
         AND price_range_high > price_range_low AND national_total_services > 1000
       ORDER BY price_spread DESC LIMIT 50`
    ).all<Procedure>();
    return r.results;
  }).then(rows => rows.slice(0, limit));
}

// --- Procedure Categories ---
export function getAllCategories(db: D1Database): Promise<ProcedureCategory[]> {
  return cached('categories:all', async () => {
    const r = await db.prepare(
      'SELECT * FROM procedure_categories ORDER BY procedure_count DESC'
    ).all<ProcedureCategory>();
    return r.results;
  });
}

export async function getCategoryBySlug(db: D1Database, slug: string): Promise<ProcedureCategory | null> {
  const r = await db.prepare('SELECT * FROM procedure_categories WHERE slug = ?1').bind(slug).first<ProcedureCategory>();
  return r;
}

// --- Procedure State Prices ---
export async function getProcedureStatePrices(db: D1Database, procedureCode: string): Promise<ProcedureStatePriceWithState[]> {
  const r = await db.prepare(
    `SELECT p.*, s.name as state_name
     FROM procedure_state_prices p
     JOIN states s ON p.state = s.abbr
     WHERE p.procedure_code = ?1
     ORDER BY p.avg_medicare_payment DESC`
  ).bind(procedureCode).all<ProcedureStatePriceWithState>();
  return r.results;
}

export async function getProcedureStatePrice(db: D1Database, procedureCode: string, state: string): Promise<ProcedureStatePriceWithState | null> {
  const r = await db.prepare(
    `SELECT p.*, s.name as state_name
     FROM procedure_state_prices p
     JOIN states s ON p.state = s.abbr
     WHERE p.procedure_code = ?1 AND p.state = ?2`
  ).bind(procedureCode, state).first<ProcedureStatePriceWithState>();
  return r;
}

export async function getStateProcedurePrices(db: D1Database, state: string, limit: number = 50): Promise<Array<ProcedureStatePrice & { description: string; slug: string; category: string }>> {
  const r = await db.prepare(
    `SELECT p.*, pr.description, pr.slug, pr.category
     FROM procedure_state_prices p
     JOIN procedures pr ON p.procedure_code = pr.code
     WHERE p.state = ?1
     ORDER BY p.total_services DESC
     LIMIT ?2`
  ).bind(state, limit).all<ProcedureStatePrice & { description: string; slug: string; category: string }>();
  return r.results;
}

export async function getMostExpensiveInState(db: D1Database, state: string, limit: number = 10): Promise<Array<ProcedureStatePrice & { description: string; slug: string }>> {
  const r = await db.prepare(
    `SELECT p.*, pr.description, pr.slug
     FROM procedure_state_prices p
     JOIN procedures pr ON p.procedure_code = pr.code
     WHERE p.state = ?1 AND p.avg_medicare_payment > 0
     ORDER BY p.avg_medicare_payment DESC
     LIMIT ?2`
  ).bind(state, limit).all<ProcedureStatePrice & { description: string; slug: string }>();
  return r.results;
}

export async function getCheapestInState(db: D1Database, state: string, limit: number = 10): Promise<Array<ProcedureStatePrice & { description: string; slug: string }>> {
  const r = await db.prepare(
    `SELECT p.*, pr.description, pr.slug
     FROM procedure_state_prices p
     JOIN procedures pr ON p.procedure_code = pr.code
     WHERE p.state = ?1 AND p.avg_medicare_payment > 0 AND p.total_services > 100
     ORDER BY p.avg_medicare_payment ASC
     LIMIT ?2`
  ).bind(state, limit).all<ProcedureStatePrice & { description: string; slug: string }>();
  return r.results;
}

// --- Hospitals ---
export function getAllHospitals(db: D1Database): Promise<Hospital[]> {
  return cached('hospitals:all', async () => {
    const r = await db.prepare(
      'SELECT * FROM hospitals ORDER BY name COLLATE NOCASE'
    ).all<Hospital>();
    return r.results;
  });
}

export async function getHospitalBySlug(db: D1Database, slug: string): Promise<Hospital | null> {
  const r = await db.prepare('SELECT * FROM hospitals WHERE slug = ?1').bind(slug).first<Hospital>();
  return r;
}

export async function getHospitalsByState(db: D1Database, state: string): Promise<Hospital[]> {
  const r = await db.prepare(
    'SELECT * FROM hospitals WHERE state = ?1 ORDER BY name COLLATE NOCASE'
  ).bind(state).all<Hospital>();
  return r.results;
}

// --- Providers for a procedure in a state ---
export async function getProcedureStateProviders(db: D1Database, procedureCode: string, state: string, limit: number = 25): Promise<ProviderProcedureWithDetails[]> {
  const r = await db.prepare(
    `SELECT pp.npi, pp.procedure_code, pp.total_services, pp.total_beneficiaries,
            pp.avg_medicare_payment, pp.avg_submitted_charge, pp.avg_allowed_amount, pp.place_of_service,
            pv.name as provider_name, pv.slug as provider_slug,
            pv.city as provider_city, pv.state as provider_state,
            pv.provider_type, pv.credentials
     FROM provider_procedures pp
     JOIN providers pv ON pp.npi = pv.npi
     WHERE pp.procedure_code = ?1 AND pv.state = ?2
     ORDER BY pp.total_services DESC
     LIMIT ?3`
  ).bind(procedureCode, state, limit).all<ProviderProcedureWithDetails>();
  return r.results;
}

// --- Search ---
export async function searchProcedures(db: D1Database, query: string, limit: number = 20): Promise<Procedure[]> {
  const q = `%${query}%`;
  const r = await db.prepare(
    `SELECT * FROM procedures
     WHERE description LIKE ?1 OR code LIKE ?2
     ORDER BY national_total_services DESC
     LIMIT ?3`
  ).bind(q, q, limit).all<Procedure>();
  return r.results;
}

export async function searchHospitals(db: D1Database, query: string, limit: number = 20): Promise<Hospital[]> {
  const q = `%${query}%`;
  const r = await db.prepare(
    `SELECT * FROM hospitals
     WHERE name LIKE ?1 OR city LIKE ?2
     ORDER BY name COLLATE NOCASE
     LIMIT ?3`
  ).bind(q, q, limit).all<Hospital>();
  return r.results;
}

// --- Enriched Pricing ---
export async function getProcedurePricing(db: D1Database, code: string): Promise<ProcedurePricing | null> {
  const r = await db.prepare(
    `SELECT code, est_commercial_low, est_commercial_avg, est_commercial_high,
            est_cash_price, markup_ratio, commercial_to_medicare, methodology
     FROM procedure_pricing WHERE code = ?1`
  ).bind(code).first<ProcedurePricing>();
  return r;
}

export async function getStateProcedurePricingAll(db: D1Database, procedureCode: string): Promise<StateProcedurePricingRow[]> {
  const r = await db.prepare(
    `SELECT procedure_code, state, est_commercial_avg, est_cash_price, commercial_ratio
     FROM state_procedure_pricing WHERE procedure_code = ?1 ORDER BY state`
  ).bind(procedureCode).all<StateProcedurePricingRow>();
  return r.results;
}

export async function getStateProcedurePricingSingle(db: D1Database, procedureCode: string, state: string): Promise<StateProcedurePricingRow | null> {
  const r = await db.prepare(
    `SELECT procedure_code, state, est_commercial_avg, est_cash_price, commercial_ratio
     FROM state_procedure_pricing WHERE procedure_code = ?1 AND state = ?2`
  ).bind(procedureCode, state).first<StateProcedurePricingRow>();
  return r;
}

export async function getProviderProcedurePricingByState(db: D1Database, procedureCode: string, state: string, limit: number = 25): Promise<ProviderProcedurePricingRow[]> {
  const r = await db.prepare(
    `SELECT npi, provider_name, state, city, procedure_code, est_commercial, est_cash, total_services
     FROM provider_procedure_pricing WHERE procedure_code = ?1 AND state = ?2
     ORDER BY total_services DESC LIMIT ?3`
  ).bind(procedureCode, state, limit).all<ProviderProcedurePricingRow>();
  return r.results;
}

// --- All slugs for sitemaps ---
export function getAllProcedureSlugs(db: D1Database): Promise<Array<{ slug: string }>> {
  return cached('slugs:procedures', async () => {
    const r = await db.prepare('SELECT slug FROM procedures ORDER BY slug').all<{ slug: string }>();
    return r.results;
  });
}

export function getAllHospitalSlugs(db: D1Database): Promise<Array<{ slug: string }>> {
  return cached('slugs:hospitals', async () => {
    const r = await db.prepare('SELECT slug FROM hospitals ORDER BY slug').all<{ slug: string }>();
    return r.results;
  });
}

export function getAllCategorySlugs(db: D1Database): Promise<Array<{ slug: string }>> {
  return cached('slugs:categories', async () => {
    const r = await db.prepare('SELECT slug FROM procedure_categories ORDER BY slug').all<{ slug: string }>();
    return r.results;
  });
}

export function getProcedureStateSlugs(db: D1Database): Promise<Array<{ slug: string; state: string }>> {
  return cached('slugs:proc-states', async () => {
    const r = await db.prepare(
      `SELECT pr.slug, psp.state
       FROM procedure_state_prices psp
       JOIN procedures pr ON psp.procedure_code = pr.code
       WHERE psp.total_services > 100
       ORDER BY pr.slug, psp.state`
    ).all<{ slug: string; state: string }>();
    return r.results;
  });
}

// --- Cache Warming ---
export async function warmQueryCache(db: D1Database): Promise<void> {
  console.log('[cache] Starting query cache warming...');
  const start = Date.now();

  // Priority 1: Homepage data
  await getStats(db);
  await getTopProcedures(db, 50);
  await getAllCategories(db);
  await getMostExpensiveProcedures(db, 50);
  await getHighestMarkupProcedures(db, 50);
  await getWidestPriceRange(db, 50);

  // Priority 2: Indexes
  await getAllStates(db);
  await getAllProcedures(db);
  await getAllHospitals(db);

  const elapsed = Date.now() - start;
  console.log(`[cache] Warming complete: ${queryCache.size} entries in ${elapsed}ms`);
}
