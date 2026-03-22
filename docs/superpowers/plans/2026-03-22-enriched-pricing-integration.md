# Enriched Pricing Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire estimated commercial insurance and cash/self-pay pricing into procedure profile, state, and provider pages using data already in the DB.

**Architecture:** Add 3 new query functions in `db.ts` and 3 new TypeScript interfaces in `types.ts`. Extend 2 existing Astro pages with new card sections and table columns. No new components — follows existing inline patterns.

**Tech Stack:** Astro 5 SSR, TypeScript, better-sqlite3, Tailwind CSS

**Spec:** `docs/superpowers/specs/2026-03-22-enriched-pricing-integration-design.md`

**Repo:** `~/Projects/portal-plainprocedure`

---

### Task 1: Add Types and Query Functions

**Files:**
- Modify: `src/lib/types.ts`
- Modify: `src/lib/db.ts`

- [ ] **Step 1: Add new interfaces to `types.ts`**

Append after the `HospitalProcedure` interface (line 137):

```typescript
export interface ProcedurePricing {
  code: string;
  est_commercial_low: number | null;
  est_commercial_avg: number | null;
  est_commercial_high: number | null;
  est_cash_price: number | null;
  markup_ratio: number | null;
  commercial_to_medicare: number | null;
  methodology: string;
}

export interface StateProcedurePricingRow {
  procedure_code: string;
  state: string;
  est_commercial_avg: number | null;
  est_cash_price: number | null;
  commercial_ratio: number | null;
}

export interface ProviderProcedurePricingRow {
  npi: string;
  provider_name: string | null;
  state: string | null;
  city: string | null;
  procedure_code: string;
  est_commercial: number | null;
  est_cash: number | null;
  total_services: number | null;
}
```

- [ ] **Step 2: Add query functions to `db.ts`**

Add import of new types at line 1 (extend existing import):

```typescript
import type {
  // ... existing imports ...
  ProcedurePricing,
  StateProcedurePricingRow,
  ProviderProcedurePricingRow,
} from './types';
```

Add these functions after the `searchHospitals` function (after line 255):

```typescript
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
```

- [ ] **Step 3: Verify the build compiles**

Run: `cd ~/Projects/portal-plainprocedure && npm run build 2>&1 | tail -20`
Expected: Build succeeds (new exports are unused but shouldn't break anything)

- [ ] **Step 4: Commit**

```bash
cd ~/Projects/portal-plainprocedure
git add src/lib/types.ts src/lib/db.ts
git commit -m "feat: add enriched pricing types and query functions

Add ProcedurePricing, StateProcedurePricingRow, ProviderProcedurePricingRow
interfaces and 4 query functions for the new enriched pricing tables."
```

---

### Task 2: Enhance Procedure Profile Page

**Files:**
- Modify: `src/pages/procedure/[slug].astro`

**Depends on:** Task 1

- [ ] **Step 1: Add data fetching**

In the frontmatter (after line 10, after `statePrices` fetch), add:

```typescript
import { getProcedureBySlug, getProcedureStatePrices, getProcedurePricing, getStateProcedurePricingAll } from '../../lib/db';

const pricing = await getProcedurePricing(db, procedure.code);
const statePricingMap = new Map<string, { est_commercial_avg: number | null; est_cash_price: number | null }>();
if (pricing) {
  const statePricingRows = await getStateProcedurePricingAll(db, procedure.code);
  for (const row of statePricingRows) {
    statePricingMap.set(row.state, { est_commercial_avg: row.est_commercial_avg, est_cash_price: row.est_cash_price });
  }
}
```

Also update the import at line 2 to include the new functions.

- [ ] **Step 2: Add "What You Might Pay" section**

Insert after the Price Range section (after the closing `)}` at line 127), before the State-by-State table:

```astro
{/* What You Might Pay */}
{pricing && pricing.est_commercial_avg && (
  <div class="mb-8">
    <h2 class="text-2xl font-bold mb-4">What You Might Pay</h2>
    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-3">
      <div class="card">
        <div class="text-sm text-[var(--color-muted)] mb-1">Est. Commercial Insurance</div>
        <div class="text-3xl font-bold text-blue-600 dark:text-blue-400">{fmt(pricing.est_commercial_avg)}</div>
        <div class="text-xs text-[var(--color-muted)] mt-1">Range: {fmt(pricing.est_commercial_low)} – {fmt(pricing.est_commercial_high)}</div>
      </div>
      <div class="card">
        <div class="text-sm text-[var(--color-muted)] mb-1">Est. Cash / Self-Pay</div>
        <div class="text-3xl font-bold text-emerald-600 dark:text-emerald-400">{fmt(pricing.est_cash_price)}</div>
        <div class="text-xs text-[var(--color-muted)] mt-1">Typical self-pay discount</div>
      </div>
    </div>
    <p class="text-sm text-gray-500 dark:text-gray-400 mb-2">Estimated using RAND 2024 commercial-to-Medicare ratios. Actual prices vary by insurer, plan, and facility.</p>
    <details class="text-sm text-gray-500 dark:text-gray-400">
      <summary class="cursor-pointer hover:text-[var(--color-accent)]">How we estimate these prices</summary>
      <div class="mt-2 p-3 bg-gray-50 dark:bg-gray-800 rounded-lg text-xs leading-relaxed">
        <p>These estimates are based on the <strong>RAND Hospital Price Transparency Study</strong> (4th Edition, 2024), which found that commercial insurance prices average 224% of Medicare rates nationally. We apply category-specific ratios: <strong>{procedure.category}</strong> procedures average <strong>{pricing.commercial_to_medicare}x</strong> Medicare rates. Cash/self-pay estimates blend typical cash discounts (55% of billed charges) with Medicare-based estimates (150% of allowed amounts). These are statistical estimates, not quotes. Contact your insurer or provider for actual costs.</p>
      </div>
    </details>
  </div>
)}
```

- [ ] **Step 3: Add commercial/cash columns to state table**

In the state-by-state table header (around line 136-144), add 2 new `<th>` elements before the Providers column:

```html
<th class="text-right py-3 px-2 font-semibold hidden md:table-cell">Est. Commercial</th>
<th class="text-right py-3 px-2 font-semibold hidden md:table-cell">Est. Cash</th>
```

In the table body row (around line 147-168), add 2 new `<td>` elements before the Providers cell:

```astro
{(() => {
  const sp_pricing = statePricingMap.get(sp.state);
  return (
    <>
      <td class="py-2 px-2 text-right hidden md:table-cell text-blue-600 dark:text-blue-400">{sp_pricing ? fmt(sp_pricing.est_commercial_avg) : 'N/A'}</td>
      <td class="py-2 px-2 text-right hidden md:table-cell text-emerald-600 dark:text-emerald-400">{sp_pricing ? fmt(sp_pricing.est_cash_price) : 'N/A'}</td>
    </>
  );
})()}
```

- [ ] **Step 4: Add new FAQ question and update schema**

In the FAQ section (around line 186), add a new `<details>` block after the existing two:

```astro
{pricing && pricing.est_commercial_avg && (
  <details class="group py-4">
    <summary class="flex cursor-pointer items-center justify-between font-medium [&::-webkit-details-marker]:hidden list-none">
      How much does {title} cost with insurance?
      <svg class="h-5 w-5 shrink-0 text-[var(--color-muted)] transition-transform group-open:rotate-180" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>
    </summary>
    <p class="mt-2 text-sm text-[var(--color-muted)]">With commercial insurance, {title} costs an estimated {fmt(pricing.est_commercial_avg)} on average (range: {fmt(pricing.est_commercial_low)} – {fmt(pricing.est_commercial_high)}). Without insurance, the estimated cash price is {fmt(pricing.est_cash_price)}. These estimates are based on RAND 2024 research on commercial-to-Medicare price ratios. Your actual cost depends on your insurer, plan, and provider.</p>
  </details>
)}
```

Also update the FAQPage schema JSON (around line 199) to include the new question when pricing data exists. Add to the `mainEntity` array:

```javascript
...(pricing?.est_commercial_avg ? [{ "@type": "Question", "name": `How much does ${title} cost with insurance?`, "acceptedAnswer": { "@type": "Answer", "text": `With commercial insurance, ${title} costs an estimated ${fmt(pricing.est_commercial_avg)} on average. Without insurance, the estimated cash price is ${fmt(pricing.est_cash_price)}.` } }] : [])
```

- [ ] **Step 5: Build and verify**

Run: `cd ~/Projects/portal-plainprocedure && npm run build 2>&1 | tail -20`
Expected: Build succeeds

- [ ] **Step 6: Commit**

```bash
cd ~/Projects/portal-plainprocedure
git add src/pages/procedure/\[slug\].astro
git commit -m "feat: add commercial + cash pricing to procedure profile page

Add 'What You Might Pay' section with estimated commercial and cash
prices, collapsible methodology note, new FAQ, and 2 new columns
in state-by-state table."
```

---

### Task 3: Enhance Procedure + State Page

**Files:**
- Modify: `src/pages/procedure/[slug]/[state].astro`

**Depends on:** Task 1

- [ ] **Step 1: Add data fetching**

Update the import at line 3 to include new functions:

```typescript
import { getProcedureBySlug, getProcedureStatePrice, getStateByAbbr, getProcedureStateProviders, getProcedurePricing, getStateProcedurePricingSingle, getProviderProcedurePricingByState } from '../../../lib/db';
```

After line 18 (after `providers` fetch), add:

```typescript
const pricing = await getProcedurePricing(db, procedure.code);
const statePricing = await getStateProcedurePricingSingle(db, procedure.code, stateAbbr);
const providerPricingMap = new Map<string, { est_commercial: number | null; est_cash: number | null }>();
if (statePricing) {
  const provPricing = await getProviderProcedurePricingByState(db, procedure.code, stateAbbr, 25);
  for (const pp of provPricing) {
    providerPricingMap.set(pp.npi, { est_commercial: pp.est_commercial, est_cash: pp.est_cash });
  }
}
```

- [ ] **Step 2: Add commercial/cash cards below existing hero**

After the closing `</div>` of the 3-card grid (after line 91), add:

```astro
{/* Estimated Prices */}
{statePricing && statePricing.est_commercial_avg && (
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-8">
    {(() => {
      const natCommercial = pricing?.est_commercial_avg;
      const stateCommercial = statePricing.est_commercial_avg;
      const commAbove = natCommercial && stateCommercial ? stateCommercial > natCommercial : false;
      return (
        <div class="card">
          <div class="text-sm text-[var(--color-muted)] mb-1">Est. Commercial ({stateInfo.abbr})</div>
          <div class={`text-3xl font-bold ${commAbove ? 'text-amber-600 dark:text-amber-400' : 'text-teal-600 dark:text-teal-400'}`}>
            {fmt(stateCommercial)}
          </div>
          <div class="text-xs text-[var(--color-muted)] mt-1">
            {natCommercial ? `National avg: ${fmt(natCommercial)}` : 'Estimated commercial rate'}
          </div>
        </div>
      );
    })()}
    <div class="card">
      <div class="text-sm text-[var(--color-muted)] mb-1">Est. Cash / Self-Pay ({stateInfo.abbr})</div>
      <div class="text-3xl font-bold text-emerald-600 dark:text-emerald-400">{fmt(statePricing.est_cash_price)}</div>
      <div class="text-xs text-[var(--color-muted)] mt-1">Typical self-pay discount</div>
    </div>
  </div>
)}
{statePricing && statePricing.est_commercial_avg && (
  <p class="text-sm text-gray-500 dark:text-gray-400 mb-8 -mt-4">Estimated using RAND 2024 commercial-to-Medicare ratios. Actual prices vary by insurer, plan, and facility.</p>
)}
```

- [ ] **Step 3: Add columns to provider table**

In the table header (around line 120-127), add 2 new `<th>` before the Services column:

```html
<th class="text-right py-3 px-2 font-semibold hidden md:table-cell">Est. Commercial</th>
<th class="text-right py-3 px-2 font-semibold hidden md:table-cell">Est. Cash</th>
```

In the table body (around line 130-141), add 2 new `<td>` before the Services cell:

```astro
{(() => {
  const pp = providerPricingMap.get(pv.npi);
  return (
    <>
      <td class="py-2 px-2 text-right hidden md:table-cell text-blue-600 dark:text-blue-400">{pp ? fmt(pp.est_commercial) : 'N/A'}</td>
      <td class="py-2 px-2 text-right hidden md:table-cell text-emerald-600 dark:text-emerald-400">{pp ? fmt(pp.est_cash) : 'N/A'}</td>
    </>
  );
})()}
```

- [ ] **Step 4: Build and verify**

Run: `cd ~/Projects/portal-plainprocedure && npm run build 2>&1 | tail -20`
Expected: Build succeeds

- [ ] **Step 5: Commit**

```bash
cd ~/Projects/portal-plainprocedure
git add src/pages/procedure/\[slug\]/\[state\].astro
git commit -m "feat: add commercial + cash pricing to state procedure page

Add estimated commercial and cash price cards with state vs national
comparison coloring, and 2 new columns in provider table."
```

---

### Task 4: Deploy and Verify

**Files:** None (deployment)

**Depends on:** Tasks 1-3

- [ ] **Step 1: Push to GitHub and let CI/CD deploy**

```bash
cd ~/Projects/portal-plainprocedure
git push origin main
```

Wait for GitHub Actions to complete. Check: `gh run list -L 1`

- [ ] **Step 2: Verify procedure profile page**

Open `https://plainprocedure.com/procedure/office-outpatient-visit-established-14` (99214) and verify:
- Existing 3 hero cards still display correctly
- "What You Might Pay" section appears below price range
- Commercial estimate shows ~1.8x allowed amount (E&M ratio)
- Cash estimate is reasonable
- Methodology expandable works
- State table has 2 new columns on desktop
- New FAQ question appears

- [ ] **Step 3: Verify state page**

Open `https://plainprocedure.com/procedure/office-outpatient-visit-established-14/ca` and verify:
- 2 new cards appear below existing 3 cards
- Commercial card color reflects CA ratio (2.40x, likely amber/above national)
- Provider table has 2 new columns on desktop
- Inline disclaimer visible

- [ ] **Step 4: Verify mobile responsiveness**

Check both pages at mobile width:
- New columns hidden in tables
- Cards stack vertically
- No horizontal overflow

- [ ] **Step 5: Verify dark mode**

Toggle dark mode and check:
- Blue/emerald colors display correctly
- Methodology expandable has proper dark background
- Disclaimer text is readable
