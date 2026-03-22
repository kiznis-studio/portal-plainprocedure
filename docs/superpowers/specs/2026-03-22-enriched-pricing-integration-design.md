# Enriched Pricing Integration — PlainProcedure

**Date:** 2026-03-22
**Status:** Approved
**Portal:** plainprocedure.com (Mercury)

## Problem

PlainProcedure currently displays a 2-price model (Medicare Payment + Hospital Billed Charge). Users searching for procedure costs want to know what they'll actually pay — which depends on whether they have commercial insurance or are paying cash. The portal has no commercial or cash pricing data.

## Solution

Enrich procedure pages with estimated commercial insurance and cash/self-pay pricing using RAND 2024 commercial-to-Medicare ratios applied to existing CMS 2023 data. Display estimates as a clearly-labeled secondary section below the authoritative Medicare data.

## Data Source

Three new tables already exist in `plainprocedure.db` (213 MB, deployed to Mercury):

| Table | Rows | Key Fields |
|-------|------|------------|
| `procedure_pricing` | 9,297 | `est_commercial_low/avg/high`, `est_cash_price`, `markup_ratio`, `commercial_to_medicare` |
| `state_procedure_pricing` | 198,291 | `est_commercial_avg`, `est_cash_price`, `commercial_ratio` |
| `provider_procedure_pricing` | 360,728 | `est_commercial`, `est_cash` |

**Methodology:**
- Commercial: Medicare allowed amount x RAND ratio (category-specific: Surgery 2.54x, E&M 1.80x, Radiology 2.10x, default 2.24x; state-specific: MA 2.60x, ND 2.00x, etc.)
- Cash: blend of (submitted charge x 0.55) + (allowed amount x 1.50) / 2
- Source: RAND Hospital Price Transparency Study, 4th Edition (2024)

## Page Changes

### 1. Procedure Profile (`/procedure/[slug].astro`)

**Current hero:** 3 stat cards — Medicare Payment | Billed Charge | Markup Ratio

**Addition:** New "What You Might Pay" section below hero:
- 2 stat cards in a row:
  - **Estimated Commercial Insurance** — `est_commercial_avg` formatted as currency, with `est_commercial_low` – `est_commercial_high` range as small text below
  - **Estimated Cash / Self-Pay** — `est_cash_price` formatted as currency
- Inline disclaimer: "Estimated using RAND 2024 commercial-to-Medicare ratios. Actual prices vary by insurer, plan, and facility."
- Collapsible "How we estimate these prices" with methodology detail and RAND citation
- Category-specific ratio shown (e.g., "Surgery procedures average 2.54x Medicare rates nationally")

**State-by-state table:** Add 2 columns:
- Est. Commercial (from `state_procedure_pricing.est_commercial_avg`)
- Est. Cash (from `state_procedure_pricing.est_cash_price`)
- Both hidden on mobile (`hidden md:table-cell`)

**FAQ schema:** Add question: "How much does {procedure} cost with insurance?" with answer referencing commercial estimate range.

### 2. Procedure + State (`/procedure/[slug]/[state].astro`)

**Current hero:** 3 comparison cards — State Medicare | National Medicare | State Billed

**Addition:** New row of 2 cards below existing:
- **Est. Commercial (State)** — from `state_procedure_pricing.est_commercial_avg`
  - Color: teal if below national `procedure_pricing.est_commercial_avg`, amber if above
- **Est. Cash (State)** — from `state_procedure_pricing.est_cash_price`
- Inline disclaimer (same text)

**Provider table:** Add 2 columns:
- Est. Commercial (from `provider_procedure_pricing.est_commercial`)
- Est. Cash (from `provider_procedure_pricing.est_cash`)
- Hidden on mobile (`hidden md:table-cell`)

### 3. No Changes to Homepage or Hospital Pages

Homepage already shows top procedures with Medicare/Billed. Commercial estimates would clutter it without adding navigation value. Hospital pages show quality metrics, not pricing — different data domain.

## Data Layer Changes

### `src/lib/types.ts` — New Interfaces

```typescript
interface ProcedurePricing {
  code: string;
  est_commercial_low: number | null;
  est_commercial_avg: number | null;
  est_commercial_high: number | null;
  est_cash_price: number | null;
  markup_ratio: number | null;
  commercial_to_medicare: number | null;
  methodology: string;
}

interface StateProcedurePricing {
  procedure_code: string;
  state: string;
  est_commercial_avg: number | null;
  est_cash_price: number | null;
  commercial_ratio: number | null;
}

interface ProviderProcedurePricing {
  npi: string;
  provider_name: string | null;
  state: string | null;
  city: string | null;
  procedure_code: string;
  est_commercial: number | null;
  est_cash: number | null;
}
```

### `src/lib/db.ts` — New Query Functions

```typescript
function getProcedurePricing(db, code: string): ProcedurePricing | null
// SELECT est_commercial_low, est_commercial_avg, est_commercial_high,
//        est_cash_price, markup_ratio, commercial_to_medicare, methodology
// FROM procedure_pricing WHERE code = ?

function getStateProcedurePricing(db, code: string): StateProcedurePricing[]
// SELECT * FROM state_procedure_pricing WHERE procedure_code = ? ORDER BY state

function getStateProcedurePricingSingle(db, code: string, state: string): StateProcedurePricing | null
// SELECT * FROM state_procedure_pricing WHERE procedure_code = ? AND state = ?

function getProviderProcedurePricing(db, code: string, state: string, limit = 25): ProviderProcedurePricing[]
// SELECT * FROM provider_procedure_pricing WHERE procedure_code = ? AND state = ? ORDER BY total_services DESC LIMIT ?
```

### Cache Warming

Add `getProcedurePricing` for top 50 procedures to `warmQueryCache` in `cluster-entry.mjs`. State and provider queries are too numerous to pre-cache — they'll be cached on first access.

## UI Patterns

- Cards use existing `.card` CSS class with `bg-white dark:bg-gray-800` pattern
- Currency formatting uses existing `fmt(n)` helper
- Color coding: teal for "below average" (`text-teal-600 dark:text-teal-400`), amber for "above average" (`text-amber-600 dark:text-amber-400`)
- Section header: `<h2>` with existing typography scale
- Disclaimer: `text-sm text-gray-500 dark:text-gray-400`
- Collapsible: `<details><summary>` native HTML element (no JS needed)

## Disclaimer Text

**Inline:** "Estimated using RAND 2024 commercial-to-Medicare ratios. Actual prices vary by insurer, plan, and facility."

**Expanded methodology:**
> These estimates are based on the RAND Hospital Price Transparency Study (4th Edition, 2024), which found that commercial insurance prices average 224% of Medicare rates nationally. We apply category-specific ratios: {category} procedures average {ratio}x Medicare rates. Cash/self-pay estimates blend typical cash discounts (55% of billed charges) with Medicare-based estimates (150% of allowed amounts). These are statistical estimates, not quotes. Contact your insurer or provider for actual costs.

## Testing

- Verify new tables exist and have data: `SELECT COUNT(*) FROM procedure_pricing`
- Spot-check a common procedure (99213, 99214) — commercial should be ~1.8x allowed amount
- Verify state page shows state-specific ratio, not national
- Verify provider table shows per-provider estimates
- Check mobile: new columns hidden, cards stack vertically
- Check dark mode: all new elements respect dark variants
