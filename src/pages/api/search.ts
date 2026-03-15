import type { APIRoute } from 'astro';
import type { D1Database } from '../../lib/d1-adapter';
import { searchProcedures, searchHospitals } from '../../lib/db';

export const prerender = false;

export const GET: APIRoute = async ({ locals, url }) => {
  const db = (locals as any).runtime?.env?.DB as D1Database;
  const query = url.searchParams.get('q') || '';

  if (!query.trim()) {
    return new Response(JSON.stringify({ procedures: [], hospitals: [] }), {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=60' },
    });
  }

  const procedures = await searchProcedures(db, query.trim(), 10);
  const hospitals = await searchHospitals(db, query.trim(), 5);

  const simplifiedProcs = procedures.map(p => ({
    code: p.code,
    description: p.description,
    slug: p.slug,
    category: p.category,
    medicare_payment: p.national_avg_medicare_payment,
    submitted_charge: p.national_avg_submitted_charge,
  }));

  const simplifiedHosps = hospitals.map(h => ({
    name: h.name,
    slug: h.slug,
    city: h.city,
    state: h.state,
    rating: h.overall_rating,
  }));

  return new Response(JSON.stringify({ query, procedures: simplifiedProcs, hospitals: simplifiedHosps }), {
    headers: { 'Content-Type': 'application/json', 'Cache-Control': 'public, max-age=300' },
  });
};
