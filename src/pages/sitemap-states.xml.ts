import type { APIRoute } from 'astro';
import type { D1Database } from '../lib/d1-adapter';
import { getAllStates } from '../lib/db';

export const prerender = false;

const siteUrl = 'https://plainprocedure.com';

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime?.env?.DB as D1Database;
  const states = await getAllStates(db);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${states.map(s => `  <url>
    <loc>${siteUrl}/state/${s.abbr}</loc>
    <changefreq>monthly</changefreq>
    <priority>0.7</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: {
      'Content-Type': 'application/xml',
      'Cache-Control': 'public, max-age=86400, s-maxage=86400',
    },
  });
};
