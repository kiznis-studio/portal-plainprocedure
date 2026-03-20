import type { APIRoute } from 'astro';
import { getAllProcedureSlugs } from '../lib/db';

export const prerender = false;

const siteUrl = 'https://plainprocedure.com';

export const GET: APIRoute = async ({ locals }) => {
  const db = locals.runtime.env.DB;
  const slugs = await getAllProcedureSlugs(db);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${slugs.map(s => `  <url>
    <loc>${siteUrl}/procedure/${s.slug}</loc>
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
