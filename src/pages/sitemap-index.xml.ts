import type { APIRoute } from 'astro';

export const prerender = false;

const siteUrl = 'https://plainprocedure.com';

export const GET: APIRoute = async () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>${siteUrl}/sitemap-static.xml</loc>
  </sitemap>
  <sitemap>
    <loc>${siteUrl}/sitemap-procedures.xml</loc>
  </sitemap>
  <sitemap>
    <loc>${siteUrl}/sitemap-hospitals.xml</loc>
  </sitemap>
  <sitemap>
    <loc>${siteUrl}/sitemap-states.xml</loc>
  </sitemap>
  <sitemap>
    <loc>${siteUrl}/sitemap-categories.xml</loc>
  </sitemap>
</sitemapindex>`;

  return new Response(xml, {
    headers: {
      'Content-Type': 'application/xml',
      'Cache-Control': 'public, max-age=86400, s-maxage=86400',
    },
  });
};
