import type { APIRoute } from 'astro';

export const prerender = false;

const siteUrl = 'https://plainprocedure.com';

export const GET: APIRoute = async () => {
  const pages = [
    { loc: '/', priority: '1.0', changefreq: 'weekly' },
    { loc: '/procedures/', priority: '0.9', changefreq: 'weekly' },
    { loc: '/hospitals/', priority: '0.8', changefreq: 'weekly' },
    { loc: '/states/', priority: '0.8', changefreq: 'weekly' },
    { loc: '/search', priority: '0.6', changefreq: 'monthly' },
    { loc: '/about', priority: '0.4', changefreq: 'monthly' },
    { loc: '/privacy', priority: '0.2', changefreq: 'yearly' },
    { loc: '/terms', priority: '0.2', changefreq: 'yearly' },
    { loc: '/contact', priority: '0.3', changefreq: 'yearly' },
  ];

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${pages.map(p => `  <url>
    <loc>${siteUrl}${p.loc}</loc>
    <changefreq>${p.changefreq}</changefreq>
    <priority>${p.priority}</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: {
      'Content-Type': 'application/xml',
      'Cache-Control': 'public, max-age=86400, s-maxage=86400',
    },
  });
};
