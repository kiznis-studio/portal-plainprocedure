import { defineConfig } from 'astro/config';
import node from '@astrojs/node';
import tailwindcss from '@tailwindcss/vite';
import sentry from '@sentry/astro';

export default defineConfig({
  output: 'server',
  adapter: node({ mode: 'standalone' }),
  site: 'https://plainprocedure.com',
  build: {
    inlineStylesheets: 'auto',
  },
  vite: {
    plugins: [tailwindcss()],
    build: { target: 'es2022' },
  },
  integrations: [
    sentry({
      dsn: 'https://0e5e54309b9825136e8fb5984f990b08@o4510827630231552.ingest.de.sentry.io/4511049896558672',
      enabled: { client: false, server: true },
      sourceMapsUploadOptions: {
        enabled: false,
      },
    }),
  ],
});
