FROM node:24-slim AS builder
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build
# Slim node_modules: remove docs, types, tests, source maps not needed at runtime
RUN rm -rf node_modules/@types && \
    find node_modules \( \
      -name '*.md' -o -name 'LICENSE*' -o -name 'CHANGELOG*' \
      -o -name '*.d.ts' -o -name '*.d.ts.map' -o -name '*.js.map' \
      -o -name 'tsconfig*.json' -o -name '.npmignore' \
      -o -name '.eslintrc*' -o -name '.prettierrc*' \
    \) -type f -delete 2>/dev/null; \
    find node_modules \( \
      -name 'test' -o -name 'tests' -o -name '__tests__' \
      -o -name '.github' -o -name 'docs' -o -name 'doc' \
      -o -name 'example' -o -name 'examples' \
    \) -type d -exec rm -rf {} + 2>/dev/null; true

FROM node:24-slim
RUN apt-get update && apt-get install -y tini curl && rm -rf /var/lib/apt/lists/*
RUN addgroup --system --gid 1001 app && adduser --system --uid 1001 --ingroup app app
WORKDIR /app
COPY --from=builder --chown=app:app /app/dist ./dist
COPY --from=builder --chown=app:app /app/node_modules ./node_modules
COPY --from=builder --chown=app:app /app/package.json ./
COPY --chown=app:app cluster-entry.mjs ./
USER app
ENV HOST=0.0.0.0
ENV PORT=4321
EXPOSE 4321
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "require('http').get('http://localhost:4321/health',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"
ENTRYPOINT ["tini", "--"]
CMD ["node", "cluster-entry.mjs"]
