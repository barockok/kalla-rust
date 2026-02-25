# Stage 1: Dependencies for kalla-web
FROM node:22-alpine AS deps
WORKDIR /app
COPY kalla-web/package*.json ./
RUN npm ci

# Stage 2: Build kalla-mcp (AI tool server)
FROM node:22-alpine AS mcp-builder
WORKDIR /mcp
COPY kalla-mcp/package*.json ./
RUN npm ci
COPY kalla-mcp/ .
RUN npm run build

# Stage 3: Build kalla-web
FROM node:22-alpine AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY kalla-web/ .
RUN npm run build

# Stage 4: Runtime
FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production

COPY --from=builder /app/.next/standalone ./
COPY --from=builder /app/.next/static ./.next/static
COPY --from=builder /app/public ./public

# Include kalla-mcp server (spawned via stdio by mcp-client.ts)
COPY --from=mcp-builder /mcp/dist /kalla-mcp/dist
COPY --from=mcp-builder /mcp/node_modules /kalla-mcp/node_modules
COPY --from=mcp-builder /mcp/package.json /kalla-mcp/package.json

ENV MCP_SERVER_PATH=/kalla-mcp/dist/index.js

EXPOSE 3000
ENV HOSTNAME=0.0.0.0
CMD ["node", "server.js"]
