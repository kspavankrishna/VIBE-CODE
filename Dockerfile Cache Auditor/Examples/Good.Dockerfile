# syntax=docker/dockerfile:1
FROM node:22.4.0-slim@sha256:0000000000000000000000000000000000000000000000000000000000000000 AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci

FROM deps AS build
COPY . .
RUN npm run build

FROM node:22.4.0-slim@sha256:0000000000000000000000000000000000000000000000000000000000000000 AS runtime
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
ARG GIT_SHA
LABEL org.opencontainers.image.revision=$GIT_SHA
CMD ["node", "dist/server.js"]
