# Dockerfile pour le client web
# Build context : racine du repo (cf. docker-compose.yml).

FROM node:24-alpine

WORKDIR /app

COPY client/package.json client/package-lock.json ./
RUN npm ci --omit=dev

COPY client/server.js ./
COPY client/public ./public

EXPOSE 8081

CMD ["node", "server.js"]
