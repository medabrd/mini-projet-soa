# Dockerfile pour API Gateway
# Build context : racine du repo (pour acceder a proto/)

FROM node:24-alpine

# Le proto est partage a la racine du repo, on le copie au meme niveau relatif
# que ce que le code attend (path.resolve(__dirname, '..', '..', 'proto', ...) depuis src/)
COPY proto/ /proto/

WORKDIR /app

# Manifests d'abord pour beneficier du cache Docker
COPY gateway/package.json gateway/package-lock.json ./
RUN npm ci --omit=dev

# Puis le code du gateway
COPY gateway/ ./

# REST + GraphQL sur ce port
EXPOSE 3000

CMD ["node", "server.js"]
