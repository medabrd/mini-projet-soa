# Dockerfile pour tracking-service
# Build context : racine du repo (pour acceder a proto/)

FROM node:24-alpine

# Le proto est partage a la racine du repo, on le copie au meme niveau relatif
# que ce que le code attend (path.resolve(__dirname, '..', '..', '..', 'proto', ...) depuis src/)
COPY proto/ /proto/

WORKDIR /app

# Manifests d'abord pour beneficier du cache Docker sur npm ci
COPY services/tracking-service/package.json services/tracking-service/package-lock.json ./
RUN npm ci --omit=dev

# Puis le code
COPY services/tracking-service/ ./

# gRPC sur ce port
EXPOSE 50053

CMD ["node", "--experimental-sqlite", "server.js"]
