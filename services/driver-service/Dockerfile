# Dockerfile pour driver-service
# Build context : racine du repo (pour acceder a proto/)

FROM node:24-alpine

# Le proto est partage a la racine du repo, on le copie au meme niveau relatif
# que ce que le code attend (path.resolve(__dirname, '..', '..', '..', 'proto', ...) depuis src/)
COPY proto/ /proto/

WORKDIR /app

# Manifests d'abord pour beneficier du cache Docker sur npm ci
COPY services/driver-service/package.json services/driver-service/package-lock.json ./
RUN npm ci --omit=dev

# Puis le code
COPY services/driver-service/ ./

# gRPC sur ce port
EXPOSE 50052

CMD ["node", "server.js"]
