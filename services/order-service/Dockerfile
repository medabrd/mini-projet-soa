# Dockerfile pour order-service
# Build context : racine du repo (pas le dossier du service), pour pouvoir accéder à proto/
# Exemple: docker compose build order-service

FROM node:24-alpine

# Le proto est partagé à la racine du repo, on le copie au même niveau relatif
# que ce que le code attend (path.resolve(__dirname, '..', '..', '..', 'proto', ...) depuis src/)
COPY proto/ /proto/

WORKDIR /app

# Copier d'abord les manifests pour profiter du cache Docker sur npm ci
COPY services/order-service/package.json services/order-service/package-lock.json ./
RUN npm ci --omit=dev

# Puis le code du service
COPY services/order-service/ ./

# Le service écoute en gRPC sur ce port
EXPOSE 50051

# node:sqlite est expérimental dans Node 24, le flag est obligatoire
CMD ["node", "--experimental-sqlite", "server.js"]
