Tests Postman
=============

Ce dossier rassemble la doc et les exemples de payloads pour tester chaque microservice du projet avec Postman. Quand tous les services seront prêts, je créerai un workspace public Postman avec les collections correspondantes (REST, GraphQL, gRPC) — c'est un des livrables à déposer sur Classroom.


Note importante sur l'import gRPC
----------------------------------

Postman supporte le gRPC depuis la v9, mais le format de **collection JSON v2.1 ne contient pas de structure officielle pour les requêtes gRPC**. Si on exporte/importe une collection contenant des requêtes gRPC, Postman ne sait pas les recréer comme des "vraies" requêtes gRPC : elles arrivent comme des requêtes HTTP avec une méthode bizarre `INVOKE` et l'URL `grpc://...`, et ça donne une erreur "Invalid protocol: grpc".

Donc on **crée les requêtes manuellement dans Postman** au moment du dépôt, en suivant les fichiers `*-tests.md` de ce dossier qui contiennent tous les payloads. C'est l'affaire de 5-10 minutes par service.


Prérequis
---------

- Postman v9 ou plus (support gRPC standard aujourd'hui)
- Le service à tester doit tourner (`docker compose up -d` à la racine du repo)


Procédure de remplissage (à faire une fois à la fin)
-----------------------------------------------------

Pour chaque microservice :

1. **Créer une collection** : sidebar Collections → bouton **+ Create new** → Collection → nommer comme le service (ex: `order-service`)
2. **Pour chaque RPC du service**, ajouter une requête gRPC :
   - Clic droit sur la collection → **Add request** → **gRPC** (important : pas HTTP)
   - Renommer la requête (ex: `CreateOrder`)
   - **Server URL** : `localhost:<port>` (50051 pour order, 50052 pour driver, 50053 pour tracking)
   - Onglet **Service definition** → **Import a .proto file** → choisir le `.proto` dans le dossier `proto/` du repo (uniquement la 1re fois suffit, ensuite Postman se souvient)
   - Menu **Select a method** → choisir le RPC correspondant
   - Onglet **Message** → coller le payload JSON (depuis le fichier `<service>-tests.md`)
   - Cliquer **Invoke** pour lancer et avoir la réponse
   - **Ctrl+S** pour sauvegarder dans la collection
3. **Rendre le workspace public** : Settings du workspace → Visibility → Public
4. **Copier le lien public** du workspace pour le déposer sur Classroom


Comment je vérifie que le service marche sans Postman
-------------------------------------------------------

Chaque service a un fichier `test-client.js` (ou équivalent) qui fait les mêmes appels gRPC via Node.js. Pour valider rapidement qu'un service marche :

    cd services/<nom-du-service>
    node test-client.js

→ enchaîne les RPCs et affiche les résultats. Pratique pour le dev, et ça prouve que le code marche indépendamment de Postman.


Liste des services et leurs ports
----------------------------------

| Service | Port gRPC | Proto |
|---------|-----------|-------|
| order-service | 50051 | `proto/order.proto` |
| driver-service | 50052 | `proto/driver.proto` |
| tracking-service | 50053 | `proto/tracking.proto` |
| API Gateway | 3000 (REST + GraphQL sur le même port) | — |


Fichiers dans ce dossier
-------------------------

- `order-service-tests.md` — payloads et réponses attendues pour les 5 RPCs de order-service
- `driver-service-tests.md` — payloads pour les 5 RPCs de driver-service (dont le server-streaming)
- `tracking-service-tests.md` — payloads pour les 5 RPCs de tracking-service + scénario de chaîne complète Kafka
- `gateway-tests.md` — endpoints REST + queries/mutations GraphQL exposés par l'API Gateway (avec joins cross-services)
- `gateway-rest.postman_collection.json` — collection Postman v2.1 importable directement (REST)
- `gateway-graphql.postman_collection.json` — collection Postman v2.1 importable directement (GraphQL)

Note : les fichiers `*.postman_collection.json` n'existent que pour le gateway parce que REST et GraphQL sont supportés par le format Postman v2.1. Pour les 3 microservices gRPC, on doit créer les requêtes manuellement dans Postman (cf. note plus haut sur l'import gRPC).
