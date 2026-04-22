Tests Postman
=============

Ce dossier rassemble la doc et les exemples de payloads pour tester chaque microservice du projet avec Postman. Quand tous les services seront prêts, je créerai un workspace public Postman avec les collections correspondantes (REST, GraphQL, gRPC) — c'est un des livrables à déposer sur Classroom.


Prérequis
---------

- Postman v9 ou plus (support gRPC ajouté à partir de cette version, c'est le cas par défaut aujourd'hui)
- Le service à tester doit tourner localement (`node server.js` dans le dossier du service)
- Kafka peut tourner ou pas selon le test, ça change rien aux RPCs CRUD


Comment tester un service en gRPC
----------------------------------

1. Postman → New → gRPC Request
2. Server URL : `localhost:50051` (50051 pour order-service, 50052 pour driver, 50053 pour tracking)
3. Service definition : "Import a .proto file" → choisir le fichier dans le dossier `proto/` du repo (ex: `proto/order.proto`)
4. Une fois importé, sélectionner le service (ex: `order.OrderService`) et la méthode (ex: `CreateOrder`) dans les menus déroulants
5. Coller le payload JSON dans l'onglet "Message"
6. Cliquer "Invoke" → la réponse apparaît avec son code de statut gRPC

Pour les requêtes avec un ID retourné par une autre requête (ex: GetOrder après CreateOrder), copier l'ID depuis la réponse de CreateOrder et le coller dans le payload de GetOrder.


Pour le dépôt Classroom
------------------------

Une fois toutes les requêtes testées et les réponses sauvegardées dans Postman :

1. Créer un workspace Postman (Settings → New Workspace)
2. Le rendre public : Settings du workspace → Visibility → Public
3. Importer / créer les collections (une par service ou une globale)
4. Lancer les requêtes pour avoir les réponses sauvegardées (le prof veut voir les réponses, pas juste les requêtes)
5. Copier le lien public du workspace pour le déposer sur Google Classroom


Liste des services et leurs ports
----------------------------------

| Service | Port gRPC | Proto |
|---------|-----------|-------|
| order-service | 50051 | `proto/order.proto` |
| driver-service | 50052 (à venir) | `proto/driver.proto` |
| tracking-service | 50053 (à venir) | `proto/tracking.proto` |
| API Gateway | 3000 REST + 4000 GraphQL (à venir) | — |


Fichiers dans ce dossier
-------------------------

- `order-service-tests.md` — exemples de payloads pour les 5 RPCs de order-service
- (driver, tracking, gateway à venir au fil des phases)
