Système de livraison en temps réel
===================================

Mini-projet du cours SoA et Microservices (M. Gontara, AU 2025-2026).

L'idée c'est de faire une appli de livraison style Uber Eats en plus simple, avec plusieurs microservices qui communiquent. Le client passe une commande, un livreur est assigné, et on suit la livraison en temps réel sur une carte.


Stack technique
---------------

- Node.js pour tous les services
- gRPC pour la communication entre l'API Gateway et les services
- Kafka pour les events asynchrones entre les services
- REST + GraphQL exposés par l'API Gateway au client
- SQLite3 et RxDB pour les bases (chaque service a sa propre base)
- Docker Compose pour Kafka


Les services
------------

**order-service** — gère les commandes (création, statut, etc.). Base SQLite3.

**driver-service** — gère les livreurs et leurs positions GPS. Comme les positions arrivent en flux, j'utilise RxDB qui est plus adapté que du SQL pour ce genre de données.

**tracking-service** — fait le lien entre les commandes et les livreurs. Suit la livraison de A à Z et garde l'historique des trajets. Base SQLite3.

L'API Gateway parle aux 3 services en gRPC. Les services entre eux communiquent via Kafka avec 3 topics : `order.events`, `driver.events`, `delivery.events`.


Lancer Kafka
------------

Kafka est dans un docker compose pour pas avoir à l'installer en local.

    docker compose up -d

Pour vérifier que c'est bien parti :

    docker compose ps

Le broker écoute sur localhost:9092. Y'a aussi une interface web sur http://localhost:8080 pour voir les topics et les messages qui passent (Kafka UI).

Pour arrêter :

    docker compose down


Avancement
----------

- [x] Phase 0 — setup du projet et de Kafka
- [ ] Phase 1 — order-service
- [ ] Phase 2 — driver-service
- [ ] Phase 3 — tracking-service
- [ ] Phase 4 — API Gateway
- [ ] Phase 5 — client web
- [ ] Phase 6 — doc finale
