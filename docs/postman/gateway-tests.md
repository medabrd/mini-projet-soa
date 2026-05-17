Tests Gateway — REST + GraphQL
==============================

- Endpoints REST : `http://localhost:3000/api/*`
- Endpoint GraphQL : `http://localhost:3000/graphql`
- Apollo Sandbox auto-affiche dans le navigateur si on visite `/graphql` en GET

Le gateway sert de **point d'entrée unique** pour le client. Il expose :
- une API REST classique (CRUD JSON)
- un endpoint GraphQL flexible (queries + mutations + joins cross-services)
- des endpoints SSE (Server-Sent Events) pour relayer les flux gRPC streaming en HTTP


Test rapide automatisé
----------------------

    cd gateway
    node test-client.js

→ exerce REST (orders, drivers, deliveries) et GraphQL (query avec joins, mutation).


REST : exemples de requêtes
----------------------------

### Health

    GET http://localhost:3000/health

Réponse : `{ "status": "ok", "service": "gateway" }`


### Orders

**POST /api/orders** (créer)
Headers : `Content-Type: application/json`

Body :
```json
{
  "customer_id": "cust-230",
  "customer_name": "Med Abroud",
  "delivery_address": "sahloul 5 rue de l'honneur",
  "items": [
    { "product_name": "Pizza Neptune", "quantity": 2, "unit_price": 12.5 },
    { "product_name": "Sabrine 1L", "quantity": 1, "unit_price": 3 }
  ]
}
```

**GET /api/orders/:id** (récupérer)
**GET /api/orders?customer_id=cust-230&limit=10** (lister avec filtres)
**PATCH /api/orders/:id/status** (changer le statut)
Body : `{ "status": "ASSIGNED", "assigned_driver_id": "driver-42" }`

**POST /api/orders/:id/cancel** (annuler)
Body : `{ "reason": "Client a change d'avis" }`

**DELETE /api/orders/:id** (supprimer définitivement — admin)
→ Réponse : `{ "deleted": true }`. Refusée avec code **412 Precondition Failed** si la commande n'est pas DELIVERED ou CANCELLED.


### Drivers

**POST /api/drivers** (enregistrer)
Body :
```json
{ "name": "Karim Ben Salah", "phone": "+216 22 123 456", "vehicle_type": "scooter" }
```

**GET /api/drivers/:id**
**GET /api/drivers/available?limit=10**
**PATCH /api/drivers/:id/location**
Body :
```json
{ "latitude": 35.8245, "longitude": 10.6347, "speed_kmh": 32, "heading_deg": 90 }
```

**GET /api/drivers/:id/stream** (Server-Sent Events qui relaie le streaming gRPC)
→ une connexion HTTP qui reste ouverte, chaque update de position arrive comme un message SSE.
Tester en navigateur ou avec `curl -N http://localhost:3000/api/drivers/<id>/stream`.

**DELETE /api/drivers/:id** (supprimer — admin)
→ Réponse : `{ "deleted": true }`. Refusée avec code **412 Precondition Failed** si le livreur est BUSY.


### Deliveries

**GET /api/deliveries?limit=10**
**GET /api/deliveries/:id**
**GET /api/deliveries/:id/history**
**PATCH /api/deliveries/:id/status**
Body : `{ "new_status": "PICKED_UP" }`

**GET /api/deliveries/:id/watch** (SSE)


GraphQL : exemples
-------------------

Endpoint : `POST http://localhost:3000/graphql`
Headers : `Content-Type: application/json`

### Apollo Sandbox

Visiter `http://localhost:3000/graphql` dans le navigateur pour avoir l'IDE Apollo Sandbox interactif (autocomplete sur le schema, doc, historique). Pratique pour explorer.

### Query : une commande avec son driver et sa delivery (join 3 services en 1 requête)

```graphql
query GetOrderWithJoins($id: ID!) {
  order(id: $id) {
    id
    status
    total_amount
    customer_name
    items { product_name quantity unit_price }
    driver {
      id
      name
      vehicle_type
      status
      last_location { latitude longitude timestamp }
    }
    delivery {
      id
      status
      driver_name
      history {
        event_type
        created_at
      }
    }
  }
}
```

Variables :
```json
{ "id": "<order id>" }
```

→ Une **seule requête HTTP** qui récupère la commande (depuis order-service via gRPC), son livreur (depuis driver-service), sa livraison + son historique (depuis tracking-service). C'est le power-move de GraphQL : éviter au client de faire plusieurs round-trips.


### Query : liste des livraisons en cours avec leurs drivers

```graphql
query InTransitDeliveries {
  deliveries(status: "IN_TRANSIT", limit: 20) {
    total
    deliveries {
      id
      delivery_address
      driver {
        id
        name
        last_location { latitude longitude }
      }
      order {
        customer_name
        total_amount
      }
    }
  }
}
```

→ Pour un dashboard "livraisons en cours" qui montre tout d'un coup.


### Mutation : créer une commande

```graphql
mutation CreateOrder($input: CreateOrderInput!) {
  createOrder(input: $input) {
    id
    status
    total_amount
    delivery {
      id
      status
    }
  }
}
```

Variables :
```json
{
  "input": {
    "customer_id": "cust-230",
    "customer_name": "Med Abroud",
    "delivery_address": "sahloul 5 rue de l'honneur",
    "items": [
      { "product_name": "Pizza Neptune", "quantity": 2, "unit_price": 12.5 }
    ]
  }
}
```

→ Crée la commande, et grâce au resolver `Order.delivery`, on peut récupérer dans la même requête la delivery qui sera créée par la chaîne Kafka (au prochain rafraîchissement, parce qu'au moment de la mutation Kafka n'a pas encore propagé).


### Mutation : avancer une livraison

```graphql
mutation AdvanceDelivery($id: ID!, $newStatus: String!) {
  advanceDelivery(delivery_id: $id, new_status: $newStatus) {
    id
    status
    order {
      id
      status
    }
  }
}
```

Variables :
```json
{ "id": "<delivery id>", "newStatus": "PICKED_UP" }
```

→ Avance la delivery, et grâce au resolver `Delivery.order`, on récupère la commande mise à jour. (Note : il faut quelques ms pour que la chaîne Kafka delivery.events → order.events propage le status. La query suivante affichera le bon état.)


Pour tester dans Postman
-------------------------

### Import direct des collections (recommandé)

Contrairement au gRPC, REST et GraphQL sont parfaitement supportés par le format Postman v2.1. **Deux collections JSON sont fournies dans ce dossier**, prêtes à importer :

- `gateway-rest.postman_collection.json` — 18 requêtes REST organisées en 4 dossiers (Health, Orders, Drivers, Deliveries) incluant les endpoints SSE et les DELETE admin
- `gateway-graphql.postman_collection.json` — 5 opérations GraphQL (3 queries + 2 mutations) avec joins cross-services

Procédure :
1. Postman → bouton **Import** (en haut à gauche)
2. Glisser-déposer les 2 fichiers `.json` (ou `File → Choose Files`)
3. Postman crée 2 nouvelles collections : `gateway-rest` et `gateway-graphql`
4. Variables de collection à renseigner après les premières requêtes :
   - `gateway-rest` → `order_id`, `driver_id`, `delivery_id` (récupérés depuis les réponses des Create/Register)
   - `gateway-graphql` → `order_id`, `delivery_id`
5. Cliquer **Send** sur chaque requête, puis **Save Response → Save as example** pour figer la réponse dans la collection (livrable demandé)

### Création manuelle (si on veut refaire à zéro)

#### Collection REST

1. Créer une collection `gateway-rest`
2. Pour chaque endpoint REST, **Add request → HTTP** (pas gRPC cette fois)
3. Coller URL + body, save as example après Invoke

#### Collection GraphQL

1. Créer une collection `gateway-graphql`
2. **Add request → GraphQL** (Postman supporte GraphQL nativement)
3. URL : `http://localhost:3000/graphql`
4. Onglet GraphQL → Query : coller la query, Variables : coller le JSON
5. Send → save as example


Scénario de test complet
-------------------------

1. `POST /api/drivers` → enregistrer un livreur, noter l'id
2. `POST /api/orders` → créer une commande, noter l'id
3. (attendre 1-2 sec pour que la chaîne Kafka tourne)
4. `GET /api/orders/:id` → la commande est passée à ASSIGNED, driver assigné
5. `GET /api/deliveries` → la delivery existe avec status ASSIGNED
6. **GraphQL query** sur la commande avec joins → voir tout d'un coup
7. `PATCH /api/deliveries/:id/status` body `{ "new_status": "PICKED_UP" }`
8. (attendre)
9. `GET /api/orders/:id` → la commande est passée à PICKED_UP via la chaîne delivery.events → order
10. Idem avec IN_TRANSIT puis DELIVERED
