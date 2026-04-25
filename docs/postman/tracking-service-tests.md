Tests gRPC — tracking-service
=============================

- Service : `tracking.TrackingService`
- Serveur : `localhost:50053`
- Proto : `proto/tracking.proto`

Particularité : tracking-service est principalement **event-driven**. Les livraisons sont créées automatiquement à partir des events Kafka publiés par order-service et driver-service. Il n'y a donc pas de RPC "CreateDelivery" : on consulte les livraisons et on avance leur statut.


Test rapide automatisé (sans Postman)
--------------------------------------

Le client de test couvre la chaîne complète : il crée une commande dans order-service, attend que la delivery soit créée par les events Kafka, puis exerce les 5 RPCs de tracking.

    cd services/tracking-service
    node test-client.js

→ pour ça il faut que les 3 services (order, driver, tracking) ET le broker Kafka tournent.


Préparer une livraison pour tester
-----------------------------------

Comme il n'y a pas de RPC pour créer une delivery, il faut en provoquer une via la chaîne Kafka :

1. S'assurer qu'au moins un livreur est enregistré (`RegisterDriver` dans driver-service collection)
2. `CreateOrder` dans order-service collection
3. Attendre 1-2 secondes (le temps que les events traversent Kafka)
4. Une delivery est créée automatiquement dans tracking-service avec status `ASSIGNED`

Pour récupérer l'`id` de la delivery créée, faire un `ListDeliveries` (voir 2 ci-dessous).


1. GetDelivery
--------------

Récupère une livraison par son id.

**Requête :**

```json
{
  "id": "<delivery id>"
}
```

**Réponse attendue :**

```json
{
  "id": "<uuid>",
  "order_id": "<order uuid>",
  "customer_id": "cust-230",
  "customer_name": "Med Abroud",
  "delivery_address": "sahloul...",
  "driver_id": "<driver uuid>",
  "driver_name": "Karim Ben Salah",
  "status": "ASSIGNED",
  "created_at": "...",
  "updated_at": "..."
}
```

**Erreur si id inconnu :** `NOT_FOUND`.


2. ListDeliveries
-----------------

Liste les livraisons avec filtres optionnels.

**Requête (toutes) :**

```json
{
  "limit": 10,
  "offset": 0
}
```

**Requête (par status) :**

```json
{
  "status": "ASSIGNED",
  "limit": 10
}
```

**Requête (par driver) :**

```json
{
  "driver_id": "<driver id>",
  "limit": 10
}
```

**Réponse :** `{ "deliveries": [...], "total": N }`


3. GetDeliveryHistory
---------------------

Renvoie tous les events qui ont touché cette livraison (création, assignation, updates de position, advance status, cancellation...).

**Requête :**

```json
{
  "delivery_id": "<id>"
}
```

**Réponse :** `{ "events": [{ id, delivery_id, event_type, event_data_json, created_at }, ...] }`

Types possibles :
- `created` (suite à `order.placed`)
- `assigned` (suite à `driver.assigned`)
- `location-update` (suite à `driver.location-updated`)
- `status-advanced` (suite à AdvanceDeliveryStatus)
- `cancelled` (suite à `order.cancelled` ou advance-status CANCELLED)


4. AdvanceDeliveryStatus
-------------------------

Avance le status manuellement (typiquement déclenché par le client web quand le livreur dit "je viens de récupérer la commande", "je suis arrivé", etc.).

**Requête (passage à PICKED_UP) :**

```json
{
  "delivery_id": "<id>",
  "new_status": "PICKED_UP"
}
```

**Statuses autorisés :** PICKED_UP, IN_TRANSIT, DELIVERED, CANCELLED.

**Réponse :** la Delivery mise à jour.

→ À chaque advance, un event `delivery.events` est publié sur Kafka, qui sera consommé par order-service pour mettre à jour le status de la commande.


5. WatchDelivery (server-streaming)
------------------------------------

Pousse l'état complet de la delivery au client à chaque changement de status. Implémenté via un EventEmitter interne au service : chaque update du repo (création, assignation, advance, cancel) déclenche un emit qui est propagé au stream gRPC.

**Comment tester dans Postman :**

1. Onglet 1 : `WatchDelivery` avec `{ "delivery_id": "<id>" }` → Invoke. La 1re réponse est l'état courant.
2. Onglet 2 : `AdvanceDeliveryStatus` avec un nouveau status → Invoke.
3. → Sur l'onglet 1, la nouvelle version apparaît immédiatement.
4. Cancel pour fermer le stream.


Scénario de test complet
-------------------------

1. Vérifier qu'un driver existe : `RegisterDriver` dans driver-service collection si besoin
2. Déclencher une commande : `CreateOrder` dans order-service collection
3. Attendre 1-2 sec, puis `ListDeliveries` ici → la delivery doit apparaître avec status `ASSIGNED` (ou `PENDING_ASSIGNMENT` si le driver.assigned n'a pas encore été processé)
4. Noter le `id` de la delivery
5. `GetDelivery` avec cet id → confirme les données
6. `WatchDelivery` dans un onglet séparé, laissé ouvert
7. `AdvanceDeliveryStatus` → PICKED_UP, puis IN_TRANSIT, puis DELIVERED
8. → Onglet WatchDelivery reçoit chaque update
9. `GetDeliveryHistory` → on voit toute l'histoire (created, assigned, status-advanced * 3)
10. (Bonus) Vérifier dans Kafka UI sur le topic `delivery.events` qu'on a bien 4 messages publiés (delivery.assigned, delivery.picked-up, delivery.in-transit, delivery.delivered)
11. (Bonus) Vérifier dans order-service que la commande est passée elle aussi à PICKED_UP/IN_TRANSIT/DELIVERED grâce au consumer delivery.events


Vérification de la chaîne complète
-----------------------------------

Une fois tout le stack lancé, on peut vérifier que les 3 services se parlent via Kafka :

1. Kafka UI (http://localhost:8080) → Topics → on a bien `order.events`, `driver.events`, `delivery.events`
2. À chaque CreateOrder dans order-service :
   - `order.events` reçoit `order.placed`
   - driver-service consomme et publie `driver.assigned` sur `driver.events`
   - tracking-service consomme les deux et publie `delivery.assigned` sur `delivery.events`
   - order-service consomme `delivery.assigned` et passe la commande en status ASSIGNED
3. À chaque AdvanceDeliveryStatus dans tracking-service :
   - `delivery.events` reçoit le nouveau type (ex: delivery.picked-up)
   - order-service consomme et met à jour le status de la commande

C'est la chaîne événementielle complète du projet.
