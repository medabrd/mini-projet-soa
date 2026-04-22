Tests gRPC — order-service
==========================

- Service : `order.OrderService`
- Serveur : `localhost:50051`
- Proto : `proto/order.proto`

Les payloads ci-dessous sont à utiliser dans Postman → gRPC Request → onglet Message.


1. CreateOrder
--------------

Crée une nouvelle commande. Le service génère un UUID, calcule le total à partir des items, et met le statut à PENDING.

**Requête :**

```json
{
  "customer_id": "cust-001",
  "customer_name": "Med Abroud",
  "delivery_address": "12 Avenue Habib Bourguiba, Tunis",
  "items": [
    { "product_name": "Pizza Margherita", "quantity": 2, "unit_price": 12.5 },
    { "product_name": "Coca 33cl", "quantity": 1, "unit_price": 3 }
  ]
}
```

**Réponse attendue (code OK) :**

```json
{
  "id": "<uuid généré côté serveur>",
  "customer_id": "cust-001",
  "customer_name": "Med Abroud",
  "delivery_address": "12 Avenue Habib Bourguiba, Tunis",
  "items": [
    { "product_name": "Pizza Margherita", "quantity": 2, "unit_price": 12.5 },
    { "product_name": "Coca 33cl", "quantity": 1, "unit_price": 3 }
  ],
  "total_amount": 28,
  "status": "PENDING",
  "assigned_driver_id": "",
  "created_at": "2026-05-12T17:00:00.000Z",
  "updated_at": "2026-05-12T17:00:00.000Z"
}
```

**Erreur si items vide :** code `INVALID_ARGUMENT`, message "La commande doit contenir au moins un article".

→ Si Kafka est lancé, un event `order.placed` est publié sur le topic `order.events`. Sinon le RPC réussit quand même (Kafka est fire-and-forget).


2. GetOrder
-----------

Récupère une commande par son ID.

**Requête :**

```json
{
  "id": "<id retourné par CreateOrder>"
}
```

**Réponse attendue :** l'objet Order complet (même structure que la réponse de CreateOrder).

**Erreur si ID inconnu :** code `NOT_FOUND`, message "Commande introuvable".


3. ListOrders
-------------

Liste les commandes avec filtres optionnels et pagination.

**Requête (sans filtre, juste pagination) :**

```json
{
  "limit": 10,
  "offset": 0
}
```

**Requête (filtre par client) :**

```json
{
  "customer_id": "cust-001",
  "limit": 10,
  "offset": 0
}
```

**Requête (filtre par statut) :**

```json
{
  "status": "PENDING",
  "limit": 10,
  "offset": 0
}
```

**Réponse attendue :**

```json
{
  "orders": [ /* tableau d'Order */ ],
  "total": 5
}
```

→ Si on ne passe pas `status`, ou si on passe `ORDER_STATUS_UNSPECIFIED`, le filtre statut n'est pas appliqué.
→ Limit par défaut = 100 si non précisé. Offset par défaut = 0.


4. UpdateOrderStatus
--------------------

Met à jour le statut d'une commande. Peut aussi renseigner l'ID du livreur assigné.

**Requête (passage à ASSIGNED avec livreur) :**

```json
{
  "id": "<order id>",
  "status": "ASSIGNED",
  "assigned_driver_id": "driver-42"
}
```

**Requête (changement de statut sans toucher au livreur) :**

```json
{
  "id": "<order id>",
  "status": "IN_TRANSIT"
}
```

**Réponse attendue :** l'Order mis à jour avec son nouveau statut et `updated_at` rafraîchi.

**Erreur si ID inconnu :** code `NOT_FOUND`.

→ En pratique cette RPC sera surtout déclenchée automatiquement par le consumer Kafka qui écoute `delivery.events`. Mais elle reste utile pour les tests manuels et le panneau admin.


5. CancelOrder
--------------

Annule une commande. Échec si la commande est déjà livrée.

**Requête :**

```json
{
  "id": "<order id>",
  "reason": "Client a change d'avis"
}
```

**Réponse attendue :** l'Order avec `status: "CANCELLED"`.

**Erreur si ID inconnu :** code `NOT_FOUND`.
**Erreur si déjà livrée :** code `FAILED_PRECONDITION`, message "Impossible d'annuler une commande deja livree".

→ Si Kafka est lancé, un event `order.cancelled` est publié sur `order.events`.


Scénario de test complet (à exécuter dans cet ordre)
-----------------------------------------------------

1. `CreateOrder` avec le payload exemple → noter l'`id` retourné
2. `GetOrder` avec cet id → confirme qu'on récupère bien la même commande
3. `ListOrders` → la commande doit apparaître dans le tableau (total ≥ 1)
4. `UpdateOrderStatus` → status `ASSIGNED`, assigned_driver_id `driver-42`
5. `GetOrder` à nouveau → status est bien ASSIGNED, driver présent, updated_at a changé
6. `CancelOrder` avec une raison → status devient CANCELLED
7. `GetOrder` une dernière fois → confirme l'annulation et que updated_at est encore plus récent

Si tous ces appels passent dans l'ordre, le service est fonctionnel de bout en bout côté CRUD gRPC.


Test du consumer Kafka (avancé, nécessite Kafka démarré)
---------------------------------------------------------

Pour vérifier que le consumer `delivery.events` met bien à jour le statut :

1. Créer une commande (CreateOrder), récupérer son id
2. Publier manuellement un event sur le topic `delivery.events` via Kafka UI (http://localhost:8080) :
   ```json
   {
     "type": "delivery.assigned",
     "order_id": "<id de la commande>",
     "driver_id": "driver-99"
   }
   ```
3. GetOrder → la commande doit avoir status `ASSIGNED` et `assigned_driver_id: "driver-99"`

Types d'events gérés : `delivery.assigned`, `delivery.picked-up`, `delivery.in-transit`, `delivery.delivered`.
