Tests gRPC — driver-service
===========================

- Service : `driver.DriverService`
- Serveur : `localhost:50052`
- Proto : `proto/driver.proto`

Les payloads ci-dessous sont à coller dans Postman → gRPC Request → onglet Message. Voir `README.md` de ce dossier pour la procédure de création des requêtes.


Test rapide automatisé (sans Postman)
--------------------------------------

Avant de passer par Postman, on peut valider que le service marche, y compris le streaming :

    cd services/driver-service
    node test-client.js

→ enchaîne les 5 RPCs (dont le server-streaming) et affiche les résultats.


1. RegisterDriver
-----------------

Enregistre un nouveau livreur. Le service génère un UUID, met le statut à AVAILABLE.

**Requête :**

```json
{
  "name": "Karim Ben Salah",
  "phone": "+216 22 123 456",
  "vehicle_type": "scooter"
}
```

**Réponse attendue :**

```json
{
  "id": "<uuid>",
  "name": "Karim Ben Salah",
  "phone": "+216 22 123 456",
  "vehicle_type": "scooter",
  "status": "AVAILABLE",
  "current_order_id": "",
  "last_location": null,
  "created_at": "2026-05-12T...",
  "updated_at": "2026-05-12T..."
}
```


2. GetDriver
------------

Récupère un livreur par son ID.

**Requête :**

```json
{
  "id": "<id retourné par RegisterDriver>"
}
```

**Réponse attendue :** l'objet Driver complet.

**Erreur si ID inconnu :** code `NOT_FOUND`, message "Livreur introuvable".


3. ListAvailableDrivers
-----------------------

Liste tous les livreurs avec status `AVAILABLE` (pas occupés, pas hors-ligne).

**Requête :**

```json
{
  "limit": 10
}
```

**Réponse attendue :**

```json
{
  "drivers": [ /* tableau de Driver */ ],
  "total": 1
}
```


4. UpdateLocation
-----------------

Met à jour la position GPS d'un livreur. Cette action déclenche aussi un event Kafka `driver.location-updated` et alimente le stream `StreamDriverLocation` (voir 5).

**Requête :**

```json
{
  "driver_id": "<id du livreur>",
  "latitude": 35.8245,
  "longitude": 10.6347,
  "speed_kmh": 32,
  "heading_deg": 90
}
```

**Réponse attendue :** l'objet Location avec le `timestamp` rempli côté serveur.


5. StreamDriverLocation (server-streaming)
-------------------------------------------

C'est le RPC bonus : le serveur **garde la connexion ouverte** et pousse une nouvelle position à chaque fois que la position du livreur change. Implémenté avec les observables RxDB : on s'abonne au document du livreur, et tout changement est propagé au client.

**Comment le tester dans Postman :**

1. Créer une requête gRPC sur la méthode `StreamDriverLocation`
2. Message :
   ```json
   {
     "driver_id": "<id du livreur>"
   }
   ```
3. Cliquer **Invoke** → Postman ouvre une connexion streaming. Tant que la connexion est ouverte, chaque update de position envoyé via `UpdateLocation` (depuis une autre requête Postman, depuis le client de test, ou depuis le client web final) apparaît instantanément dans la zone Response.
4. Pour terminer le stream : cliquer **Cancel** (le bouton change de Invoke à Cancel pendant le stream).

**Test reproductible** :
- Onglet 1 : `StreamDriverLocation` lancé pour driver X → reste ouvert
- Onglet 2 : `UpdateLocation` pour driver X avec une nouvelle lat/lng
- → Onglet 1 reçoit immédiatement la nouvelle position

Si Postman ne supporte pas bien l'affichage des messages streaming au-delà du premier, utiliser `node test-client.js` qui couvre ce cas et affiche les positions reçues en temps réel.


Scénario de test complet (à exécuter dans cet ordre)
-----------------------------------------------------

1. `RegisterDriver` → noter l'`id`
2. `GetDriver` → confirme la récupération
3. `ListAvailableDrivers` → le livreur apparaît dans la liste (`total >= 1`)
4. `UpdateLocation` → la position est sauvée et un event Kafka `driver.location-updated` est publié
5. `StreamDriverLocation` dans un onglet séparé, laissé ouvert
6. Refaire `UpdateLocation` avec une autre lat/lng → la nouvelle position apparaît dans l'onglet streaming
7. Annuler le stream


Intégration Kafka avec order-service
-------------------------------------

Le driver-service consomme automatiquement les events `order.placed` publiés par order-service. À chaque commande créée, driver-service :

1. Cherche un livreur `AVAILABLE`
2. Le marque `BUSY` avec `current_order_id` rempli
3. Publie un event `driver.assigned` sur le topic `driver.events`

**Test end-to-end côté Kafka** :

1. Avec driver-service + order-service tous les deux lancés, enregistrer au moins 1 livreur (`RegisterDriver`)
2. Côté order-service, lancer `CreateOrder` dans Postman
3. Ouvrir Kafka UI (http://localhost:8080) → topic `driver.events` → on voit le message `driver.assigned`
4. `GetDriver` sur l'id du livreur → son statut est passé de AVAILABLE à BUSY, son `current_order_id` est l'id de la commande

Types d'events publiés sur `driver.events` :
- `driver.assigned` quand un livreur est assigné à une commande
- `driver.location-updated` à chaque appel de UpdateLocation
