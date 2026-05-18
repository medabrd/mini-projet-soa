// =========================================================================
//  kafka.js - Couche integration Kafka du microservice driver-service
// =========================================================================
//
//  Role : LE PLUS COMPLEXE des kafka.js du projet. Driver-service est le
//  "cerveau" de l'auto-assignation : c'est lui qui decide quel livreur
//  prend quelle commande, et qui maintient une FILE D'ATTENTE in-memory
//  pour les commandes sans livreur disponible.
//
//  Jobs :
//    1. PRODUCER : publier driver.events (driver.assigned, driver.location-updated)
//    2. CONSUMER : ecouter order.events ET delivery.events (2 topics)
//    3. AUTO-ASSIGNATION : a chaque order.placed -> trouve un driver dispo
//       et l'assigne (publish driver.assigned). Sinon -> enqueue.
//    4. LIBERATION : a chaque delivery.delivered/cancelled -> libere le
//       driver et lui donne la prochaine commande en attente s'il y en a.
//    5. NETTOYAGE QUEUE : retire des entrees obsoletes (order.cancelled,
//       order.status-updated avec status != PENDING).
//
//  Etat in-memory : `pendingOrders` est un tableau JS. Volatile au restart
//  (cohere avec le choix RxDB en memoire pour ce service).
//
//  Pour les conventions generales (fail-soft, LegacyPartitioner, groupId,
//  fromBeginning), voir order-service/src/kafka.js.
// =========================================================================

const { Kafka, Partitioners, logLevel } = require('kafkajs');
const repo = require('./drivers-repo');


// --- Configuration --------------------------------------------------------

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const CLIENT_ID = 'driver-service';

// 3 topics au total (1 publie + 2 consommes) :
const DRIVER_TOPIC = 'driver.events';      // PUBLIE
const ORDER_TOPIC = 'order.events';        // CONSOMME
const DELIVERY_TOPIC = 'delivery.events';  // CONSOMME


// --- Client Kafka, producer, consumer ------------------------------------

const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: [BROKER],
  logLevel: logLevel.WARN,
  retry: { retries: 3, initialRetryTime: 500 },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

// Un consumer abonne aux 2 topics (order + delivery). Meme pattern que
// tracking-service : kafkajs delivre dans la meme boucle eachMessage
// avec l'info `topic` pour distinguer.
const consumer = kafka.consumer({ groupId: 'driver-service-group' });

let producerReady = false;
let consumerReady = false;


// --- LA QUEUE in-memory --------------------------------------------------
//
// Elle existe pour le scenario suivant :
//   1. Une commande est creee (order.placed)
//   2. AUCUN driver n'est AVAILABLE
//   3. -> On ne peut pas l'assigner tout de suite
//   4. -> On la met en attente : pendingOrders.push({order_id, queued_at})
//   5. Plus tard, quand un driver se libere (delivery.delivered) OU
//      qu'un nouveau driver est cree (RegisterDriver), on prend la 1ere
//      de la queue (FIFO) et on lui assigne.
//
// Volontairement IN-MEMORY (donc perdue au restart). Coherent avec RxDB
// en memoire pour ce service. En vraie prod on stockerait en BDD.
const pendingOrders = [];


// --- Initialisation des topics --------------------------------------------

async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        { topic: DRIVER_TOPIC, numPartitions: 1, replicationFactor: 1 },
        { topic: ORDER_TOPIC, numPartitions: 1, replicationFactor: 1 },
        { topic: DELIVERY_TOPIC, numPartitions: 1, replicationFactor: 1 },
      ],
      waitForLeaders: true,
    });
  } finally {
    await admin.disconnect();
  }
}


// --- Connexion -----------------------------------------------------------

async function connect() {
  try {
    await producer.connect();
    producerReady = true;
    console.log(`Kafka producer connecte (${BROKER})`);
  } catch (err) {
    console.warn(`Kafka producer indisponible (${err.message}). Les publish seront ignores.`);
  }

  try {
    await ensureTopics();
    console.log(`Topics Kafka prets : ${DRIVER_TOPIC}, ${ORDER_TOPIC}, ${DELIVERY_TOPIC}`);

    await consumer.connect();
    // 2 subscribes successifs = abonnement aux 2 topics. Equivalent a
    // un seul appel avec { topics: [...] } mais explicite.
    await consumer.subscribe({ topic: ORDER_TOPIC, fromBeginning: false });
    await consumer.subscribe({ topic: DELIVERY_TOPIC, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          const evt = JSON.parse(message.value.toString());
          // Router selon le topic vers le bon handler.
          if (topic === ORDER_TOPIC) await handleOrderEvent(evt);
          else if (topic === DELIVERY_TOPIC) await handleDeliveryEvent(evt);
        } catch (err) {
          console.error('Erreur traitement message Kafka:', err.message);
        }
      },
    });
    consumerReady = true;
    console.log(`Kafka consumer abonne aux topics ${ORDER_TOPIC}, ${DELIVERY_TOPIC}`);
  } catch (err) {
    console.warn(`Kafka consumer indisponible (${err.message}). Le service ne reagira pas aux order.events.`);
  }
}


// --- Publishers ----------------------------------------------------------

// Publie quand un driver est assigne a une commande (auto ou via queue).
// Consomme par tracking-service qui va UPDATE la delivery existante avec
// le driver_id + driver_name + passer son status a ASSIGNED.
async function publishDriverAssigned(driver, orderId) {
  if (!producerReady) return;
  try {
    await producer.send({
      topic: DRIVER_TOPIC,
      messages: [
        {
          key: driver.id,   // key = driver.id pour grouper les events du meme driver sur la meme partition
          value: JSON.stringify({
            type: 'driver.assigned',
            driver_id: driver.id,
            driver_name: driver.name,
            order_id: orderId,
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`Event publie: driver.assigned ${driver.id} -> order ${orderId}`);
  } catch (err) {
    console.error('Echec publish driver.assigned:', err.message);
  }
}

// Publie a chaque update de position. Consomme par tracking-service
// qui ajoute une ligne dans delivery_events (audit log GPS).
// Pas de retour dans la cascade : c'est juste informatif.
async function publishLocationUpdated(driverId, location) {
  if (!producerReady) return;
  try {
    await producer.send({
      topic: DRIVER_TOPIC,
      messages: [
        {
          key: driverId,
          value: JSON.stringify({
            type: 'driver.location-updated',
            driver_id: driverId,
            latitude: location.latitude,
            longitude: location.longitude,
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
  } catch (err) {
    console.error('Echec publish driver.location-updated:', err.message);
  }
}


// === Consumer handlers ===================================================

// Traite les events recus sur order.events.
// 3 types geres :
//   - order.cancelled       -> nettoie la queue
//   - order.status-updated  -> nettoie la queue si status != PENDING (fix bug)
//   - order.placed          -> auto-assigne ou enqueue
async function handleOrderEvent(evt) {

  // CAS 1 : Commande annulee -> retirer de la queue si presente
  if (evt.type === 'order.cancelled') {
    if (!evt.order_id) return;
    const idx = pendingOrders.findIndex(p => p.order_id === evt.order_id);
    if (idx >= 0) {
      pendingOrders.splice(idx, 1);
      console.log(`Order ${evt.order_id} retiree de la file (cancelled, queue=${pendingOrders.length})`);
    }
    return;
  }

  // CAS 2 : Si une commande sort de PENDING par un autre chemin (ex: passage
  // manuel a DELIVERED via l'API admin), on la retire AUSSI de la file pour
  // eviter qu'un futur RegisterDriver ne se voie attribuer une commande
  // deja close. Fix d'un bug rencontre pendant les tests Postman.
  if (evt.type === 'order.status-updated') {
    if (!evt.order_id || evt.new_status === 'PENDING') return;
    const idx = pendingOrders.findIndex(p => p.order_id === evt.order_id);
    if (idx >= 0) {
      pendingOrders.splice(idx, 1);
      console.log(`Order ${evt.order_id} retiree de la file (status=${evt.new_status}, queue=${pendingOrders.length})`);
    }
    return;
  }

  // CAS 3 : Nouvelle commande -> auto-assignation
  if (evt.type !== 'order.placed') return;
  if (!evt.order_id) {
    console.error('Event order.placed sans order_id:', evt);
    return;
  }

  // Cherche un driver dispo
  const available = await repo.pickAvailableDriver();
  if (!available) {
    // Personne de dispo -> on enqueue avec timestamp pour l'audit
    pendingOrders.push({ order_id: evt.order_id, queued_at: new Date().toISOString() });
    console.warn(`Order ${evt.order_id} mis en file d'attente (aucun driver dispo, queue=${pendingOrders.length})`);
    return;
  }

  // Driver trouve -> on l'assigne et on publie driver.assigned
  const updated = await repo.assignDriverToOrder(available.id, evt.order_id);
  console.log(`Auto-assignation : ${updated.name} (${updated.id}) -> commande ${evt.order_id}`);
  await publishDriverAssigned(updated, evt.order_id);
}


// Traite les events recus sur delivery.events.
// On ne reagit qu'aux events TERMINAUX (delivered / cancelled) qui liberent
// le driver. Les events intermediaires (assigned, picked-up, in-transit)
// ne nous concernent pas ici.
async function handleDeliveryEvent(evt) {
  if (evt.type !== 'delivery.delivered' && evt.type !== 'delivery.cancelled') {
    return;
  }
  if (!evt.driver_id) return;

  // Libere le driver (BUSY -> AVAILABLE) idempotent.
  const released = await repo.releaseDriver(evt.driver_id);
  if (released) {
    console.log(`Livreur libere : ${released.name} (${released.id}) suite a ${evt.type}`);
    // Bonus : on lui donne tout de suite la prochaine commande en attente
    // s'il y en a une dans la queue. Optimisation pour ne pas attendre un
    // prochain RegisterDriver.
    await tryAssignToPending(released);
  }
}


// --- Helper auto-assignation depuis la queue -----------------------------
//
// Appelee depuis 2 endroits :
//   - grpc-server.js > RegisterDriver (nouveau driver cree)
//   - kafka.js > handleDeliveryEvent (driver libere de sa livraison)
//
// Prend la 1ere commande de la queue (FIFO) et l'assigne au driver donne.
// Si l'assignation echoue (driver supprime entre temps), on remet la
// commande EN TETE de file (unshift) pour le prochain candidat.
async function tryAssignToPending(driver) {
  if (!driver || pendingOrders.length === 0) return false;
  const next = pendingOrders.shift();
  const updated = await repo.assignDriverToOrder(driver.id, next.order_id);
  if (!updated) {
    // Driver disparu (DeleteDriver entre temps?) : on remet en tete
    pendingOrders.unshift(next);
    return false;
  }
  console.log(`Queue -> ${updated.name} (${updated.id}) prend order ${next.order_id} (reste ${pendingOrders.length})`);
  await publishDriverAssigned(updated, next.order_id);
  return true;
}


// Helper de debug, pas utilise par les autres modules mais expose au cas ou
// on voudrait l'ajouter dans un endpoint /admin/queue par exemple.
function getPendingCount() {
  return pendingOrders.length;
}


async function disconnect() {
  if (consumerReady) await consumer.disconnect().catch(() => {});
  if (producerReady) await producer.disconnect().catch(() => {});
}


// --- Exports --------------------------------------------------------------

// On expose :
//   - connect/disconnect       : lifecycle
//   - les 2 publishers         : pour grpc-server.js (UpdateLocation, etc.)
//   - tryAssignToPending       : pour grpc-server.js RegisterDriver
//   - getPendingCount          : pour eventuel endpoint d'admin
module.exports = {
  connect,
  disconnect,
  publishDriverAssigned,
  publishLocationUpdated,
  tryAssignToPending,
  getPendingCount,
};
