// =========================================================================
//  kafka.js - Couche integration Kafka du microservice tracking-service
// =========================================================================
//
//  Role : connecter tracking-service au broker Kafka pour 3 jobs distincts
//  (un de plus que order-service) :
//
//    1. PRODUCER : publier des events sur le topic `delivery.events` a
//       chaque changement de status d'une delivery (chaine cascading vers
//       order-service qui synchronise le status de la commande).
//
//    2. CONSUMER multi-topics : ecouter A LA FOIS `order.events` (pour
//       creer/annuler une delivery quand la commande source change) et
//       `driver.events` (pour attacher un driver assigne, logger les
//       updates GPS).
//
//    3. PONT bus<->Kafka : le bus interne du repo emet a chaque update.
//       On s'inscrit dessus pour traduire automatiquement en publish Kafka.
//       Cela evite que chaque RPC ait a se rappeler de publier.
//
//  Particularite vs order-service : tracking est LE plus "carrefour"
//  des 3 services. Il consomme 2 topics et publie sur 1. Il est aussi
//  PURMENT REACTIF : aucune RPC ne cree une delivery, tout part de
//  events Kafka.
//
//  Pour les conventions generales (fail-soft, LegacyPartitioner,
//  groupId, fromBeginning, etc.) voir order-service/src/kafka.js.
// =========================================================================

const { Kafka, Partitioners, logLevel } = require('kafkajs');

// On a besoin du repo pour appliquer les mutations sur les deliveries
// declenchees par les events Kafka, ET pour s'abonner a son bus interne.
const repo = require('./deliveries-repo');


// --- Configuration --------------------------------------------------------

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const CLIENT_ID = 'tracking-service';

// 3 topics au total :
//   - 2 qu'on CONSOMME (events qui declenchent la creation/maj des deliveries)
//   - 1 qu'on PUBLIE (pour propager nos propres changements de status)
const ORDER_TOPIC = 'order.events';        // CONSOMME
const DRIVER_TOPIC = 'driver.events';      // CONSOMME
const DELIVERY_TOPIC = 'delivery.events';  // PUBLIE


// --- Client Kafka, producer et consumer ----------------------------------

const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: [BROKER],
  logLevel: logLevel.WARN,
  retry: { retries: 3, initialRetryTime: 500 },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

// Un seul consumer abonne aux 2 topics order.events + driver.events.
// Pas besoin de 2 consumers separes : kafkajs gere le multi-subscription
// nativement et delivre tous les messages dans la meme boucle eachMessage,
// avec l'info `topic` pour distinguer.
const consumer = kafka.consumer({ groupId: 'tracking-service-group' });

let producerReady = false;
let consumerReady = false;


// --- Initialisation des topics --------------------------------------------

// Cree les 3 topics si pas deja presents. Idempotent.
async function ensureTopics() {
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        { topic: ORDER_TOPIC, numPartitions: 1, replicationFactor: 1 },
        { topic: DRIVER_TOPIC, numPartitions: 1, replicationFactor: 1 },
        { topic: DELIVERY_TOPIC, numPartitions: 1, replicationFactor: 1 },
      ],
      waitForLeaders: true,
    });
  } finally {
    await admin.disconnect();
  }
}


// --- Connexion au demarrage -----------------------------------------------

async function connect() {
  // Branche 1 : producer (pour publier delivery.events)
  try {
    await producer.connect();
    producerReady = true;
    console.log(`Kafka producer connecte (${BROKER})`);
  } catch (err) {
    console.warn(`Kafka producer indisponible (${err.message}). Les publish seront ignores.`);
  }

  // Branche 2 : consumer + topics
  try {
    await ensureTopics();
    console.log(`Topics Kafka prets : ${ORDER_TOPIC}, ${DRIVER_TOPIC}, ${DELIVERY_TOPIC}`);

    await consumer.connect();

    // Abonnement a 2 topics d'un coup. On aurait pu faire 2 appels
    // consumer.subscribe({...}), c'est equivalent.
    await consumer.subscribe({ topics: [ORDER_TOPIC, DRIVER_TOPIC], fromBeginning: false });

    // Boucle de consommation. kafkajs nous donne le `topic` du message
    // recu en plus de la `message`, ce qui permet de router vers le bon
    // handler. Sans cette info on serait oblige d'avoir 2 consumers separes.
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          const evt = JSON.parse(message.value.toString());
          if (topic === ORDER_TOPIC) {
            await handleOrderEvent(evt);
          } else if (topic === DRIVER_TOPIC) {
            await handleDriverEvent(evt);
          }
          // Si topic inconnu : on ignore silencieusement (ne devrait pas
          // arriver vu qu'on a subscribe explicitement, mais defense en
          // profondeur si ensureTopics se trompe).
        } catch (err) {
          // L'erreur n'arrete pas le consumer. Le message reste consomme
          // (offset avance) : OK pour notre cas, evite un blocage indefini.
          console.error(`Erreur traitement message Kafka [${topic}]:`, err.message);
        }
      },
    });
    consumerReady = true;
    console.log(`Kafka consumer abonne aux topics ${ORDER_TOPIC}, ${DRIVER_TOPIC}`);
  } catch (err) {
    console.warn(`Kafka consumer indisponible (${err.message}). Tracking ne reagira pas aux events.`);
  }

  // ----- PONT bus<->Kafka -----
  //
  // C'est LA particularite de tracking-service. Le repo expose un
  // EventEmitter (`bus`) qui emet a chaque modification de delivery.
  // Ce bus sert deja a alimenter le streaming gRPC WatchDelivery.
  // ICI on s'inscrit aussi dessus pour TRADUIRE chaque modification
  // en publish Kafka delivery.events.
  //
  // Avantage du pattern : les fonctions du repo (attachDriver,
  // advanceStatus, cancelDelivery, etc.) n'ont PAS besoin de connaitre
  // Kafka. Elles emettent juste sur le bus, et on transforme ca en
  // event Kafka automatiquement. Couplage minimal.
  repo.bus.on('delivery-changed', async (delivery) => {
    await publishDeliveryStatus(delivery);
  });
}


// --- Publisher : delivery -> Kafka ---------------------------------------

// Convertit une Delivery (objet metier) en event Kafka delivery.events.
// Appelee automatiquement depuis le bus du repo (cf connect()).
async function publishDeliveryStatus(delivery) {
  if (!producerReady) return;

  // Mapping declaratif status -> type d'event.
  // Pourquoi ce mapping : les autres services (notamment order-service)
  // matchent sur le 'type' de l'event, pas sur le status brut.
  // Convention : 'delivery.X' au format kebab-case.
  const STATUS_TO_TYPE = {
    PENDING_ASSIGNMENT: 'delivery.created',
    ASSIGNED: 'delivery.assigned',
    PICKED_UP: 'delivery.picked-up',
    IN_TRANSIT: 'delivery.in-transit',
    DELIVERED: 'delivery.delivered',
    CANCELLED: 'delivery.cancelled',
  };
  const type = STATUS_TO_TYPE[delivery.status];
  if (!type) return;   // status inconnu : on ne publie pas (defense)

  try {
    await producer.send({
      topic: DELIVERY_TOPIC,
      messages: [
        {
          // Key = order_id (pas delivery_id !) : c'est order_id qui sert
          // de cle de jointure cross-service, on garantit ainsi que tous
          // les events touchant a une meme commande/livraison vont sur
          // la meme partition (ordre preserve).
          key: delivery.order_id,
          value: JSON.stringify({
            type,
            delivery_id: delivery.id,
            order_id: delivery.order_id,
            driver_id: delivery.driver_id || '',
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`Event publie: ${type} delivery=${delivery.id} order=${delivery.order_id}`);
  } catch (err) {
    console.error(`Echec publish ${type}:`, err.message);
  }
}


// === Consumer handlers ====================================================

// Traite les events recus sur order.events (publies par order-service).
// 2 types geres :
//   - order.placed    -> creer une delivery PENDING_ASSIGNMENT
//   - order.cancelled -> annuler la delivery liee a cette commande
//
// Tous les autres types (order.status-updated par ex) sont ignores.
async function handleOrderEvent(evt) {
  if (evt.type === 'order.placed') {
    if (!evt.order_id) return;
    // createDeliveryFromOrder lit les champs customer_*, delivery_address,
    // items, total_amount de l'event et cree la ligne en BDD.
    // Emet 'delivery-changed' sur le bus -> declenche publishDeliveryStatus
    // (event delivery.created publie) ET update les clients WatchDelivery
    // qui suivent deja.
    repo.createDeliveryFromOrder(evt);
  } else if (evt.type === 'order.cancelled') {
    if (!evt.order_id) return;
    // Cherche la delivery par order_id et la passe en CANCELLED.
    // Idem : emet sur le bus.
    repo.cancelDelivery(evt.order_id);
  }
}

// Traite les events recus sur driver.events (publies par driver-service).
// 2 types geres :
//   - driver.assigned        -> attacher le driver a la delivery existante
//   - driver.location-updated -> logger l'update dans delivery_events (audit)
//
// La gestion location-updated ne change PAS le status de la delivery
// (ce n'est qu'une trace historique). Donc pas d'event publie en cascade.
async function handleDriverEvent(evt) {
  if (evt.type === 'driver.assigned') {
    if (!evt.order_id || !evt.driver_id) return;
    // Trouve la delivery par order_id, lui rattache driver_id + driver_name
    // et passe son status a ASSIGNED. Emet sur le bus -> publishDeliveryStatus
    // declenche delivery.assigned -> order-service synchronise sa commande.
    repo.attachDriver(evt.order_id, evt.driver_id, evt.driver_name);
  } else if (evt.type === 'driver.location-updated') {
    if (!evt.driver_id) return;
    // N'update PAS la delivery elle-meme, juste insere une ligne dans
    // delivery_events. Permet de reconstituer une trace GPS chronologique
    // pour cette delivery sans avoir a stocker des coordonnees actuelles.
    repo.logLocationUpdate(evt.driver_id, {
      latitude: evt.latitude,
      longitude: evt.longitude,
      timestamp: evt.timestamp,
    });
  }
}


// --- Deconnexion propre ---------------------------------------------------

async function disconnect() {
  if (consumerReady) await consumer.disconnect().catch(() => {});
  if (producerReady) await producer.disconnect().catch(() => {});
}


// --- Exports ---------------------------------------------------------------

// On exporte uniquement connect/disconnect (lifecycle).
// Les handlers de consumer et publishDeliveryStatus sont internes :
//   - les consumer handlers sont appeles via le run loop interne
//   - publishDeliveryStatus est appele via le bus du repo (auto-pont)
// Aucune RPC du service n'a besoin d'appeler explicitement Kafka.
module.exports = { connect, disconnect };
