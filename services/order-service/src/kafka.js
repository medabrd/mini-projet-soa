// =========================================================================
//  kafka.js - Couche integration Kafka du microservice order-service
// =========================================================================
//
//  Role : connecter order-service au broker Kafka pour 2 jobs :
//
//    1. PRODUCER : publier des events sur le topic `order.events` quand
//       quelque chose change cote commande (creation, annulation, update).
//       Les autres services (driver-service, tracking-service) s'abonnent
//       a ce topic et reagissent.
//
//    2. CONSUMER : ecouter le topic `delivery.events` pour synchroniser
//       le statut des commandes avec l'avancement des livraisons.
//       (Ex: delivery.delivered -> on passe la commande en DELIVERED.)
//
//  Principe directeur : RESILIENCE. Si Kafka est down au demarrage, le
//  service ne crashe PAS - il continue a servir les RPCs gRPC, mais sans
//  publier d'events ni reagir. Pattern "fail-soft".
// =========================================================================

// --- Imports ---------------------------------------------------------------

// kafkajs : librairie cliente Kafka pure-JS pour Node.
//   - Kafka       : classe principale pour creer un "client" (configuration broker, retries...)
//   - Partitioners: strategies de routage messages -> partitions
//   - logLevel    : pour controler la verbosite des logs internes
const { Kafka, Partitioners, logLevel } = require('kafkajs');

// On a besoin du repo pour appliquer les updates de statut declenches
// par les events delivery.* qu'on consomme.
const repo = require('./orders-repo');


// --- Configuration --------------------------------------------------------

// Adresse du broker. Variable d'env injectee par docker-compose en prod
// (KAFKA_BROKER=kafka:29092, en reseau Docker interne). Fallback sur
// localhost pour les tests directs (sans Docker).
const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';

// Identifiant logique du client dans Kafka. Apparait dans les logs serveur
// et permet a Kafka UI de distinguer qui se connecte. Convention : nom
// du service. Ne pas confondre avec le groupId du consumer (different concept).
const CLIENT_ID = 'order-service';

// Topics utilises. Les constantes evitent les fautes de frappe ailleurs
// dans le fichier.
const ORDER_TOPIC = 'order.events';        // sur lequel on PUBLIE
const DELIVERY_TOPIC = 'delivery.events';  // sur lequel on CONSOMME


// --- Client Kafka principal -----------------------------------------------

// Un seul objet Kafka pour le service. Il sert a creer producer + consumer
// + admin (pour creer les topics). Connexion etablie au premier .connect().
const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: [BROKER],             // tableau (Kafka supporte plusieurs brokers)
  logLevel: logLevel.WARN,       // seulement warnings + errors (sinon trop verbose)
  retry: {
    retries: 3,                  // max tentatives en cas d'echec reseau
    initialRetryTime: 500,       // delai initial 500ms, backoff exponentiel ensuite
  },
});


// --- Producer -------------------------------------------------------------

// Un producer pour envoyer des messages sur les topics.
// L'option createPartitioner force le LegacyPartitioner : c'est l'ancien
// algorithme de routage de message vers partition. kafkajs v2 a change le
// defaut, ce qui peut casser la compatibilite avec d'anciens consumers.
// Le legacy est plus stable pour interop avec Java/Confluent stack.
const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

// Un consumer associe a un "consumer group".
// Concept Kafka cle : tous les consumers ayant le meme groupId se PARTAGENT
// la charge (chaque message n'est lu qu'UNE FOIS par le groupe). Si on
// scale order-service a 3 instances avec le meme groupId, les messages
// se repartissent entre elles. Differents groupIds = chacun lit tout
// independamment.
const consumer = kafka.consumer({ groupId: 'order-service-group' });


// Etats de connexion. Sert au pattern fail-soft : si pas connecte, les
// fonctions de publish no-op au lieu de planter.
let producerReady = false;
let consumerReady = false;


// --- Initialisation des topics --------------------------------------------

// Cree les topics s'ils n'existent pas deja. En theorie, KAFKA_AUTO_CREATE_TOPICS_ENABLE
// est a "true" dans notre docker-compose donc les topics se creent au premier
// publish/subscribe. Mais ce n'est pas garanti partout (souvent OFF en prod
// pour la securite), donc on les cree explicitement pour etre safe.
async function ensureTopics() {
  // L'admin client donne acces aux operations d'administration.
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: [
        // numPartitions=1 : pas de parallelisme intra-topic, suffisant
        // pour ce projet ou l'ordre des events compte plus que le debit.
        // replicationFactor=1 : 1 seul broker, donc 1 seule copie.
        { topic: ORDER_TOPIC, numPartitions: 1, replicationFactor: 1 },
        { topic: DELIVERY_TOPIC, numPartitions: 1, replicationFactor: 1 },
      ],
      // Attendre que le leader de chaque partition soit elu avant de
      // continuer. Evite les race conditions au premier message.
      waitForLeaders: true,
    });
    // Note : si les topics existent deja, createTopics ne throw pas, juste
    // un warning silencieux. Idempotent, on peut l'appeler a chaque demarrage.
  } finally {
    // Toujours deconnecter l'admin, meme en cas d'erreur. C'est une
    // connexion ponctuelle, pas un client de longue vie.
    await admin.disconnect();
  }
}


// --- Connexion au demarrage -----------------------------------------------

// Appele par server.js apres initDatabase().
async function connect() {
  // Strategie : producer et consumer connectes INDEPENDAMMENT.
  // Si l'un echoue, l'autre peut continuer. Pourquoi ? Producer et consumer
  // ouvrent des connexions TCP separees vers Kafka, peuvent etre dans des
  // etats differents. Mieux vaut un service "demi-fonctionnel" qu'un service
  // qui crashe completement.

  // Branche 1 : Producer
  try {
    await producer.connect();
    producerReady = true;
    console.log(`Kafka producer connecte (${BROKER})`);
  } catch (err) {
    // Pas de throw : on log et on continue. Les publish ulterieurs no-op.
    console.warn(`Kafka producer indisponible (${err.message}). Les publish seront ignores.`);
  }

  // Branche 2 : Consumer + topics
  try {
    await ensureTopics();
    console.log(`Topics Kafka prets : ${ORDER_TOPIC}, ${DELIVERY_TOPIC}`);

    await consumer.connect();

    // S'abonne au topic des deliveries.
    // fromBeginning: false = on commence aux nouveaux messages, pas l'historique.
    //   - true serait "rattraper tout depuis le debut du topic" (pratique
    //     pour replay/debug, mais pas pour production : on rejouerait des
    //     events deja appliques en BDD, doublons garantis).
    //   - false = consumer demarre au "offset commit" du group, ou au bout
    //     du topic si jamais consomme.
    await consumer.subscribe({ topic: DELIVERY_TOPIC, fromBeginning: false });

    // Lance la boucle de consommation. eachMessage est appelee a chaque
    // nouveau message recu (sequentiellement, pas en parallele dans un meme
    // consumer). Le try/catch INSIDE evite qu'un message corrompu tue le
    // consumer entier.
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          // Le payload est un Buffer (binaire). On le passe en string puis JSON.
          // Convention dans ce projet : tous les events sont stringifies en JSON.
          // En "vrai" prod on utiliserait Avro / Protobuf pour les events aussi,
          // mais JSON est plus simple a debug en Kafka UI.
          const evt = JSON.parse(message.value.toString());
          handleDeliveryEvent(evt);
        } catch (err) {
          // Le message reste "consomme" (l'offset avance malgre l'erreur),
          // ce qui est OK pour notre cas : un message malforme bloquerait
          // sinon la file indefiniment. Pour de la finance ce serait grave,
          // ici on s'en fiche.
          console.error('Erreur traitement message Kafka:', err.message);
        }
      },
    });
    consumerReady = true;
    console.log(`Kafka consumer abonne au topic ${DELIVERY_TOPIC}`);
  } catch (err) {
    console.warn(`Kafka consumer indisponible (${err.message}). Le service ne reagira pas aux delivery.events.`);
  }
}


// --- Publishers (cote producer) -------------------------------------------

// Pattern commun a toutes les fonctions publishX :
//   1. Garde fail-soft : si pas connecte au broker, on return silencieusement.
//   2. producer.send avec UN message (un seul element dans le tableau).
//   3. Structure du message : { key, value }.
//      - key : sert au routage par partition (meme key = meme partition,
//              garantit l'ordre pour cette key). On met l'order.id pour
//              que tous les events d'une meme commande soient ordonnes
//              relativement. Ici on a 1 seule partition donc peu impact,
//              mais c'est le bon reflexe pour scale-out plus tard.
//      - value: la payload, en String. On stringify du JSON.
//   4. try/catch local : un publish qui echoue ne doit pas casser l'appelant.

async function publishOrderPlaced(order) {
  if (!producerReady) return;
  try {
    await producer.send({
      topic: ORDER_TOPIC,
      messages: [
        {
          key: order.id,
          value: JSON.stringify({
            type: 'order.placed',
            order_id: order.id,
            customer_id: order.customer_id,
            customer_name: order.customer_name,
            delivery_address: order.delivery_address,
            items: order.items,
            total_amount: order.total_amount,
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`Event publie: order.placed ${order.id}`);
  } catch (err) {
    console.error('Echec publish order.placed:', err.message);
  }
}

// Publie a chaque changement de statut via UpdateOrderStatus. Sert
// notamment a driver-service pour nettoyer sa file d'attente quand une
// commande sort de l'etat PENDING par un autre chemin que order.cancelled
// (ex: passage manuel a DELIVERED via l'API admin). Fix d'un bug rencontre.
async function publishOrderStatusUpdated(order) {
  if (!producerReady) return;
  try {
    await producer.send({
      topic: ORDER_TOPIC,
      messages: [
        {
          key: order.id,
          value: JSON.stringify({
            type: 'order.status-updated',
            order_id: order.id,
            new_status: order.status,
            assigned_driver_id: order.assigned_driver_id || '',
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`Event publie: order.status-updated ${order.id} -> ${order.status}`);
  } catch (err) {
    console.error('Echec publish order.status-updated:', err.message);
  }
}

// Declenche la chaine d'annulation cross-services :
//   driver-service : libere le livreur s'il etait BUSY sur cette commande,
//                    retire de la queue si en attente
//   tracking-service: passe la delivery en CANCELLED
async function publishOrderCancelled(orderId, reason) {
  if (!producerReady) return;
  try {
    await producer.send({
      topic: ORDER_TOPIC,
      messages: [
        {
          key: orderId,
          value: JSON.stringify({
            type: 'order.cancelled',
            order_id: orderId,
            reason: reason || '',
            timestamp: new Date().toISOString(),
          }),
        },
      ],
    });
    console.log(`Event publie: order.cancelled ${orderId}`);
  } catch (err) {
    console.error('Echec publish order.cancelled:', err.message);
  }
}


// --- Consumer handler (cote subscriber) -----------------------------------

// Mapping declaratif : pour chaque type d'event delivery.X, quel status
// correspondant sur la commande. Garde le code de handleDeliveryEvent
// court (lookup table > if/else chain).
const DELIVERY_TO_STATUS = {
  'delivery.assigned': 'ASSIGNED',
  'delivery.picked-up': 'PICKED_UP',
  'delivery.in-transit': 'IN_TRANSIT',
  'delivery.delivered': 'DELIVERED',
  // Note : 'delivery.cancelled' n'est PAS dans ce mapping. La commande
  // associee est annulee directement via CancelOrder (qui publie alors
  // order.cancelled). Eviter de retraiter cote ce consumer evite des
  // boucles ou doubles updates.
};

// Traite un message du topic delivery.events.
// Recoit l'objet event deserialise (deja un JS object).
function handleDeliveryEvent(evt) {
  const newStatus = DELIVERY_TO_STATUS[evt.type];
  if (!newStatus) {
    return; // type d'event qu'on ne gere pas (ex: delivery.cancelled, delivery.location-updated)
  }
  if (!evt.order_id) {
    console.error('Event delivery sans order_id:', evt);
    return;
  }
  try {
    // Update direct via le repo, sync car SQLite est sync.
    // Le 3eme arg (evt.driver_id) renseigne le champ assigned_driver_id
    // s'il est present dans l'event (presence garantie pour delivery.assigned).
    const updated = repo.updateOrderStatus(evt.order_id, newStatus, evt.driver_id);
    if (updated) {
      console.log(`Order ${evt.order_id} -> ${newStatus} (suite a ${evt.type})`);
    } else {
      // Cas typique : delivery cree pour un order_id qu'on ne connait pas
      // (donnees orphelines, ou order supprime entre temps).
      console.warn(`Event ${evt.type} pour order inconnu: ${evt.order_id}`);
    }
  } catch (err) {
    console.error('Erreur update status depuis Kafka:', err.message);
  }
}


// --- Deconnexion propre ---------------------------------------------------

// Appelee depuis server.js quand le process recoit SIGINT (Ctrl+C, docker stop).
// Important pour ne pas laisser des sockets ouvertes cote broker, et pour
// committer les offsets en cours cote consumer (sinon on rejoue au prochain
// demarrage).
async function disconnect() {
  // .catch(() => {}) absorbe les erreurs : si on est deja deconnecte ou
  // si Kafka est down, on ne veut pas crasher pendant un shutdown.
  if (consumerReady) {
    await consumer.disconnect().catch(() => {});
  }
  if (producerReady) {
    await producer.disconnect().catch(() => {});
  }
}


// --- Exports publics ------------------------------------------------------

// On exporte :
//   - connect/disconnect : pour lifecycle dans server.js
//   - les 3 publishers   : pour que grpc-server.js puisse publier
// On N'exporte PAS handleDeliveryEvent : c'est purement interne (consumer
// l'invoque directement via la closure).
module.exports = {
  connect,
  disconnect,
  publishOrderPlaced,
  publishOrderCancelled,
  publishOrderStatusUpdated,
};
