const { Kafka, Partitioners, logLevel } = require('kafkajs');
const repo = require('./drivers-repo');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const CLIENT_ID = 'driver-service';
const DRIVER_TOPIC = 'driver.events';
const ORDER_TOPIC = 'order.events';
const DELIVERY_TOPIC = 'delivery.events';

const kafka = new Kafka({
  clientId: CLIENT_ID,
  brokers: [BROKER],
  logLevel: logLevel.WARN,
  retry: { retries: 3, initialRetryTime: 500 },
});

const producer = kafka.producer({
  createPartitioner: Partitioners.LegacyPartitioner,
});

const consumer = kafka.consumer({ groupId: 'driver-service-group' });

let producerReady = false;
let consumerReady = false;

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
    await consumer.subscribe({ topic: ORDER_TOPIC, fromBeginning: false });
    await consumer.subscribe({ topic: DELIVERY_TOPIC, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          const evt = JSON.parse(message.value.toString());
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

async function publishDriverAssigned(driver, orderId) {
  if (!producerReady) return;
  try {
    await producer.send({
      topic: DRIVER_TOPIC,
      messages: [
        {
          key: driver.id,
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

// Quand une commande est creee (order.placed), on auto-assigne un livreur disponible
async function handleOrderEvent(evt) {
  if (evt.type !== 'order.placed') {
    return; // on ignore les autres types pour l'instant (cancelled, etc.)
  }
  if (!evt.order_id) {
    console.error('Event order.placed sans order_id:', evt);
    return;
  }

  const available = await repo.pickAvailableDriver();
  if (!available) {
    console.warn(`Pas de livreur dispo pour la commande ${evt.order_id}`);
    return;
  }

  const updated = await repo.assignDriverToOrder(available.id, evt.order_id);
  console.log(`Auto-assignation : ${updated.name} (${updated.id}) -> commande ${evt.order_id}`);
  await publishDriverAssigned(updated, evt.order_id);
}

// Quand une livraison se termine ou est annulee, on libere le livreur
// (status -> AVAILABLE) pour qu'il puisse prendre une nouvelle commande.
async function handleDeliveryEvent(evt) {
  if (evt.type !== 'delivery.delivered' && evt.type !== 'delivery.cancelled') {
    return;
  }
  if (!evt.driver_id) return;
  const released = await repo.releaseDriver(evt.driver_id);
  if (released) {
    console.log(`Livreur libere : ${released.name} (${released.id}) suite a ${evt.type}`);
  }
}

async function disconnect() {
  if (consumerReady) await consumer.disconnect().catch(() => {});
  if (producerReady) await producer.disconnect().catch(() => {});
}

module.exports = {
  connect,
  disconnect,
  publishDriverAssigned,
  publishLocationUpdated,
};
