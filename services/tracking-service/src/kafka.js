const { Kafka, Partitioners, logLevel } = require('kafkajs');
const repo = require('./deliveries-repo');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const CLIENT_ID = 'tracking-service';
const ORDER_TOPIC = 'order.events';
const DRIVER_TOPIC = 'driver.events';
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

// Un seul consumer abonne aux 2 topics order.events + driver.events
const consumer = kafka.consumer({ groupId: 'tracking-service-group' });

let producerReady = false;
let consumerReady = false;

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
    console.log(`Topics Kafka prets : ${ORDER_TOPIC}, ${DRIVER_TOPIC}, ${DELIVERY_TOPIC}`);

    await consumer.connect();
    await consumer.subscribe({ topics: [ORDER_TOPIC, DRIVER_TOPIC], fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ topic, message }) => {
        try {
          const evt = JSON.parse(message.value.toString());
          if (topic === ORDER_TOPIC) {
            await handleOrderEvent(evt);
          } else if (topic === DRIVER_TOPIC) {
            await handleDriverEvent(evt);
          }
        } catch (err) {
          console.error(`Erreur traitement message Kafka [${topic}]:`, err.message);
        }
      },
    });
    consumerReady = true;
    console.log(`Kafka consumer abonne aux topics ${ORDER_TOPIC}, ${DRIVER_TOPIC}`);
  } catch (err) {
    console.warn(`Kafka consumer indisponible (${err.message}). Tracking ne reagira pas aux events.`);
  }

  // Pont entre le bus interne (changements de status) et Kafka :
  // chaque fois qu'une delivery change de status, on republie un event delivery.events.
  repo.bus.on('delivery-changed', async (delivery) => {
    await publishDeliveryStatus(delivery);
  });
}

async function publishDeliveryStatus(delivery) {
  if (!producerReady) return;
  // Mapping status -> type d'event
  const STATUS_TO_TYPE = {
    PENDING_ASSIGNMENT: 'delivery.created',
    ASSIGNED: 'delivery.assigned',
    PICKED_UP: 'delivery.picked-up',
    IN_TRANSIT: 'delivery.in-transit',
    DELIVERED: 'delivery.delivered',
    CANCELLED: 'delivery.cancelled',
  };
  const type = STATUS_TO_TYPE[delivery.status];
  if (!type) return;

  try {
    await producer.send({
      topic: DELIVERY_TOPIC,
      messages: [
        {
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

// === Consumer handlers ===

async function handleOrderEvent(evt) {
  if (evt.type === 'order.placed') {
    if (!evt.order_id) return;
    repo.createDeliveryFromOrder(evt);
  } else if (evt.type === 'order.cancelled') {
    if (!evt.order_id) return;
    repo.cancelDelivery(evt.order_id);
  }
}

async function handleDriverEvent(evt) {
  if (evt.type === 'driver.assigned') {
    if (!evt.order_id || !evt.driver_id) return;
    repo.attachDriver(evt.order_id, evt.driver_id, evt.driver_name);
  } else if (evt.type === 'driver.location-updated') {
    if (!evt.driver_id) return;
    repo.logLocationUpdate(evt.driver_id, {
      latitude: evt.latitude,
      longitude: evt.longitude,
      timestamp: evt.timestamp,
    });
  }
}

async function disconnect() {
  if (consumerReady) await consumer.disconnect().catch(() => {});
  if (producerReady) await producer.disconnect().catch(() => {});
}

module.exports = { connect, disconnect };
