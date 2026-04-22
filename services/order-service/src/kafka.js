const { Kafka, Partitioners, logLevel } = require('kafkajs');
const repo = require('./orders-repo');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const CLIENT_ID = 'order-service';
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

const consumer = kafka.consumer({ groupId: 'order-service-group' });

let connected = false;

async function connect() {
  try {
    await producer.connect();
    console.log(`Kafka producer connecte (${BROKER})`);

    await consumer.connect();
    await consumer.subscribe({ topic: DELIVERY_TOPIC, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const evt = JSON.parse(message.value.toString());
          handleDeliveryEvent(evt);
        } catch (err) {
          console.error('Erreur traitement message Kafka:', err.message);
        }
      },
    });
    console.log(`Kafka consumer abonne au topic ${DELIVERY_TOPIC}`);

    connected = true;
  } catch (err) {
    console.warn(`Kafka indisponible (${err.message}). Le service gRPC tourne quand meme.`);
    connected = false;
  }
}

async function publishOrderPlaced(order) {
  if (!connected) return;
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

async function publishOrderCancelled(orderId, reason) {
  if (!connected) return;
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

// Mapping des events de livraison vers les statuts de la commande
const DELIVERY_TO_STATUS = {
  'delivery.assigned': 'ASSIGNED',
  'delivery.picked-up': 'PICKED_UP',
  'delivery.in-transit': 'IN_TRANSIT',
  'delivery.delivered': 'DELIVERED',
};

function handleDeliveryEvent(evt) {
  const newStatus = DELIVERY_TO_STATUS[evt.type];
  if (!newStatus) {
    return; // event delivery non géré ici, on ignore
  }
  if (!evt.order_id) {
    console.error('Event delivery sans order_id:', evt);
    return;
  }
  try {
    const updated = repo.updateOrderStatus(evt.order_id, newStatus, evt.driver_id);
    if (updated) {
      console.log(`Order ${evt.order_id} -> ${newStatus} (suite a ${evt.type})`);
    } else {
      console.warn(`Event ${evt.type} pour order inconnu: ${evt.order_id}`);
    }
  } catch (err) {
    console.error('Erreur update status depuis Kafka:', err.message);
  }
}

async function disconnect() {
  if (!connected) return;
  await consumer.disconnect().catch(() => {});
  await producer.disconnect().catch(() => {});
  connected = false;
}

module.exports = {
  connect,
  disconnect,
  publishOrderPlaced,
  publishOrderCancelled,
};
