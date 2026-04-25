// Test end-to-end des 5 RPCs gRPC de order-service
// Usage : depuis services/order-service/  ->  node test-client.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const fs = require('fs');

const PROTO_PATH = path.resolve(__dirname, '..', '..', 'proto', 'order.proto');
const SERVER = process.env.SERVER || 'localhost:50051';

const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const orderProto = grpc.loadPackageDefinition(pkgDef).order;
const client = new orderProto.OrderService(SERVER, grpc.credentials.createInsecure());

function call(method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

const results = {};

async function main() {
  console.log(`>>> Test gRPC end-to-end sur ${SERVER}\n`);

  // 1. CreateOrder
  const createReq = {
    customer_id: 'cust-230',
    customer_name: 'Med Abroud',
    delivery_address: "sahloul 5 rue de l'honneur",
    items: [
      { product_name: 'Pizza Neptune', quantity: 2, unit_price: 12.5 },
      { product_name: 'Sabrine 1L', quantity: 1, unit_price: 3 },
    ],
  };
  const created = await call('CreateOrder', createReq);
  results.CreateOrder = { request: createReq, response: created };
  console.log('1) CreateOrder OK - id:', created.id, '- status:', created.status, '- total:', created.total_amount);

  const id = created.id;

  // 2. GetOrder
  const getReq = { id };
  const fetched = await call('GetOrder', getReq);
  results.GetOrder = { request: getReq, response: fetched };
  console.log('2) GetOrder OK - items:', fetched.items.length, '- status:', fetched.status);

  // 3. ListOrders
  const listReq = { limit: 10, offset: 0 };
  const list = await call('ListOrders', listReq);
  results.ListOrders = { request: listReq, response: list };
  console.log('3) ListOrders OK - total:', list.total, '- retournees:', list.orders.length);

  // 4. UpdateOrderStatus
  const updateReq = { id, status: 'ASSIGNED', assigned_driver_id: 'driver-42' };
  const updated = await call('UpdateOrderStatus', updateReq);
  results.UpdateOrderStatus = { request: updateReq, response: updated };
  console.log('4) UpdateOrderStatus OK - status:', updated.status, '- driver:', updated.assigned_driver_id);

  // 5. CancelOrder
  const cancelReq = { id, reason: "Client a change d'avis" };
  const cancelled = await call('CancelOrder', cancelReq);
  results.CancelOrder = { request: cancelReq, response: cancelled };
  console.log('5) CancelOrder OK - status:', cancelled.status);

  client.close();

  // Sauvegarde des réponses pour pouvoir les inclure dans la doc Postman
  const outPath = path.resolve(__dirname, 'last-test-results.json');
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(`\nResultats sauves dans ${outPath}`);
  console.log('\nTest end-to-end OK - les 5 RPCs marchent.');
}

main().catch(err => {
  console.error('Echec du test:', err.code, err.message);
  process.exit(1);
});
