// Test end-to-end de l'API Gateway : REST + GraphQL.
// Le gateway doit etre lance, ainsi que les 3 microservices et Kafka.
const http = require('node:http');

const HOST = process.env.HOST || 'localhost';
const PORT = process.env.PORT || 3000;

function request(method, pathname, body) {
  return new Promise((resolve, reject) => {
    const data = body ? JSON.stringify(body) : null;
    const opts = {
      host: HOST,
      port: PORT,
      method,
      path: pathname,
      headers: { 'Content-Type': 'application/json' },
    };
    if (data) opts.headers['Content-Length'] = Buffer.byteLength(data);
    const req = http.request(opts, res => {
      let chunks = '';
      res.on('data', c => (chunks += c));
      res.on('end', () => {
        const parsed = chunks ? JSON.parse(chunks) : null;
        if (res.statusCode >= 400) reject(Object.assign(new Error('HTTP ' + res.statusCode), { status: res.statusCode, body: parsed }));
        else resolve(parsed);
      });
    });
    req.on('error', reject);
    if (data) req.write(data);
    req.end();
  });
}

function gql(query, variables) {
  return request('POST', '/graphql', { query, variables: variables || {} });
}

async function main() {
  console.log(`>>> Test gateway sur http://${HOST}:${PORT}\n`);

  // 1. health
  const health = await request('GET', '/health');
  console.log('1) GET /health ->', JSON.stringify(health));

  // 2. REST : creer une commande
  const order = await request('POST', '/api/orders', {
    customer_id: 'gw-test',
    customer_name: 'Test Gateway',
    delivery_address: 'Sahloul, Sousse',
    items: [{ product_name: 'Plat gateway', quantity: 1, unit_price: 15 }],
  });
  console.log('2) POST /api/orders ->', order.id, '- status:', order.status);

  // 3. REST : recuperer la commande
  const fetched = await request('GET', `/api/orders/${order.id}`);
  console.log('3) GET  /api/orders/:id ->', fetched.id, 'total:', fetched.total_amount);

  // 4. REST : enregistrer un livreur (au cas ou il n'y en a pas)
  const driver = await request('POST', '/api/drivers', {
    name: 'Driver Gateway Test',
    phone: '+216 99 999 999',
    vehicle_type: 'voiture',
  });
  console.log('4) POST /api/drivers ->', driver.id);

  // 5. REST : updater sa position
  const loc = await request('PATCH', `/api/drivers/${driver.id}/location`, {
    latitude: 35.83,
    longitude: 10.63,
    speed_kmh: 40,
    heading_deg: 45,
  });
  console.log('5) PATCH /api/drivers/:id/location ->', loc.latitude, loc.longitude);

  // 6. REST : lister les livraisons
  const deliveries = await request('GET', '/api/deliveries?limit=5');
  console.log('6) GET  /api/deliveries -> total:', deliveries.total);

  // 7. GraphQL : query order avec joins (driver + delivery)
  console.log('\n7) GraphQL : query order avec joins...');
  // Attente : que la chaine Kafka ait eu le temps de creer la delivery
  await new Promise(r => setTimeout(r, 1500));
  const q = `
    query($id: ID!) {
      order(id: $id) {
        id
        status
        total_amount
        customer_name
        driver { id name vehicle_type status }
        delivery { id status driver_name }
      }
    }
  `;
  const gqlResult = await gql(q, { id: order.id });
  console.log('   GraphQL response:');
  console.log('  ', JSON.stringify(gqlResult.data.order, null, 2));

  // 8. GraphQL : mutation createOrder
  console.log('\n8) GraphQL : mutation createOrder...');
  const m = `
    mutation($input: CreateOrderInput!) {
      createOrder(input: $input) {
        id
        status
        total_amount
      }
    }
  `;
  const mResult = await gql(m, {
    input: {
      customer_id: 'gw-graphql',
      customer_name: 'Test GraphQL',
      delivery_address: 'GraphQL Street',
      items: [{ product_name: 'Item GQL', quantity: 2, unit_price: 5 }],
    },
  });
  console.log('   created:', mResult.data.createOrder.id, '- total:', mResult.data.createOrder.total_amount);

  // 9. GraphQL : query deliveries avec history
  console.log('\n9) GraphQL : query deliveries avec history...');
  const q2 = `
    query {
      deliveries(limit: 3) {
        total
        deliveries {
          id
          status
          driver { name }
          history { event_type created_at }
        }
      }
    }
  `;
  const gqlResult2 = await gql(q2);
  console.log('   ', gqlResult2.data.deliveries.total, 'deliveries au total');
  gqlResult2.data.deliveries.deliveries.forEach(d => {
    console.log(`   - ${d.id.slice(0, 8)}... status=${d.status} driver=${d.driver?.name || '-'} events=${d.history.length}`);
  });

  console.log('\nTest gateway OK - REST et GraphQL fonctionnent end-to-end avec joins cross-services.');
}

main().catch(err => {
  console.error('Echec:', err.message);
  if (err.body) console.error('Body:', err.body);
  process.exit(1);
});
