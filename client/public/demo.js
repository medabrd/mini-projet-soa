// Demo interactive : 2 flows complets (REST et GraphQL) qui enchainent
// toutes les operations exposees par le gateway, etape par etape, avec
// visualisation de chaque requete et de chaque reponse.
// Les variables (order_id, driver_id, delivery_id, ...) sont chainees
// automatiquement entre etapes via le mecanisme {{var}} dans les payloads.

const GW = 'http://localhost:3000';

// ----------------- Helpers -----------------
function getByPath(obj, path) {
  if (!obj || !path) return undefined;
  const parts = path.split('.');
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = /^\d+$/.test(p) ? cur[Number(p)] : cur[p];
  }
  return cur;
}

function resolveTemplate(value, ctx) {
  if (value == null) return value;
  if (typeof value === 'string') {
    return value.replace(/\{\{(\w+)\}\}/g, (_, key) => ctx[key] != null ? ctx[key] : '');
  }
  if (Array.isArray(value)) return value.map(v => resolveTemplate(v, ctx));
  if (typeof value === 'object') {
    const out = {};
    for (const k of Object.keys(value)) out[k] = resolveTemplate(value[k], ctx);
    return out;
  }
  return value;
}

function wait(ms) { return new Promise(r => setTimeout(r, ms)); }

// ----------------- Definitions des flows -----------------

const REST_FLOW = {
  title: 'Demonstration complete - REST',
  intro: 'Enchaine tous les endpoints REST du gateway : Health, Orders, Drivers, Deliveries. Le flow montre la chaine Kafka en action (order -> driver -> delivery -> retour vers order quand on avance les statuts).',
  steps: [
    {
      title: 'GET /health',
      desc: 'Verifier que le gateway repond.',
      rest: { method: 'GET', path: '/health' },
    },
    {
      title: 'GET /api/drivers/available?limit=10',
      desc: 'Lister les livreurs AVAILABLE. driver-service ajoute 3 seeds au demarrage (Karim, Sami, Anis).',
      rest: { method: 'GET', path: '/api/drivers/available?limit=10' },
      extract: { seed_driver_id: 'drivers.0.id' },
    },
    {
      title: 'POST /api/drivers',
      desc: 'Enregistrer un nouveau livreur cote driver-service via gRPC RegisterDriver.',
      rest: {
        method: 'POST', path: '/api/drivers',
        body: { name: 'Walid Mansouri', phone: '+216 99 888 777', vehicle_type: 'moto' },
      },
      extract: { driver_id: 'id' },
    },
    {
      title: 'GET /api/drivers/{{driver_id}}',
      desc: 'Recuperer le livreur qu\'on vient de creer.',
      rest: { method: 'GET', path: '/api/drivers/{{driver_id}}' },
    },
    {
      title: 'PATCH /api/drivers/{{driver_id}}/location',
      desc: 'Mettre a jour la position GPS. Publie driver.location-updated sur Kafka.',
      rest: {
        method: 'PATCH', path: '/api/drivers/{{driver_id}}/location',
        body: { latitude: 35.8245, longitude: 10.6347, speed_kmh: 32, heading_deg: 90 },
      },
    },
    {
      title: 'POST /api/orders',
      desc: 'Creer une commande. Publie order.placed sur Kafka -> driver-service auto-assigne un livreur.',
      rest: {
        method: 'POST', path: '/api/orders',
        body: {
          customer_id: 'demo-rest', customer_name: 'Demo REST',
          delivery_address: 'sahloul 5 rue de l\'honneur',
          items: [
            { product_name: 'Pizza Neptune', quantity: 2, unit_price: 12.5 },
            { product_name: 'Sabrine 1L', quantity: 1, unit_price: 3 },
          ],
        },
      },
      extract: { order_id: 'id' },
    },
    {
      title: 'Attente 2.5s - propagation Kafka',
      desc: 'Chaine : order.placed -> driver.assigned -> delivery.created+assigned -> order ASSIGNED.',
      wait: 2500,
    },
    {
      title: 'GET /api/orders/{{order_id}}',
      desc: 'La commande doit etre passee a ASSIGNED avec un assigned_driver_id.',
      rest: { method: 'GET', path: '/api/orders/{{order_id}}' },
      kafkaNote: 'Status synchronise par delivery.assigned recu sur le consumer order.',
    },
    {
      title: 'GET /api/orders?customer_id=demo-rest&limit=10',
      desc: 'Lister les commandes filtrees par customer_id.',
      rest: { method: 'GET', path: '/api/orders?customer_id=demo-rest&limit=10' },
    },
    {
      title: 'GET /api/deliveries?limit=10',
      desc: 'Lister les livraisons. Doit contenir celle creee par la chaine Kafka.',
      rest: { method: 'GET', path: '/api/deliveries?limit=10' },
      extractFn: (resp, ctx) => {
        const d = (resp.deliveries || []).find(x => x.order_id === ctx.order_id);
        return d ? { delivery_id: d.id } : {};
      },
    },
    {
      title: 'GET /api/deliveries/{{delivery_id}}',
      desc: 'Recuperer la livraison en detail.',
      rest: { method: 'GET', path: '/api/deliveries/{{delivery_id}}' },
    },
    {
      title: 'GET /api/deliveries/{{delivery_id}}/history',
      desc: 'Historique des events de la livraison : delivery.created, delivery.assigned, ...',
      rest: { method: 'GET', path: '/api/deliveries/{{delivery_id}}/history' },
    },
    {
      title: 'PATCH /api/deliveries/{{delivery_id}}/status (PICKED_UP)',
      desc: 'Avancer la livraison. Publie delivery.picked-up sur Kafka.',
      rest: {
        method: 'PATCH', path: '/api/deliveries/{{delivery_id}}/status',
        body: { new_status: 'PICKED_UP' },
      },
    },
    { title: 'Attente 1s - Kafka', wait: 1000 },
    {
      title: 'PATCH /api/deliveries/{{delivery_id}}/status (IN_TRANSIT)',
      desc: 'Continuer l\'avancement.',
      rest: {
        method: 'PATCH', path: '/api/deliveries/{{delivery_id}}/status',
        body: { new_status: 'IN_TRANSIT' },
      },
    },
    { title: 'Attente 1s - Kafka', wait: 1000 },
    {
      title: 'PATCH /api/deliveries/{{delivery_id}}/status (DELIVERED)',
      desc: 'Terminer la livraison. Publie delivery.delivered -> order passe a DELIVERED + driver libere.',
      rest: {
        method: 'PATCH', path: '/api/deliveries/{{delivery_id}}/status',
        body: { new_status: 'DELIVERED' },
      },
    },
    { title: 'Attente 1.5s - propagation Kafka inverse', wait: 1500 },
    {
      title: 'GET /api/orders/{{order_id}}',
      desc: 'La commande doit etre passee a DELIVERED (chaine delivery.events -> order-service consumer).',
      rest: { method: 'GET', path: '/api/orders/{{order_id}}' },
      kafkaNote: 'Demonstration de la propagation arriere : delivery -> order.',
    },
    {
      title: 'PATCH /api/orders/{{order_id}}/status (override manuel)',
      desc: 'Endpoint d\'override manuel du statut, rarement utilise (Kafka fait le job).',
      rest: {
        method: 'PATCH', path: '/api/orders/{{order_id}}/status',
        body: { status: 'DELIVERED' },
      },
    },
    {
      title: 'POST /api/orders (2eme commande pour demo cancel)',
      desc: 'Creer une nouvelle commande pour tester l\'endpoint cancel.',
      rest: {
        method: 'POST', path: '/api/orders',
        body: {
          customer_id: 'demo-rest', customer_name: 'Demo REST',
          delivery_address: 'avenue Habib Bourguiba, Sousse',
          items: [{ product_name: 'Burger', quantity: 1, unit_price: 8 }],
        },
      },
      extract: { order2_id: 'id' },
    },
    {
      title: 'POST /api/orders/{{order2_id}}/cancel',
      desc: 'Annuler la commande. Publie order.cancelled.',
      rest: {
        method: 'POST', path: '/api/orders/{{order2_id}}/cancel',
        body: { reason: 'Demo : annulation cliente' },
      },
    },
    {
      title: 'GET /api/orders/{{order2_id}}',
      desc: 'Verifier que la commande est bien CANCELLED.',
      rest: { method: 'GET', path: '/api/orders/{{order2_id}}' },
    },
  ],
};

const GRAPHQL_FLOW = {
  title: 'Demonstration complete - GraphQL',
  intro: 'Enchaine toutes les operations GraphQL (8 queries + 5 mutations). Le power de GraphQL apparait a l\'etape "Query order avec joins" : 1 seule requete HTTP combine order + driver + delivery + history via 3 appels gRPC internes.',
  steps: [
    {
      title: 'Query availableDrivers',
      desc: 'Lister les livreurs AVAILABLE via GraphQL.',
      gql: {
        query: `query AvailableDrivers($limit: Int) {
  availableDrivers(limit: $limit) {
    total
    drivers { id name vehicle_type }
  }
}`,
        variables: { limit: 20 },
      },
    },
    {
      title: 'Mutation registerDriver',
      desc: 'Enregistrer un nouveau livreur via mutation.',
      gql: {
        query: `mutation RegisterDriver($input: RegisterDriverInput!) {
  registerDriver(input: $input) {
    id name phone vehicle_type status created_at
  }
}`,
        variables: { input: { name: 'Manel Bouzid', phone: '+216 50 333 222', vehicle_type: 'velo' } },
      },
      extract: { driver_id: 'data.registerDriver.id' },
    },
    {
      title: 'Mutation updateDriverLocation',
      desc: 'Mettre a jour la position GPS via GraphQL.',
      gql: {
        query: `mutation UpdateLocation($input: UpdateLocationInput!) {
  updateDriverLocation(input: $input) {
    driver_id latitude longitude speed_kmh timestamp
  }
}`,
        variables: { input: { driver_id: '{{driver_id}}', latitude: 35.825, longitude: 10.634, speed_kmh: 28, heading_deg: 45 } },
      },
    },
    {
      title: 'Query driver(id) avec current_order',
      desc: 'Recuperer le driver et joindre sa commande en cours via le resolver Driver.current_order.',
      gql: {
        query: `query GetDriver($id: ID!) {
  driver(id: $id) {
    id name vehicle_type status
    last_location { latitude longitude }
    current_order { id status customer_name }
  }
}`,
        variables: { id: '{{driver_id}}' },
      },
    },
    {
      title: 'Mutation createOrder',
      desc: 'Creer une commande via GraphQL. Declenche la chaine Kafka en backend.',
      gql: {
        query: `mutation CreateOrder($input: CreateOrderInput!) {
  createOrder(input: $input) {
    id status total_amount customer_name
    items { product_name quantity unit_price }
  }
}`,
        variables: {
          input: {
            customer_id: 'demo-gql', customer_name: 'Demo GraphQL',
            delivery_address: 'sahloul 5 rue de l\'honneur',
            items: [{ product_name: 'Pizza Neptune', quantity: 2, unit_price: 12.5 }],
          },
        },
      },
      extract: { order_id: 'data.createOrder.id' },
    },
    {
      title: 'Attente 2.5s - propagation Kafka',
      desc: 'Le temps que order.placed -> driver.assigned -> delivery.assigned propage.',
      wait: 2500,
    },
    {
      title: 'Query order(id) AVEC JOINS - power move',
      desc: 'UNE SEULE requete HTTP qui recupere : order (gRPC order-service) + driver complet (gRPC driver-service) + delivery + history (gRPC tracking-service). En REST, il faudrait 3 round-trips.',
      gql: {
        query: `query GetOrderWithJoins($id: ID!) {
  order(id: $id) {
    id status total_amount customer_name delivery_address
    items { product_name quantity unit_price }
    driver { id name vehicle_type status last_location { latitude longitude } }
    delivery {
      id status driver_name
      history { event_type created_at }
    }
  }
}`,
        variables: { id: '{{order_id}}' },
      },
      extract: { delivery_id: 'data.order.delivery.id' },
      kafkaNote: '3 services interroges en 1 requete : c\'est ca le power de GraphQL.',
    },
    {
      title: 'Query orders (filter customer_id)',
      desc: 'Lister les commandes du client demo-gql.',
      gql: {
        query: `query ListOrders($customer_id: String, $limit: Int) {
  orders(customer_id: $customer_id, limit: $limit) {
    total
    orders { id status customer_name total_amount created_at }
  }
}`,
        variables: { customer_id: 'demo-gql', limit: 10 },
      },
    },
    {
      title: 'Query deliveryByOrder(order_id)',
      desc: 'Trouver une delivery a partir d\'un order_id (sans connaitre le delivery_id directement).',
      gql: {
        query: `query DeliveryByOrder($order_id: ID!) {
  deliveryByOrder(order_id: $order_id) {
    id status driver_name delivery_address
  }
}`,
        variables: { order_id: '{{order_id}}' },
      },
    },
    {
      title: 'Query delivery(id) avec order+driver+history',
      desc: 'Detail d\'une delivery + son order + son driver + tout son historique.',
      gql: {
        query: `query GetDelivery($id: ID!) {
  delivery(id: $id) {
    id status driver_name delivery_address
    order { id status customer_name total_amount }
    driver { id name vehicle_type status }
    history { event_type created_at }
  }
}`,
        variables: { id: '{{delivery_id}}' },
      },
    },
    {
      title: 'Query deliveries (filter ASSIGNED)',
      desc: 'Lister les livraisons en cours.',
      gql: {
        query: `query InProgressDeliveries {
  deliveries(status: "ASSIGNED", limit: 20) {
    total
    deliveries {
      id status delivery_address
      driver { name }
      order { customer_name total_amount }
    }
  }
}`,
      },
    },
    {
      title: 'Mutation advanceDelivery (PICKED_UP)',
      desc: 'Avancer la delivery. Resolver Delivery.order ramene aussi la commande mise a jour.',
      gql: {
        query: `mutation AdvanceDelivery($id: ID!, $s: String!) {
  advanceDelivery(delivery_id: $id, new_status: $s) {
    id status driver_name
    order { id status }
  }
}`,
        variables: { id: '{{delivery_id}}', s: 'PICKED_UP' },
      },
    },
    { title: 'Attente 1s - Kafka', wait: 1000 },
    {
      title: 'Mutation advanceDelivery (IN_TRANSIT)',
      gql: {
        query: `mutation AdvanceDelivery($id: ID!, $s: String!) {
  advanceDelivery(delivery_id: $id, new_status: $s) {
    id status order { id status }
  }
}`,
        variables: { id: '{{delivery_id}}', s: 'IN_TRANSIT' },
      },
    },
    { title: 'Attente 1s - Kafka', wait: 1000 },
    {
      title: 'Mutation advanceDelivery (DELIVERED)',
      desc: 'Terminer. Publie delivery.delivered -> order DELIVERED + driver libere automatiquement.',
      gql: {
        query: `mutation AdvanceDelivery($id: ID!, $s: String!) {
  advanceDelivery(delivery_id: $id, new_status: $s) {
    id status order { id status }
  }
}`,
        variables: { id: '{{delivery_id}}', s: 'DELIVERED' },
      },
    },
    { title: 'Attente 1.5s - propagation arriere', wait: 1500 },
    {
      title: 'Query order(id) final avec joins',
      desc: 'Verification finale : l\'order doit etre DELIVERED, et le driver doit etre re-AVAILABLE.',
      gql: {
        query: `query GetOrderFinal($id: ID!) {
  order(id: $id) {
    id status total_amount
    driver { id name status }
    delivery { id status history { event_type } }
  }
}`,
        variables: { id: '{{order_id}}' },
      },
      kafkaNote: 'driver.status doit etre AVAILABLE (libere par delivery.delivered).',
    },
    {
      title: 'Mutation createOrder (2eme pour demo cancel)',
      desc: 'Creer une commande pour tester cancelOrder.',
      gql: {
        query: `mutation CreateOrder($input: CreateOrderInput!) {
  createOrder(input: $input) { id status }
}`,
        variables: {
          input: {
            customer_id: 'demo-gql', customer_name: 'Demo GraphQL',
            delivery_address: 'avenue de la Republique, Sousse',
            items: [{ product_name: 'Burger', quantity: 1, unit_price: 9 }],
          },
        },
      },
      extract: { order2_id: 'data.createOrder.id' },
    },
    {
      title: 'Mutation cancelOrder',
      desc: 'Annuler la commande via GraphQL.',
      gql: {
        query: `mutation CancelOrder($id: ID!, $reason: String) {
  cancelOrder(id: $id, reason: $reason) { id status updated_at }
}`,
        variables: { id: '{{order2_id}}', reason: 'Demo GraphQL cancel' },
      },
    },
    {
      title: 'Query order(id) - verif CANCELLED',
      gql: {
        query: `query VerifyCancel($id: ID!) {
  order(id: $id) {
    id status delivery { id status }
  }
}`,
        variables: { id: '{{order2_id}}' },
      },
    },
  ],
};

// ----------------- Runner -----------------

const modal = document.getElementById('demo-modal');
const titleEl = document.getElementById('demo-title');
const introEl = document.getElementById('demo-intro');
const stepsEl = document.getElementById('demo-steps');
const runBtn = document.getElementById('demo-run');
const resetBtn = document.getElementById('demo-reset');
const closeBtn = document.getElementById('demo-close');

let currentFlow = null;
let running = false;

document.getElementById('open-rest-demo').addEventListener('click', () => openFlow(REST_FLOW));
document.getElementById('open-graphql-demo').addEventListener('click', () => openFlow(GRAPHQL_FLOW));
closeBtn.addEventListener('click', () => modal.classList.add('hidden'));
resetBtn.addEventListener('click', () => { if (!running) renderFlow(currentFlow); });
runBtn.addEventListener('click', () => runFlow());

function openFlow(flow) {
  currentFlow = flow;
  renderFlow(flow);
  modal.classList.remove('hidden');
}

function renderFlow(flow) {
  titleEl.textContent = flow.title;
  introEl.textContent = flow.intro || '';
  stepsEl.innerHTML = '';
  flow.steps.forEach((step, i) => {
    const li = document.createElement('li');
    li.className = 'step collapsed';
    li.dataset.idx = String(i);
    li.innerHTML = `
      <div class="step-head">
        <span class="step-title">${step.title}</span>
        <span class="step-status">idle</span>
      </div>
      ${step.desc ? `<p class="step-desc">${step.desc}</p>` : ''}
      ${step.kafkaNote ? `<span class="kafka-note">Kafka : ${step.kafkaNote}</span>` : ''}
      <div class="step-detail"></div>
    `;
    li.querySelector('.step-head').addEventListener('click', () => li.classList.toggle('collapsed'));
    stepsEl.appendChild(li);
  });
  runBtn.disabled = false;
}

async function runFlow() {
  if (!currentFlow || running) return;
  running = true;
  runBtn.disabled = true;
  resetBtn.disabled = true;
  const ctx = {};

  for (let i = 0; i < currentFlow.steps.length; i++) {
    const step = currentFlow.steps[i];
    const li = stepsEl.children[i];
    li.classList.remove('collapsed');
    li.className = 'step running';
    li.querySelector('.step-status').textContent = 'running';
    const detail = li.querySelector('.step-detail');

    try {
      if (step.wait) {
        await wait(step.wait);
        li.className = 'step ok';
        li.querySelector('.step-status').textContent = 'OK';
        detail.innerHTML = `<div><span class="code-box-label">Action</span><div class="code-box req">await ${step.wait}ms</div></div>`;
      } else if (step.rest) {
        const rest = resolveTemplate(step.rest, ctx);
        const reqBox = `${rest.method} ${rest.path}${rest.body ? '\n\n' + JSON.stringify(rest.body, null, 2) : ''}`;
        detail.innerHTML = `
          <div><span class="code-box-label">Requete REST</span><div class="code-box req">${escapeHtml(reqBox)}</div></div>
          <div><span class="code-box-label">Reponse</span><div class="code-box res">en attente...</div></div>
        `;
        const resp = await callRest(rest);
        detail.querySelector('.res').textContent = JSON.stringify(resp, null, 2);
        applyExtract(step, resp, ctx);
        li.className = 'step ok';
        li.querySelector('.step-status').textContent = 'OK';
      } else if (step.gql) {
        const gql = resolveTemplate(step.gql, ctx);
        const reqBox = `POST /graphql\n\n${gql.query}\n\nvariables:\n${JSON.stringify(gql.variables || {}, null, 2)}`;
        detail.innerHTML = `
          <div><span class="code-box-label">Requete GraphQL</span><div class="code-box req">${escapeHtml(reqBox)}</div></div>
          <div><span class="code-box-label">Reponse</span><div class="code-box res">en attente...</div></div>
        `;
        const resp = await callGql(gql);
        detail.querySelector('.res').textContent = JSON.stringify(resp, null, 2);
        if (resp.errors) {
          throw new Error(resp.errors.map(e => e.message).join('; '));
        }
        applyExtract(step, resp, ctx);
        li.className = 'step ok';
        li.querySelector('.step-status').textContent = 'OK';
      }
    } catch (err) {
      li.className = 'step fail';
      li.querySelector('.step-status').textContent = 'FAIL';
      const errBox = detail.querySelector('.res') || (() => {
        const d = document.createElement('div');
        d.innerHTML = '<span class="code-box-label">Erreur</span><div class="code-box err"></div>';
        detail.appendChild(d);
        return d.querySelector('.err');
      })();
      errBox.className = 'code-box err';
      errBox.textContent = err.message || String(err);
      break;
    }
    await wait(250);
  }

  running = false;
  runBtn.disabled = false;
  resetBtn.disabled = false;
}

function applyExtract(step, resp, ctx) {
  if (step.extract) {
    for (const [key, path] of Object.entries(step.extract)) {
      const v = getByPath(resp, path);
      if (v != null) ctx[key] = v;
    }
  }
  if (step.extractFn) {
    Object.assign(ctx, step.extractFn(resp, ctx) || {});
  }
}

async function callRest(rest) {
  const opts = {
    method: rest.method,
    headers: { 'Content-Type': 'application/json' },
  };
  if (rest.body) opts.body = JSON.stringify(rest.body);
  const res = await fetch(GW + rest.path, opts);
  const text = await res.text();
  const parsed = text ? JSON.parse(text) : null;
  if (!res.ok) {
    const msg = parsed && parsed.error ? parsed.error : `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return parsed;
}

async function callGql(gql) {
  const res = await fetch(GW + '/graphql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query: gql.query, variables: gql.variables || {} }),
  });
  return res.json();
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}
