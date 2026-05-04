// Version GraphQL des 3 interfaces (Client / Admin / Livreur).
// Toutes les operations passent par POST /graphql.
// SEULE EXCEPTION : le streaming GPS du livreur cote Client utilise toujours
// le SSE REST /api/drivers/:id/stream parce que les Subscriptions GraphQL
// n'ont pas ete configurees (WebSocket Apollo pas active).
// Les changements de statut (timeline) utilisent du POLLING GraphQL toutes
// les 1.5s (au lieu du SSE /watch en REST).

const GW = 'http://localhost:3000';
const GQL_URL = GW + '/graphql';
const POLL_MS = 1500;
const PICKUP = { lat: 35.823, lng: 10.629, label: 'Restaurant Pizza Neptune' };
const DELIVERY = { lat: 35.828, lng: 10.640, label: 'Sahloul, 5 rue de l\'honneur' };
const DEFAULT_START = { lat: 35.825, lng: 10.634 };

function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
const wait = ms => new Promise(r => setTimeout(r, ms));
const STATUS_ORDER = ['PENDING', 'ASSIGNED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERED'];

async function gql(query, variables) {
  const r = await fetch(GQL_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ query, variables: variables || {} }),
  });
  const json = await r.json();
  if (json.errors) throw new Error(json.errors.map(e => e.message).join('; '));
  return json.data;
}

// ============ Gateway status ============
async function checkHealth() {
  const el = document.getElementById('gateway-status');
  const label = el.querySelector('.pill-label');
  try {
    // Ping GraphQL : __typename est gratuit et indique si le serveur tourne
    await gql(`{ __typename }`);
    el.className = 'pill pill-on';
    label.textContent = 'GraphQL OK';
  } catch (e) {
    el.className = 'pill pill-err';
    label.textContent = 'GraphQL off';
  }
}

// ============ Tabs ============
function setupTabs() {
  document.querySelectorAll('.tab').forEach(t => {
    t.addEventListener('click', () => switchMainTab(t.dataset.tab));
  });
  document.querySelectorAll('.subtab').forEach(t => {
    t.addEventListener('click', () => {
      document.querySelectorAll('.subtab').forEach(x => x.classList.toggle('active', x === t));
      document.querySelectorAll('.subpane').forEach(p => p.classList.toggle('active', p.dataset.sub === t.dataset.sub));
    });
  });
}
function switchMainTab(target) {
  document.querySelectorAll('.tab').forEach(x => x.classList.toggle('active', x.dataset.tab === target));
  document.querySelectorAll('.pane').forEach(p => p.classList.toggle('active', p.dataset.tab === target));
  setTimeout(() => {
    if (clientMap) clientMap.invalidateSize();
    if (livreurMap) livreurMap.invalidateSize();
  }, 50);
}

// ============================================================
// ============ INTERFACE CLIENT (GraphQL + polling) ============
// ============================================================
function getClientCustomerId() {
  let id = localStorage.getItem('soa.gqlClientCustomerId');
  if (!id) {
    id = 'gqlclient-' + Math.random().toString(36).slice(2, 10);
    localStorage.setItem('soa.gqlClientCustomerId', id);
  }
  return id;
}

const clientState = {
  customerId: getClientCustomerId(),
  orderId: null,
  deliveryId: null,
  driverId: null,
  pollTimer: null,    // setInterval pour le polling delivery (statut)
  streamEs: null,     // EventSource pour le streaming GPS (REST conserve)
  lastStatus: null,
};
let clientMap = null;
let clientMarker = null;
let clientTrail = null;
const clientTrailPoints = [];

function ensureClientMap() {
  if (clientMap) return;
  clientMap = L.map('cl-map').setView([(PICKUP.lat + DELIVERY.lat) / 2, (PICKUP.lng + DELIVERY.lng) / 2], 14);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: 'OSM', maxZoom: 19 }).addTo(clientMap);
  L.circleMarker([PICKUP.lat, PICKUP.lng], { color: '#f59e0b', radius: 8, fillOpacity: 0.7 })
    .bindTooltip(PICKUP.label, { permanent: true, direction: 'top', offset: [0, -6] }).addTo(clientMap);
  L.circleMarker([DELIVERY.lat, DELIVERY.lng], { color: '#10b981', radius: 8, fillOpacity: 0.7 })
    .bindTooltip(DELIVERY.label, { permanent: true, direction: 'top', offset: [0, -6] }).addTo(clientMap);
}

function renderClientTimeline(status) {
  const tl = document.getElementById('cl-timeline');
  tl.classList.toggle('cancelled', status === 'CANCELLED');
  const idx = STATUS_ORDER.indexOf(status);
  tl.querySelectorAll('.t-step').forEach((step, i) => {
    step.classList.remove('done', 'current');
    if (status === 'CANCELLED') return;
    if (idx < 0) return;
    if (i < idx) step.classList.add('done');
    else if (i === idx) step.classList.add('current');
  });
  tl.dataset.progress = status === 'CANCELLED' ? '0' : Math.max(0, idx);
  document.getElementById('cl-cancel-banner').classList.toggle('hidden', status !== 'CANCELLED');
  const cancelBtn = document.getElementById('cl-cancel-order');
  cancelBtn.classList.toggle('hidden', status === 'DELIVERED' || status === 'CANCELLED');
}

async function submitClientOrder() {
  let items;
  try {
    items = JSON.parse(document.getElementById('cl-items').value);
  } catch (e) { alert('JSON invalide : ' + e.message); return; }
  try {
    const data = await gql(`
      mutation Create($input: CreateOrderInput!) {
        createOrder(input: $input) {
          id status total_amount customer_id customer_name created_at
          items { product_name quantity unit_price }
        }
      }`, {
      input: {
        customer_id: clientState.customerId,
        customer_name: document.getElementById('cl-name').value,
        delivery_address: document.getElementById('cl-address').value,
        items,
      },
    });
    await refreshClientOrdersList();
    selectClientOrder(data.createOrder);
  } catch (e) {
    alert('Erreur creation : ' + e.message);
  }
}

async function refreshClientOrdersList() {
  try {
    const data = await gql(`
      query MyOrders($cid: String!) {
        orders(customer_id: $cid, limit: 50) {
          orders { id status total_amount created_at items { quantity unit_price } }
        }
      }`, { cid: clientState.customerId });
    const orders = (data.orders.orders || []).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
    const ul = document.getElementById('cl-orders-list');
    ul.innerHTML = '';
    for (const o of orders) {
      const li = document.createElement('li');
      li.className = 'order-item' + (o.id === clientState.orderId ? ' active' : '');
      li.dataset.orderId = o.id;
      const total = (o.items || []).reduce((s, i) => s + i.quantity * i.unit_price, 0);
      const when = new Date(o.created_at).toLocaleTimeString();
      li.innerHTML = `
        <div class="order-item-head">
          <span class="order-item-id">${o.id.slice(0, 8)}...</span>
          <span class="status-pill ${o.status}">${o.status}</span>
        </div>
        <div class="order-item-meta">
          <span>${when}</span>
          <span class="total">${total.toFixed(2)} TND</span>
        </div>`;
      li.addEventListener('click', () => selectClientOrder(o));
      ul.appendChild(li);
    }
  } catch (e) { console.error('refreshClientOrdersList:', e); }
}

async function selectClientOrder(order) {
  stopClientPolling();
  closeClientStream();
  clientState.orderId = order.id;
  clientState.deliveryId = null;
  clientState.driverId = null;
  clientState.lastStatus = null;
  document.getElementById('cl-empty').classList.add('hidden');
  document.getElementById('cl-order-card').classList.remove('hidden');
  document.getElementById('cl-map-card').classList.remove('hidden');
  const cancelBtn = document.getElementById('cl-cancel-order');
  cancelBtn.disabled = false;
  cancelBtn.textContent = 'Annuler ma commande';
  if (clientMarker) { clientMap.removeLayer(clientMarker); clientMarker = null; }
  if (clientTrail) { clientMap.removeLayer(clientTrail); clientTrail = null; clientTrailPoints.length = 0; }
  document.querySelectorAll('.order-item').forEach(el => el.classList.toggle('active', el.dataset.orderId === order.id));

  // Charge le detail complet via GraphQL (avec joins driver+delivery)
  try {
    const data = await gql(`
      query OrderDetail($id: ID!) {
        order(id: $id) {
          id status total_amount assigned_driver_id
          items { product_name quantity unit_price }
          driver { id name }
          delivery { id status driver_name }
        }
      }`, { id: order.id });
    const o = data.order;
    document.getElementById('cl-order-id-sub').textContent = o.id;
    document.getElementById('cl-order-total').textContent = (o.total_amount || 0).toFixed(2) + ' TND';
    document.getElementById('cl-order-driver').textContent = o.driver ? o.driver.name : 'en attente d\'attribution...';
    document.getElementById('cl-order-items').innerHTML = (o.items || [])
      .map(i => `<div>• ${escapeHtml(i.product_name)} x${i.quantity} (${i.unit_price} TND)</div>`).join('');
    const pill = document.getElementById('cl-order-status-pill');
    pill.className = 'status-pill ' + o.status;
    pill.textContent = o.status;
    renderClientTimeline(o.status);

    if (o.delivery) {
      clientState.deliveryId = o.delivery.id;
      startClientPolling();
    } else {
      // pas encore de delivery, on retentera dans X ms via le polling
      startClientPolling();
    }
    if (o.driver && o.driver.id) attachClientDriver(o.driver.id, o.driver.name);
  } catch (e) {
    console.error('selectClientOrder:', e);
  }
}

// Polling GraphQL : remplace le SSE /watch
// Toutes les POLL_MS, on query order(id) -> status + delivery + driver
function startClientPolling() {
  stopClientPolling();
  const tick = async () => {
    if (!clientState.orderId) return;
    try {
      const data = await gql(`
        query Tick($id: ID!) {
          order(id: $id) {
            status assigned_driver_id
            driver { id name }
            delivery { id status driver_name }
          }
        }`, { id: clientState.orderId });
      const o = data.order;
      if (!o) return;
      const status = o.status;
      if (status !== clientState.lastStatus) {
        clientState.lastStatus = status;
        renderClientTimeline(status);
        const pill = document.getElementById('cl-order-status-pill');
        pill.className = 'status-pill ' + status;
        pill.textContent = status;
        document.getElementById('cl-order-driver').textContent = o.driver ? o.driver.name : 'en attente d\'attribution...';
        refreshClientOrdersList();
      }
      if (o.delivery && !clientState.deliveryId) clientState.deliveryId = o.delivery.id;
      if (o.driver && o.driver.id && o.driver.id !== clientState.driverId) {
        attachClientDriver(o.driver.id, o.driver.name);
      }
      // Stoppe le polling sur etat final
      if (status === 'DELIVERED' || status === 'CANCELLED') {
        stopClientPolling();
      }
    } catch (e) { console.error('poll error:', e); }
  };
  tick();
  clientState.pollTimer = setInterval(tick, POLL_MS);
}

function stopClientPolling() {
  if (clientState.pollTimer) {
    clearInterval(clientState.pollTimer);
    clientState.pollTimer = null;
  }
}

// SSE GPS conserve en REST (pas de Subscription GraphQL configuree)
function attachClientDriver(driverId, driverName) {
  clientState.driverId = driverId;
  document.getElementById('cl-order-driver').textContent = driverName || driverId.slice(0, 8);
  ensureClientMap();
  document.getElementById('cl-map-hint').textContent = `Suivi de ${driverName || 'livreur'} (SSE REST conserve)`;
  if (clientState.streamEs) clientState.streamEs.close();
  clientState.streamEs = new EventSource(`${GW}/api/drivers/${driverId}/stream`);
  clientState.streamEs.onmessage = (ev) => {
    const loc = JSON.parse(ev.data);
    moveClientMarker(loc.latitude, loc.longitude);
  };
}
function closeClientStream() {
  if (clientState.streamEs) { clientState.streamEs.close(); clientState.streamEs = null; }
}

function moveClientMarker(lat, lng) {
  if (!clientMap) ensureClientMap();
  const pt = [lat, lng];
  if (!clientMarker) clientMarker = L.marker(pt).addTo(clientMap).bindTooltip('Livreur', { permanent: true, direction: 'top', offset: [-15, -10] });
  else clientMarker.setLatLng(pt);
  clientTrailPoints.push(pt);
  if (clientTrailPoints.length > 200) clientTrailPoints.shift();
  if (!clientTrail) clientTrail = L.polyline(clientTrailPoints, { color: '#3b82f6', weight: 3 }).addTo(clientMap);
  else clientTrail.setLatLngs(clientTrailPoints);
  clientMap.panTo(pt);
}

async function cancelClientOrder() {
  if (!clientState.orderId) return;
  if (!confirm('Annuler cette commande ?')) return;
  const btn = document.getElementById('cl-cancel-order');
  btn.disabled = true;
  btn.textContent = 'Annulation...';
  try {
    await gql(`mutation Cancel($id: ID!) { cancelOrder(id: $id, reason: "Annule par le client GraphQL") { id status } }`, { id: clientState.orderId });
    renderClientTimeline('CANCELLED');
    const pill = document.getElementById('cl-order-status-pill');
    pill.className = 'status-pill CANCELLED'; pill.textContent = 'CANCELLED';
    refreshClientOrdersList();
  } catch (e) {
    alert('Echec : ' + e.message);
    btn.disabled = false; btn.textContent = 'Annuler ma commande';
  }
}

function resetClientSession() {
  if (!confirm('Reinitialiser la session client GraphQL ?')) return;
  stopClientPolling(); closeClientStream();
  localStorage.removeItem('soa.gqlClientCustomerId');
  clientState.customerId = getClientCustomerId();
  clientState.orderId = clientState.deliveryId = clientState.driverId = null;
  document.getElementById('cl-order-card').classList.add('hidden');
  document.getElementById('cl-map-card').classList.add('hidden');
  document.getElementById('cl-empty').classList.remove('hidden');
  document.getElementById('cl-session-info').textContent = 'Session : ' + clientState.customerId;
  if (clientMarker) { clientMap.removeLayer(clientMarker); clientMarker = null; }
  if (clientTrail) { clientMap.removeLayer(clientTrail); clientTrail = null; clientTrailPoints.length = 0; }
  refreshClientOrdersList();
}

function setupClient() {
  document.getElementById('cl-session-info').textContent = 'Session : ' + clientState.customerId;
  document.getElementById('cl-submit').addEventListener('click', submitClientOrder);
  document.getElementById('cl-clear-session').addEventListener('click', resetClientSession);
  document.getElementById('cl-cancel-order').addEventListener('click', cancelClientOrder);
  refreshClientOrdersList();
  setInterval(refreshClientOrdersList, 3000);
}

// ============================================================
// ============ INTERFACE ADMIN (GraphQL pure) ============
// ============================================================
async function fetchAllDrivers() {
  // GraphQL n'expose pas "list all drivers". On reconstitue :
  // - availableDrivers pour les AVAILABLE
  // - deliveries non-finales pour deduire les BUSY (driver_id), puis driver(id) pour chaque
  const data = await gql(`
    query AllDrivers {
      availableDrivers(limit: 100) { drivers { id name phone vehicle_type status current_order_id } }
      deliveries(limit: 200) { deliveries { id order_id driver_id driver_name status delivery_address customer_name } }
    }`);
  const seen = new Map();
  for (const d of data.availableDrivers.drivers || []) seen.set(d.id, d);
  const busyIds = new Set();
  const orderByDriver = new Map();
  for (const dl of data.deliveries.deliveries || []) {
    if (dl.driver_id && !['DELIVERED', 'CANCELLED'].includes(dl.status)) {
      busyIds.add(dl.driver_id);
      orderByDriver.set(dl.driver_id, dl);
    }
  }
  if (busyIds.size > 0) {
    const driverFields = `id name phone vehicle_type status current_order_id`;
    const aliases = [...busyIds].map((id, i) => `d${i}: driver(id: "${id}") { ${driverFields} }`).join('\n');
    const dq = await gql(`query { ${aliases} }`);
    for (const k of Object.keys(dq)) if (dq[k]) seen.set(dq[k].id, dq[k]);
  }
  return { drivers: [...seen.values()], orderByDriver };
}

async function refreshDrivers() {
  try {
    const { drivers, orderByDriver } = await fetchAllDrivers();
    const tbody = document.getElementById('ad-drivers-body');
    tbody.innerHTML = '';
    const sorted = drivers.sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    if (sorted.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="empty-row">Aucun livreur. Utilise le formulaire ci-dessus.</td></tr>';
      return;
    }
    for (const d of sorted) {
      const tr = document.createElement('tr');
      const orderInfo = orderByDriver.get(d.id);
      const deletable = d.status !== 'BUSY';
      tr.className = d.status === 'BUSY' ? 'clickable-row' : '';
      tr.innerHTML = `
        <td>${escapeHtml(d.name)}</td>
        <td>${escapeHtml(d.phone)}</td>
        <td>${escapeHtml(d.vehicle_type)}</td>
        <td><span class="status-pill ${d.status}">${d.status}</span></td>
        <td>${orderInfo ? `<code title="${orderInfo.order_id}">${orderInfo.order_id.slice(0, 8)}...</code> <span class="status-pill ${orderInfo.status}">${orderInfo.status}</span>` : '-'}</td>
        <td><button class="row-action-btn" ${deletable ? '' : 'disabled title="BUSY"'} data-delete-id="${d.id}">Supprimer</button></td>`;
      const delBtn = tr.querySelector('button[data-delete-id]');
      if (delBtn && deletable) {
        delBtn.addEventListener('click', async (ev) => {
          ev.stopPropagation();
          if (!confirm(`Supprimer ${d.name} ?`)) return;
          try {
            await gql(`mutation Del($id: ID!) { deleteDriver(id: $id) }`, { id: d.id });
            refreshDrivers(); refreshLivreurDropdown();
          } catch (e) { alert('Echec : ' + e.message); }
        });
      }
      if (d.status === 'BUSY') {
        tr.addEventListener('click', (ev) => {
          if (ev.target.closest('button')) return;
          switchMainTab('livreur');
          loadLivreurFor(d.id);
        });
      }
      tbody.appendChild(tr);
    }
  } catch (e) { console.error('refreshDrivers:', e); }
}

async function refreshOrders() {
  try {
    const status = document.getElementById('ad-filter-status').value;
    const customer = document.getElementById('ad-filter-customer').value.trim();
    const search = document.getElementById('ad-filter-search').value.trim().toLowerCase();
    const data = await gql(`
      query AllOrders($cid: String, $st: String) {
        orders(customer_id: $cid, status: $st, limit: 100) {
          orders {
            id customer_id customer_name total_amount status assigned_driver_id created_at
            items { product_name quantity unit_price }
          }
        }
      }`, { cid: customer || null, st: status || null });
    let orders = data.orders.orders || [];
    if (search) {
      orders = orders.filter(o =>
        (o.id || '').toLowerCase().includes(search) ||
        (o.customer_name || '').toLowerCase().includes(search) ||
        (o.customer_id || '').toLowerCase().includes(search));
    }
    const tbody = document.getElementById('ad-orders-body');
    tbody.innerHTML = '';
    if (orders.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" class="empty-row">Aucune commande.</td></tr>';
      return;
    }
    for (const o of orders) {
      const tr = document.createElement('tr');
      const isFinal = ['DELIVERED', 'CANCELLED'].includes(o.status);
      const itemsCount = (o.items || []).length;
      const actionBtn = isFinal
        ? `<button class="row-action-btn" data-delete-order="${o.id}">Supprimer</button>`
        : `<button class="row-action-btn" data-cancel-id="${o.id}">Annuler</button>`;
      tr.innerHTML = `
        <td><code title="${o.id}">${o.id.slice(0, 8)}...</code></td>
        <td>${escapeHtml(o.customer_name)}<br><small style="color:#9ca3af">${escapeHtml(o.customer_id)}</small></td>
        <td>${itemsCount} article(s)</td>
        <td>${(o.total_amount || 0).toFixed(2)} TND</td>
        <td><span class="status-pill ${o.status}">${o.status}</span></td>
        <td>${o.assigned_driver_id ? `<code>${o.assigned_driver_id.slice(0, 8)}...</code>` : '-'}</td>
        <td>${new Date(o.created_at).toLocaleTimeString()}</td>
        <td>${actionBtn}</td>`;
      const cancelBtn = tr.querySelector('button[data-cancel-id]');
      if (cancelBtn) {
        cancelBtn.addEventListener('click', async (ev) => {
          ev.stopPropagation();
          if (!confirm('Annuler ' + o.id.slice(0, 8) + ' ?')) return;
          try {
            await gql(`mutation Cancel($id: ID!) { cancelOrder(id: $id, reason: "Admin GraphQL") { id } }`, { id: o.id });
            refreshOrders(); refreshDrivers();
          } catch (e) { alert('Echec : ' + e.message); }
        });
      }
      const delBtn = tr.querySelector('button[data-delete-order]');
      if (delBtn) {
        delBtn.addEventListener('click', async (ev) => {
          ev.stopPropagation();
          if (!confirm('Supprimer ' + o.id.slice(0, 8) + ' ?')) return;
          try {
            await gql(`mutation Del($id: ID!) { deleteOrder(id: $id) }`, { id: o.id });
            refreshOrders();
          } catch (e) { alert('Echec : ' + e.message); }
        });
      }
      tbody.appendChild(tr);
    }
  } catch (e) { console.error('refreshOrders:', e); }
}

async function addAdminDriver() {
  const status = document.getElementById('ad-add-status');
  status.className = 'status running'; status.textContent = 'envoi...';
  try {
    const data = await gql(`
      mutation Reg($input: RegisterDriverInput!) {
        registerDriver(input: $input) { id name }
      }`, {
      input: {
        name: document.getElementById('ad-name').value,
        phone: document.getElementById('ad-phone').value,
        vehicle_type: document.getElementById('ad-vehicle').value,
      },
    });
    const lat = DEFAULT_START.lat + (Math.random() - 0.5) * 0.01;
    const lng = DEFAULT_START.lng + (Math.random() - 0.5) * 0.01;
    await gql(`
      mutation Loc($input: UpdateLocationInput!) {
        updateDriverLocation(input: $input) { driver_id }
      }`, { input: { driver_id: data.registerDriver.id, latitude: lat, longitude: lng, speed_kmh: 0, heading_deg: 0 } });
    status.className = 'status ok'; status.textContent = 'OK : ' + data.registerDriver.name;
    refreshDrivers(); refreshLivreurDropdown();
  } catch (e) {
    status.className = 'status fail'; status.textContent = 'KO : ' + e.message;
  }
}

function setupAdmin() {
  document.getElementById('ad-add').addEventListener('click', addAdminDriver);
  document.getElementById('ad-refresh-drivers').addEventListener('click', refreshDrivers);
  document.getElementById('ad-refresh-orders').addEventListener('click', refreshOrders);
  ['ad-filter-status', 'ad-filter-customer', 'ad-filter-search'].forEach(id =>
    document.getElementById(id).addEventListener('input', refreshOrders));
  setInterval(refreshDrivers, 2000);
  setInterval(refreshOrders, 2000);
  refreshDrivers(); refreshOrders();
}

// ============================================================
// ============ INTERFACE LIVREUR (GraphQL) ============
// ============================================================
const livreurState = { driverId: null, driver: null, delivery: null, animating: false };
let livreurMap = null, livreurMarker = null, livreurTrail = null;
const livreurTrailPoints = [];

function ensureLivreurMap() {
  if (livreurMap) return;
  livreurMap = L.map('lv-map').setView([(PICKUP.lat + DELIVERY.lat) / 2, (PICKUP.lng + DELIVERY.lng) / 2], 14);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: 'OSM', maxZoom: 19 }).addTo(livreurMap);
  L.circleMarker([PICKUP.lat, PICKUP.lng], { color: '#f59e0b', radius: 8, fillOpacity: 0.7 })
    .bindTooltip('Pickup', { permanent: true, direction: 'top', offset: [0, -6] }).addTo(livreurMap);
  L.circleMarker([DELIVERY.lat, DELIVERY.lng], { color: '#10b981', radius: 8, fillOpacity: 0.7 })
    .bindTooltip('Livraison', { permanent: true, direction: 'top', offset: [0, -6] }).addTo(livreurMap);
}

async function refreshLivreurDropdown() {
  try {
    const { drivers } = await fetchAllDrivers();
    const sel = document.getElementById('lv-select');
    const current = sel.value;
    sel.innerHTML = '<option value="">-- Choisir --</option>';
    drivers.sort((a, b) => (a.name || '').localeCompare(b.name || ''))
      .forEach(d => {
        const opt = document.createElement('option');
        opt.value = d.id;
        opt.textContent = `${d.name} [${d.status}] - ${d.vehicle_type}`;
        sel.appendChild(opt);
      });
    if (current && [...sel.options].some(o => o.value === current)) sel.value = current;
  } catch (e) { console.error('refreshLivreurDropdown:', e); }
}

async function loadLivreurFor(driverId) {
  livreurState.driverId = driverId;
  document.getElementById('lv-select').value = driverId;
  await renderLivreurPanel();
}

async function renderLivreurPanel() {
  const id = livreurState.driverId;
  const noOrder = document.getElementById('lv-no-order-card');
  const current = document.getElementById('lv-current-card');
  if (!id) { noOrder.classList.add('hidden'); current.classList.add('hidden'); return; }
  try {
    // 1 seule query GraphQL : driver + son current_order + delivery liee
    const data = await gql(`
      query DriverFull($id: ID!) {
        driver(id: $id) {
          id name vehicle_type status current_order_id
          last_location { latitude longitude }
          current_order { id customer_name delivery_address }
        }
      }`, { id });
    const d = data.driver;
    livreurState.driver = d;
    if (d.status !== 'BUSY' || !d.current_order_id || !d.current_order) {
      current.classList.add('hidden'); noOrder.classList.remove('hidden');
      livreurState.delivery = null;
      return;
    }
    // Recupere la delivery via deliveryByOrder (1 seule query)
    const dlData = await gql(`
      query Dl($oid: ID!) {
        deliveryByOrder(order_id: $oid) {
          id status driver_name customer_name delivery_address
        }
      }`, { oid: d.current_order_id });
    const delivery = dlData.deliveryByOrder;
    if (!delivery || ['DELIVERED', 'CANCELLED'].includes(delivery.status)) {
      current.classList.add('hidden'); noOrder.classList.remove('hidden');
      livreurState.delivery = null;
      return;
    }
    livreurState.delivery = delivery;
    document.getElementById('lv-driver-name').textContent = d.name;
    document.getElementById('lv-driver-vehicle').textContent = d.vehicle_type;
    document.getElementById('lv-driver-status').innerHTML = `<span class="status-pill ${d.status}">${d.status}</span>`;
    document.getElementById('lv-order-id').textContent = d.current_order_id;
    document.getElementById('lv-order-customer').textContent = d.current_order.customer_name || '-';
    document.getElementById('lv-order-address').textContent = d.current_order.delivery_address || '-';
    document.getElementById('lv-delivery-status').innerHTML = `<span class="status-pill ${delivery.status}">${delivery.status}</span>`;
    noOrder.classList.add('hidden'); current.classList.remove('hidden');
    ensureLivreurMap();
    if (d.last_location) updateLivreurMarker(d.last_location.latitude, d.last_location.longitude);
    updateLivreurButtons(delivery.status);
  } catch (e) { alert('Erreur : ' + e.message); }
}

function updateLivreurButtons(deliveryStatus) {
  const moveEnabled = !['DELIVERED', 'CANCELLED'].includes(deliveryStatus);
  document.getElementById('lv-go-pickup').disabled = !moveEnabled || livreurState.animating;
  document.getElementById('lv-go-deliver').disabled = !moveEnabled || livreurState.animating;
  document.getElementById('lv-pickup').disabled = deliveryStatus !== 'ASSIGNED';
  document.getElementById('lv-transit').disabled = deliveryStatus !== 'PICKED_UP';
  document.getElementById('lv-deliver').disabled = deliveryStatus !== 'IN_TRANSIT';
}

function updateLivreurMarker(lat, lng) {
  if (!livreurMap) ensureLivreurMap();
  const pt = [lat, lng];
  if (!livreurMarker) livreurMarker = L.marker(pt).addTo(livreurMap).bindTooltip('Moi', { permanent: true, direction: 'top', offset: [-15, -10] });
  else livreurMarker.setLatLng(pt);
  livreurTrailPoints.push(pt);
  if (livreurTrailPoints.length > 200) livreurTrailPoints.shift();
  if (!livreurTrail) livreurTrail = L.polyline(livreurTrailPoints, { color: '#f59e0b', weight: 3 }).addTo(livreurMap);
  else livreurTrail.setLatLngs(livreurTrailPoints);
  livreurMap.panTo(pt);
}

async function animateMove(target) {
  if (livreurState.animating) return;
  livreurState.animating = true;
  document.getElementById('lv-go-pickup').disabled = true;
  document.getElementById('lv-go-deliver').disabled = true;

  const id = livreurState.driverId;
  const d = livreurState.driver;
  const start = d.last_location
    ? { lat: d.last_location.latitude, lng: d.last_location.longitude }
    : { lat: DEFAULT_START.lat, lng: DEFAULT_START.lng };
  const FRAMES = 16;
  const status = document.getElementById('lv-action-status');
  status.className = 'status running'; status.textContent = 'deplacement...';

  try {
    for (let i = 1; i <= FRAMES; i++) {
      const t = i / FRAMES;
      const lat = start.lat + (target.lat - start.lat) * t;
      const lng = start.lng + (target.lng - start.lng) * t;
      await gql(`
        mutation L($i: UpdateLocationInput!) { updateDriverLocation(input: $i) { driver_id } }
      `, { i: { driver_id: id, latitude: lat, longitude: lng, speed_kmh: 25 + Math.random() * 10, heading_deg: Math.random() * 360 } });
      updateLivreurMarker(lat, lng);
      await wait(250);
    }
    livreurState.driver.last_location = { latitude: target.lat, longitude: target.lng };
    status.className = 'status ok'; status.textContent = 'arrive a ' + target.label;
  } catch (e) {
    status.className = 'status fail'; status.textContent = e.message;
  } finally {
    livreurState.animating = false;
    if (livreurState.delivery) updateLivreurButtons(livreurState.delivery.status);
  }
}

async function advanceLivreurStatus(newStatus) {
  const status = document.getElementById('lv-action-status');
  status.className = 'status running'; status.textContent = `passage a ${newStatus}...`;
  try {
    await gql(`
      mutation Adv($id: ID!, $s: String!) { advanceDelivery(delivery_id: $id, new_status: $s) { id status } }
    `, { id: livreurState.delivery.id, s: newStatus });
    status.className = 'status ok'; status.textContent = newStatus;
    await renderLivreurPanel();
    if (newStatus === 'DELIVERED') setTimeout(refreshLivreurDropdown, 1000);
  } catch (e) {
    status.className = 'status fail'; status.textContent = e.message;
  }
}

function setupLivreur() {
  document.getElementById('lv-select').addEventListener('change', (ev) => {
    if (ev.target.value) loadLivreurFor(ev.target.value);
    else renderLivreurPanel();
  });
  document.getElementById('lv-go-pickup').addEventListener('click', () => animateMove(PICKUP));
  document.getElementById('lv-go-deliver').addEventListener('click', () => animateMove(DELIVERY));
  document.getElementById('lv-pickup').addEventListener('click', () => advanceLivreurStatus('PICKED_UP'));
  document.getElementById('lv-transit').addEventListener('click', () => advanceLivreurStatus('IN_TRANSIT'));
  document.getElementById('lv-deliver').addEventListener('click', () => advanceLivreurStatus('DELIVERED'));
  refreshLivreurDropdown();
  setInterval(refreshLivreurDropdown, 5000);
}

// ============ Init ============
function init() {
  setupTabs();
  setupClient();
  setupAdmin();
  setupLivreur();
  checkHealth();
  setInterval(checkHealth, 10000);
}
init();
