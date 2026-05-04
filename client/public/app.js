// 3 interfaces metier : Client (passe une commande), Admin (drivers + commandes),
// Livreur (joue le role du livreur, deplace sa position en mode hardcode).

const GW = 'http://localhost:3000';

const PICKUP = { lat: 35.823, lng: 10.629, label: 'Restaurant Pizza Neptune' };
const DELIVERY = { lat: 35.828, lng: 10.640, label: 'Sahloul, 5 rue de l\'honneur' };
const DEFAULT_START = { lat: 35.825, lng: 10.634 };

// ============ Helpers ============
function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}
async function callGw(method, path, body) {
  const opts = { method, headers: { 'Content-Type': 'application/json' } };
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(GW + path, opts);
  const text = await r.text();
  const data = text ? JSON.parse(text) : null;
  if (!r.ok) throw new Error((data && data.error) || `HTTP ${r.status}`);
  return data;
}
const wait = ms => new Promise(r => setTimeout(r, ms));

const STATUS_ORDER = ['PENDING', 'ASSIGNED', 'PICKED_UP', 'IN_TRANSIT', 'DELIVERED'];

// ============ Gateway status (header pill) ============
async function checkHealth() {
  const el = document.getElementById('gateway-status');
  const label = el.querySelector('.pill-label');
  try {
    const r = await fetch(`${GW}/health`);
    el.className = 'pill ' + (r.ok ? 'pill-on' : 'pill-err');
    label.textContent = r.ok ? 'Gateway OK' : 'Gateway KO';
  } catch (e) {
    el.className = 'pill pill-err';
    label.textContent = 'Gateway off';
  }
}

// ============ Onglets ============
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
  // Hook : ressize les cartes Leaflet quand l'onglet devient visible
  setTimeout(() => {
    if (clientMap) clientMap.invalidateSize();
    if (livreurMap) livreurMap.invalidateSize();
  }, 50);
}

// ============================================================
// ============ INTERFACE CLIENT ============
// ============================================================
// Session client : un customer_id persiste en localStorage pour
// que la liste "Mes commandes" survive aux refresh.
function getClientCustomerId() {
  let id = localStorage.getItem('soa.clientCustomerId');
  if (!id) {
    id = 'client-' + Math.random().toString(36).slice(2, 10);
    localStorage.setItem('soa.clientCustomerId', id);
  }
  return id;
}

const clientState = {
  customerId: getClientCustomerId(),
  orderId: null,
  deliveryId: null,
  driverId: null,
  watchEs: null,
  streamEs: null,
};
let clientMap = null;
let clientMarker = null;
let clientPickupMarker = null;
let clientDeliveryMarker = null;
let clientTrail = null;
const clientTrailPoints = [];

function ensureClientMap() {
  if (clientMap) return;
  clientMap = L.map('cl-map').setView([(PICKUP.lat + DELIVERY.lat) / 2, (PICKUP.lng + DELIVERY.lng) / 2], 14);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: 'OSM', maxZoom: 19 }).addTo(clientMap);
  clientPickupMarker = L.circleMarker([PICKUP.lat, PICKUP.lng], { color: '#f59e0b', radius: 8, fillOpacity: 0.7 })
    .bindTooltip(PICKUP.label, { permanent: true, direction: 'top', offset: [0, -6] }).addTo(clientMap);
  clientDeliveryMarker = L.circleMarker([DELIVERY.lat, DELIVERY.lng], { color: '#10b981', radius: 8, fillOpacity: 0.7 })
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
  // Bouton "Annuler ma commande" visible si l'order n'est pas DELIVERED ni CANCELLED
  const cancelBtn = document.getElementById('cl-cancel-order');
  const cancellable = status !== 'DELIVERED' && status !== 'CANCELLED';
  cancelBtn.classList.toggle('hidden', !cancellable);
}

async function submitClientOrder() {
  let items;
  try {
    items = JSON.parse(document.getElementById('cl-items').value);
  } catch (e) {
    alert('JSON des articles invalide : ' + e.message);
    return;
  }
  const body = {
    customer_id: clientState.customerId,
    customer_name: document.getElementById('cl-name').value,
    delivery_address: document.getElementById('cl-address').value,
    items,
  };
  try {
    const order = await callGw('POST', '/api/orders', body);
    await refreshClientOrdersList();
    selectClientOrder(order);
  } catch (e) {
    alert('Erreur creation : ' + e.message);
  }
}

async function refreshClientOrdersList() {
  try {
    const list = await callGw('GET', `/api/orders?customer_id=${encodeURIComponent(clientState.customerId)}&limit=50`);
    const ul = document.getElementById('cl-orders-list');
    ul.innerHTML = '';
    const orders = (list.orders || []).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
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
  } catch (e) {
    console.error('refreshClientOrdersList:', e);
  }
}

function selectClientOrder(order) {
  closeClientStreams();
  clientState.orderId = order.id;
  clientState.deliveryId = null;
  clientState.driverId = null;
  // UI : masquer empty, montrer detail+map
  document.getElementById('cl-empty').classList.add('hidden');
  document.getElementById('cl-order-card').classList.remove('hidden');
  document.getElementById('cl-map-card').classList.remove('hidden');
  // Reset bouton annuler
  const cancelBtn = document.getElementById('cl-cancel-order');
  cancelBtn.disabled = false;
  cancelBtn.textContent = 'Annuler ma commande';
  // Reset map markers (sauf pickup/delivery)
  if (clientMarker) { clientMap.removeLayer(clientMarker); clientMarker = null; }
  if (clientTrail) { clientMap.removeLayer(clientTrail); clientTrail = null; clientTrailPoints.length = 0; }
  // Highlight dans la liste
  document.querySelectorAll('.order-item').forEach(el => el.classList.toggle('active', el.dataset.orderId === order.id));
  // Render details
  const total = (order.items || []).reduce((s, i) => s + i.quantity * i.unit_price, 0);
  document.getElementById('cl-order-id-sub').textContent = order.id;
  document.getElementById('cl-order-total').textContent = total.toFixed(2) + ' TND';
  document.getElementById('cl-order-driver').textContent = order.assigned_driver_id ? order.assigned_driver_id : 'en attente d\'attribution...';
  document.getElementById('cl-order-items').innerHTML = (order.items || [])
    .map(i => `<div>• ${escapeHtml(i.product_name)} x${i.quantity} (${i.unit_price} TND)</div>`).join('');
  const pill = document.getElementById('cl-order-status-pill');
  pill.className = 'status-pill ' + order.status;
  pill.textContent = order.status;
  renderClientTimeline(order.status);
  pollUntilDeliveryFound(order.id);
}

async function pollUntilDeliveryFound(orderId, tries = 0) {
  if (tries > 20) return;
  try {
    const list = await callGw('GET', '/api/deliveries?limit=100');
    const d = list.deliveries.find(x => x.order_id === orderId);
    if (d) {
      clientState.deliveryId = d.id;
      openClientWatch(d.id);
      if (d.driver_id) attachClientDriver(d.driver_id, d.driver_name);
      return;
    }
  } catch (_) {}
  setTimeout(() => pollUntilDeliveryFound(orderId, tries + 1), 500);
}

function openClientWatch(deliveryId) {
  if (clientState.watchEs) clientState.watchEs.close();
  clientState.watchEs = new EventSource(`${GW}/api/deliveries/${deliveryId}/watch`);
  clientState.watchEs.onmessage = (ev) => {
    const d = JSON.parse(ev.data);
    // Mapping statut Delivery -> statut Order (timeline) :
    // PENDING_ASSIGNMENT -> PENDING, autres = identiques
    const mapped = d.status === 'PENDING_ASSIGNMENT' ? 'PENDING' : d.status;
    renderClientTimeline(mapped);
    const pill = document.getElementById('cl-order-status-pill');
    pill.className = 'status-pill ' + mapped;
    pill.textContent = mapped;
    if (d.driver_id && d.driver_id !== clientState.driverId) {
      attachClientDriver(d.driver_id, d.driver_name);
    }
    // Met aussi a jour la liste pour refleter le nouveau status
    refreshClientOrdersList();
  };
}

function attachClientDriver(driverId, driverName) {
  clientState.driverId = driverId;
  document.getElementById('cl-order-driver').textContent = driverName || driverId.slice(0, 8);
  ensureClientMap();
  document.getElementById('cl-map-hint').textContent = `Suivi de ${driverName || 'livreur'} en temps reel.`;
  if (clientState.streamEs) clientState.streamEs.close();
  clientState.streamEs = new EventSource(`${GW}/api/drivers/${driverId}/stream`);
  clientState.streamEs.onmessage = (ev) => {
    const loc = JSON.parse(ev.data);
    moveClientMarker(loc.latitude, loc.longitude);
  };
}

function moveClientMarker(lat, lng) {
  if (!clientMap) ensureClientMap();
  const pt = [lat, lng];
  if (!clientMarker) {
    clientMarker = L.marker(pt).addTo(clientMap).bindTooltip('Livreur', { permanent: true, direction: 'top', offset: [-15, -10] });
  } else {
    clientMarker.setLatLng(pt);
  }
  clientTrailPoints.push(pt);
  if (clientTrailPoints.length > 200) clientTrailPoints.shift();
  if (!clientTrail) {
    clientTrail = L.polyline(clientTrailPoints, { color: '#3b82f6', weight: 3 }).addTo(clientMap);
  } else {
    clientTrail.setLatLngs(clientTrailPoints);
  }
  clientMap.panTo(pt);
}

function closeClientStreams() {
  if (clientState.watchEs) clientState.watchEs.close();
  if (clientState.streamEs) clientState.streamEs.close();
  clientState.watchEs = clientState.streamEs = null;
}

function resetClientSession() {
  if (!confirm('Reinitialiser la session client ? Un nouveau customer_id sera genere et la liste de tes commandes sera vide.')) return;
  closeClientStreams();
  localStorage.removeItem('soa.clientCustomerId');
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

async function cancelClientOrder() {
  if (!clientState.orderId) return;
  if (!confirm('Annuler cette commande ? Action irreversible.')) return;
  const btn = document.getElementById('cl-cancel-order');
  // Verrouille immediatement le bouton (anti double-click pendant
  // que l'event Kafka order.cancelled propage jusqu'au SSE delivery)
  btn.disabled = true;
  btn.textContent = 'Annulation en cours...';
  try {
    await callGw('POST', `/api/orders/${clientState.orderId}/cancel`, { reason: 'Annulee par le client' });
    // Met a jour l'UI tout de suite, sans attendre le SSE
    renderClientTimeline('CANCELLED');
    const pill = document.getElementById('cl-order-status-pill');
    pill.className = 'status-pill CANCELLED';
    pill.textContent = 'CANCELLED';
    refreshClientOrdersList();
  } catch (e) {
    alert('Echec annulation : ' + e.message);
    btn.disabled = false;
    btn.textContent = 'Annuler ma commande';
  }
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
// ============ INTERFACE ADMIN ============
// ============================================================
let adminDriversTimer = null;
let adminOrdersTimer = null;

async function refreshDrivers() {
  try {
    // Pour avoir TOUS les drivers (pas juste AVAILABLE), on combine 2 sources :
    // - GET /api/drivers/available pour les AVAILABLE
    // - GET /api/deliveries pour deduire les drivers BUSY (via la jointure)
    // Plus simple : on liste les available, et pour les BUSY on prend les drivers
    // referenced dans les deliveries non-finales.
    const [avail, deliveries] = await Promise.all([
      callGw('GET', '/api/drivers/available?limit=100'),
      callGw('GET', '/api/deliveries?limit=200'),
    ]);
    // ids deja vus dans available
    const seen = new Map();
    for (const d of avail.drivers || []) seen.set(d.id, d);
    // Drivers BUSY : on les recupere via GetDriver pour chaque driver_id distinct des deliveries en cours
    const busyIds = new Set();
    const orderByDriver = new Map();
    for (const dl of deliveries.deliveries || []) {
      if (dl.driver_id && !['DELIVERED', 'CANCELLED'].includes(dl.status)) {
        busyIds.add(dl.driver_id);
        orderByDriver.set(dl.driver_id, dl);
      }
    }
    const busyDrivers = await Promise.all([...busyIds].map(id => callGw('GET', `/api/drivers/${id}`).catch(() => null)));
    for (const d of busyDrivers) if (d) seen.set(d.id, d);

    const tbody = document.getElementById('ad-drivers-body');
    tbody.innerHTML = '';
    const sorted = [...seen.values()].sort((a, b) => (a.name || '').localeCompare(b.name || ''));
    if (sorted.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="empty-row">Aucun livreur. Utilise le formulaire ci-dessus pour en ajouter.</td></tr>';
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
        <td><button class="row-action-btn" ${deletable ? '' : 'disabled title="BUSY : impossible de supprimer"'} data-delete-id="${d.id}">Supprimer</button></td>`;
      const delBtn = tr.querySelector('button[data-delete-id]');
      if (delBtn && deletable) {
        delBtn.addEventListener('click', async (ev) => {
          ev.stopPropagation();
          if (!confirm(`Supprimer le livreur ${d.name} ?`)) return;
          try {
            await callGw('DELETE', `/api/drivers/${d.id}`);
            refreshDrivers();
            refreshLivreurDropdown();
          } catch (e) {
            alert('Echec suppression : ' + e.message);
          }
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
  } catch (e) {
    console.error('refreshDrivers:', e);
  }
}

async function refreshOrders() {
  try {
    const status = document.getElementById('ad-filter-status').value;
    const customer = document.getElementById('ad-filter-customer').value.trim();
    const search = document.getElementById('ad-filter-search').value.trim().toLowerCase();
    const params = new URLSearchParams();
    if (status) params.set('status', status);
    if (customer) params.set('customer_id', customer);
    params.set('limit', '100');
    const list = await callGw('GET', '/api/orders?' + params.toString());
    let orders = list.orders || [];
    if (search) {
      orders = orders.filter(o =>
        (o.id || '').toLowerCase().includes(search) ||
        (o.customer_name || '').toLowerCase().includes(search) ||
        (o.customer_id || '').toLowerCase().includes(search)
      );
    }
    const tbody = document.getElementById('ad-orders-body');
    tbody.innerHTML = '';
    if (orders.length === 0) {
      tbody.innerHTML = '<tr><td colspan="8" class="empty-row">Aucune commande pour le moment.</td></tr>';
      return;
    }
    for (const o of orders) {
      const tr = document.createElement('tr');
      const isFinal = ['DELIVERED', 'CANCELLED'].includes(o.status);
      const itemsCount = (o.items || []).length;
      // Bouton conditionnel : Annuler tant qu'active, Supprimer une fois terminee
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
          if (!confirm('Annuler la commande ' + o.id.slice(0, 8) + ' ?')) return;
          try {
            await callGw('POST', `/api/orders/${o.id}/cancel`, { reason: 'Annule par admin' });
            refreshOrders();
            refreshDrivers();
          } catch (e) {
            alert('Echec annulation : ' + e.message);
          }
        });
      }
      const delBtn = tr.querySelector('button[data-delete-order]');
      if (delBtn) {
        delBtn.addEventListener('click', async (ev) => {
          ev.stopPropagation();
          if (!confirm('Supprimer definitivement la commande ' + o.id.slice(0, 8) + ' ? Cette action est irreversible.')) return;
          try {
            await callGw('DELETE', `/api/orders/${o.id}`);
            refreshOrders();
          } catch (e) {
            alert('Echec suppression : ' + e.message);
          }
        });
      }
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error('refreshOrders:', e);
  }
}

async function addAdminDriver() {
  const status = document.getElementById('ad-add-status');
  status.className = 'status running';
  status.textContent = 'envoi...';
  try {
    const body = {
      name: document.getElementById('ad-name').value,
      phone: document.getElementById('ad-phone').value,
      vehicle_type: document.getElementById('ad-vehicle').value,
    };
    const d = await callGw('POST', '/api/drivers', body);
    // Position initiale aleatoire autour du centre Sousse pour que le marker apparaisse
    const lat = DEFAULT_START.lat + (Math.random() - 0.5) * 0.01;
    const lng = DEFAULT_START.lng + (Math.random() - 0.5) * 0.01;
    await callGw('PATCH', `/api/drivers/${d.id}/location`, { latitude: lat, longitude: lng, speed_kmh: 0, heading_deg: 0 });
    status.className = 'status ok';
    status.textContent = 'OK : ' + d.name;
    await refreshDrivers();
    await refreshLivreurDropdown();
  } catch (e) {
    status.className = 'status fail';
    status.textContent = 'KO : ' + e.message;
  }
}

function setupAdmin() {
  document.getElementById('ad-add').addEventListener('click', addAdminDriver);
  document.getElementById('ad-refresh-drivers').addEventListener('click', refreshDrivers);
  document.getElementById('ad-refresh-orders').addEventListener('click', refreshOrders);
  ['ad-filter-status', 'ad-filter-customer', 'ad-filter-search'].forEach(id =>
    document.getElementById(id).addEventListener('input', refreshOrders));
  adminDriversTimer = setInterval(refreshDrivers, 2000);
  adminOrdersTimer = setInterval(refreshOrders, 2000);
  refreshDrivers();
  refreshOrders();
}

// ============================================================
// ============ INTERFACE LIVREUR ============
// ============================================================
const livreurState = { driverId: null, driver: null, delivery: null, animTimer: null };
let livreurMap = null;
let livreurMarker = null;
let livreurPickupMarker = null;
let livreurDeliveryMarker = null;
let livreurTrail = null;
const livreurTrailPoints = [];

function ensureLivreurMap() {
  if (livreurMap) return;
  livreurMap = L.map('lv-map').setView([(PICKUP.lat + DELIVERY.lat) / 2, (PICKUP.lng + DELIVERY.lng) / 2], 14);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', { attribution: 'OSM', maxZoom: 19 }).addTo(livreurMap);
  livreurPickupMarker = L.circleMarker([PICKUP.lat, PICKUP.lng], { color: '#f59e0b', radius: 8, fillOpacity: 0.7 })
    .bindTooltip('Pickup', { permanent: true, direction: 'top', offset: [0, -6] }).addTo(livreurMap);
  livreurDeliveryMarker = L.circleMarker([DELIVERY.lat, DELIVERY.lng], { color: '#10b981', radius: 8, fillOpacity: 0.7 })
    .bindTooltip('Livraison', { permanent: true, direction: 'top', offset: [0, -6] }).addTo(livreurMap);
}

async function refreshLivreurDropdown() {
  try {
    const [avail, deliveries] = await Promise.all([
      callGw('GET', '/api/drivers/available?limit=100'),
      callGw('GET', '/api/deliveries?limit=200'),
    ]);
    const seen = new Map();
    for (const d of avail.drivers || []) seen.set(d.id, d);
    const busyIds = new Set();
    for (const dl of deliveries.deliveries || []) {
      if (dl.driver_id && !['DELIVERED', 'CANCELLED'].includes(dl.status)) busyIds.add(dl.driver_id);
    }
    const busy = await Promise.all([...busyIds].map(id => callGw('GET', `/api/drivers/${id}`).catch(() => null)));
    for (const d of busy) if (d) seen.set(d.id, d);
    const sel = document.getElementById('lv-select');
    const current = sel.value;
    sel.innerHTML = '<option value="">-- Choisir --</option>';
    [...seen.values()].sort((a, b) => (a.name || '').localeCompare(b.name || ''))
      .forEach(d => {
        const opt = document.createElement('option');
        opt.value = d.id;
        opt.textContent = `${d.name} [${d.status}] - ${d.vehicle_type}`;
        sel.appendChild(opt);
      });
    if (current && [...sel.options].some(o => o.value === current)) sel.value = current;
  } catch (e) {
    console.error('refreshLivreurDropdown:', e);
  }
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
  if (!id) {
    noOrder.classList.add('hidden');
    current.classList.add('hidden');
    return;
  }
  try {
    const driver = await callGw('GET', `/api/drivers/${id}`);
    livreurState.driver = driver;
    if (driver.status !== 'BUSY' || !driver.current_order_id) {
      current.classList.add('hidden');
      noOrder.classList.remove('hidden');
      return;
    }
    // Recuperer la delivery liee
    const deliveries = await callGw('GET', '/api/deliveries?limit=200');
    const delivery = (deliveries.deliveries || []).find(x => x.driver_id === id && !['DELIVERED', 'CANCELLED'].includes(x.status));
    if (!delivery) {
      // Pas de delivery active : driver vient peut-etre d'etre cancel
      current.classList.add('hidden');
      noOrder.classList.remove('hidden');
      return;
    }
    livreurState.delivery = delivery;
    document.getElementById('lv-driver-name').textContent = driver.name;
    document.getElementById('lv-driver-vehicle').textContent = driver.vehicle_type;
    document.getElementById('lv-driver-status').innerHTML = `<span class="status-pill ${driver.status}">${driver.status}</span>`;
    document.getElementById('lv-order-id').textContent = delivery.order_id;
    document.getElementById('lv-order-customer').textContent = delivery.customer_name || '-';
    document.getElementById('lv-order-address').textContent = delivery.delivery_address || '-';
    document.getElementById('lv-delivery-status').innerHTML = `<span class="status-pill ${delivery.status}">${delivery.status}</span>`;
    noOrder.classList.add('hidden');
    current.classList.remove('hidden');
    ensureLivreurMap();
    if (driver.last_location) updateLivreurMarker(driver.last_location.latitude, driver.last_location.longitude);
    updateLivreurButtons(delivery.status);
  } catch (e) {
    alert('Erreur chargement livreur : ' + e.message);
  }
}

function updateLivreurButtons(deliveryStatus) {
  const map = {
    'lv-go-pickup': deliveryStatus === 'ASSIGNED',
    'lv-pickup':    deliveryStatus === 'ASSIGNED',
    'lv-go-deliver': deliveryStatus === 'PICKED_UP',
    'lv-transit':   deliveryStatus === 'PICKED_UP',
    'lv-deliver':   deliveryStatus === 'IN_TRANSIT',
  };
  for (const [id, enabled] of Object.entries(map)) {
    document.getElementById(id).disabled = !enabled;
  }
}

function updateLivreurMarker(lat, lng) {
  if (!livreurMap) ensureLivreurMap();
  const pt = [lat, lng];
  if (!livreurMarker) {
    livreurMarker = L.marker(pt).addTo(livreurMap).bindTooltip('Moi', { permanent: true, direction: 'top', offset: [-15, -10] });
  } else {
    livreurMarker.setLatLng(pt);
  }
  livreurTrailPoints.push(pt);
  if (livreurTrailPoints.length > 200) livreurTrailPoints.shift();
  if (!livreurTrail) {
    livreurTrail = L.polyline(livreurTrailPoints, { color: '#f59e0b', weight: 3 }).addTo(livreurMap);
  } else {
    livreurTrail.setLatLngs(livreurTrailPoints);
  }
  livreurMap.panTo(pt);
}

// Animation : interpolation lineaire entre 2 points sur 4 secondes,
// 16 frames -> 250ms par frame -> 16 PATCH /location au gateway.
async function animateMove(target) {
  if (livreurState.animTimer) return;
  const id = livreurState.driverId;
  const driver = livreurState.driver;
  const start = driver.last_location
    ? { lat: driver.last_location.latitude, lng: driver.last_location.longitude }
    : { lat: DEFAULT_START.lat, lng: DEFAULT_START.lng };
  const FRAMES = 16;
  const status = document.getElementById('lv-action-status');
  status.className = 'status running';
  status.textContent = 'deplacement...';

  for (let i = 1; i <= FRAMES; i++) {
    const t = i / FRAMES;
    const lat = start.lat + (target.lat - start.lat) * t;
    const lng = start.lng + (target.lng - start.lng) * t;
    try {
      await callGw('PATCH', `/api/drivers/${id}/location`, {
        latitude: lat, longitude: lng,
        speed_kmh: 25 + Math.random() * 10,
        heading_deg: Math.random() * 360,
      });
      updateLivreurMarker(lat, lng);
    } catch (e) {
      status.className = 'status fail';
      status.textContent = e.message;
      return;
    }
    await wait(250);
  }
  livreurState.driver.last_location = { latitude: target.lat, longitude: target.lng };
  status.className = 'status ok';
  status.textContent = 'arrive a ' + target.label;
}

async function advanceLivreurStatus(newStatus) {
  const status = document.getElementById('lv-action-status');
  status.className = 'status running';
  status.textContent = `passage a ${newStatus}...`;
  try {
    await callGw('PATCH', `/api/deliveries/${livreurState.delivery.id}/status`, { new_status: newStatus });
    status.className = 'status ok';
    status.textContent = newStatus;
    await renderLivreurPanel();
    if (newStatus === 'DELIVERED') {
      // Le driver est libere automatiquement cote backend (Kafka).
      // On rafraichit la dropdown apres un petit delai.
      setTimeout(refreshLivreurDropdown, 1000);
    }
  } catch (e) {
    status.className = 'status fail';
    status.textContent = e.message;
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

// ============================================================
// ============ Init ============
// ============================================================
function init() {
  setupTabs();
  setupClient();
  setupAdmin();
  setupLivreur();
  checkHealth();
  setInterval(checkHealth, 10000);
}
init();
