// REST playground : 16 cartes (1 par endpoint du gateway) avec UI, chainage des ids
// et 2 cartes SSE (Open/Close stream).
//
// Chaque carte est decrite par un objet et rendue dynamiquement. Les ids
// extraits des reponses (driver_id, order_id, ...) sont stockes dans `vars`
// et reinjectes dans les champs marques 'auto:true' avant chaque Send.

const GW = 'http://localhost:3000';
const VAR_KEYS = ['driver_id', 'order_id', 'order2_id', 'delivery_id'];
const vars = Object.fromEntries(VAR_KEYS.map(k => [k, '']));

function escapeHtml(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
function getByPath(obj, p) {
  if (!obj || !p) return undefined;
  const parts = p.split('.');
  let cur = obj;
  for (const k of parts) { if (cur == null) return undefined; cur = /^\d+$/.test(k) ? cur[Number(k)] : cur[k]; }
  return cur;
}

// ============ Cartes ============
const CARDS = [
  // ---- HEALTH ----
  {
    section: 'health', id: 'health',
    method: 'GET', path: '/health',
    desc: 'Ping de base. Reponse : { status: "ok", service: "gateway" }.',
  },

  // ---- DRIVERS ----
  {
    section: 'drivers', id: 'register-driver',
    method: 'POST', path: '/api/drivers',
    name: 'RegisterDriver',
    desc: 'Enregistre un nouveau livreur. Si la file d\'attente contient des orders en attente, ce driver est immediatement assigne a la 1ere.',
    fields: [
      { name: 'name', label: 'name', default: 'Karim Ben Salah' },
      { name: 'phone', label: 'phone', default: '+216 22 123 456' },
      { name: 'vehicle_type', label: 'vehicle_type', default: 'scooter' },
    ],
    buildBody: (i) => ({ name: i.name, phone: i.phone, vehicle_type: i.vehicle_type }),
    extract: { driver_id: 'id' },
  },
  {
    section: 'drivers', id: 'list-available',
    method: 'GET', path: '/api/drivers/available',
    name: 'ListAvailableDrivers',
    desc: 'Liste les livreurs en status AVAILABLE.',
    fields: [{ name: 'limit', label: 'limit (query)', default: '20' }],
    buildPath: (i) => `/api/drivers/available?limit=${encodeURIComponent(i.limit)}`,
  },
  {
    section: 'drivers', id: 'get-driver',
    method: 'GET', path: '/api/drivers/:id',
    name: 'GetDriver',
    desc: 'Recupere un livreur par son id.',
    fields: [{ name: 'driver_id', label: 'driver_id', autoVar: 'driver_id' }],
    buildPath: (i) => `/api/drivers/${encodeURIComponent(i.driver_id)}`,
  },
  {
    section: 'drivers', id: 'update-location',
    method: 'PATCH', path: '/api/drivers/:id/location',
    name: 'UpdateLocation',
    desc: 'Met a jour la position GPS du livreur. Publie driver.location-updated sur Kafka.',
    fields: [
      { name: 'driver_id', label: 'driver_id', autoVar: 'driver_id', cls: 'full' },
      { name: 'latitude', label: 'latitude', default: '35.8245' },
      { name: 'longitude', label: 'longitude', default: '10.6347' },
      { name: 'speed_kmh', label: 'speed_kmh', default: '30' },
      { name: 'heading_deg', label: 'heading_deg', default: '90' },
    ],
    buildPath: (i) => `/api/drivers/${encodeURIComponent(i.driver_id)}/location`,
    buildBody: (i) => ({
      latitude: Number(i.latitude), longitude: Number(i.longitude),
      speed_kmh: Number(i.speed_kmh), heading_deg: Number(i.heading_deg),
    }),
  },
  {
    section: 'drivers', id: 'stream-driver', sse: true,
    method: 'SSE', path: '/api/drivers/:id/stream',
    name: 'StreamDriverLocation',
    desc: 'Flux SSE qui pousse chaque mise a jour de position en temps reel. Pour declencher des events, lancer cette carte puis utiliser PATCH .../location sur le meme driver dans une autre carte.',
    fields: [{ name: 'driver_id', label: 'driver_id', autoVar: 'driver_id', cls: 'full' }],
    buildPath: (i) => `/api/drivers/${encodeURIComponent(i.driver_id)}/stream`,
  },

  // ---- ORDERS ----
  {
    section: 'orders', id: 'create-order',
    method: 'POST', path: '/api/orders',
    name: 'CreateOrder',
    desc: 'Cree une commande. Publie order.placed -> driver-service tente l\'assignation. Si aucun driver dispo, l\'order est mise en file d\'attente.',
    fields: [
      { name: 'customer_id', label: 'customer_id', default: 'cust-001', cls: 'full' },
      { name: 'customer_name', label: 'customer_name', default: 'Med Abroud' },
      { name: 'delivery_address', label: 'delivery_address', default: 'Sahloul 5 rue de l\'honneur' },
      { name: 'items_json', label: 'items (JSON)', cls: 'full', textarea: true, default: JSON.stringify([
        { product_name: 'Pizza Neptune', quantity: 2, unit_price: 12.5 },
        { product_name: 'Sabrine 1L', quantity: 1, unit_price: 3 },
      ], null, 2) },
    ],
    buildBody: (i) => ({
      customer_id: i.customer_id, customer_name: i.customer_name,
      delivery_address: i.delivery_address,
      items: JSON.parse(i.items_json),
    }),
    extract: { order_id: 'id' },
  },
  {
    section: 'orders', id: 'get-order',
    method: 'GET', path: '/api/orders/:id',
    name: 'GetOrder',
    desc: 'Recupere une commande. Apres POST /api/orders, attendre 1-2 secondes puis re-lancer cette carte pour voir le status passer a ASSIGNED.',
    fields: [{ name: 'order_id', label: 'order_id', autoVar: 'order_id', cls: 'full' }],
    buildPath: (i) => `/api/orders/${encodeURIComponent(i.order_id)}`,
  },
  {
    section: 'orders', id: 'list-orders',
    method: 'GET', path: '/api/orders',
    name: 'ListOrders',
    desc: 'Liste les commandes avec filtres optionnels (customer_id, status).',
    fields: [
      { name: 'customer_id', label: 'customer_id (query, optionnel)', default: '' },
      { name: 'status', label: 'status (query, optionnel)', default: '' },
      { name: 'limit', label: 'limit', default: '10' },
    ],
    buildPath: (i) => {
      const p = new URLSearchParams();
      if (i.customer_id) p.set('customer_id', i.customer_id);
      if (i.status) p.set('status', i.status);
      p.set('limit', i.limit);
      return '/api/orders?' + p.toString();
    },
  },
  {
    section: 'orders', id: 'update-order-status',
    method: 'PATCH', path: '/api/orders/:id/status',
    name: 'UpdateOrderStatus (manuel)',
    desc: 'Override manuel du statut. Rarement utilise : la chaine Kafka gere normalement les transitions automatiquement.',
    fields: [
      { name: 'order_id', label: 'order_id', autoVar: 'order_id', cls: 'full' },
      { name: 'status', label: 'status', default: 'IN_TRANSIT' },
      { name: 'assigned_driver_id', label: 'assigned_driver_id (optionnel)', autoVar: 'driver_id' },
    ],
    buildPath: (i) => `/api/orders/${encodeURIComponent(i.order_id)}/status`,
    buildBody: (i) => {
      const b = { status: i.status };
      if (i.assigned_driver_id) b.assigned_driver_id = i.assigned_driver_id;
      return b;
    },
  },
  {
    section: 'orders', id: 'cancel-order',
    method: 'POST', path: '/api/orders/:id/cancel',
    name: 'CancelOrder',
    desc: 'Annule la commande. Publie order.cancelled -> tracking-service met la delivery a CANCELLED.',
    fields: [
      { name: 'order_id', label: 'order_id', autoVar: 'order_id', cls: 'full' },
      { name: 'reason', label: 'reason', default: 'Client a change d\'avis', cls: 'full' },
    ],
    buildPath: (i) => `/api/orders/${encodeURIComponent(i.order_id)}/cancel`,
    buildBody: (i) => ({ reason: i.reason }),
  },

  // ---- DELIVERIES ----
  {
    section: 'deliveries', id: 'list-deliveries',
    method: 'GET', path: '/api/deliveries',
    name: 'ListDeliveries',
    desc: 'Liste les livraisons. Astuce : le bouton "Use latest by order_id" extrait la delivery liee a votre order_id courant.',
    fields: [
      { name: 'status', label: 'status (optionnel)', default: '' },
      { name: 'driver_id', label: 'driver_id (optionnel)', default: '' },
      { name: 'limit', label: 'limit', default: '20' },
    ],
    buildPath: (i) => {
      const p = new URLSearchParams();
      if (i.status) p.set('status', i.status);
      if (i.driver_id) p.set('driver_id', i.driver_id);
      p.set('limit', i.limit);
      return '/api/deliveries?' + p.toString();
    },
    extractFn: (resp) => {
      if (!resp || !resp.deliveries) return {};
      const matching = resp.deliveries.find(d => d.order_id === vars.order_id);
      return matching ? { delivery_id: matching.id } : {};
    },
  },
  {
    section: 'deliveries', id: 'get-delivery',
    method: 'GET', path: '/api/deliveries/:id',
    name: 'GetDelivery',
    desc: 'Recupere une livraison precise.',
    fields: [{ name: 'delivery_id', label: 'delivery_id', autoVar: 'delivery_id', cls: 'full' }],
    buildPath: (i) => `/api/deliveries/${encodeURIComponent(i.delivery_id)}`,
  },
  {
    section: 'deliveries', id: 'get-delivery-history',
    method: 'GET', path: '/api/deliveries/:id/history',
    name: 'GetDeliveryHistory',
    desc: 'Historique des events de la livraison (delivery.created, delivery.assigned, delivery.picked-up, ...).',
    fields: [{ name: 'delivery_id', label: 'delivery_id', autoVar: 'delivery_id', cls: 'full' }],
    buildPath: (i) => `/api/deliveries/${encodeURIComponent(i.delivery_id)}/history`,
  },
  {
    section: 'deliveries', id: 'advance-delivery',
    method: 'PATCH', path: '/api/deliveries/:id/status',
    name: 'AdvanceDeliveryStatus',
    desc: 'Avance la livraison. Publie delivery.<status> sur Kafka -> order-service consume et met a jour l\'order.',
    fields: [
      { name: 'delivery_id', label: 'delivery_id', autoVar: 'delivery_id', cls: 'full' },
      { name: 'new_status', label: 'new_status', kind: 'select', options: ['PICKED_UP', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED'], default: 'PICKED_UP' },
    ],
    buildPath: (i) => `/api/deliveries/${encodeURIComponent(i.delivery_id)}/status`,
    buildBody: (i) => ({ new_status: i.new_status }),
  },
  {
    section: 'deliveries', id: 'watch-delivery', sse: true,
    method: 'SSE', path: '/api/deliveries/:id/watch',
    name: 'WatchDelivery',
    desc: 'Flux SSE qui pousse chaque changement de la livraison. Lancer cette carte puis utiliser AdvanceDeliveryStatus dans une autre carte pour voir arriver les events.',
    fields: [{ name: 'delivery_id', label: 'delivery_id', autoVar: 'delivery_id', cls: 'full' }],
    buildPath: (i) => `/api/deliveries/${encodeURIComponent(i.delivery_id)}/watch`,
  },
];

// ============ Variables panel ============
function refreshVarsUi() {
  for (const k of VAR_KEYS) {
    const el = document.getElementById('var-' + k);
    if (!el) continue;
    el.textContent = vars[k] || '(vide)';
    el.classList.toggle('empty', !vars[k]);
  }
}
function setVar(key, value) {
  if (!VAR_KEYS.includes(key) || !value) return;
  vars[key] = value;
  refreshVarsUi();
  // pousse la valeur dans tous les champs autoVar correspondants
  document.querySelectorAll(`input[data-auto-var="${key}"]`).forEach(inp => {
    if (!inp.dataset.userEdited) inp.value = value;
  });
}
document.getElementById('vars-reset').addEventListener('click', () => {
  for (const k of VAR_KEYS) vars[k] = '';
  refreshVarsUi();
  document.querySelectorAll('input[data-auto-var]').forEach(i => { i.value = ''; delete i.dataset.userEdited; });
});
document.querySelectorAll('.var-copy').forEach(btn => {
  btn.addEventListener('click', () => {
    const k = btn.dataset.key;
    if (!vars[k]) return;
    navigator.clipboard.writeText(vars[k]);
    btn.textContent = 'ok';
    setTimeout(() => (btn.textContent = 'copy'), 800);
  });
});

// ============ Health check au demarrage ============
async function checkGatewayHealth() {
  const el = document.getElementById('gateway-status');
  try {
    const r = await fetch(`${GW}/health`);
    el.className = 'status on';
    el.textContent = r.ok ? 'Gateway OK' : 'Gateway KO';
  } catch (e) {
    el.className = 'status err';
    el.textContent = 'Gateway hors ligne';
  }
}

// ============ Render des cartes ============
function renderCard(card) {
  const tpl = document.createElement('div');
  tpl.className = 'card collapsed';
  tpl.dataset.cardId = card.id;
  const fieldsHtml = (card.fields || []).map(f => {
    const auto = f.autoVar ? `<span class="auto-tag">auto:${f.autoVar}</span>` : '';
    let input;
    if (f.kind === 'select') {
      input = `<select name="${f.name}">${f.options.map(o => `<option value="${o}"${o === f.default ? ' selected' : ''}>${o}</option>`).join('')}</select>`;
    } else if (f.textarea) {
      input = `<textarea name="${f.name}" rows="6">${escapeHtml(f.default || '')}</textarea>`;
    } else {
      const val = f.autoVar ? (vars[f.autoVar] || '') : (f.default || '');
      input = `<input type="text" name="${f.name}" value="${escapeHtml(val)}"${f.autoVar ? ` data-auto-var="${f.autoVar}"` : ''} />`;
    }
    return `<div class="field ${f.cls || ''}"><label>${f.label}${auto}</label>${input}</div>`;
  }).join('');

  const actions = card.sse
    ? `<button class="btn-send btn-open">Open stream</button>
       <button class="btn-send btn-close" disabled>Close</button>
       <span class="card-status">idle</span>`
    : `<button class="btn-send">Send</button><span class="card-status">idle</span>`;

  tpl.innerHTML = `
    <div class="card-head">
      <span class="method ${card.method}">${card.method}</span>
      <span class="path">${card.path}</span>
      <span class="name">${card.name || ''}</span>
      <span class="chev">[+]</span>
    </div>
    <div class="card-body">
      ${card.desc ? `<p class="desc">${card.desc}</p>` : ''}
      <div class="fields">${fieldsHtml}</div>
      <div class="card-actions">${actions}</div>
      ${card.sse ? `<div class="stream-log" data-empty="1"><span class="ev"><span class="t">--:--:--</span>(stream ferme)</span></div>`
                 : `<div class="resp empty">(reponse a venir)</div>`}
    </div>`;
  // toggles
  tpl.querySelector('.card-head').addEventListener('click', () => {
    tpl.classList.toggle('collapsed');
    tpl.querySelector('.chev').textContent = tpl.classList.contains('collapsed') ? '[+]' : '[-]';
  });
  // edited inputs are no longer auto-overwritten
  tpl.querySelectorAll('input[data-auto-var]').forEach(inp => {
    inp.addEventListener('input', () => { inp.dataset.userEdited = '1'; });
  });

  // Bind action(s)
  if (card.sse) {
    tpl.querySelector('.btn-open').addEventListener('click', () => openSse(card, tpl));
    tpl.querySelector('.btn-close').addEventListener('click', () => closeSse(tpl));
  } else {
    tpl.querySelector('.btn-send').addEventListener('click', () => runCard(card, tpl));
  }
  return tpl;
}

function collectInputs(card, tpl) {
  const i = {};
  (card.fields || []).forEach(f => {
    const el = tpl.querySelector(`[name="${f.name}"]`);
    i[f.name] = el ? el.value : '';
  });
  return i;
}

async function runCard(card, tpl) {
  const input = collectInputs(card, tpl);
  const path = card.buildPath ? card.buildPath(input) : card.path;
  const body = card.buildBody ? card.buildBody(input) : null;
  const status = tpl.querySelector('.card-status');
  const resp = tpl.querySelector('.resp');
  status.className = 'card-status running';
  status.textContent = 'running';
  resp.className = 'resp';
  resp.textContent = '...';
  try {
    const opts = { method: card.method, headers: { 'Content-Type': 'application/json' } };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(GW + path, opts);
    const text = await res.text();
    const data = text ? JSON.parse(text) : null;
    if (!res.ok) {
      resp.className = 'resp err';
      resp.textContent = `HTTP ${res.status}\n\n${JSON.stringify(data, null, 2)}`;
      status.className = 'card-status fail';
      status.textContent = `HTTP ${res.status}`;
      return;
    }
    resp.textContent = JSON.stringify(data, null, 2);
    status.className = 'card-status ok';
    status.textContent = `${res.status} OK`;
    // Extraction
    if (card.extract) {
      for (const [k, p] of Object.entries(card.extract)) {
        const v = getByPath(data, p);
        if (v) setVar(k, v);
      }
    }
    if (card.extractFn) {
      const out = card.extractFn(data) || {};
      for (const [k, v] of Object.entries(out)) setVar(k, v);
    }
  } catch (e) {
    status.className = 'card-status fail';
    status.textContent = 'erreur';
    resp.className = 'resp err';
    resp.textContent = e.message;
  }
}

// ============ SSE handling ============
const openStreams = new WeakMap();
function nowHHMMSS() { return new Date().toLocaleTimeString(); }

function openSse(card, tpl) {
  const input = collectInputs(card, tpl);
  const path = card.buildPath(input);
  const url = GW + path;
  const logEl = tpl.querySelector('.stream-log');
  const status = tpl.querySelector('.card-status');
  const openBtn = tpl.querySelector('.btn-open');
  const closeBtn = tpl.querySelector('.btn-close');

  closeSse(tpl);
  logEl.innerHTML = '';
  logEl.removeAttribute('data-empty');
  appendStreamLine(logEl, `Connexion: ${url}`);

  const es = new EventSource(url);
  openStreams.set(tpl, es);
  status.className = 'card-status streaming';
  status.textContent = 'streaming';
  openBtn.disabled = true;
  closeBtn.disabled = false;

  es.onmessage = (ev) => appendStreamLine(logEl, ev.data);
  es.onerror = () => {
    appendStreamLine(logEl, '[erreur: SSE coupe (id invalide ou serveur indisponible)]');
    status.className = 'card-status fail';
    status.textContent = 'erreur SSE';
    closeSse(tpl);
  };
}

function closeSse(tpl) {
  const es = openStreams.get(tpl);
  if (es) { es.close(); openStreams.delete(tpl); }
  tpl.querySelector('.btn-open').disabled = false;
  tpl.querySelector('.btn-close').disabled = true;
  const s = tpl.querySelector('.card-status');
  if (s.textContent === 'streaming') {
    s.className = 'card-status';
    s.textContent = 'closed';
  }
}

function appendStreamLine(logEl, text) {
  const li = document.createElement('span');
  li.className = 'ev';
  li.innerHTML = `<span class="t">${nowHHMMSS()}</span>${escapeHtml(text)}`;
  logEl.appendChild(li);
  logEl.appendChild(document.createElement('br'));
  logEl.scrollTop = logEl.scrollHeight;
}

// ============ Init ============
function init() {
  refreshVarsUi();
  for (const card of CARDS) {
    const container = document.getElementById('cards-' + card.section);
    if (container) container.appendChild(renderCard(card));
  }
  checkGatewayHealth();
  setInterval(checkGatewayHealth, 10000);
}
init();
