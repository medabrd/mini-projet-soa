// Client web : carte Leaflet centree sur Sousse + connexion SSE au gateway
// pour suivre la position d'un livreur en temps reel.
// On consomme aussi REST (/api/drivers/available) et GraphQL (driver + current_order)
// pour remplir la liste deroulante et afficher la commande en cours.

const GATEWAY = 'http://localhost:3000';
const SOUSSE = [35.8254, 10.6346]; // centre de la carte par defaut

const map = L.map('map').setView(SOUSSE, 13);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  attribution: 'OpenStreetMap',
  maxZoom: 19,
}).addTo(map);

let marker = null;
let trail = null;
let trailPoints = [];
let eventSource = null;
let currentDriverId = null;
let simulatorTimer = null;

const els = {
  select: document.getElementById('driver-select'),
  trackBtn: document.getElementById('track-btn'),
  stopBtn: document.getElementById('stop-btn'),
  refreshBtn: document.getElementById('refresh-drivers'),
  simBtn: document.getElementById('sim-btn'),
  status: document.getElementById('connection-status'),
  driverInfo: document.getElementById('driver-info'),
  orderInfo: document.getElementById('order-info'),
  log: document.getElementById('log'),
};

function setStatus(label, cls) {
  els.status.textContent = label;
  els.status.className = 'status ' + cls;
}

function logEvent(text) {
  const li = document.createElement('li');
  const t = new Date().toLocaleTimeString();
  li.textContent = `[${t}] ${text}`;
  els.log.prepend(li);
  while (els.log.children.length > 50) els.log.removeChild(els.log.lastChild);
}

async function loadDrivers() {
  try {
    const res = await fetch(`${GATEWAY}/api/drivers/available?limit=50`);
    const data = await res.json();
    els.select.innerHTML = '<option value="">-- Choisir --</option>';
    (data.drivers || []).forEach(d => {
      const opt = document.createElement('option');
      opt.value = d.id;
      opt.textContent = `${d.name} (${d.vehicle_type})`;
      els.select.appendChild(opt);
    });
    logEvent(`${data.total} livreur(s) AVAILABLE charge(s)`);
  } catch (err) {
    logEvent(`Erreur fetch drivers : ${err.message}`);
  }
}

els.select.addEventListener('change', () => {
  els.trackBtn.disabled = !els.select.value;
});

els.refreshBtn.addEventListener('click', loadDrivers);

els.trackBtn.addEventListener('click', () => {
  const id = els.select.value;
  if (!id) return;
  startTracking(id);
});

els.stopBtn.addEventListener('click', stopTracking);

els.simBtn.addEventListener('click', () => {
  if (simulatorTimer) stopSimulator();
  else startSimulator();
});

function startTracking(driverId) {
  stopTracking();
  currentDriverId = driverId;
  trailPoints = [];
  if (trail) { map.removeLayer(trail); trail = null; }

  eventSource = new EventSource(`${GATEWAY}/api/drivers/${driverId}/stream`);
  setStatus('Connecte (SSE)', 'on');
  logEvent(`SSE ouvert sur driver ${driverId.slice(0, 8)}...`);

  eventSource.onmessage = (evt) => {
    try {
      const loc = JSON.parse(evt.data);
      updateMarker(loc);
    } catch (err) {
      logEvent(`Parse error : ${err.message}`);
    }
  };

  eventSource.onerror = () => {
    setStatus('Erreur SSE', 'err');
    logEvent('Erreur SSE (driver hors ligne ou connexion coupee ?)');
  };

  els.trackBtn.disabled = true;
  els.stopBtn.disabled = false;
  els.simBtn.disabled = false;

  refreshDriverDetails(driverId);
}

function stopTracking() {
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  stopSimulator();
  setStatus('Deconnecte', 'off');
  els.stopBtn.disabled = true;
  els.simBtn.disabled = true;
  els.trackBtn.disabled = !els.select.value;
  currentDriverId = null;
}

function updateMarker(loc) {
  if (loc.latitude == null || loc.longitude == null) return;
  const latlng = [loc.latitude, loc.longitude];
  if (!marker) {
    marker = L.marker(latlng).addTo(map);
  } else {
    marker.setLatLng(latlng);
  }
  trailPoints.push(latlng);
  if (trailPoints.length > 200) trailPoints.shift();
  if (!trail) {
    trail = L.polyline(trailPoints, { color: '#3b82f6', weight: 3 }).addTo(map);
  } else {
    trail.setLatLngs(trailPoints);
  }
  map.panTo(latlng);

  document.getElementById('info-position').textContent =
    `${loc.latitude.toFixed(5)}, ${loc.longitude.toFixed(5)}`;
  document.getElementById('info-speed').textContent =
    `${(loc.speed_kmh || 0).toFixed(1)} km/h`;
  document.getElementById('info-timestamp').textContent =
    new Date(loc.timestamp).toLocaleTimeString();
  logEvent(`Position : ${loc.latitude.toFixed(4)}, ${loc.longitude.toFixed(4)}`);
}

async function refreshDriverDetails(driverId) {
  const query = `
    query($id: ID!) {
      driver(id: $id) {
        name
        vehicle_type
        status
        current_order {
          id
          status
          customer_name
          delivery_address
          total_amount
        }
      }
    }`;
  try {
    const res = await fetch(`${GATEWAY}/graphql`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ query, variables: { id: driverId } }),
    });
    const json = await res.json();
    const d = json.data && json.data.driver;
    if (!d) return;
    document.getElementById('info-name').textContent = d.name;
    document.getElementById('info-vehicle').textContent = d.vehicle_type;
    document.getElementById('info-status').textContent = d.status;
    els.driverInfo.classList.remove('hidden');

    if (d.current_order) {
      document.getElementById('order-id').textContent = d.current_order.id.slice(0, 8) + '...';
      document.getElementById('order-status').textContent = d.current_order.status;
      document.getElementById('order-customer').textContent = d.current_order.customer_name;
      document.getElementById('order-address').textContent = d.current_order.delivery_address;
      document.getElementById('order-total').textContent = `${d.current_order.total_amount} TND`;
      els.orderInfo.classList.remove('hidden');
    } else {
      els.orderInfo.classList.add('hidden');
    }
  } catch (err) {
    logEvent(`Erreur GraphQL driver : ${err.message}`);
  }
}

// Simulateur : envoie des PATCH /api/drivers/:id/location toutes les 1.5s
// avec une petite marche aleatoire autour de Sousse, pour faire bouger la carte
// sans avoir a appeler Postman a la main.
function startSimulator() {
  if (!currentDriverId) return;
  let lat = SOUSSE[0];
  let lng = SOUSSE[1];
  els.simBtn.textContent = 'Arreter simulateur';
  els.simBtn.classList.add('active');
  logEvent('Simulateur demarre');
  simulatorTimer = setInterval(async () => {
    lat += (Math.random() - 0.5) * 0.002;
    lng += (Math.random() - 0.5) * 0.002;
    try {
      await fetch(`${GATEWAY}/api/drivers/${currentDriverId}/location`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          latitude: lat,
          longitude: lng,
          speed_kmh: 20 + Math.random() * 30,
          heading_deg: Math.random() * 360,
        }),
      });
    } catch (err) {
      logEvent(`Sim error : ${err.message}`);
    }
  }, 1500);
}

function stopSimulator() {
  if (simulatorTimer) {
    clearInterval(simulatorTimer);
    simulatorTimer = null;
    els.simBtn.textContent = 'Demarrer simulateur';
    els.simBtn.classList.remove('active');
    logEvent('Simulateur arrete');
  }
}

loadDrivers();
