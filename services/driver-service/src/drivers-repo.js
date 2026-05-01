const { v4: uuidv4 } = require('uuid');
const { getDb } = require('./db');

function nowIso() {
  return new Date().toISOString();
}

function docToDriver(doc) {
  if (!doc) return null;
  const j = doc.toJSON ? doc.toJSON() : doc;
  return {
    id: j.id,
    name: j.name || '',
    phone: j.phone || '',
    vehicle_type: j.vehicle_type || '',
    status: j.status || 'OFFLINE',
    current_order_id: j.current_order_id || '',
    last_location: j.last_location || null,
    created_at: j.created_at,
    updated_at: j.updated_at,
  };
}

async function registerDriver({ name, phone, vehicle_type }) {
  if (!name) {
    throw new Error('Le nom du livreur est requis');
  }
  const db = getDb();
  const id = uuidv4();
  const now = nowIso();
  const doc = await db.drivers.insert({
    id,
    name,
    phone: phone || '',
    vehicle_type: vehicle_type || '',
    status: 'AVAILABLE',
    current_order_id: '',
    last_location: null,
    created_at: now,
    updated_at: now,
  });
  return docToDriver(doc);
}

async function getDriver(id) {
  const db = getDb();
  const doc = await db.drivers.findOne(id).exec();
  return docToDriver(doc);
}

async function listAvailableDrivers(limit = 50) {
  const db = getDb();
  const docs = await db.drivers
    .find({ selector: { status: 'AVAILABLE' }, limit })
    .exec();
  return {
    drivers: docs.map(docToDriver),
    total: docs.length,
  };
}

async function updateLocation(driverId, location) {
  const db = getDb();
  const doc = await db.drivers.findOne(driverId).exec();
  if (!doc) return null;
  await doc.patch({
    last_location: {
      driver_id: driverId,
      latitude: Number(location.latitude),
      longitude: Number(location.longitude),
      speed_kmh: Number(location.speed_kmh) || 0,
      heading_deg: Number(location.heading_deg) || 0,
      timestamp: nowIso(),
    },
    updated_at: nowIso(),
  });
  const fresh = await db.drivers.findOne(driverId).exec();
  return fresh.last_location;
}

async function assignDriverToOrder(driverId, orderId) {
  const db = getDb();
  const doc = await db.drivers.findOne(driverId).exec();
  if (!doc) return null;
  await doc.patch({
    status: 'BUSY',
    current_order_id: orderId,
    updated_at: nowIso(),
  });
  const fresh = await db.drivers.findOne(driverId).exec();
  return docToDriver(fresh);
}

async function pickAvailableDriver() {
  const db = getDb();
  const doc = await db.drivers
    .findOne({ selector: { status: 'AVAILABLE' } })
    .exec();
  return docToDriver(doc);
}

// Libere un livreur quand sa livraison est terminee ou annulee :
// status repasse a AVAILABLE et current_order_id est vide.
async function releaseDriver(driverId) {
  const db = getDb();
  const doc = await db.drivers.findOne(driverId).exec();
  if (!doc) return null;
  if (doc.status !== 'BUSY') return docToDriver(doc);
  await doc.patch({
    status: 'AVAILABLE',
    current_order_id: '',
    updated_at: nowIso(),
  });
  const fresh = await db.drivers.findOne(driverId).exec();
  return docToDriver(fresh);
}

module.exports = {
  registerDriver,
  getDriver,
  listAvailableDrivers,
  updateLocation,
  assignDriverToOrder,
  pickAvailableDriver,
  releaseDriver,
};
