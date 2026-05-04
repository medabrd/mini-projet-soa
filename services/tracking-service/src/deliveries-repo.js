const { v4: uuidv4 } = require('uuid');
const EventEmitter = require('node:events');
const { getDb } = require('./db');

// Bus interne d'evenements pour notifier le streaming WatchDelivery
// quand une delivery change de statut.
const bus = new EventEmitter();
bus.setMaxListeners(0); // pas de limite

function nowIso() {
  return new Date().toISOString();
}

function rowToDelivery(row) {
  if (!row) return null;
  return {
    id: row.id,
    order_id: row.order_id,
    customer_id: row.customer_id || '',
    customer_name: row.customer_name || '',
    delivery_address: row.delivery_address || '',
    driver_id: row.driver_id || '',
    driver_name: row.driver_name || '',
    status: row.status,
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function rowToEvent(row) {
  return {
    id: row.id,
    delivery_id: row.delivery_id,
    event_type: row.event_type,
    event_data_json: row.event_data_json || '',
    created_at: row.created_at,
  };
}

function logEvent(deliveryId, eventType, eventData) {
  const db = getDb();
  db.prepare(
    'INSERT INTO delivery_events (delivery_id, event_type, event_data_json, created_at) VALUES (?, ?, ?, ?)',
  ).run(deliveryId, eventType, JSON.stringify(eventData || {}), nowIso());
}

// Cree une delivery a partir d'un event order.placed (idempotent : si une delivery existe deja
// pour ce order_id, on la renvoie sans en recreer).
function createDeliveryFromOrder(orderEvt) {
  const db = getDb();
  const existing = db
    .prepare('SELECT * FROM deliveries WHERE order_id = ?')
    .get(orderEvt.order_id);
  if (existing) {
    return rowToDelivery(existing);
  }

  const id = uuidv4();
  const now = nowIso();
  db.prepare(`
    INSERT INTO deliveries (id, order_id, customer_id, customer_name, delivery_address, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 'PENDING_ASSIGNMENT', ?, ?)
  `).run(
    id,
    orderEvt.order_id,
    orderEvt.customer_id || '',
    orderEvt.customer_name || '',
    orderEvt.delivery_address || '',
    now,
    now,
  );
  logEvent(id, 'created', { source: 'order.placed', order_id: orderEvt.order_id });

  const fresh = getDelivery(id);
  bus.emit('delivery-changed', fresh);
  return fresh;
}

function attachDriver(orderId, driverId, driverName) {
  const db = getDb();
  const existing = db.prepare('SELECT * FROM deliveries WHERE order_id = ?').get(orderId);
  if (!existing) {
    // Race condition possible : driver.assigned arrive avant order.placed.
    // On cree une coquille, l'event order.placed completera plus tard.
    const id = uuidv4();
    const now = nowIso();
    db.prepare(`
      INSERT INTO deliveries (id, order_id, driver_id, driver_name, status, created_at, updated_at)
      VALUES (?, ?, ?, ?, 'ASSIGNED', ?, ?)
    `).run(id, orderId, driverId, driverName || '', now, now);
    logEvent(id, 'assigned', { driver_id: driverId, driver_name: driverName });
    const fresh = getDelivery(id);
    bus.emit('delivery-changed', fresh);
    return fresh;
  }

  // Filet de securite : ne JAMAIS reactiver une delivery deja terminee
  // (CANCELLED ou DELIVERED). Sinon un driver assigne par erreur a une
  // commande annulee la ressusciterait via la cascade Kafka.
  if (existing.status === 'CANCELLED' || existing.status === 'DELIVERED') {
    console.warn(`attachDriver ignore : delivery ${existing.id} status=${existing.status}`);
    return null;
  }

  db.prepare(
    "UPDATE deliveries SET driver_id = ?, driver_name = ?, status = 'ASSIGNED', updated_at = ? WHERE id = ?",
  ).run(driverId, driverName || '', nowIso(), existing.id);
  logEvent(existing.id, 'assigned', { driver_id: driverId, driver_name: driverName });

  const updated = getDelivery(existing.id);
  bus.emit('delivery-changed', updated);
  return updated;
}

function logLocationUpdate(driverId, location) {
  const db = getDb();
  // On retrouve les deliveries actives associees a ce driver
  const rows = db
    .prepare(
      "SELECT id FROM deliveries WHERE driver_id = ? AND status IN ('ASSIGNED', 'PICKED_UP', 'IN_TRANSIT')",
    )
    .all(driverId);
  for (const row of rows) {
    logEvent(row.id, 'location-update', location);
  }
}

function cancelDelivery(orderId) {
  const db = getDb();
  const existing = db.prepare('SELECT * FROM deliveries WHERE order_id = ?').get(orderId);
  if (!existing) return null;
  db.prepare("UPDATE deliveries SET status = 'CANCELLED', updated_at = ? WHERE id = ?").run(
    nowIso(),
    existing.id,
  );
  logEvent(existing.id, 'cancelled', {});

  const updated = getDelivery(existing.id);
  bus.emit('delivery-changed', updated);
  return updated;
}

function advanceStatus(deliveryId, newStatus) {
  const allowed = ['PICKED_UP', 'IN_TRANSIT', 'DELIVERED', 'CANCELLED'];
  if (!allowed.includes(newStatus)) {
    throw new Error(`Statut non autorise (utiliser ${allowed.join(', ')})`);
  }
  const db = getDb();
  const existing = db.prepare('SELECT * FROM deliveries WHERE id = ?').get(deliveryId);
  if (!existing) return null;

  db.prepare('UPDATE deliveries SET status = ?, updated_at = ? WHERE id = ?').run(
    newStatus,
    nowIso(),
    deliveryId,
  );
  logEvent(deliveryId, 'status-advanced', { from: existing.status, to: newStatus });

  const updated = getDelivery(deliveryId);
  bus.emit('delivery-changed', updated);
  return updated;
}

function getDelivery(id) {
  const db = getDb();
  const row = db.prepare('SELECT * FROM deliveries WHERE id = ?').get(id);
  return rowToDelivery(row);
}

function getDeliveryByOrder(orderId) {
  const db = getDb();
  const row = db.prepare('SELECT * FROM deliveries WHERE order_id = ?').get(orderId);
  return rowToDelivery(row);
}

function listDeliveries({ status, driver_id, limit, offset } = {}) {
  const db = getDb();
  const where = [];
  const params = [];
  if (status && status !== 'DELIVERY_STATUS_UNSPECIFIED') {
    where.push('status = ?');
    params.push(status);
  }
  if (driver_id) {
    where.push('driver_id = ?');
    params.push(driver_id);
  }
  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
  const limitNum = Number(limit) > 0 ? Number(limit) : 100;
  const offsetNum = Number(offset) > 0 ? Number(offset) : 0;

  const rows = db
    .prepare(`SELECT * FROM deliveries ${whereSql} ORDER BY created_at DESC LIMIT ${limitNum} OFFSET ${offsetNum}`)
    .all(...params);
  const totalRow = db
    .prepare(`SELECT COUNT(*) as total FROM deliveries ${whereSql}`)
    .get(...params);

  return { deliveries: rows.map(rowToDelivery), total: totalRow.total };
}

function getHistory(deliveryId) {
  const db = getDb();
  const rows = db
    .prepare('SELECT * FROM delivery_events WHERE delivery_id = ? ORDER BY id ASC')
    .all(deliveryId);
  return { events: rows.map(rowToEvent) };
}

module.exports = {
  bus,
  createDeliveryFromOrder,
  attachDriver,
  logLocationUpdate,
  cancelDelivery,
  advanceStatus,
  getDelivery,
  getDeliveryByOrder,
  listDeliveries,
  getHistory,
};
