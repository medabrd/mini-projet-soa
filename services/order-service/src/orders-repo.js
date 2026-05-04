const { v4: uuidv4 } = require('uuid');
const { getDb } = require('./db');

function nowIso() {
  return new Date().toISOString();
}

function rowToOrder(row, items) {
  return {
    id: row.id,
    customer_id: row.customer_id,
    customer_name: row.customer_name,
    delivery_address: row.delivery_address,
    items: items || [],
    total_amount: row.total_amount,
    status: row.status,
    assigned_driver_id: row.assigned_driver_id || '',
    created_at: row.created_at,
    updated_at: row.updated_at,
  };
}

function getItemsForOrder(orderId) {
  const db = getDb();
  return db
    .prepare('SELECT product_name, quantity, unit_price FROM order_items WHERE order_id = ?')
    .all(orderId);
}

function createOrder({ customer_id, customer_name, delivery_address, items }) {
  if (!items || items.length === 0) {
    throw new Error('La commande doit contenir au moins un article');
  }

  const db = getDb();
  const id = uuidv4();
  const now = nowIso();
  const total = items.reduce(
    (sum, i) => sum + Number(i.quantity) * Number(i.unit_price),
    0,
  );

  const insertOrder = db.prepare(`
    INSERT INTO orders (id, customer_id, customer_name, delivery_address, total_amount, status, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, 'PENDING', ?, ?)
  `);
  const insertItem = db.prepare(`
    INSERT INTO order_items (order_id, product_name, quantity, unit_price)
    VALUES (?, ?, ?, ?)
  `);

  // Transaction manuelle (node:sqlite n'a pas l'API db.transaction de better-sqlite3)
  db.exec('BEGIN');
  try {
    insertOrder.run(id, customer_id, customer_name, delivery_address, total, now, now);
    for (const item of items) {
      insertItem.run(id, item.product_name, Number(item.quantity), Number(item.unit_price));
    }
    db.exec('COMMIT');
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }

  return getOrder(id);
}

function getOrder(id) {
  const db = getDb();
  const row = db.prepare('SELECT * FROM orders WHERE id = ?').get(id);
  if (!row) return null;
  return rowToOrder(row, getItemsForOrder(id));
}

function listOrders({ customer_id, status, limit, offset } = {}) {
  const db = getDb();
  const where = [];
  const params = [];

  if (customer_id) {
    where.push('customer_id = ?');
    params.push(customer_id);
  }
  if (status && status !== 'ORDER_STATUS_UNSPECIFIED') {
    where.push('status = ?');
    params.push(status);
  }

  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
  const limitNum = Number(limit) > 0 ? Number(limit) : 100;
  const offsetNum = Number(offset) > 0 ? Number(offset) : 0;

  const rows = db
    .prepare(`SELECT * FROM orders ${whereSql} ORDER BY created_at DESC LIMIT ${limitNum} OFFSET ${offsetNum}`)
    .all(...params);
  const totalRow = db
    .prepare(`SELECT COUNT(*) as total FROM orders ${whereSql}`)
    .get(...params);

  const orders = rows.map(row => rowToOrder(row, getItemsForOrder(row.id)));
  return { orders, total: totalRow.total };
}

function updateOrderStatus(id, status, assigned_driver_id) {
  const db = getDb();
  const now = nowIso();

  const existing = db.prepare('SELECT id FROM orders WHERE id = ?').get(id);
  if (!existing) return null;

  if (assigned_driver_id) {
    db.prepare(
      'UPDATE orders SET status = ?, assigned_driver_id = ?, updated_at = ? WHERE id = ?',
    ).run(status, assigned_driver_id, now, id);
  } else {
    db.prepare('UPDATE orders SET status = ?, updated_at = ? WHERE id = ?').run(status, now, id);
  }

  return getOrder(id);
}

function cancelOrder(id) {
  const db = getDb();
  const now = nowIso();

  const existing = db.prepare('SELECT id, status FROM orders WHERE id = ?').get(id);
  if (!existing) return null;
  if (existing.status === 'DELIVERED') {
    throw new Error("Impossible d'annuler une commande deja livree");
  }

  db.prepare('UPDATE orders SET status = ?, updated_at = ? WHERE id = ?').run(
    'CANCELLED',
    now,
    id,
  );

  return getOrder(id);
}

// Suppression definitive d'une commande. Autorisee uniquement si l'order
// est dans un etat final (DELIVERED ou CANCELLED) pour eviter de supprimer
// une commande active dont la chaine Kafka serait en cours.
function deleteOrder(id) {
  const db = getDb();
  const existing = db.prepare('SELECT id, status FROM orders WHERE id = ?').get(id);
  if (!existing) return { deleted: false, reason: 'not_found' };
  if (!['DELIVERED', 'CANCELLED'].includes(existing.status)) {
    return { deleted: false, reason: 'not_final' };
  }
  db.prepare('DELETE FROM order_items WHERE order_id = ?').run(id);
  db.prepare('DELETE FROM orders WHERE id = ?').run(id);
  return { deleted: true };
}

module.exports = {
  createOrder,
  getOrder,
  listOrders,
  updateOrderStatus,
  cancelOrder,
  deleteOrder,
};
