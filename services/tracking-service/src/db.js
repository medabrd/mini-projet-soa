const sqlite = require('node:sqlite');
const path = require('path');
const fs = require('fs');

const DATA_DIR = path.join(__dirname, '..', 'data');
const DB_PATH = path.join(DATA_DIR, 'tracking.sqlite');

let db = null;

function initDatabase() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
  }

  db = new sqlite.DatabaseSync(DB_PATH);

  db.exec(`
    CREATE TABLE IF NOT EXISTS deliveries (
      id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL UNIQUE,
      customer_id TEXT,
      customer_name TEXT,
      delivery_address TEXT,
      driver_id TEXT,
      driver_name TEXT,
      status TEXT NOT NULL DEFAULT 'PENDING_ASSIGNMENT',
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS delivery_events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      delivery_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      event_data_json TEXT,
      created_at TEXT NOT NULL,
      FOREIGN KEY (delivery_id) REFERENCES deliveries(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
    CREATE INDEX IF NOT EXISTS idx_deliveries_driver ON deliveries(driver_id);
    CREATE INDEX IF NOT EXISTS idx_deliveries_order ON deliveries(order_id);
    CREATE INDEX IF NOT EXISTS idx_events_delivery ON delivery_events(delivery_id);
  `);

  console.log(`Base SQLite initialisee: ${DB_PATH}`);
  return db;
}

function getDb() {
  if (!db) {
    throw new Error('Base non initialisee, appeler initDatabase() avant');
  }
  return db;
}

module.exports = { initDatabase, getDb };
