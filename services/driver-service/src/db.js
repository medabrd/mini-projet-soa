const { createRxDatabase, addRxPlugin } = require('rxdb');
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');
const { RxDBDevModePlugin } = require('rxdb/plugins/dev-mode');

// dev-mode active des validations supplementaires en developpement
if (process.env.NODE_ENV !== 'production') {
  addRxPlugin(RxDBDevModePlugin);
}

const driverSchema = {
  version: 0,
  primaryKey: 'id',
  type: 'object',
  properties: {
    id: { type: 'string', maxLength: 100 },
    name: { type: 'string' },
    phone: { type: 'string' },
    vehicle_type: { type: 'string' },
    status: { type: 'string', enum: ['AVAILABLE', 'BUSY', 'OFFLINE'] },
    current_order_id: { type: 'string' },
    last_location: {
      type: 'object',
      properties: {
        driver_id: { type: 'string' },
        latitude: { type: 'number' },
        longitude: { type: 'number' },
        speed_kmh: { type: 'number' },
        heading_deg: { type: 'number' },
        timestamp: { type: 'string' },
      },
    },
    created_at: { type: 'string' },
    updated_at: { type: 'string' },
  },
  required: ['id', 'name', 'status', 'created_at', 'updated_at'],
};

let db = null;

async function initDatabase() {
  db = await createRxDatabase({
    name: 'driverdb',
    storage: getRxStorageMemory(),
    ignoreDuplicate: true,
  });

  await db.addCollections({
    drivers: { schema: driverSchema },
  });

  console.log('Base RxDB initialisee (storage memory)');
  return db;
}

function getDb() {
  if (!db) {
    throw new Error('Base non initialisee, appeler initDatabase() avant');
  }
  return db;
}

module.exports = { initDatabase, getDb };
