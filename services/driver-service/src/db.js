// =========================================================================
//  db.js - Couche persistance du microservice driver-service
// =========================================================================
//
//  Role : initialiser RxDB en memoire et exposer l'instance aux autres
//  modules. Equivalent du db.js d'order-service mais avec une techno
//  TOTALEMENT differente.
//
//  Pourquoi RxDB et pas SQLite comme les 2 autres services ?
//  ----------------------------------------------------------
//  Le cahier de charges autorise UNIQUEMENT SQLite3 OU RxDB. On veut
//  utiliser les deux pour demontrer qu'on sait. Driver-service est le
//  candidat ideal pour RxDB parce que :
//
//    1. Ses donnees sont CHAUDES (position GPS qui change toutes les sec)
//       -> RxDB est optimise pour les modifications frequentes.
//    2. On a besoin du SERVER-STREAMING StreamDriverLocation : RxDB fournit
//       des OBSERVABLES natifs (cf $.subscribe() dans grpc-server.js).
//       En SQLite il faudrait poller en boucle, beaucoup plus moche.
//    3. Pas besoin de durabilite forte (un livreur perdu au restart =
//       re-enregistrement en 5 sec). storage-memory = parfait, ultra rapide.
//
//  Le storage memory perd tout au restart du conteneur. Volontaire. Cf
//  README.md > "Notes de conception" > "Pourquoi RxDB pour driver-service".
// =========================================================================

const { createRxDatabase, addRxPlugin } = require('rxdb');

// Storage memory = donnees stockees dans la RAM du process Node, jamais
// ecrites sur disque. Tres rapide, mais perdues au restart.
// Alternative : 'rxdb/plugins/storage-dexie' (IndexedDB pour navigateur),
// 'rxdb/plugins/storage-sqlite' (persistance disque). Pas notre cas.
const { getRxStorageMemory } = require('rxdb/plugins/storage-memory');

// Plugin de dev : ajoute des verifications supplementaires (schema strict,
// detection d'usage incorrect, etc.). A activer seulement en developpement
// car il a un cout en perf.
const { RxDBDevModePlugin } = require('rxdb/plugins/dev-mode');

if (process.env.NODE_ENV !== 'production') {
  addRxPlugin(RxDBDevModePlugin);
}


// --- Schema RxDB pour les drivers ----------------------------------------
//
// RxDB exige un schema JSON Schema pour chaque collection. Sert a :
//   - valider les inserts/updates (refuse si forme incorrecte)
//   - documenter la structure des documents
//   - generer un typage si on utilise TypeScript (pas notre cas)
//
// Notez la difference avec SQLite :
//   - SQLite : CREATE TABLE + colonnes typees
//   - RxDB   : JSON Schema (plus flexible mais plus verbeux)
//
// version: 0 = version initiale du schema. Si on modifie la forme plus
// tard, on incrementerait et RxDB demanderait une migration.
const driverSchema = {
  version: 0,
  primaryKey: 'id',                   // champ id sert d'identifiant unique
  type: 'object',
  properties: {
    id: { type: 'string', maxLength: 100 },  // maxLength obligatoire pour primaryKey
    name: { type: 'string' },
    phone: { type: 'string' },
    vehicle_type: { type: 'string' },
    status: { type: 'string', enum: ['AVAILABLE', 'BUSY', 'OFFLINE'] }, // validation enum
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
  // Champs OBLIGATOIRES (rejet de l'insert si absents)
  required: ['id', 'name', 'status', 'created_at', 'updated_at'],
};


// Singleton de l'instance RxDB (idem pattern que order-service db.js)
let db = null;


// --- Initialisation -------------------------------------------------------
//
// ASYNC (vs sync en SQLite) : RxDB est entierement asynchrone. Une fois
// initialisee, l'instance reste en memoire pour toute la vie du process.
async function initDatabase() {
  // createRxDatabase = factory async qui cree l'instance.
  // ignoreDuplicate: true autorise un appel multiple en dev (--watch) sans
  // throw si une instance existe deja avec le meme nom.
  db = await createRxDatabase({
    name: 'driverdb',
    storage: getRxStorageMemory(),
    ignoreDuplicate: true,
  });

  // Une "collection" = l'equivalent d'une table SQL. On en cree 1 seule
  // ici (drivers). Le schema est attache a ce moment-la.
  await db.addCollections({
    drivers: { schema: driverSchema },
  });

  console.log('Base RxDB initialisee (storage memory)');
  return db;
}


// Accesseur singleton, identique a celui d'order-service.
function getDb() {
  if (!db) {
    throw new Error('Base non initialisee, appeler initDatabase() avant');
  }
  return db;
}


module.exports = { initDatabase, getDb };
