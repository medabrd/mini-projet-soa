// =========================================================================
//  drivers-repo.js - Couche metier + acces donnees (RxDB) pour drivers
// =========================================================================
//
//  Role : equivalent d'orders-repo.js pour order-service, mais avec RxDB.
//  Toutes les fonctions sont ASYNC (vs sync en SQLite) parce que RxDB
//  l'est entierement.
//
//  Pattern queries RxDB vs SQL :
//    SQLite :    db.prepare('SELECT ...').get(id)         -> sync
//    RxDB   :    await db.drivers.findOne(id).exec()       -> async
//
//    SQLite :    db.prepare('UPDATE ...').run(...)
//    RxDB   :    await doc.patch({ ... })                  -> mute le doc
//
//    SQLite :    db.prepare('INSERT ...').run(...)
//    RxDB   :    await db.drivers.insert({ ... })
//
//    SQLite :    db.prepare('DELETE ...').run(id)
//    RxDB   :    await doc.remove()
// =========================================================================

const { v4: uuidv4 } = require('uuid');
const { getDb } = require('./db');


function nowIso() {
  return new Date().toISOString();
}


// --- Mapper : RxDB document -> objet metier Driver -----------------------
//
// Pattern equivalent du rowToOrder d'order-service. Convertit la forme RxDB
// (peut etre un RxDocument avec methodes, ou un POJO selon contexte) en
// l'objet plat attendu par le proto Driver.
//
// 2 jobs :
//   1. Appeler toJSON() si c'est un RxDocument (le retire les wrappers RxDB)
//   2. Normaliser les champs nullables : Protobuf n'accepte pas NULL pour
//      les string, on default a "". Idem last_location : null si jamais set.
function docToDriver(doc) {
  if (!doc) return null;
  // Selon le contexte, on peut recevoir un RxDocument (avec toJSON) ou un
  // POJO deja serialise. On gere les 2.
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


// --- CRUD basique --------------------------------------------------------

// Cree un nouveau driver. status defaut = AVAILABLE.
// Le service publie un trigger d'auto-assignation a la file d'attente
// juste apres (cf grpc-server.js > RegisterDriver).
async function registerDriver({ name, phone, vehicle_type }) {
  // Validation metier minimale : nom obligatoire.
  // (Le schema RxDB ne le force pas car name n'est pas dans `required`
  // pour rester flexible, donc on valide ici.)
  if (!name) {
    throw new Error('Le nom du livreur est requis');
  }
  const db = getDb();
  const id = uuidv4();
  const now = nowIso();
  // insert : equivalent INSERT. Renvoie le RxDocument cree.
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
  // findOne(id) : raccourci RxDB pour selectionner par primaryKey.
  // .exec() : execute la query (RxDB renvoie un objet "query builder",
  // pas le doc directement - cf pattern lazy d'ORM).
  const doc = await db.drivers.findOne(id).exec();
  return docToDriver(doc);
}


async function listAvailableDrivers(limit = 50) {
  const db = getDb();
  // .find() avec selector (subset de MongoDB query syntax) :
  //   { status: 'AVAILABLE' }   =   WHERE status = 'AVAILABLE'
  //   limit                     =   LIMIT
  const docs = await db.drivers
    .find({ selector: { status: 'AVAILABLE' }, limit })
    .exec();
  return {
    drivers: docs.map(docToDriver),
    total: docs.length,
  };
}


// --- Updates -------------------------------------------------------------

// Met a jour la position GPS d'un driver.
// IMPORTANT : doc.patch() = "mutation tracked" RxDB. Tous les abonnes
// (cf StreamDriverLocation) sont automatiquement notifies via l'observable.
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
  // Re-fetch pour avoir l'etat fresh post-patch (le doc initial peut etre stale).
  const fresh = await db.drivers.findOne(driverId).exec();
  return fresh.last_location;
}


// Assigne un driver a une commande : passage AVAILABLE -> BUSY.
// Appelee par kafka.js > handleOrderEvent (auto-assignation) ou
// kafka.js > tryAssignToPending (queue).
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


// Selectionne le PREMIER driver disponible. Utilise par l'auto-assignation
// quand une commande arrive : on prend "n'importe lequel" dispo.
// Pas de tri intelligent (proximite GPS, anciennete, etc.) : simplifie
// pour notre projet. En vraie prod on aurait un algo de matching.
async function pickAvailableDriver() {
  const db = getDb();
  const doc = await db.drivers
    .findOne({ selector: { status: 'AVAILABLE' } })
    .exec();
  return docToDriver(doc);
}


// Libere un driver quand sa livraison est terminee ou annulee.
// BUSY -> AVAILABLE, current_order_id vide. Appelee par kafka.js >
// handleDeliveryEvent sur delivery.delivered / delivery.cancelled.
// Idempotente : si le driver n'est pas BUSY (deja libere), on retourne
// l'etat actuel sans rien faire.
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


// --- Suppression ---------------------------------------------------------

// Supprime un driver. REFUSE si BUSY pour eviter qu'une livraison en cours
// pointe vers un driver inexistant (orphelin cote tracking-service).
//
// Pattern de retour { deleted, reason } : evite les exceptions pour gerer
// les cas "predits" (non trouve, occupe). grpc-server.js mappe en codes
// gRPC adequats.
async function deleteDriver(id) {
  const db = getDb();
  const doc = await db.drivers.findOne(id).exec();
  if (!doc) return { deleted: false, reason: 'not_found' };
  if (doc.status === 'BUSY') return { deleted: false, reason: 'busy' };
  // remove() : suppression du document. Notifie aussi les observables
  // (utile si quelqu'un ecoutait via StreamDriverLocation - le stream
  // recevra un emit avec undefined puis se fermera).
  await doc.remove();
  return { deleted: true };
}


module.exports = {
  registerDriver,
  getDriver,
  listAvailableDrivers,
  updateLocation,
  assignDriverToOrder,
  pickAvailableDriver,
  releaseDriver,
  deleteDriver,
};
