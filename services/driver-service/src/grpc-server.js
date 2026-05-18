// =========================================================================
//  grpc-server.js - Couche API du microservice driver-service
// =========================================================================
//
//  Role : implementer concretement les RPCs declares dans driver.proto.
//  Similar a order-service et tracking-service, AVEC 2 specificites :
//
//    1. Tous les handlers sont ASYNC (vs sync pour order-service) parce
//       que le repo l'est (cf RxDB explication dans drivers-repo.js).
//
//    2. UN RPC SERVER-STREAMING : StreamDriverLocation, qui exploite les
//       OBSERVABLES NATIFS de RxDB pour pousser les positions GPS au
//       client gRPC chaque fois que le document driver change.
//
//  La grosse difference avec WatchDelivery (tracking-service) :
//    - tracking-service a du construire un EventEmitter manuellement
//      (parce que SQLite n'a pas d'observables)
//    - driver-service utilise directement RxDB.$ qui EST un observable
//      RxJS pret a l'emploi. Plus elegant, moins de code.
//
//  Pour les conventions communes (loadProto, codes erreur gRPC, bind 0.0.0.0),
//  voir les commentaires de services/order-service/src/grpc-server.js.
// =========================================================================

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const repo = require('./drivers-repo');
const kafka = require('./kafka');
// On a besoin d'access direct a la base pour le streaming observable
// (le repo n'expose pas l'observable, on bypasse).
const { getDb } = require('./db');

const PROTO_PATH = path.resolve(__dirname, '..', '..', '..', 'proto', 'driver.proto');

function loadProto() {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(packageDefinition).driver;
}

function notFoundError(message = 'Livreur introuvable') {
  return { code: grpc.status.NOT_FOUND, message };
}

function internalError(err) {
  return { code: grpc.status.INTERNAL, message: err.message || 'Erreur interne' };
}


const handlers = {

  // RegisterDriver : note le pattern callback PUIS auto-assignation async.
  // On REPOND tout de suite au client (callback null, driver), puis on
  // declenche en arriere-plan tryAssignToPending qui va lui assigner
  // potentiellement une commande en attente. Le .catch() final empeche
  // une erreur de l'auto-assign de faire crasher le service.
  //
  // Pourquoi cette sequence et pas await + callback ? Parce que :
  //   - le client n'a pas besoin d'attendre l'auto-assignation pour avoir
  //     son driver cree (UX plus snappy)
  //   - si la queue est vide, ca evite un round-trip Kafka inutile
  RegisterDriver: async (call, callback) => {
    try {
      const driver = await repo.registerDriver(call.request);
      callback(null, driver);
      kafka.tryAssignToPending(driver).catch(err =>
        console.error('Echec auto-assignation queue:', err.message)
      );
    } catch (err) {
      // Erreur typique : nom manquant (validation cote repo).
      callback({ code: grpc.status.INVALID_ARGUMENT, message: err.message });
    }
  },

  GetDriver: async (call, callback) => {
    try {
      const driver = await repo.getDriver(call.request.id);
      if (!driver) return callback(notFoundError());
      callback(null, driver);
    } catch (err) {
      callback(internalError(err));
    }
  },

  ListAvailableDrivers: async (call, callback) => {
    try {
      const result = await repo.listAvailableDrivers(call.request.limit);
      callback(null, result);
    } catch (err) {
      callback(internalError(err));
    }
  },

  // UpdateLocation : sauve la position + publie un event Kafka driver.location-updated.
  // L'observable RxDB (cf StreamDriverLocation) est automatiquement notifie
  // par le patch() interne du repo - pas besoin de declencher manuellement.
  UpdateLocation: async (call, callback) => {
    try {
      const location = await repo.updateLocation(call.request.driver_id, call.request);
      if (!location) return callback(notFoundError());
      // Fire-and-forget Kafka (consomme par tracking-service pour ajouter
      // une ligne dans l'historique delivery_events).
      kafka.publishLocationUpdated(call.request.driver_id, location);
      callback(null, location);
    } catch (err) {
      callback(internalError(err));
    }
  },


  // ----- StreamDriverLocation : SERVER-STREAMING via OBSERVABLE RxDB -----
  //
  //  C'est le coeur de pourquoi on a choisi RxDB pour ce service.
  //  RxDB expose chaque query comme un Observable RxJS via le getter ".$".
  //  Subscriber a cet observable = etre notifie a chaque modification.
  //
  //  Sequence :
  //    1. Valide driver_id et existence du driver
  //    2. Si position deja connue, push tout de suite (replay state)
  //    3. Subscribe au document via findOne(id).$ -> stream RxJS
  //    4. A chaque emit, on push la nouvelle last_location au client gRPC
  //    5. Quand le client deconnecte, on unsubscribe pour eviter le leak
  //
  //  vs tracking-service WatchDelivery (qui utilise un EventEmitter custom) :
  //    - Ici on a 0 ligne de code pour gerer le bus -> RxDB le fait nativement
  //    - On filtre PAS par id : findOne(id).$ ne notifie que pour ce doc precis
  // ---------------------------------------------------------------------
  StreamDriverLocation: async (call) => {
    const driverId = call.request.driver_id;
    if (!driverId) {
      call.emit('error', { code: grpc.status.INVALID_ARGUMENT, message: 'driver_id requis' });
      return;
    }

    const db = getDb();
    const driverDoc = await db.drivers.findOne(driverId).exec();
    if (!driverDoc) {
      call.emit('error', notFoundError());
      return;
    }

    // Replay : envoie la position courante si elle existe deja
    // (cf pattern "replay current state then subscribe" dans WatchDelivery).
    if (driverDoc.last_location) {
      call.write(driverDoc.last_location);
    }

    // S'abonner aux changements futurs.
    //   db.drivers.findOne(driverId)   -> query (lazy)
    //   .$                              -> getter qui retourne un Observable RxJS
    //   .subscribe(callback)            -> appelle callback a chaque emit
    //
    // L'observable emit :
    //   - au moins une fois immediatement avec l'etat courant (current value)
    //   - puis a chaque modification du document
    //   - undefined quand le document est supprime
    const sub = db.drivers
      .findOne(driverId)
      .$.subscribe(doc => {
        if (doc && doc.last_location) {
          try {
            call.write(doc.last_location);
          } catch (e) {
            // Connexion probablement fermee entre 2 emits.
            // On absorbe silencieusement, le cleanup s'occupera du reste.
          }
        }
      });

    // Nettoyage : unsubscribe quand le client se deconnecte.
    // Sans ca, l'observable garde la reference au call -> memory leak.
    // RxJS Subscription.unsubscribe() est idempotent : safe d'appeler 2 fois.
    const cleanup = () => {
      try { sub.unsubscribe(); } catch (_) {}
    };
    call.on('cancelled', cleanup);
    call.on('close', cleanup);
    call.on('end', cleanup);
  },


  DeleteDriver: async (call, callback) => {
    try {
      // Pattern { deleted, reason } : evite les exceptions pour cas predits.
      const result = await repo.deleteDriver(call.request.id);
      if (!result.deleted) {
        if (result.reason === 'not_found') return callback(notFoundError());
        if (result.reason === 'busy') {
          // Driver en cours de livraison -> on refuse explicitement
          // pour proteger la coherence cross-service.
          return callback({
            code: grpc.status.FAILED_PRECONDITION,
            message: 'Livreur BUSY : impossible de supprimer un livreur en cours de livraison',
          });
        }
      }
      callback(null, { deleted: true });
    } catch (err) {
      callback(internalError(err));
    }
  },
};


function startGrpcServer(port) {
  const driverProto = loadProto();
  const server = new grpc.Server();
  server.addService(driverProto.DriverService.service, handlers);

  const address = `0.0.0.0:${port}`;
  server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
    if (err) {
      console.error('Echec du bind:', err);
      process.exit(1);
    }
    console.log(`driver-service ecoute sur le port ${boundPort} (gRPC)`);
  });
}

module.exports = { startGrpcServer };
