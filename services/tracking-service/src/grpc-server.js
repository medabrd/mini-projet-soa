// =========================================================================
//  grpc-server.js - Couche API du microservice tracking-service
// =========================================================================
//
//  Role : implementer concretement les RPCs declares dans tracking.proto.
//  Comme order-service/grpc-server.js, mais avec 2 specificites :
//    1. PAS de CreateDelivery (les deliveries sont creees automatiquement
//       depuis les events Kafka -> cf kafka.js).
//    2. UN RPC SERVER-STREAMING : WatchDelivery (le client garde la
//       connexion ouverte, on push chaque changement de la delivery).
//
//  Pour les conventions communes (loadProto, codes d'erreur gRPC, pattern
//  handlers unary, bind 0.0.0.0, etc.), voir les commentaires detailles
//  dans services/order-service/src/grpc-server.js.
// =========================================================================

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const repo = require('./deliveries-repo');

// Chemin vers tracking.proto a la racine du repo (idem ordre-service mais
// avec le bon nom de fichier).
const PROTO_PATH = path.resolve(__dirname, '..', '..', '..', 'proto', 'tracking.proto');

function loadProto() {
  // Memes options que dans order-service : keepCase pour garder snake_case,
  // enums en String pour lisibilite, defaults pour eviter les undefined.
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  // .tracking = package declare dans tracking.proto
  return grpc.loadPackageDefinition(packageDefinition).tracking;
}

// Helpers d'erreurs gRPC. Le message par defaut est specifique au domaine
// (livraison vs commande).
function notFoundError(message = 'Livraison introuvable') {
  return { code: grpc.status.NOT_FOUND, message };
}

function internalError(err) {
  return { code: grpc.status.INTERNAL, message: err.message || 'Erreur interne' };
}


// --- Handlers : un par RPC declare dans tracking.proto --------------------
//
// 4 unary + 1 streaming. Les 4 premiers suivent exactement le meme pattern
// que order-service (call.request -> repo -> callback(err, response)).
// WatchDelivery est traite a part en bas (signature differente).

const handlers = {

  GetDelivery: (call, callback) => {
    try {
      const d = repo.getDelivery(call.request.id);
      if (!d) return callback(notFoundError());
      callback(null, d);
    } catch (err) {
      callback(internalError(err));
    }
  },

  ListDeliveries: (call, callback) => {
    try {
      // Le repo lit les filtres directement depuis call.request
      // (status, driver_id, limit, offset).
      const result = repo.listDeliveries(call.request);
      callback(null, result);
    } catch (err) {
      callback(internalError(err));
    }
  },

  GetDeliveryHistory: (call, callback) => {
    try {
      // Note: ici on lit `delivery_id` et pas `id` (cf proto).
      // Renvoie tous les DeliveryEvent associes a cette delivery,
      // tries chronologiquement.
      const result = repo.getHistory(call.request.delivery_id);
      callback(null, result);
    } catch (err) {
      callback(internalError(err));
    }
  },

  AdvanceDeliveryStatus: (call, callback) => {
    try {
      // Le repo valide la transition (refuse par exemple PICKED_UP -> ASSIGNED).
      // En cas de transition invalide, il throw -> INVALID_ARGUMENT.
      const d = repo.advanceStatus(call.request.delivery_id, call.request.new_status);
      if (!d) return callback(notFoundError());
      // Pas de publish Kafka explicite ici ! C'est le bus interne du repo
      // qui est branche sur Kafka (cf kafka.js : repo.bus.on('delivery-changed',
      // publishDeliveryStatus)). Avantage : separer les preoccupations
      // (API ignore Kafka, Kafka ignore l'API).
      callback(null, d);
    } catch (err) {
      // Erreur typique : transition de status invalide.
      callback({ code: grpc.status.INVALID_ARGUMENT, message: err.message });
    }
  },


  // -------------------------------------------------------------------
  //  WatchDelivery : SERVER-STREAMING (signature et logique differentes)
  // -------------------------------------------------------------------
  //
  //  Concept : le client appelle une seule fois en envoyant un delivery_id.
  //  Le serveur garde la connexion ouverte et push la nouvelle version de
  //  la delivery a chaque changement (advance status, attach driver, etc.).
  //  Le client recoit les updates en live sans avoir a re-appeler.
  //
  //  Signature : (call) seul, PAS de callback. La reponse se construit
  //  avec call.write(msg) que l'on appelle autant de fois que necessaire.
  //
  //  Mecanisme : le repo expose un EventEmitter (`repo.bus`) qui emet
  //  'delivery-changed' a chaque modification. On s'inscrit comme listener,
  //  on filtre par delivery_id, et on forward au stream gRPC.
  // -------------------------------------------------------------------
  WatchDelivery: (call) => {
    // call.request = WatchDeliveryRequest { delivery_id }
    const deliveryId = call.request.delivery_id;

    // Validation : si delivery_id vide, on termine le stream avec une erreur.
    // En streaming on emet 'error' (l'equivalent du callback(err) d'unary).
    // Le return immediat empeche d'attacher des listeners en aval.
    if (!deliveryId) {
      call.emit('error', { code: grpc.status.INVALID_ARGUMENT, message: 'delivery_id requis' });
      return;
    }

    // PATTERN "replay current state then subscribe" :
    // On envoie immediatement l'etat courant au client si la delivery
    // existe deja. Sans ca, le client devrait attendre une modification
    // pour savoir ou en est sa delivery. UX bien meilleure.
    const current = repo.getDelivery(deliveryId);
    if (current) {
      call.write(current);
    }

    // Listener qui filtre les events du bus.
    // Le bus emit a chaque modification de N'IMPORTE QUELLE delivery,
    // on ne forward au client que les modifications de la delivery
    // qu'il a demandee.
    const listener = (delivery) => {
      if (delivery && delivery.id === deliveryId) {
        try {
          call.write(delivery);
        } catch (e) {
          // Race condition possible : le client s'est deconnecte entre
          // 2 messages mais notre cleanup n'a pas encore tourne.
          // On ignore silencieusement (la connexion sera bientot fermee).
        }
      }
    };
    repo.bus.on('delivery-changed', listener);

    // CLEANUP : crucial pour eviter les memory leaks. Sans ces 3 lignes,
    // les listeners s'accumuleraient dans le bus chaque fois qu'un client
    // se connecte/deconnecte. Au bout de 1000 clients on aurait 1000
    // listeners qui filtreraient pour rien a chaque modif.
    //
    // On ecoute 3 events car selon comment le client se deconnecte
    // (cancel explicite, crash, fin normale...), c'est l'un ou l'autre
    // qui se declenche. EventEmitter.off est idempotent donc pas de
    // probleme si appele 2 fois.
    const cleanup = () => {
      repo.bus.off('delivery-changed', listener);
    };
    call.on('cancelled', cleanup);   // call.cancel() cote client
    call.on('close', cleanup);       // connexion TCP coupee (crash, kill, timeout)
    call.on('end', cleanup);         // fin normale du stream (rare en watch perpetuel)
  },
};


// --- Demarrage du serveur gRPC --------------------------------------------
// Idem order-service : bind 0.0.0.0 (pour accepter les connexions inter-conteneurs),
// credentials Insecure (OK car trafic interne au reseau Docker).
function startGrpcServer(port) {
  const trackingProto = loadProto();
  const server = new grpc.Server();
  server.addService(trackingProto.TrackingService.service, handlers);

  const address = `0.0.0.0:${port}`;
  server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
    if (err) {
      console.error('Echec du bind:', err);
      process.exit(1);
    }
    console.log(`tracking-service ecoute sur le port ${boundPort} (gRPC)`);
  });
}

module.exports = { startGrpcServer };
