// =========================================================================
//  grpc-server.js - Couche API du microservice order-service
// =========================================================================
//
//  Role : implementer concretement les RPCs declares dans order.proto.
//  C'est l'equivalent JS d'un "Controller" REST : recoit la requete, la
//  valide, delegue le travail metier au repo, publie eventuellement un
//  event Kafka, puis renvoie la reponse.
//
//  Ne contient AUCUNE logique metier (calculs, regles d'integrite, SQL).
//  Tout ca vit dans `orders-repo.js`. Si tu te retrouves a ecrire du
//  metier ici, c'est que tu deborde de couche.
// =========================================================================

// --- Imports ---------------------------------------------------------------

// La librairie gRPC pure-JS pour Node (alternative au binding C++ deprecie).
const grpc = require('@grpc/grpc-js');

// Le parser de fichiers .proto. Charge le contrat AU RUNTIME (lecture du
// fichier .proto a chaque demarrage) au lieu de generer du code en amont.
// Avantage : pas d'etape de build supplementaire. Inconvenient : un peu
// plus lent au demarrage et moins de typage statique (sans TypeScript).
const protoLoader = require('@grpc/proto-loader');

const path = require('path');

// Le repo qui contient la logique metier + acces SQLite. Notre seul lien
// vers la couche du dessous.
const repo = require('./orders-repo');

// Le module Kafka, pour publier les events apres certaines actions
// (CreateOrder -> order.placed, CancelOrder -> order.cancelled, etc.).
const kafka = require('./kafka');


// --- Chemin vers le .proto -------------------------------------------------

// Le fichier proto est PARTAGE a la racine du repo (cf. explications dans
// le Dockerfile). Depuis services/order-service/src/, il faut remonter
// 3 niveaux pour atteindre la racine puis descendre dans proto/.
//
// __dirname = dossier du fichier courant = /app/src/ dans le conteneur
//             = .../services/order-service/src/ en local
// Remontee : '..' x 3 = racine du repo
// Descente : 'proto/order.proto'
const PROTO_PATH = path.resolve(__dirname, '..', '..', '..', 'proto', 'order.proto');


// --- Chargement du proto en memoire ---------------------------------------

// loadProto() est appelee une seule fois, au demarrage du serveur.
// Elle parse le fichier .proto et construit un objet JS qui represente
// le package + les services + les messages.
function loadProto() {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    // Conserve la casse originale des champs du .proto.
    // Sans ca, "customer_id" deviendrait "customerId" (camelCase JS-style).
    // On garde snake_case pour rester aligne avec les payloads Postman et la BDD.
    keepCase: true,

    // Convertit les int64 (trop grands pour Number JS) en String.
    // Sans option, on aurait des BigInt parfois imprevisibles a serialiser.
    // Pas crucial ici (on n'utilise pas d'int64 dans order.proto) mais safe.
    longs: String,

    // Les enums sont passes en String ("PENDING") au lieu de leur index (1).
    // Plus lisible cote JS et plus stable si on reorganise l'ordre.
    enums: String,

    // Remplit les champs absents avec la valeur par defaut Protobuf
    // (string="", int=0, etc.) au lieu de les laisser undefined.
    // Evite plein de "if (x === undefined)" dans le code.
    defaults: true,

    // Active la deserialisation des "oneof" (champs alternatifs).
    // On n'en a pas dans nos .proto, mais c'est l'option recommandee.
    oneofs: true,
  });

  // grpc.loadPackageDefinition transforme la definition brute en un objet
  // utilisable. On accede au package "order" defini en haut du .proto.
  // .order.OrderService donne acces au service et a ses methodes.
  return grpc.loadPackageDefinition(packageDefinition).order;
}


// --- Helpers pour construire des erreurs gRPC standard --------------------

// gRPC a son propre systeme de codes d'erreur (different de HTTP).
// Liste complete : https://grpc.github.io/grpc/core/md_doc_statuscodes.html
// Les plus utilises ici :
//   - NOT_FOUND (5)            : ressource inexistante
//   - INVALID_ARGUMENT (3)     : payload invalide / validation echouee
//   - FAILED_PRECONDITION (9)  : etat non compatible avec l'action
//   - INTERNAL (13)            : bug serveur
//
// Le gateway les remappe ensuite vers les bons codes HTTP (cf grpcErrorToHttp).

function notFoundError(message = 'Commande introuvable') {
  return { code: grpc.status.NOT_FOUND, message };
}

function internalError(err) {
  return { code: grpc.status.INTERNAL, message: err.message || 'Erreur interne' };
}


// --- Handlers : un par RPC declare dans order.proto -----------------------
//
// Pattern uniforme :
//   1. recevoir (call, callback)
//   2. lire call.request (= les champs du message d'entree)
//   3. deleguer au repo
//   4. si event Kafka pertinent, le publier (fire-and-forget)
//   5. callback(null, response) en cas de succes
//   6. callback({code, message}) en cas d'erreur
//
// `call`     : l'objet decrivant l'appel en cours (request, metadata...).
// `callback` : a appeler EXACTEMENT une fois, sinon le client reste bloque.
//              Signature Node-style : (erreur, reponse). Pour signaler une
//              erreur : callback(errObj). Pour le succes : callback(null, resp).
const handlers = {

  CreateOrder: (call, callback) => {
    try {
      // call.request = { customer_id, customer_name, delivery_address, items }
      const order = repo.createOrder(call.request);

      // Fire-and-forget : on ne fait pas await, on n'attend pas la confirmation
      // Kafka. Si Kafka est down, le service no-op (cf kafka.js producerReady).
      // L'idee : la commande est creee en BDD, c'est l'essentiel. La notification
      // est best-effort.
      kafka.publishOrderPlaced(order);

      callback(null, order);
    } catch (err) {
      // Erreur typique ici : validation du repo (ex: items vide).
      // On remappe en INVALID_ARGUMENT (=> 400 cote HTTP).
      callback({ code: grpc.status.INVALID_ARGUMENT, message: err.message });
    }
  },

  GetOrder: (call, callback) => {
    try {
      const order = repo.getOrder(call.request.id);
      if (!order) return callback(notFoundError());
      callback(null, order);
    } catch (err) {
      callback(internalError(err));
    }
  },

  ListOrders: (call, callback) => {
    try {
      // Le repo lit les filtres directement depuis l'objet request.
      // Les champs absents sont remplis par les defauts Protobuf grace a
      // l'option `defaults: true` du loader.
      const result = repo.listOrders(call.request);
      callback(null, result);
    } catch (err) {
      callback(internalError(err));
    }
  },

  UpdateOrderStatus: (call, callback) => {
    try {
      const order = repo.updateOrderStatus(
        call.request.id,
        call.request.status,
        call.request.assigned_driver_id,
      );
      if (!order) return callback(notFoundError());

      // Publish important : notifie les autres services que cette commande
      // a change de status. driver-service ecoute pour nettoyer sa file
      // d'attente pendingOrders si la commande sort de PENDING (ex: passage
      // manuel a DELIVERED via l'API admin sans passer par la chaine Kafka
      // normale). Fix d'un bug rencontre pendant les tests.
      kafka.publishOrderStatusUpdated(order);

      callback(null, order);
    } catch (err) {
      callback(internalError(err));
    }
  },

  CancelOrder: (call, callback) => {
    try {
      const order = repo.cancelOrder(call.request.id);
      if (!order) return callback(notFoundError());

      // Publish : declenche la chaine cote driver-service (libere le livreur
      // s'il etait BUSY sur cette commande) et tracking-service (passe la
      // delivery associee en CANCELLED).
      kafka.publishOrderCancelled(order.id, call.request.reason);

      callback(null, order);
    } catch (err) {
      // Erreur typique : tentative d'annulation d'une commande deja DELIVERED.
      // On remappe en FAILED_PRECONDITION (=> 412 cote HTTP).
      callback({ code: grpc.status.FAILED_PRECONDITION, message: err.message });
    }
  },

  DeleteOrder: (call, callback) => {
    try {
      // Le repo retourne un objet { deleted, reason } pour qu'on puisse
      // distinguer "pas trouve" (404) de "etat interdit" (412), sans
      // utiliser d'exceptions (qui auraient ete plus couteuses).
      const result = repo.deleteOrder(call.request.id);
      if (!result.deleted) {
        if (result.reason === 'not_found') return callback(notFoundError());
        if (result.reason === 'not_final') {
          return callback({
            code: grpc.status.FAILED_PRECONDITION,
            message: 'Suppression autorisee uniquement pour les commandes DELIVERED ou CANCELLED',
          });
        }
      }
      // Pas d'event Kafka publie : la suppression est une operation admin
      // locale, pas un changement d'etat metier qui interesse les autres
      // services (la commande est deja "morte" cote metier au moment
      // de la suppression).
      callback(null, { deleted: true });
    } catch (err) {
      callback(internalError(err));
    }
  },
};


// --- Demarrage du serveur gRPC --------------------------------------------

// Appele par server.js, apres initDatabase() et kafka.connect().
function startGrpcServer(port) {
  const orderProto = loadProto();

  // Cree un nouveau serveur gRPC. Le constructeur ne prend aucun argument
  // (les options sont passees plus tard via addService / bindAsync).
  const server = new grpc.Server();

  // Enregistre nos handlers sur le service OrderService du proto.
  // orderProto.OrderService.service = la definition des methodes attendues.
  // handlers = notre implementation. Le mapping se fait par NOM de methode :
  // si le proto declare "CreateOrder" et qu'on a "CreateOrder" dans handlers,
  // c'est associe. Si on en oublie un, gRPC throw au demarrage.
  server.addService(orderProto.OrderService.service, handlers);

  // Bind sur toutes les interfaces (0.0.0.0) du conteneur, port donne.
  // 0.0.0.0 et pas 'localhost' parce qu'en Docker il faut accepter les
  // connexions venant d'autres conteneurs sur le reseau Docker.
  const address = `0.0.0.0:${port}`;

  // createInsecure() = pas de TLS. C'est OK pour notre cas (trafic interne
  // au reseau Docker, jamais expose en dehors). En prod publique on
  // utiliserait createSsl() avec des certificats.
  server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
    if (err) {
      console.error('Echec du bind:', err);
      // Si le port est deja pris ou autre probleme reseau, le service
      // ne sert a rien -> on quitte avec un code d'erreur. Docker
      // detectera le crash et redemarrera (restart: unless-stopped).
      process.exit(1);
    }
    console.log(`order-service ecoute sur le port ${boundPort} (gRPC)`);
  });
}


// On exporte UNIQUEMENT la fonction de demarrage. Les handlers et le repo
// restent internes au module, pas d'API publique a se proteger.
module.exports = { startGrpcServer };
