// =========================================================================
//  resolvers.js - Implementation des operations declarees dans schema.js
// =========================================================================
//
//  Role : pour chaque champ du schema, fournir UNE FONCTION qui sait le
//  remplir. Apollo Server appelle la bonne fonction selon la query du
//  client, et compose la reponse finale.
//
//  Organisation de l'objet `resolvers` :
//
//    {
//      Query:    { ... },     <- resolvers des queries top-level (lecture)
//      Mutation: { ... },     <- resolvers des mutations top-level (ecriture)
//      Order:    { ... },     <- resolvers field-level du type Order
//      Driver:   { ... },     <- resolvers field-level du type Driver
//      Delivery: { ... },     <- resolvers field-level du type Delivery
//    }
//
//  Les champs scalaires (id, name, status, ...) sont resolus
//  AUTOMATIQUEMENT par Apollo s'ils existent dans l'objet retourne.
//  Pas besoin de les declarer ici. On declare uniquement les champs
//  qui necessitent une logique custom (= les joins cross-services).
//
//  Signature d'un resolver :
//    (parent, args, context, info) => value | Promise<value>
//
//    parent  : l'objet "au-dessus" dans la query (pour les field-level)
//    args    : les arguments du champ (ex: { id: "abc" })
//    context : objet partage entre tous les resolvers d'une requete (non utilise ici)
//    info    : metadata sur la query (non utilise)
//
//  Convention JS : on ecrit (_, { id }) pour ignorer parent et destructurer
//  args. Underscore = "je n'utilise pas".
// =========================================================================

const { orderClient, driverClient, trackingClient } = require('../grpc-clients');


// --- Helper : transforme NOT_FOUND en null ---------------------------------
//
// Sans ca, un gRPC NOT_FOUND deviendrait une erreur GraphQL qui ferait
// echouer toute la query. Or, pour les joins cross-services, c'est normal
// qu'une cible n'existe pas (ex: un Driver supprime alors qu'une Order
// le reference encore). On prefere retourner null silencieusement et
// laisser le client gerer.
//
// Le code gRPC 5 = NOT_FOUND (cf grpc.status). Pour les autres codes
// d'erreur, on relance l'exception pour qu'Apollo la rapporte au client.
async function safe(grpcCall) {
  try {
    return await grpcCall;
  } catch (err) {
    if (err.code === 5) return null;
    throw err;
  }
}


const resolvers = {

  // =====================================================================
  //  Query : operations de LECTURE
  // =====================================================================
  //
  //  Pattern uniforme : prendre les args, appeler le bon RPC gRPC,
  //  retourner le resultat (qu'Apollo va serialiser selon le schema).
  // =====================================================================

  Query: {
    // order(id: ID!): Order
    // Lecture simple : un id, on retourne l'Order ou null.
    order: (_, { id }) => safe(orderClient.GetOrder({ id })),

    // orders(...): OrdersPage!
    // Avec filtres optionnels + pagination. On normalise les defauts ici
    // pour ne pas envoyer d'undefined cote gRPC (qui crasherait).
    orders: async (_, args) => {
      const result = await orderClient.ListOrders({
        customer_id: args.customer_id || '',
        // Convention proto : envoyer la string UNSPECIFIED pour "pas de filtre"
        status: args.status || 'ORDER_STATUS_UNSPECIFIED',
        limit: args.limit || 100,
        offset: args.offset || 0,
      });
      return result;   // { orders: [...], total: N }
    },

    // Pareil pour Driver
    driver: (_, { id }) => safe(driverClient.GetDriver({ id })),
    availableDrivers: async (_, args) => {
      const result = await driverClient.ListAvailableDrivers({ limit: args.limit || 50 });
      // Le proto repond { drivers, total }, exactement la forme attendue.
      // On peut return directement, mais on explicite pour clarte.
      return { drivers: result.drivers, total: result.total };
    },

    // Pareil pour Delivery
    delivery: (_, { id }) => safe(trackingClient.GetDelivery({ id })),
    deliveries: async (_, args) => {
      const result = await trackingClient.ListDeliveries({
        status: args.status || 'DELIVERY_STATUS_UNSPECIFIED',
        driver_id: args.driver_id || '',
        limit: args.limit || 100,
        offset: args.offset || 0,
      });
      return result;
    },

    // deliveryByOrder(order_id: ID!): Delivery
    // Raccourci pour eviter au client de faire ListDeliveries + filter.
    // ATTENTION : tracking-service n'a pas de RPC dedie GetDeliveryByOrder
    // (oubli de design), donc on fait un ListDeliveries large et on filtre
    // cote gateway. Pas optimal en perf (N+1 latent), mais OK pour la
    // taille de notre projet. Cf note plus bas pour Order.delivery.
    deliveryByOrder: async (_, { order_id }) => {
      const result = await trackingClient.ListDeliveries({ limit: 1000, offset: 0 });
      return result.deliveries.find(d => d.order_id === order_id) || null;
    },
  },


  // =====================================================================
  //  Mutation : operations d'ECRITURE
  // =====================================================================
  //
  //  Meme pattern que Query : prendre args/input, appeler le RPC, retourner
  //  l'objet modifie. Apollo serialise selon le schema.
  // =====================================================================

  Mutation: {
    // createOrder(input: CreateOrderInput!): Order!
    // L'input GraphQL a EXACTEMENT la meme forme que CreateOrderRequest proto.
    // On peut donc passer input directement au RPC. Pas de mapping a faire.
    createOrder: (_, { input }) => orderClient.CreateOrder(input),

    // cancelOrder(id: ID!, reason: String): Order!
    // reason est optionnel (pas de !), on default a "" pour proto3.
    cancelOrder: (_, { id, reason }) =>
      orderClient.CancelOrder({ id, reason: reason || '' }),

    // deleteOrder(id: ID!): Boolean!
    // Le RPC renvoie { deleted: true }, mais le schema GraphQL retourne
    // un simple Boolean. On convertit explicitement avec return true.
    // Si DeleteOrder throw (ex: commande encore active), l'erreur remonte
    // a Apollo qui la rapporte au client.
    deleteOrder: async (_, { id }) => {
      await orderClient.DeleteOrder({ id });
      return true;
    },

    // Drivers - meme schema
    registerDriver: (_, { input }) => driverClient.RegisterDriver(input),

    // updateDriverLocation : un peu plus verbose car on default les champs
    // optionnels (speed_kmh et heading_deg) a 0 pour eviter undefined.
    updateDriverLocation: (_, { input }) =>
      driverClient.UpdateLocation({
        driver_id: input.driver_id,
        latitude: input.latitude,
        longitude: input.longitude,
        speed_kmh: input.speed_kmh || 0,
        heading_deg: input.heading_deg || 0,
      }),

    deleteDriver: async (_, { id }) => {
      await driverClient.DeleteDriver({ id });
      return true;
    },

    // Deliveries
    advanceDelivery: (_, args) =>
      trackingClient.AdvanceDeliveryStatus({
        delivery_id: args.delivery_id,
        new_status: args.new_status,
      }),
  },


  // =====================================================================
  //  RESOLVERS FIELD-LEVEL : les joins cross-services (LE POWER MOVE)
  // =====================================================================
  //
  //  Principe : pour chaque champ "join" declare dans le schema (ex:
  //  Order.driver, Delivery.order, etc.), on declare ici une fonction
  //  qui sait le resoudre.
  //
  //  Apollo appelle ces fonctions SEULEMENT SI le client demande le
  //  champ dans sa query (lazy loading). Donc une query simple ne paye
  //  pas le cout des joins.
  //
  //  La fonction recoit `parent` = l'objet "au-dessus" dans la query.
  //  Pour Order.driver, parent = l'Order qu'on vient de charger.
  //
  //  Exemple :
  //    query { order(id: "abc") { id status driver { name } } }
  //
  //  Sequence :
  //    1. Apollo appelle Query.order -> recupere l'Order via gRPC
  //    2. Apollo voit que le client veut "driver" -> appelle Order.driver
  //       avec parent = l'Order
  //    3. Order.driver lit parent.assigned_driver_id, fait un GetDriver gRPC
  //    4. Apollo assemble la reponse finale
  // =====================================================================

  Order: {
    // Order.driver : Driver
    // Si l'Order a un livreur assigne, on le recupere via driver-service.
    // Sinon (pas encore assigne), on retourne null.
    driver: async (parent) => {
      if (!parent.assigned_driver_id) return null;
      return safe(driverClient.GetDriver({ id: parent.assigned_driver_id }));
    },

    // Order.delivery : Delivery
    // PROBLEME : tracking-service n'expose pas de RPC GetDeliveryByOrder.
    // Workaround : on liste un gros batch (1000) et on filtre cote gateway.
    //
    // ⚠️ N+1 problem latent : si le client demande une liste de N orders
    // et veut leur delivery pour chacun, on fait N appels ListDeliveries
    // (1000 elements chacun). En pratique OK car notre dataset est petit,
    // mais a refactorer en ajoutant un RPC dedie cote tracking si on
    // scalait. Alternative : utiliser DataLoader pour batcher les appels.
    delivery: async (parent) => {
      const result = await trackingClient.ListDeliveries({ limit: 1000, offset: 0 });
      return result.deliveries.find(d => d.order_id === parent.id) || null;
    },
  },

  Driver: {
    // Driver.current_order : Order
    // Si le driver est BUSY (current_order_id rempli), on recupere l'Order.
    // Si AVAILABLE, current_order_id = "" et on retourne null.
    current_order: async (parent) => {
      if (!parent.current_order_id) return null;
      return safe(orderClient.GetOrder({ id: parent.current_order_id }));
    },
  },

  Delivery: {
    // Delivery.order : Order
    // Resolution directe via order_id (qui existe toujours sur Delivery).
    order: async (parent) => {
      if (!parent.order_id) return null;
      return safe(orderClient.GetOrder({ id: parent.order_id }));
    },

    // Delivery.driver : Driver
    // Si la delivery a un driver assigne (driver_id non vide).
    driver: async (parent) => {
      if (!parent.driver_id) return null;
      return safe(driverClient.GetDriver({ id: parent.driver_id }));
    },

    // Delivery.history : [DeliveryEvent!]!
    // RPC dedie cote tracking : GetDeliveryHistory.
    // On extrait juste le tableau events (le RPC renvoie { events: [...] }).
    history: async (parent) => {
      const result = await trackingClient.GetDeliveryHistory({ delivery_id: parent.id });
      return result.events;
    },
  },
};


// On exporte le dictionnaire complet. Apollo le combinera avec typeDefs
// dans server.js : new ApolloServer({ typeDefs, resolvers }).
module.exports = { resolvers };
