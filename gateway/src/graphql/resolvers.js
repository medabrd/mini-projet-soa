const { orderClient, driverClient, trackingClient } = require('../grpc-clients');

// Helper : on retourne null sur NOT_FOUND plutot que de faire planter la query
// (utile pour les joins ou la cible peut ne pas exister)
async function safe(grpcCall) {
  try {
    return await grpcCall;
  } catch (err) {
    if (err.code === 5) return null; // NOT_FOUND
    throw err;
  }
}

const resolvers = {
  Query: {
    order: (_, { id }) => safe(orderClient.GetOrder({ id })),
    orders: async (_, args) => {
      const result = await orderClient.ListOrders({
        customer_id: args.customer_id || '',
        status: args.status || 'ORDER_STATUS_UNSPECIFIED',
        limit: args.limit || 100,
        offset: args.offset || 0,
      });
      return result;
    },

    driver: (_, { id }) => safe(driverClient.GetDriver({ id })),
    availableDrivers: async (_, args) => {
      const result = await driverClient.ListAvailableDrivers({ limit: args.limit || 50 });
      return { drivers: result.drivers, total: result.total };
    },

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
    deliveryByOrder: async (_, { order_id }) => {
      const result = await trackingClient.ListDeliveries({ limit: 1000, offset: 0 });
      return result.deliveries.find(d => d.order_id === order_id) || null;
    },
  },

  Mutation: {
    createOrder: (_, { input }) => orderClient.CreateOrder(input),
    cancelOrder: (_, { id, reason }) =>
      orderClient.CancelOrder({ id, reason: reason || '' }),

    registerDriver: (_, { input }) => driverClient.RegisterDriver(input),
    updateDriverLocation: (_, { input }) =>
      driverClient.UpdateLocation({
        driver_id: input.driver_id,
        latitude: input.latitude,
        longitude: input.longitude,
        speed_kmh: input.speed_kmh || 0,
        heading_deg: input.heading_deg || 0,
      }),

    advanceDelivery: (_, args) =>
      trackingClient.AdvanceDeliveryStatus({
        delivery_id: args.delivery_id,
        new_status: args.new_status,
      }),
  },

  // Resolvers field-level pour les joins cross-services.
  // GraphQL appelle ces resolvers seulement si le champ est demande dans la query
  // (lazy loading). Donc pas de surcout si le client veut juste l'order sans le driver.
  Order: {
    driver: async (parent) => {
      if (!parent.assigned_driver_id) return null;
      return safe(driverClient.GetDriver({ id: parent.assigned_driver_id }));
    },
    delivery: async (parent) => {
      // tracking n'a pas de RPC GetDeliveryByOrder, on filtre la liste
      const result = await trackingClient.ListDeliveries({ limit: 1000, offset: 0 });
      return result.deliveries.find(d => d.order_id === parent.id) || null;
    },
  },

  Driver: {
    current_order: async (parent) => {
      if (!parent.current_order_id) return null;
      return safe(orderClient.GetOrder({ id: parent.current_order_id }));
    },
  },

  Delivery: {
    order: async (parent) => {
      if (!parent.order_id) return null;
      return safe(orderClient.GetOrder({ id: parent.order_id }));
    },
    driver: async (parent) => {
      if (!parent.driver_id) return null;
      return safe(driverClient.GetDriver({ id: parent.driver_id }));
    },
    history: async (parent) => {
      const result = await trackingClient.GetDeliveryHistory({ delivery_id: parent.id });
      return result.events;
    },
  },
};

module.exports = { resolvers };
