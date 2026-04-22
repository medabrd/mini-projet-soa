const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const repo = require('./orders-repo');

// Le fichier proto est partagé à la racine du repo
const PROTO_PATH = path.resolve(__dirname, '..', '..', '..', 'proto', 'order.proto');

function loadProto() {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(packageDefinition).order;
}

function notFoundError(message = 'Commande introuvable') {
  return { code: grpc.status.NOT_FOUND, message };
}

function internalError(err) {
  return { code: grpc.status.INTERNAL, message: err.message || 'Erreur interne' };
}

const handlers = {
  CreateOrder: (call, callback) => {
    try {
      const order = repo.createOrder(call.request);
      callback(null, order);
    } catch (err) {
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
      callback(null, order);
    } catch (err) {
      callback(internalError(err));
    }
  },

  CancelOrder: (call, callback) => {
    try {
      const order = repo.cancelOrder(call.request.id);
      if (!order) return callback(notFoundError());
      callback(null, order);
    } catch (err) {
      callback({ code: grpc.status.FAILED_PRECONDITION, message: err.message });
    }
  },
};

function startGrpcServer(port) {
  const orderProto = loadProto();
  const server = new grpc.Server();
  server.addService(orderProto.OrderService.service, handlers);

  const address = `0.0.0.0:${port}`;
  server.bindAsync(address, grpc.ServerCredentials.createInsecure(), (err, boundPort) => {
    if (err) {
      console.error('Echec du bind:', err);
      process.exit(1);
    }
    console.log(`order-service ecoute sur le port ${boundPort} (gRPC)`);
  });
}

module.exports = { startGrpcServer };
