const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const repo = require('./deliveries-repo');

// Proto partage a la racine du repo
const PROTO_PATH = path.resolve(__dirname, '..', '..', '..', 'proto', 'tracking.proto');

function loadProto() {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(packageDefinition).tracking;
}

function notFoundError(message = 'Livraison introuvable') {
  return { code: grpc.status.NOT_FOUND, message };
}

function internalError(err) {
  return { code: grpc.status.INTERNAL, message: err.message || 'Erreur interne' };
}

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
      const result = repo.listDeliveries(call.request);
      callback(null, result);
    } catch (err) {
      callback(internalError(err));
    }
  },

  GetDeliveryHistory: (call, callback) => {
    try {
      const result = repo.getHistory(call.request.delivery_id);
      callback(null, result);
    } catch (err) {
      callback(internalError(err));
    }
  },

  AdvanceDeliveryStatus: (call, callback) => {
    try {
      const d = repo.advanceStatus(call.request.delivery_id, call.request.new_status);
      if (!d) return callback(notFoundError());
      callback(null, d);
    } catch (err) {
      callback({ code: grpc.status.INVALID_ARGUMENT, message: err.message });
    }
  },

  // Server-streaming : pousse l'etat de la livraison a chaque changement.
  // On ecoute le bus interne emis par le repo : a chaque update (assigned, advance, cancel...),
  // on pousse la nouvelle version au client gRPC.
  WatchDelivery: (call) => {
    const deliveryId = call.request.delivery_id;
    if (!deliveryId) {
      call.emit('error', { code: grpc.status.INVALID_ARGUMENT, message: 'delivery_id requis' });
      return;
    }

    // Etat courant si la livraison existe deja
    const current = repo.getDelivery(deliveryId);
    if (current) {
      call.write(current);
    }

    const listener = (delivery) => {
      if (delivery && delivery.id === deliveryId) {
        try {
          call.write(delivery);
        } catch (e) {
          // ignore : connexion fermee
        }
      }
    };
    repo.bus.on('delivery-changed', listener);

    const cleanup = () => {
      repo.bus.off('delivery-changed', listener);
    };
    call.on('cancelled', cleanup);
    call.on('close', cleanup);
    call.on('end', cleanup);
  },
};

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
