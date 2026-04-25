const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const repo = require('./drivers-repo');
const kafka = require('./kafka');
const { getDb } = require('./db');

// Proto partage a la racine du repo
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
  RegisterDriver: async (call, callback) => {
    try {
      const driver = await repo.registerDriver(call.request);
      callback(null, driver);
    } catch (err) {
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

  UpdateLocation: async (call, callback) => {
    try {
      const location = await repo.updateLocation(call.request.driver_id, call.request);
      if (!location) return callback(notFoundError());
      kafka.publishLocationUpdated(call.request.driver_id, location);
      callback(null, location);
    } catch (err) {
      callback(internalError(err));
    }
  },

  // Server-streaming : pousse la position du livreur a chaque update via les observables RxDB.
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

    // Envoyer la position courante si elle existe deja
    if (driverDoc.last_location) {
      call.write(driverDoc.last_location);
    }

    // S'abonner aux changements futurs sur ce document (observable RxDB)
    const sub = db.drivers
      .findOne(driverId)
      .$.subscribe(doc => {
        if (doc && doc.last_location) {
          try {
            call.write(doc.last_location);
          } catch (e) {
            // ignore : connexion probablement fermee
          }
        }
      });

    // Nettoyage quand le client se deconnecte ou que le stream se termine
    const cleanup = () => {
      try { sub.unsubscribe(); } catch (_) {}
    };
    call.on('cancelled', cleanup);
    call.on('close', cleanup);
    call.on('end', cleanup);
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
