const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

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

function unimplemented(call, callback) {
  callback({ code: grpc.status.UNIMPLEMENTED, message: 'Pas encore implemente' });
}

function startGrpcServer(port) {
  const trackingProto = loadProto();
  const server = new grpc.Server();

  server.addService(trackingProto.TrackingService.service, {
    GetDelivery: unimplemented,
    ListDeliveries: unimplemented,
    GetDeliveryHistory: unimplemented,
    AdvanceDeliveryStatus: unimplemented,
    WatchDelivery: (call) => { call.end(); },
  });

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
