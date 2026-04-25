const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

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

function unimplemented(call, callback) {
  callback({ code: grpc.status.UNIMPLEMENTED, message: 'Pas encore implemente' });
}

function startGrpcServer(port) {
  const driverProto = loadProto();
  const server = new grpc.Server();

  server.addService(driverProto.DriverService.service, {
    RegisterDriver: unimplemented,
    GetDriver: unimplemented,
    ListAvailableDrivers: unimplemented,
    UpdateLocation: unimplemented,
    StreamDriverLocation: (call) => {
      call.end();
    },
  });

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
