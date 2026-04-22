const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

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

// Handler temporaire pour les RPCs pas encore branchés
function unimplemented(call, callback) {
  callback({
    code: grpc.status.UNIMPLEMENTED,
    message: 'Pas encore implemente',
  });
}

function startGrpcServer(port) {
  const orderProto = loadProto();
  const server = new grpc.Server();

  server.addService(orderProto.OrderService.service, {
    CreateOrder: unimplemented,
    GetOrder: unimplemented,
    ListOrders: unimplemented,
    UpdateOrderStatus: unimplemented,
    CancelOrder: unimplemented,
  });

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
