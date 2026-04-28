// Centralise la creation des clients gRPC vers les 3 microservices.
// Chaque client expose une version "promisified" des methodes (callbacks -> Promises)
// pour s'integrer naturellement avec async/await dans Express et les resolvers GraphQL.

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

const PROTO_DIR = path.resolve(__dirname, '..', '..', 'proto');

const ORDER_TARGET = process.env.ORDER_SERVICE || 'localhost:50051';
const DRIVER_TARGET = process.env.DRIVER_SERVICE || 'localhost:50052';
const TRACKING_TARGET = process.env.TRACKING_SERVICE || 'localhost:50053';

function loadProto(name, packageName) {
  const def = protoLoader.loadSync(path.join(PROTO_DIR, name), {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  return grpc.loadPackageDefinition(def)[packageName];
}

// Wrap chaque RPC unaire (callback) en Promise pour pouvoir l'appeler avec await.
function promisifyClient(client) {
  const wrapped = {};
  const proto = Object.getPrototypeOf(client);
  Object.getOwnPropertyNames(proto).forEach(method => {
    if (method.startsWith('_') || method === 'constructor' || method === 'close') return;
    const fn = proto[method];
    if (typeof fn !== 'function') return;
    // Pour les RPCs en streaming, on garde le client original (call.write, call.on('data'))
    // pour les unaires, on wrap en Promise.
    if (fn.responseStream || fn.requestStream) {
      wrapped[method] = (...args) => fn.apply(client, args);
    } else {
      wrapped[method] = (request, metadata) =>
        new Promise((resolve, reject) => {
          client[method](request || {}, metadata || new grpc.Metadata(), (err, response) => {
            if (err) reject(err);
            else resolve(response);
          });
        });
    }
  });
  // Garder l'acces au client raw pour les streams
  wrapped._raw = client;
  return wrapped;
}

const orderProto = loadProto('order.proto', 'order');
const driverProto = loadProto('driver.proto', 'driver');
const trackingProto = loadProto('tracking.proto', 'tracking');

const orderClient = promisifyClient(
  new orderProto.OrderService(ORDER_TARGET, grpc.credentials.createInsecure()),
);
const driverClient = promisifyClient(
  new driverProto.DriverService(DRIVER_TARGET, grpc.credentials.createInsecure()),
);
const trackingClient = promisifyClient(
  new trackingProto.TrackingService(TRACKING_TARGET, grpc.credentials.createInsecure()),
);

module.exports = {
  orderClient,
  driverClient,
  trackingClient,
};
