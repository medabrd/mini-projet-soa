const { startGrpcServer } = require('./src/grpc-server');

const PORT = process.env.PORT || 50051;

console.log('Demarrage du service order...');
startGrpcServer(PORT);
