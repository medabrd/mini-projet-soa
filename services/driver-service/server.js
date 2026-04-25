const { startGrpcServer } = require('./src/grpc-server');

const PORT = process.env.PORT || 50052;

console.log('Demarrage du service driver...');
startGrpcServer(PORT);
