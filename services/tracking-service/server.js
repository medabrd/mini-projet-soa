const { startGrpcServer } = require('./src/grpc-server');

const PORT = process.env.PORT || 50053;

console.log('Demarrage du service tracking...');
startGrpcServer(PORT);
