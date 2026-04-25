const { initDatabase } = require('./src/db');
const { startGrpcServer } = require('./src/grpc-server');
const kafka = require('./src/kafka');

const PORT = process.env.PORT || 50052;

async function main() {
  console.log('Demarrage du service driver...');
  await initDatabase();
  await kafka.connect();
  startGrpcServer(PORT);
}

process.on('SIGINT', async () => {
  console.log('\nArret du service...');
  await kafka.disconnect();
  process.exit(0);
});

main().catch(err => {
  console.error('Erreur fatale:', err);
  process.exit(1);
});
