const { initDatabase } = require('./src/db');
const { startGrpcServer } = require('./src/grpc-server');
const kafka = require('./src/kafka');

const PORT = process.env.PORT || 50052;

async function main() {
  console.log('Demarrage du service driver...');
  await initDatabase();
  // Pas de seed : la base demarre vide. Les drivers sont crees a la demande
  // via RegisterDriver. Les orders sans driver disponible sont mises en
  // file d'attente cote kafka.js (voir handleOrderEvent + tryAssignToPending).
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
