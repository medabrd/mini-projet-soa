// Test end-to-end gRPC pour driver-service
// Couvre les 5 RPCs y compris le server-streaming.
// Usage : cd services/driver-service && node test-client.js
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');
const fs = require('fs');

const PROTO_PATH = path.resolve(__dirname, '..', '..', 'proto', 'driver.proto');
const SERVER = process.env.SERVER || 'localhost:50052';

const pkgDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const driverProto = grpc.loadPackageDefinition(pkgDef).driver;
const client = new driverProto.DriverService(SERVER, grpc.credentials.createInsecure());

function call(method, request) {
  return new Promise((resolve, reject) => {
    client[method](request, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}

const results = {};

async function main() {
  console.log(`>>> Test gRPC end-to-end driver-service sur ${SERVER}\n`);

  // 1. RegisterDriver
  const registerReq = {
    name: 'Karim Ben Salah',
    phone: '+216 22 123 456',
    vehicle_type: 'scooter',
  };
  const driver = await call('RegisterDriver', registerReq);
  results.RegisterDriver = { request: registerReq, response: driver };
  console.log('1) RegisterDriver OK - id:', driver.id, '- status:', driver.status);

  const driverId = driver.id;

  // 2. GetDriver
  const getReq = { id: driverId };
  const fetched = await call('GetDriver', getReq);
  results.GetDriver = { request: getReq, response: fetched };
  console.log('2) GetDriver OK - name:', fetched.name, '- vehicle:', fetched.vehicle_type);

  // 3. ListAvailableDrivers
  const listReq = { limit: 10 };
  const list = await call('ListAvailableDrivers', listReq);
  results.ListAvailableDrivers = { request: listReq, response: list };
  console.log('3) ListAvailableDrivers OK - total dispo:', list.total);

  // 4. UpdateLocation
  const updateReq = {
    driver_id: driverId,
    latitude: 35.8245,
    longitude: 10.6347,
    speed_kmh: 32,
    heading_deg: 90,
  };
  const loc = await call('UpdateLocation', updateReq);
  results.UpdateLocation = { request: updateReq, response: loc };
  console.log('4) UpdateLocation OK - lat:', loc.latitude, '- lng:', loc.longitude);

  // 5. StreamDriverLocation (server-streaming)
  // On lance le stream, on envoie quelques updates de position en parallele,
  // et on collecte les messages pousses par le serveur.
  console.log('5) StreamDriverLocation : on ecoute pendant 3 secondes...');
  const stream = client.StreamDriverLocation({ driver_id: driverId });
  const streamed = [];
  stream.on('data', (msg) => {
    streamed.push({ lat: msg.latitude, lng: msg.longitude, ts: msg.timestamp });
    console.log(`   -> position recue: ${msg.latitude}, ${msg.longitude}`);
  });
  stream.on('error', (err) => {
    console.error('   Stream error:', err.message);
  });

  // Envoyer 3 updates de position espacees pour que le stream les recoive
  await new Promise(r => setTimeout(r, 300));
  for (let i = 1; i <= 3; i++) {
    await call('UpdateLocation', {
      driver_id: driverId,
      latitude: 35.8245 + i * 0.001,
      longitude: 10.6347 + i * 0.001,
      speed_kmh: 30 + i,
      heading_deg: 90,
    });
    await new Promise(r => setTimeout(r, 500));
  }

  // Laisser le stream finir de drainer puis fermer
  await new Promise(r => setTimeout(r, 500));
  stream.cancel();

  results.StreamDriverLocation = { request: { driver_id: driverId }, messagesRecus: streamed.length };
  console.log(`   StreamDriverLocation OK - ${streamed.length} positions recues`);

  client.close();

  // Sauvegarde des resultats
  const outPath = path.resolve(__dirname, 'last-test-results.json');
  fs.writeFileSync(outPath, JSON.stringify(results, null, 2));
  console.log(`\nResultats sauves dans ${outPath}`);
  console.log('\nTest end-to-end OK - les 5 RPCs (dont streaming) marchent.');
}

main().catch(err => {
  console.error('Echec du test:', err.code, err.message);
  process.exit(1);
});
