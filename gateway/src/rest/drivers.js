const express = require('express');
const { driverClient } = require('../grpc-clients');

const router = express.Router();

function grpcErrorToHttp(err, res) {
  const map = { 3: 400, 5: 404, 9: 412, 13: 500 };
  const status = map[err.code] || 500;
  res.status(status).json({ error: err.message, grpc_code: err.code });
}

// POST /api/drivers - enregistrer un livreur
router.post('/', async (req, res) => {
  try {
    const driver = await driverClient.RegisterDriver(req.body);
    res.status(201).json(driver);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/drivers/available - lister les livreurs disponibles
router.get('/available', async (req, res) => {
  try {
    const result = await driverClient.ListAvailableDrivers({
      limit: req.query.limit ? Number(req.query.limit) : 50,
    });
    res.json(result);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/drivers/:id - recuperer un livreur
router.get('/:id', async (req, res) => {
  try {
    const driver = await driverClient.GetDriver({ id: req.params.id });
    res.json(driver);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// PATCH /api/drivers/:id/location - mettre a jour la position
router.patch('/:id/location', async (req, res) => {
  try {
    const location = await driverClient.UpdateLocation({
      driver_id: req.params.id,
      latitude: Number(req.body.latitude),
      longitude: Number(req.body.longitude),
      speed_kmh: Number(req.body.speed_kmh) || 0,
      heading_deg: Number(req.body.heading_deg) || 0,
    });
    res.json(location);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/drivers/:id/stream - SSE (Server-Sent Events) qui relaie le streaming gRPC
// Permet de tester le flux de positions depuis un client web (ex: navigateur, curl).
router.get('/:id/stream', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const stream = driverClient.StreamDriverLocation({ driver_id: req.params.id });

  stream.on('data', loc => {
    res.write(`data: ${JSON.stringify(loc)}\n\n`);
  });
  stream.on('error', err => {
    res.write(`event: error\ndata: ${JSON.stringify({ message: err.message, code: err.code })}\n\n`);
    res.end();
  });
  stream.on('end', () => {
    res.end();
  });

  req.on('close', () => {
    try { stream.cancel(); } catch (_) {}
  });
});

module.exports = router;
