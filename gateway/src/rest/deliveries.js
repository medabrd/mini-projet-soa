const express = require('express');
const { trackingClient } = require('../grpc-clients');

const router = express.Router();

function grpcErrorToHttp(err, res) {
  const map = { 3: 400, 5: 404, 9: 412, 13: 500 };
  const status = map[err.code] || 500;
  res.status(status).json({ error: err.message, grpc_code: err.code });
}

// GET /api/deliveries - lister les livraisons
router.get('/', async (req, res) => {
  try {
    const result = await trackingClient.ListDeliveries({
      status: req.query.status || 'DELIVERY_STATUS_UNSPECIFIED',
      driver_id: req.query.driver_id || '',
      limit: req.query.limit ? Number(req.query.limit) : 100,
      offset: req.query.offset ? Number(req.query.offset) : 0,
    });
    res.json(result);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/deliveries/:id - recuperer une livraison
router.get('/:id', async (req, res) => {
  try {
    const d = await trackingClient.GetDelivery({ id: req.params.id });
    res.json(d);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/deliveries/:id/history - historique complet d'une livraison
router.get('/:id/history', async (req, res) => {
  try {
    const result = await trackingClient.GetDeliveryHistory({ delivery_id: req.params.id });
    res.json(result);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// PATCH /api/deliveries/:id/status - avancer le status
router.patch('/:id/status', async (req, res) => {
  try {
    const d = await trackingClient.AdvanceDeliveryStatus({
      delivery_id: req.params.id,
      new_status: req.body.new_status,
    });
    res.json(d);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/deliveries/:id/watch - SSE qui relaie le streaming WatchDelivery
router.get('/:id/watch', async (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  const stream = trackingClient.WatchDelivery({ delivery_id: req.params.id });

  stream.on('data', d => {
    res.write(`data: ${JSON.stringify(d)}\n\n`);
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
