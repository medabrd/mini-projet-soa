const express = require('express');
const { orderClient } = require('../grpc-clients');

const router = express.Router();

// Helper : transforme une erreur gRPC en reponse HTTP appropriee
function grpcErrorToHttp(err, res) {
  // Codes gRPC : https://grpc.github.io/grpc/core/md_doc_statuscodes.html
  const map = {
    3: 400,  // INVALID_ARGUMENT
    5: 404,  // NOT_FOUND
    9: 412,  // FAILED_PRECONDITION
    13: 500, // INTERNAL
  };
  const status = map[err.code] || 500;
  res.status(status).json({ error: err.message, grpc_code: err.code });
}

// POST /api/orders - creer une commande
router.post('/', async (req, res) => {
  try {
    const order = await orderClient.CreateOrder(req.body);
    res.status(201).json(order);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/orders/:id - recuperer une commande
router.get('/:id', async (req, res) => {
  try {
    const order = await orderClient.GetOrder({ id: req.params.id });
    res.json(order);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// GET /api/orders - lister les commandes (filtres en query string)
router.get('/', async (req, res) => {
  try {
    const result = await orderClient.ListOrders({
      customer_id: req.query.customer_id || '',
      status: req.query.status || 'ORDER_STATUS_UNSPECIFIED',
      limit: req.query.limit ? Number(req.query.limit) : 100,
      offset: req.query.offset ? Number(req.query.offset) : 0,
    });
    res.json(result);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// PATCH /api/orders/:id/status - mettre a jour le status
router.patch('/:id/status', async (req, res) => {
  try {
    const order = await orderClient.UpdateOrderStatus({
      id: req.params.id,
      status: req.body.status,
      assigned_driver_id: req.body.assigned_driver_id || '',
    });
    res.json(order);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

// POST /api/orders/:id/cancel - annuler une commande
router.post('/:id/cancel', async (req, res) => {
  try {
    const order = await orderClient.CancelOrder({
      id: req.params.id,
      reason: req.body.reason || '',
    });
    res.json(order);
  } catch (err) {
    grpcErrorToHttp(err, res);
  }
});

module.exports = router;
