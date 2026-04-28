const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');

const ordersRoutes = require('./src/rest/orders');
const driversRoutes = require('./src/rest/drivers');
const deliveriesRoutes = require('./src/rest/deliveries');

const PORT = Number(process.env.PORT) || 3000;

const app = express();
app.use(cors());
app.use(bodyParser.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'gateway' });
});

// Routes REST
app.use('/api/orders', ordersRoutes);
app.use('/api/drivers', driversRoutes);
app.use('/api/deliveries', deliveriesRoutes);

app.listen(PORT, () => {
  console.log(`API Gateway demarree sur http://localhost:${PORT}`);
  console.log(`Health        : http://localhost:${PORT}/health`);
  console.log(`REST orders    : http://localhost:${PORT}/api/orders`);
  console.log(`REST drivers   : http://localhost:${PORT}/api/drivers`);
  console.log(`REST deliveries: http://localhost:${PORT}/api/deliveries`);
});
