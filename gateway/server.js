const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');

const PORT = Number(process.env.PORT) || 3000;

const app = express();
app.use(cors());
app.use(bodyParser.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'gateway' });
});

app.listen(PORT, () => {
  console.log(`API Gateway demarree sur http://localhost:${PORT}`);
  console.log(`Health: http://localhost:${PORT}/health`);
});
