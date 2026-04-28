const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { ApolloServer } = require('@apollo/server');
const { expressMiddleware } = require('@apollo/server/express4');

const ordersRoutes = require('./src/rest/orders');
const driversRoutes = require('./src/rest/drivers');
const deliveriesRoutes = require('./src/rest/deliveries');
const { typeDefs } = require('./src/graphql/schema');
const { resolvers } = require('./src/graphql/resolvers');

const PORT = Number(process.env.PORT) || 3000;

async function main() {
  const app = express();
  app.use(cors());
  app.use(bodyParser.json());

  // Health
  app.get('/health', (_req, res) => {
    res.json({ status: 'ok', service: 'gateway' });
  });

  // REST
  app.use('/api/orders', ordersRoutes);
  app.use('/api/drivers', driversRoutes);
  app.use('/api/deliveries', deliveriesRoutes);

  // GraphQL via Apollo Server v4 monte comme middleware Express
  const apollo = new ApolloServer({ typeDefs, resolvers });
  await apollo.start();
  app.use('/graphql', expressMiddleware(apollo));

  app.listen(PORT, () => {
    console.log(`API Gateway demarree sur http://localhost:${PORT}`);
    console.log(`Health        : http://localhost:${PORT}/health`);
    console.log(`REST          : http://localhost:${PORT}/api/{orders,drivers,deliveries}`);
    console.log(`GraphQL       : http://localhost:${PORT}/graphql (POST + Apollo Sandbox)`);
  });
}

main().catch(err => {
  console.error('Erreur fatale:', err);
  process.exit(1);
});
