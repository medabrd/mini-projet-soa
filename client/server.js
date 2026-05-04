// Mini serveur Express qui sert juste les fichiers statiques du client web.
// Le client se connecte ensuite directement au gateway sur le port 3000
// (REST + GraphQL + SSE). Le gateway a deja cors() active, donc pas de probleme
// de CORS depuis ce serveur a un autre port.
const path = require('node:path');
const express = require('express');

const PORT = Number(process.env.PORT) || 8081;
const app = express();

// Redirection de /rest.html vers / (compat avec liens externes obsoletes)
app.get('/rest.html', (_req, res) => res.redirect(301, '/'));

app.use(express.static(path.join(__dirname, 'public')));

app.listen(PORT, () => {
  console.log(`Client web disponible sur http://localhost:${PORT}`);
});
