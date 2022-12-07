const express = require('express');
const redoc = require('redoc-express');
const { merge, isErrorResult } = require('openapi-merge');
const { Swagger } = require('atlassian-openapi');
 
const app = express();
const port = 3000;
const hostname = "0.0.0.0";
 
app.get('/openapi.json', (req, res) => {
  res.sendFile('generate.json', { root: './sdk/' });
});
 
app.get(
  '/',
  redoc({

    title: 'Formance Stack API Docs',
    specUrl: '/openapi.json'
  })
);
 
app.listen(port, hostname, () => console.log(`App listening on port http://${hostname}:${port}!`));