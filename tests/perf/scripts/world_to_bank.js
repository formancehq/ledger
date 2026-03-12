// k6 performance test: world_to_bank scenario
// This test simulates transactions from @world to @bank

import { check } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { connectClient, apply, scriptRequest } from './shared/utils.js';

// Bulk size (number of transactions per request)
const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');

export const options = buildOptions(config);

let client;

// Generate transaction request
function generateTransaction() {
  return scriptRequest(config.ledgerName,
    `send [USD/2 100] (
            source = @world
            destination = @bank
        )`,
  );
}

function generateRequests() {
  const requests = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    requests.push(generateTransaction());
  }
  return requests;
}

export default function () {
  if (!client) client = connectClient(config.grpcAddr);

  const requests = generateRequests();

  const startTime = Date.now();
  const response = apply(client, requests);
  const latency = Date.now() - startTime;

  bulkLatency.add(latency);

  const success = check(response, {
    'bulk operation successful': (r) => r && r.status === grpc.StatusOK,
  });

  if (!success) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}
