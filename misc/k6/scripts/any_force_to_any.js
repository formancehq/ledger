// k6 performance test: any_force_to_any scenario
// This test simulates transactions from variable sources to variable destinations using simple postings with force=true
// Force mode bypasses balance checks, allowing accounts to go negative (similar to unbounded overdraft)

import { check } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { connectClient, apply, postingsRequest } from './shared/utils.js';
import exec from 'k6/execution';

// Bulk size (number of transactions per request)
const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');

export const options = buildOptions(config);

let client;

function generateTransaction(iterationBase, index) {
  const uniqueId = iterationBase * BULK_SIZE + index;
  const source = `src:${uniqueId}`;
  const destination = `dst:${uniqueId}`;
  return postingsRequest(config.ledgerName,
    [{ source, destination, amount: 100, asset: 'USD/2' }],
    true, // force
  );
}

function generateRequests(iteration) {
  const requests = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    requests.push(generateTransaction(iteration, i));
  }
  return requests;
}

export default function () {
  if (!client) client = connectClient(config.grpcAddr);

  const requests = generateRequests(exec.scenario.iterationInTest);

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
