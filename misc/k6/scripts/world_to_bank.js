// k6 performance test: world_to_bank scenario
// This test simulates transactions from @world to @bank

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { connectClient, apply, scriptRequest } from './shared/utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);

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

export default function () {
  if (!client) client = connectClient(config.grpcAddr);

  const request = generateTransaction();

  const startTime = Date.now();
  const response = apply(client, [request]);
  const latency = Date.now() - startTime;

  bulkLatency.add(latency);

  const success = check(response, {
    'transaction created successfully': (r) => r && r.status === grpc.StatusOK,
  });

  errorRate.add(!success);
}
