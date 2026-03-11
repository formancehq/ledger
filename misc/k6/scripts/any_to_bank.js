// k6 performance test: any_to_bank scenario
// This test simulates transactions from variable sources to @bank with unbounded overdraft

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import grpc from 'k6/net/grpc';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { connectClient, apply, scriptRequest } from './shared/utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);

export const options = buildOptions(config);

let client;

function generateTransaction() {
  return scriptRequest(config.ledgerName,
    `vars {
            account $source
        }
        send [USD/2 100] (
            source = $source allowing unbounded overdraft
            destination = @bank
        )`,
    { source: `src:${uuidv4()}` },
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
