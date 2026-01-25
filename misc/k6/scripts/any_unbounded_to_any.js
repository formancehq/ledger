// k6 performance test: any_unbounded_to_any scenario
// This test simulates transactions from variable sources to variable destinations with unbounded overdraft

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkOperation } from './shared/utils.js';
import exec from 'k6/execution';

// Custom metrics
const errorRate = new Rate('errors');
const transactionLatency = new Trend('transaction_latency', true);

export const options = buildOptions(config);

function generateTransaction(iteration) {
  const source = `src:${exec.scenario.iterationInTest}`;
  const destination = `dst:${exec.scenario.iterationInTest}`;
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      postings: [
        {
          source,
          destination,
          asset: 'USD/2',
          amount: 100,
        },
      ],
    },
  };
}

export default function () {
  const ledgerName = config.ledgerName;
  const element = generateTransaction(__ITER);
  
  const startTime = Date.now();
  const response = bulkOperation(config, ledgerName, [element]);
  const latency = Date.now() - startTime;
  
  transactionLatency.add(latency);
  
  const success = check(response, {
    'transaction created successfully': (r) => r.status === 200,
    'response time < 500ms': (r) => r.timings.duration < 500,
  });

  if (!success) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
  }
}