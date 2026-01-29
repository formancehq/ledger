// k6 performance test: any_bounded_to_any scenario
// This test simulates transactions from variable sources to variable destinations with bounded overdraft

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkOperation } from './shared/utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);

export const options = buildOptions(config);

function generateTransaction(iteration) {
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
            account $source
            account $destination
        }
        send [USD/2 100] (
            source = $source allowing overdraft up to [USD/2 100]
            destination = $destination
        )`,
        vars: {
          destination: `dst:${uuidv4()}`,
          source: `src:${uuidv4()}`,
        },
      },
    },
  };
}

export default function () {
  const ledgerName = config.ledgerName;
  const element = generateTransaction(__ITER);
  
  const startTime = Date.now();
  const response = bulkOperation(config, ledgerName, [element]);
  const latency = Date.now() - startTime;
  
  bulkLatency.add(latency);
  
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
