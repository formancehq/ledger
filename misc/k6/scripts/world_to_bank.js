// k6 performance test: world_to_bank scenario
// This test simulates transactions from @world to @bank

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkOperation } from './shared/utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const transactionLatency = new Trend('transaction_latency', true);

export const options = buildOptions(config);

// Generate transaction script
function generateTransaction(iteration) {
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `send [USD/2 100] (
            source = @world
            destination = @bank
        )`,
        vars: {},
      },
    },
  };
}

export default function () {
  const ledgerName = config.ledgerName;
  // Generate transaction
  const element = generateTransaction(__ITER);
  
  // Execute bulk operation
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
