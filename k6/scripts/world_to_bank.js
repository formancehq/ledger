// k6 performance test: world_to_bank scenario
// This test simulates transactions from @world to @bank

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { config } from '../config.js';
import { bulkOperation } from '../utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const transactionLatency = new Trend('transaction_latency', true);

// Options are set dynamically in setup() to access environment variables
export const options = {
  thresholds: {
    errors: ['rate<0.1'], // Error rate should be less than 10%
    http_req_duration: ['p(95)<500'], // 95% of requests should be below 500ms
    transaction_latency: ['p(95)<500'], // 95% of transactions should be below 500ms
  },
};

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

export function setup() {
  // Set stages dynamically based on config
  options.stages = [
    { duration: '10s', target: config.vus },
    { duration: config.duration, target: config.vus },
    { duration: '10s', target: 0 },
  ];
}

export default function () {
  // Generate transaction
  const element = generateTransaction(__ITER);
  
  // Execute bulk operation
  const startTime = Date.now();
  const response = bulkOperation(config, config.ledgerName, [element]);
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

export function handleSummary(data) {
  const indent = '  ';
  
  let summary = '\n';
  summary += `${indent}Test: world_to_bank\n`;
  summary += `${indent}Duration: ${(data.state.testRunDurationMs / 1000).toFixed(2)}s\n`;
  summary += `${indent}VUs: ${data.state.vus}\n`;
  summary += `${indent}Iterations: ${data.metrics.iterations?.values?.count || 0}\n`;
  
  if (data.metrics.errors) {
    summary += `${indent}Errors: ${(data.metrics.errors.values.rate * 100).toFixed(2)}%\n`;
  }
  
  if (data.metrics.transaction_latency) {
    const p95 = data.metrics.transaction_latency.values['p(95)'];
    summary += `${indent}Transaction Latency (p95): ${p95 ? p95.toFixed(2) : 'N/A'}ms\n`;
  }
  
  if (data.metrics.http_req_duration) {
    const p95 = data.metrics.http_req_duration.values['p(95)'];
    summary += `${indent}HTTP Request Duration (p95): ${p95 ? p95.toFixed(2) : 'N/A'}ms\n`;
  }
  
  if (data.metrics.http_reqs) {
    summary += `${indent}Requests/sec: ${data.metrics.http_reqs.values.rate.toFixed(2)}\n`;
  }
  
  return {
    'stdout': summary,
    'summary.json': JSON.stringify(data),
  };
}

