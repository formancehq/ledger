// k6 performance test: any_bounded_to_any scenario
// This test simulates transactions from variable sources to variable destinations with bounded overdraft

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { config } from '../config.js';
import { bulkOperation } from '../utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const transactionLatency = new Trend('transaction_latency', true);

export const options = {
  thresholds: {
    errors: ['rate<0.1'],
    http_req_duration: ['p(95)<500'],
    transaction_latency: ['p(95)<500'],
  },
};

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

export function setup() {
  options.stages = [
    { duration: '10s', target: config.vus },
    { duration: config.duration, target: config.vus },
    { duration: '10s', target: 0 },
  ];
}

export default function () {
  const element = generateTransaction(__ITER);
  
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
  summary += `${indent}Test: any_bounded_to_any\n`;
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

