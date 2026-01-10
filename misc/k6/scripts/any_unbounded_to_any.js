// k6 performance test: any_unbounded_to_any scenario
// This test simulates transactions from variable sources to variable destinations with unbounded overdraft

import { check } from 'k6';
import { Rate, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { config } from './shared/config.js';
import { bulkOperation } from './shared/utils.js';
import exec from 'k6/execution';

// Custom metrics
const errorRate = new Rate('errors');
const transactionLatency = new Trend('transaction_latency', true);

export const options = {
  thresholds: {
    errors: ['rate<0.1'],
    http_req_duration: ['p(95)<500'],
    transaction_latency: ['p(95)<500'],
  },
  stages: [
    { duration: '30s', target: config.vus },
    { duration: config.duration, target: config.vus },
    { duration: '30s', target: 0 },
  ]
};

function generateTransaction(iteration) {
  const source = `src:${exec.scenario.iterationInTest}`
  const destination = `dst:${exec.scenario.iterationInTest}`
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: `vars {
            account $source
            account $destination
        }
        send [USD/2 100] (
            source = $source allowing unbounded overdraft
            destination = $destination
        )`,
        vars: {
          destination,
          source,
        },
      },
    },
  };
}

export function setup() {
  console.info("Start with config: ");
  console.info(JSON.stringify(options, null, 2));
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
  summary += `${indent}Test: any_unbounded_to_any\n`;
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

