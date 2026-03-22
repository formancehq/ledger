// k6 performance test: any_unbounded_to_any scenario
// This test simulates transactions from variable sources to variable destinations with unbounded overdraft

import { Rate, Trend, Counter } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkUrl, scriptBulkElement, sendBulk, checkBulkSuccess } from './shared/http_utils.js';
import exec from 'k6/execution';

// Bulk size (number of transactions per request)
const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');
const BULK_ATOMIC = (__ENV.BULK_ATOMIC || 'true') === 'true';

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');

export const options = buildOptions(config);

const url = bulkUrl(config.httpAddr, config.ledgerName, BULK_ATOMIC);

function generateElement(iterationBase, index) {
  const uniqueId = iterationBase * BULK_SIZE + index;
  const source = `src:${uniqueId}`;
  const destination = `dst:${uniqueId}`;
  return scriptBulkElement(
    `vars {
            account $source
            account $destination
        }
        send [USD/2 100] (
            source = $source allowing unbounded overdraft
            destination = $destination
        )`,
    { destination, source },
  );
}

function generateElements(iteration) {
  const elements = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    elements.push(generateElement(iteration, i));
  }
  return elements;
}

export default function () {
  const elements = generateElements(exec.scenario.iterationInTest);

  const startTime = Date.now();
  const response = sendBulk(url, elements);
  const latency = Date.now() - startTime;

  bulkLatency.add(latency);

  if (!checkBulkSuccess(response)) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}
