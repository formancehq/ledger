// k6 performance test: world_to_bank scenario
// This test simulates transactions from @world to @bank

import { Rate, Trend, Counter } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkUrl, postingsBulkElement, scriptBulkElement, sendBulk, checkBulkSuccess } from './shared/http_utils.js';
import { logError } from './shared/log_utils.js';

// Bulk size (number of transactions per request)
const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');
const BULK_ATOMIC = (__ENV.BULK_ATOMIC || 'true') === 'true';
const USE_NUMSCRIPT = (__ENV.USE_NUMSCRIPT || 'false') === 'true';

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');

export const options = buildOptions(config);

const url = bulkUrl(config.httpAddr, config.ledgerName, BULK_ATOMIC);

const NUMSCRIPT = `send [USD/2 100] (
  source = @world
  destination = @bank
)`;

function generateElements() {
  const elements = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    if (USE_NUMSCRIPT) {
      elements.push(scriptBulkElement(NUMSCRIPT));
    } else {
      elements.push(postingsBulkElement(
        [{ source: 'world', destination: 'bank', amount: 100, asset: 'USD/2' }],
        true,
      ));
    }
  }
  return elements;
}

const body = generateElements();

export default function () {
  const startTime = Date.now();
  const response = sendBulk(url, body);
  const latency = Date.now() - startTime;

  bulkLatency.add(latency);

  if (!checkBulkSuccess(response)) {
    errorRate.add(1);
    logError(response);
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}
