// k6 performance test: any_to_bank scenario
// This test simulates transactions from variable sources to @bank with unbounded overdraft

import { Rate, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkUrl, scriptBulkElement, sendBulk, checkBulkSuccess } from './shared/http_utils.js';

// Custom metrics
const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);

export const options = buildOptions(config);

const url = bulkUrl(config.httpAddr, config.ledgerName, true);

function generateElement() {
  return scriptBulkElement(
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
  const startTime = Date.now();
  const response = sendBulk(url, [generateElement()]);
  const latency = Date.now() - startTime;

  bulkLatency.add(latency);

  if (!checkBulkSuccess(response)) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
  }
}
