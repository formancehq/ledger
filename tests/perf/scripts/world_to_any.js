// k6 performance test: world_to_any scenario
// This test simulates transactions from @world to variable destinations

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
            account $destination
        }
        send [USD/2 100] (
            source = @world
            destination = $destination
        )`,
    { destination: `dst:${uuidv4()}` },
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
