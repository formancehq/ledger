// k6 performance test: reused_numscript_gen1 scenario
//
// Business scenario: marketplace merchant settlement.
// Every payment uses the same immutable, versioned Numscript template while
// the merchant account changes from one payment to the next. The transaction
// volumes therefore keep moving through Gen0, but the template's
// NumscriptVersion and NumscriptContent keys are read repeatedly without being
// written. After a cache rotation those stable keys live in Gen1, which is the
// workload targeted by the Gen1 preload optimization. The benchmark uses the
// `latest` selector so both keys are part of the admission coverage.

import { check } from 'k6';
import http from 'k6/http';
import exec from 'k6/execution';
import { Rate, Trend, Counter } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkUrl, sendBulk, checkBulkSuccess } from './shared/http_utils.js';

const BULK_SIZE = parseInt(__ENV.BULK_SIZE || '1');
const MERCHANT_POOL_SIZE = parseInt(__ENV.MERCHANT_POOL_SIZE || '10000');
const SCRIPT_NAME = __ENV.SCRIPT_NAME || 'marketplace-settlement';
const SCRIPT_SAVE_VERSION = __ENV.SCRIPT_SAVE_VERSION || '1.0.0';
const SCRIPT_REFERENCE_VERSION = __ENV.SCRIPT_REFERENCE_VERSION || 'latest';
const SETTLEMENT_ACCOUNT = __ENV.SETTLEMENT_ACCOUNT || 'platform:settlement';
const AMOUNT = __ENV.AMOUNT || 'USD/2 100';

const SETTLEMENT_SCRIPT = `vars {
    account $merchant
    monetary $amount
}
send $amount (
    source = @${SETTLEMENT_ACCOUNT} allowing unbounded overdraft
    destination = $merchant
)`;

function assertStoredTemplate(response, expectedVersion) {
  if (response.status !== 200) {
    throw new Error(`failed to read ${SCRIPT_NAME}@${expectedVersion}: HTTP ${response.status}`);
  }

  const data = response.json('data');
  if (!data || data.name !== SCRIPT_NAME || data.version !== expectedVersion || data.content !== SETTLEMENT_SCRIPT) {
    throw new Error(`stored ${SCRIPT_NAME}@${expectedVersion} does not match the benchmark template`);
  }
}

const errorRate = new Rate('errors');
const bulkLatency = new Trend('bulk_latency', true);
const transactionsCreated = new Counter('transactions_created');

export const options = buildOptions(config);

const url = bulkUrl(config.httpAddr, config.ledgerName, true);

function merchantFor(iteration, offset) {
  const id = (iteration * BULK_SIZE + offset) % MERCHANT_POOL_SIZE;
  return `merchant:${id}`;
}

function settlementElement(iteration, offset) {
  return {
    action: 'CREATE_TRANSACTION',
    data: {
      scriptReference: {
        name: SCRIPT_NAME,
        version: SCRIPT_REFERENCE_VERSION,
        vars: {
          merchant: merchantFor(iteration, offset),
          amount: AMOUNT,
        },
      },
    },
  };
}

function settlementElements(iteration) {
  const elements = [];
  for (let i = 0; i < BULK_SIZE; i++) {
    elements.push(settlementElement(iteration, i));
  }
  return elements;
}

export function setup() {
  const scriptURL = `${config.httpAddr}/v3/${config.ledgerName}/numscripts/${SCRIPT_NAME}`;
  const response = http.put(scriptURL, JSON.stringify({
    content: SETTLEMENT_SCRIPT,
    version: SCRIPT_SAVE_VERSION,
  }), { headers: { 'Content-Type': 'application/json' } });

  check(response, {
    'setup: settlement template saved': (r) => r.status === 201 || r.status === 409,
  });

  if (response.status !== 201 && response.status !== 409) {
    throw new Error(`failed to save ${SCRIPT_NAME}: HTTP ${response.status}`);
  }

  const stored = http.get(`${scriptURL}?version=${encodeURIComponent(SCRIPT_SAVE_VERSION)}`);
  assertStoredTemplate(stored, SCRIPT_SAVE_VERSION);

  const latest = http.get(scriptURL);
  assertStoredTemplate(latest, SCRIPT_SAVE_VERSION);

  console.log(`Setup complete: ${SCRIPT_NAME}@${SCRIPT_SAVE_VERSION} (reference: ${SCRIPT_REFERENCE_VERSION})`);
  console.log(`Merchant pool: ${MERCHANT_POOL_SIZE}, bulk size: ${BULK_SIZE}`);

  return { script: `${SCRIPT_NAME}@${SCRIPT_REFERENCE_VERSION}` };
}

export default function () {
  const elements = settlementElements(exec.scenario.iterationInTest);
  const startTime = Date.now();
  const response = sendBulk(url, elements);
  bulkLatency.add(Date.now() - startTime);

  if (!checkBulkSuccess(response)) {
    errorRate.add(1);
  } else {
    errorRate.add(0);
    transactionsCreated.add(BULK_SIZE);
  }
}

export function handleSummary(data) {
  const created = data.metrics.transactions_created?.values?.count || 0;
  const duration = data.state.testRunDurationMs / 1000;

  return {
    stdout: JSON.stringify({
      test: 'reused_numscript_gen1',
      config: {
        script: `${SCRIPT_NAME}@${SCRIPT_REFERENCE_VERSION}`,
        merchant_pool_size: MERCHANT_POOL_SIZE,
        bulk_size: BULK_SIZE,
        amount: AMOUNT,
      },
      metrics: {
        transactions_created: created,
        error_rate: data.metrics.errors?.values?.rate || 0,
        p50_latency: data.metrics.bulk_latency?.values?.['p(50)'] || 0,
        p95_latency: data.metrics.bulk_latency?.values?.['p(95)'] || 0,
        p99_latency: data.metrics.bulk_latency?.values?.['p(99)'] || 0,
        avg_latency: data.metrics.bulk_latency?.values?.avg || 0,
        tps: duration > 0 ? created / duration : 0,
      },
    }, null, 2),
  };
}
