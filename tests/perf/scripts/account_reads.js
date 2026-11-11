// Read one account and one page of accounts after seeding a fixed account set.
// The separate trends expose the cost of one incarnation lookup on GetAccount
// and one lookup per returned account on ListAccounts.

import http from 'k6/http';
import exec from 'k6/execution';
import { sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';
import { config } from './shared/config.js';
import { buildOptions } from './shared/options.js';
import { bulkUrl, postingsBulkElement, sendBulk } from './shared/http_utils.js';

const ACCOUNT_COUNT = parseInt(__ENV.READ_ACCOUNT_COUNT || '200');
const PAGE_SIZE = parseInt(__ENV.READ_PAGE_SIZE || '50');
const SEED_BULK_SIZE = 50;

const baseUrl = `${config.httpAddr}/v3/${config.ledgerName}/accounts`;
const listUrl = `${baseUrl}?pageSize=${PAGE_SIZE}`;
const seedUrl = bulkUrl(config.httpAddr, config.ledgerName, true);

const errors = new Rate('errors');
const getLatency = new Trend('get_account_latency', true);
const listLatency = new Trend('list_accounts_latency', true);
const getCount = new Counter('get_account_reads');
const listCount = new Counter('list_accounts_reads');

export const options = {
  ...buildOptions(config),
  thresholds: {
    errors: ['rate<0.1'],
    get_account_latency: ['p(95)<500'],
    list_accounts_latency: ['p(95)<500'],
  },
};

export function setup() {
  if (ACCOUNT_COUNT < PAGE_SIZE || PAGE_SIZE < 1) {
    throw new Error('READ_ACCOUNT_COUNT must be at least READ_PAGE_SIZE, and READ_PAGE_SIZE must be positive');
  }

  const addresses = Array.from({ length: ACCOUNT_COUNT }, (_, i) => `bench-read:${String(i).padStart(4, '0')}`);
  for (let start = 0; start < addresses.length; start += SEED_BULK_SIZE) {
    const elements = addresses.slice(start, start + SEED_BULK_SIZE).map((address) =>
      postingsBulkElement([{ source: 'world', destination: address, amount: 1, asset: 'USD/2' }], true));
    const response = sendBulk(seedUrl, elements);
    if (response.status !== 200) {
      throw new Error(`seeding accounts failed: HTTP ${response.status}: ${response.body?.substring(0, 300)}`);
    }
  }

  // Account listing follows the asynchronous index builder. Do not start the
  // timed load until the entire first page is visible on the fresh cluster.
  for (let attempt = 0; attempt < 60; attempt++) {
    const response = http.get(listUrl);
    if (response.status === 200 && response.json('data')?.length === PAGE_SIZE) {
      return { addresses };
    }
    sleep(1);
  }
  throw new Error(`account index did not expose ${PAGE_SIZE} seeded accounts within 60 seconds`);
}

export default function (data) {
  const address = data.addresses[exec.scenario.iterationInTest % data.addresses.length];
  const getResponse = http.get(`${baseUrl}/${address}`);
  getLatency.add(getResponse.timings.duration);
  getCount.add(1);

  const listResponse = http.get(listUrl);
  listLatency.add(listResponse.timings.duration);
  listCount.add(1);

  errors.add(getResponse.status !== 200 || listResponse.status !== 200);
}
