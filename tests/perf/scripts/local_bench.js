// Local runner around the existing world_to_bank workload.
import http from 'k6/http';
import { fail, sleep } from 'k6';
import worldToBank from './world_to_bank.js';
import { config } from './shared/config.js';

function integer(name, fallback, minimum) {
  const value = Number(__ENV[name] || fallback);
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}`);
  }
  return value;
}

const warmup = __ENV.BENCH_WARMUP === 'true';
const seconds = warmup ? 5 : integer('BENCH_DURATION', 60, 1);
const minTPS = integer('BENCH_MIN_TPS', 100000, 0);
const vus = integer('BENCH_VUS', 100, 1);

export const options = {
  setupTimeout: '3m',
  scenarios: {
    writes: {
      executor: 'constant-vus',
      vus,
      duration: `${seconds}s`,
      // Drain the warmup; stop measurement at the deadline, excluding unfinished bulks.
      gracefulStop: warmup ? '5s' : '0s',
    },
  },
  thresholds: {
    errors: ['rate==0'],
    transactions_created: warmup ? ['count>0'] : ['count>0', `count>=${minTPS * seconds}`],
  },
};

export function setup() {
  const deadline = Date.now() + 170000;
  while (http.get(`${config.httpAddr}/clusterz`, { timeout: '1s' }).status !== 200) {
    if (Date.now() >= deadline) fail('Ledger did not become ready; see build/bench/ledger.log');
    sleep(0.2);
  }
  if (warmup) {
    const response = http.post(`${config.httpAddr}/v3/${config.ledgerName}`, '{}', {
      headers: { 'Content-Type': 'application/json' },
      timeout: '5s',
    });
    if (response.status !== 201) fail(`Cannot create benchmark ledger: ${response.status} ${response.body}`);
  }
}

export default worldToBank;

export function handleSummary(data) {
  // Setup failures have no measured samples; avoid presenting them as zero-TPS runs.
  if (!data.metrics.bulk_latency) return {};
  const transactions = data.metrics.transactions_created?.values.count || 0;
  const errors = data.metrics.errors?.values.passes || 0;
  const p95 = data.metrics.bulk_latency?.values['p(95)'] ?? null;
  const tps = transactions / seconds;
  const summary = { seconds, vus, bulkSize: 50, minTPS, transactions, tps, errors, p95Milliseconds: p95 };
  const outputs = {
    stdout: `${warmup ? 'Warmup' : 'Benchmark'}: ${tps.toFixed(0)} TPS, p95 ${p95 === null ? 'unavailable' : `${p95.toFixed(2)} ms`}, ${errors} errors\n`,
  };
  if (!warmup) outputs['build/bench/summary.json'] = `${JSON.stringify(summary, null, 2)}\n`;
  return outputs;
}
