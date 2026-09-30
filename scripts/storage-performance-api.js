import http from 'k6/http';
import { Counter, Rate, Trend } from 'k6/metrics';
import execution from 'k6/execution';

const writeFailures = new Rate('write_failures');
const readFailures = new Rate('read_failures');
const successfulWrites = new Counter('successful_writes');
const successfulReads = new Counter('successful_reads');
const writeLatency = new Trend('write_latency_ms', true);
const readLatency = new Trend('read_latency_ms', true);

const duration = __ENV.MEASURE_SECONDS + 's';
export const options = {
  scenarios: {
    writes: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.WRITE_RATE || 30), timeUnit: '1s', duration,
      preAllocatedVUs: 16, maxVUs: 64, exec: 'write',
    },
    reads: {
      executor: 'constant-arrival-rate', rate: Number(__ENV.READ_RATE || 15), timeUnit: '1s', duration,
      preAllocatedVUs: 8, maxVUs: 32, exec: 'read',
    },
  },
  summaryTrendStats: ['avg', 'med', 'p(95)', 'p(99)', 'max'],
  thresholds: {
    write_failures: ['rate==0'], read_failures: ['rate==0'],
    http_req_failed: ['rate==0'], dropped_iterations: ['count==0'],
  },
};

function endpoint(path) {
  return `${__ENV.HTTP_ADDR}/v3/${__ENV.LEDGER_NAME}${path}`;
}

function payload(id) {
  let value = (id + 123456789) >>> 0;
  let result = '';
  const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  for (let i = 0; i < 4096; i++) {
    value ^= value << 13;
    value ^= value >>> 17;
    value ^= value << 5;
    result += alphabet[(value >>> 0) % alphabet.length];
  }
  return result;
}

export function write() {
  const id = execution.scenario.iterationInTest;
  const body = [{
    action: 'CREATE_TRANSACTION',
    data: {
      postings: [{source: 'world', destination: `dst:${__ENV.RUN_ID}:${id}`, amount: 100, asset: 'USD/2'}],
      metadata: {payload: payload(id)},
    },
  }];
  const response = http.post(endpoint('/bulk?atomic=true'), JSON.stringify(body), {
    headers: {'Content-Type': 'application/json'}, timeout: '10s',
  });
  writeLatency.add(response.timings.duration);
  let valid = response.status === 200;
  if (valid) {
    try {
      const parsed = response.json();
      valid = !parsed.errorCode && Array.isArray(parsed.data) && parsed.data.length === 1 &&
        parsed.data[0].responseType === 'CREATE_TRANSACTION' && !parsed.data[0].errorCode;
    } catch (_) { valid = false; }
  }
  writeFailures.add(!valid);
  if (valid) successfulWrites.add(1);
}

export function read() {
  const response = http.get(endpoint('/accounts?pageSize=20'), {timeout: '10s'});
  readLatency.add(response.timings.duration);
  let valid = response.status === 200;
  if (valid) {
    try { valid = !response.json().errorCode; } catch (_) { valid = false; }
  }
  readFailures.add(!valid);
  if (valid) successfulReads.add(1);
}

export function handleSummary(data) {
  const metric = (name, key) => data.metrics[name]?.values?.[key] ?? null;
  const report = {
    write_p50_ms: metric('write_latency_ms', 'med'),
    write_p95_ms: metric('write_latency_ms', 'p(95)'),
    write_p99_ms: metric('write_latency_ms', 'p(99)'),
    read_p50_ms: metric('read_latency_ms', 'med'),
    read_p95_ms: metric('read_latency_ms', 'p(95)'),
    read_p99_ms: metric('read_latency_ms', 'p(99)'),
    successful_writes: metric('successful_writes', 'count'),
    successful_reads: metric('successful_reads', 'count'),
    write_failure_rate: metric('write_failures', 'rate'),
    read_failure_rate: metric('read_failures', 'rate'),
    dropped_iterations: metric('dropped_iterations', 'count') ?? 0,
  };
  return {[__ENV.SUMMARY_PATH]: JSON.stringify(report, null, 2), stdout: JSON.stringify(report) + '\n'};
}
