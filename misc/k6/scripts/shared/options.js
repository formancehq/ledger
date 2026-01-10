export function buildOptions(config) {
  return {
    thresholds: {
      errors: ['rate<0.1'],
      http_req_duration: ['p(95)<500'],
      transaction_latency: ['p(95)<500'],
    },
    stages: [
      { duration: '30s', target: config.vus },
      { duration: config.duration, target: config.vus },
      { duration: '30s', target: 0 },
    ],
  };
}
