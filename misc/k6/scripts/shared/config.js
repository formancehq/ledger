// k6 configuration file
// This file exports configuration options for k6 tests

export const config = {
  // Base URL of the ledger service
  ledgerUrl: __ENV.LEDGER_URL || 'http://localhost:9000',
  
  // Ledger name to use for tests
  ledgerName: __ENV.LEDGER_NAME || 'ledger0',
  
  // Test duration (can be overridden by k6 options)
  duration: __ENV.DURATION || '30s',
  
  // Number of virtual users (VUs)
  vus: parseInt(__ENV.VUS || '10'),
  
  // Maximum number of VUs
  maxVUs: parseInt(__ENV.MAX_VUS || '100'),
  
  // HTTP client configuration
  http: {
    timeout: '30s',
    maxRedirects: 0,
  },
};

