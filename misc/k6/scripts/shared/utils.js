// Utility functions for k6 tests

import http from 'k6/http';
import { check } from 'k6';

/**
 * Create HTTP request options
 */
export function getRequestOptions(config) {
  return {
    headers: {
      'Content-Type': 'application/json',
    },
    timeout: config.http.timeout,
  };
}

/**
 * Execute a bulk operation
 * Returns the HTTP response object
 */
export function bulkOperation(config, ledgerName, elements) {
  const url = `${config.ledgerUrl}/${ledgerName}/_bulk`;
  const options = getRequestOptions(config);
  
  const response = http.post(url, JSON.stringify(elements), options);
  
  // Check is performed by the caller to track metrics
  check(response, {
    'bulk operation status is 200': (r) => r.status === 200,
  });

  return response;
}

/**
 * Create a transaction bulk element from script data
 */
export function createTransactionElement(script, vars = {}, idempotencyKey = null) {
  const element = {
    action: 'CREATE_TRANSACTION',
    data: {
      script: {
        plain: script,
        vars: vars,
      },
    },
  };

  if (idempotencyKey) {
    element.ik = idempotencyKey;
  }

  return element;
}
