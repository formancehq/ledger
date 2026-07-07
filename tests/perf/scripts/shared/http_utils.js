// HTTP utility functions for k6 tests

import http from 'k6/http';
import { check } from 'k6';

/**
 * Build the bulk URL for a ledger.
 */
export function bulkUrl(httpAddr, ledgerName, atomic) {
  return `${httpAddr}/v3/${ledgerName}/bulk?atomic=${atomic}`;
}

/**
 * Build a CREATE_TRANSACTION bulk element using postings with force.
 */
export function postingsBulkElement(postings, force, metadata) {
  const data = { postings, force };
  if (metadata) data.metadata = metadata;
  return { action: 'CREATE_TRANSACTION', data };
}

/**
 * Build a CREATE_TRANSACTION bulk element using a Numscript.
 */
export function scriptBulkElement(script, vars, metadata) {
  const data = { script: { plain: script, vars: vars || {} } };
  if (metadata) data.metadata = metadata;
  return { action: 'CREATE_TRANSACTION', data };
}

const defaultParams = {
  headers: { 'Content-Type': 'application/json' },
};

/**
 * Send a bulk request and return the response.
 */
export function sendBulk(url, elements) {
  return http.post(url, JSON.stringify(elements), defaultParams);
}

/**
 * Check that a bulk response is successful (HTTP 200).
 */
export function checkBulkSuccess(response) {
  return check(response, {
    'HTTP status is 200': (r) => r.status === 200,
  });
}
