// Log utility functions for k6 tests

/**
 * Log details of a failed HTTP response.
 * Logs status, error (if connection-level), and a truncated body.
 */
export function logError(response) {
  const body = response.body ? response.body.substring(0, 500) : '<empty>';
  console.error(`HTTP ${response.status} | error="${response.error || ''}" | body=${body}`);
}
