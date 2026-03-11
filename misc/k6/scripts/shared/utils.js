// Utility functions for k6 gRPC tests

import grpc from 'k6/net/grpc';
import { check } from 'k6';

const SERVICE_METHOD = 'ledger.BucketService/Apply';

/**
 * Create and connect a gRPC client with reflection.
 */
export function connectClient(grpcAddr) {
  const client = new grpc.Client();
  client.connect(grpcAddr, { plaintext: true, reflect: true, timeout: '30s' });
  return client;
}

/**
 * Convert a small integer to a Uint256 proto message.
 * Only v0 is used; amounts must be < 2^53.
 */
export function uint256(value) {
  return { v0: value, v1: 0, v2: 0, v3: 0 };
}

/**
 * Convert a flat { key: "val" } object to a MetadataSet proto message.
 */
export function metadataSet(obj) {
  if (!obj) return undefined;
  const entries = [];
  for (const [key, value] of Object.entries(obj)) {
    entries.push({ key: key, value: { value: String(value) } });
  }
  return { metadata: entries };
}

/**
 * Build a Request with LedgerApplyRequest.create_transaction using a Script.
 */
export function scriptRequest(ledgerName, script, vars, metadata) {
  const payload = {
    script: { plain: script, vars: vars || {} },
  };
  const ms = metadataSet(metadata);
  if (ms) {
    payload.metadata = ms;
  }
  return {
    apply: {
      ledger: ledgerName,
      create_transaction: payload,
    },
  };
}

/**
 * Build a Request with LedgerApplyRequest.create_transaction using Postings + force.
 */
export function postingsRequest(ledgerName, postings, force, metadata) {
  const protoPostings = postings.map((p) => ({
    source: p.source,
    destination: p.destination,
    amount: uint256(p.amount),
    asset: p.asset,
  }));
  const payload = {
    postings: protoPostings,
    force: force,
  };
  const ms = metadataSet(metadata);
  if (ms) {
    payload.metadata = ms;
  }
  return {
    apply: {
      ledger: ledgerName,
      create_transaction: payload,
    },
  };
}

/**
 * Build a Request of type create_ledger.
 */
export function createLedgerRequest(name) {
  return { create_ledger: { name: name } };
}

/**
 * Build a Request of type delete_ledger.
 */
export function deleteLedgerRequest(name) {
  return { delete_ledger: { name: name } };
}

/**
 * Invoke ledger.BucketService/Apply with the given requests.
 * Returns the gRPC response object.
 */
export function apply(client, requests) {
  const response = client.invoke(SERVICE_METHOD, { requests: requests });

  check(response, {
    'gRPC status is OK': (r) => r && r.status === grpc.StatusOK,
  });

  return response;
}
