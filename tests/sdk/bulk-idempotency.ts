import type { BulkOperationsRequest } from "../../build/bulk-sdk/esm/models/operations/bulk-operations.js";

// Compile against the regenerated SDK: an untyped extra-header escape hatch
// cannot satisfy this assertion that the operation owns a batch identity input.
export const atomicBulk: BulkOperationsRequest = {
  ledgerName: "ledger1",
  atomic: true,
  idempotencyKey: "batch-key",
  body: [],
};

export const unkeyedBulk: BulkOperationsRequest = {
  ledgerName: "ledger1",
  atomic: true,
  body: [],
};

export const sequentialBulk: BulkOperationsRequest = {
  ledgerName: "ledger1",
  atomic: false,
  body: [
    { action: "REVERT_TRANSACTION", ik: "element-one", data: { id: 1 } },
    { action: "REVERT_TRANSACTION", ik: "element-two", data: { id: 2 } },
  ],
};
