# Ledger KV

Ledger KV stores opaque byte values under UTF-8 keys scoped to one live ledger. It is separate from typed ledger metadata. The only operations are Get, Put and Delete. Put replaces the value, including with an empty byte sequence. Delete records an audited no-op when the key is already absent. Get returns NotFound for an absent key or ledger.

## Contract

- Keys contain 1–256 UTF-8 bytes. Values contain at most 1 MiB; there is no value schema, listing, compare-and-swap, or expiration.
- gRPC `BucketService.GetKV` and HTTP `GET /v3/{ledgerName}/kv/{key}` read a stable primary-store view. The existing consistency router supplies the ReadIndex barrier by default and permits stale reads through the usual consistency metadata/header. gRPC also supports a query checkpoint through `read.checkpoint_id`.
- gRPC `BucketService.Apply` accepts `PutKVRequest` and `DeleteKVRequest` within the existing atomic, signed or unsigned, idempotent batch. HTTP PUT and DELETE use the same Apply path and accept `Idempotency-Key`. HTTP values are raw `application/octet-stream` bodies and responses.
- Read requires `ledger:ReadLedger`; mutations require `ledger:WriteLedger`.

## Apply, audit and storage

Admission validates key and value sizes and declares the ledger attribute needed by `loadLiveLedgerReader`. The FSM reads that ledger only through the coverage gate. KV mutation itself has no read dependency: it stages a copied value or deletion in `WriteSet`; Merge writes the last mutation for each key into the same Pebble batch as the log, audit entry and idempotency outcome. An order rejected before Merge leaves no KV projection. The ordered audit item binds the caller's key and value, and the emitted PutKV/DeleteKV log is the replay projection.

Primary keys are `[ZonePerLedger][SubPLKV][ledger name padded to 64 bytes][UTF-8 key]`. This prefix isolates ledgers and allows `DeleteLedgerData` to purge the entire namespace. The checker folds successful chain-verified orders and compares every stored KV row against that expectation. A full checkpoint includes the KV projection. Incremental restore replays PutKV/DeleteKV logs after the checkpoint and applies the same ledger-deletion cascade. The namespace is a primary-store projection of audit-bound business truth.

## Upgrade and cost

The new order variants are not understood by earlier RC binaries. Deploy this change with a coordinated full-cluster stop: stop admission, confirm every voter stopped, replace all binaries, then restart before accepting KV writes. Do not roll old and new voters together. Existing snapshots and Pebble rows remain readable; the new keyspace starts empty and needs no backfill. Once a KV order commits, rollback to an older binary is unsupported because it cannot replay the order. The release procedure must record this boundary and forward recovery path.

One Put carries up to 1 MiB in the Raft proposal, audited order, emitted log and Pebble value, so the value limit bounds per-order memory, WAL and backup amplification. Delete carries only the key. Get is one point lookup after the consistency barrier. The checker scans the KV keyspace once per full integrity check. There is no KV preload or cache footprint.
