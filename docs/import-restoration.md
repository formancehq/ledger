# Ledger v2 log restoration

## Supported structural contract

`POST /v2/{ledger}/logs/import` restores an export into a ledger in
`initializing` state. One request may contain the complete history or the next
batch. The existing ledger lock serializes each batch against other imports and
the first live write, and stays held until the batch transaction ends.

Each request reads the greatest stored log ID and replays its batch in one
transaction. Its first ID must be greater than that stored ID; subsequent IDs
must increase too. This preserves `fctl` imports in blocks of 100 logs and
`--resume-from-last-log`, without requiring contiguous IDs or a new session API.

Send batches one at a time, in export order. The lock excludes concurrent
execution but does not guarantee the order of parallel submissions. Before
starting live writes, verify that restoration reached the final exported log.

An invalid log, hash mismatch, malformed JSON suffix or canceled request rolls
back that complete request, including earlier records within the failed batch.
Previously committed batches remain intact. Resume after the actual last stored
log; after a lost response, inspect that log before deciding which records to
resend. Replaying an already committed record is rejected. The first live write
uses the existing state transition to close restoration admission.

Log IDs must be strictly increasing. Log and transaction IDs must be present and
less than `9223372036854775807`, leaving room for the first live write's
`max(id) + 1` sequence reset. ID zero and gaps are supported. PostgreSQL sequence
allocations can leave gaps after rollback, so contiguity is not required.
Transaction IDs need not increase in log order: concurrent live writes can
allocate transaction IDs before acquiring the log insertion lock. Storage
uniqueness constraints reject duplicate IDs.

No additional ID ceiling or reserved range is imposed. IDs close to the native
`bigint` maximum remain supported if their successor is representable. Such a
restore retains the normal PostgreSQL sequence limit: subsequent writes can
consume the remaining IDs and then exhaust that sequence. This patch prevents
an unrepresentable sequence reset; it does not introduce a new allocation policy.

Restoration applies the existing posting validation: amounts must be present
and nonnegative, addresses and assets must be valid, and transactions must have
postings. Zero amounts remain valid. A new transaction cannot already carry a
revert date. Reverts require an existing, not-yet-reverted transaction and a
nonzero revert date. Their postings must be the exact ordered reverse of the
stored transaction. The claimed original postings, timestamp and reference must
match stored history.
Rejected reverts roll back the original transaction's revert marker too.

Hash validation remains conditional on the ledger's `HASH_LOGS` feature. Errors
identify the failing log without returning the expected or submitted digest.
Hashes detect corruption; they do not authenticate a history or its author.

## Trusted restoration and authorization contract

Restoration remains authorized by `ledger:write`; no administrative scope or
additional provenance verifier is introduced. The v2 service keeps its existing
authentication middleware and gateway scope enforcement.

Import is a restoration operation for data from a source trusted by its caller.
The caller is responsible for selecting that source and the history to restore.
Historical overdrafts and forced reverts remain supported. The export format
contains neither the original Numscript program nor proof that an overdraft was
authorized. Import does not replay those original authorization decisions and
must not be treated as admission of an untrusted transaction stream through the
normal transaction execution rules.

A client can calculate consistent hashes for fabricated history. Hash validation
therefore detects corruption but does not prove backup provenance or entitlement
to a historical overdraft. This is a documented trust boundary of restoration,
not a property claimed by this patch. The correction enforces atomic admission,
structural validity and the verifiable relationships between imported records.
It deliberately adds no balance restriction, signed-export format or arbitrary
ID ceiling.

## Compatibility and related work

Existing multi-request restoration clients, including `fctl`, keep their batch
and resume behavior. Atomicity is per request, not across an entire export split
into several requests. Request size determines transaction duration and resource
use. The ledger remains `initializing` between batches; its first live write
uses the existing sequence-reset and state-transition mechanism. Do not start
live writes before restoration has completed.

Restoration includes SEC-90 canonicalization of derived audit volumes before log
insertion. For created and revert transactions, storage recomputes
`postCommitVolumes`; the reverted original comes from the stored transaction.
`postCommitEffectiveVolumes` are recomputed and retained only when both
`MOVES_HISTORY=ON` and `MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES=SYNC`;
otherwise supplied effective volumes are cleared. Supplied account/asset entries
not touched by the postings are dropped. Hashes and mementos remain unchanged.
See [Canonical derived volumes on log import](database/import-log-canonical-volumes.md)
for details. No schema migration is introduced.
