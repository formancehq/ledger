# Historical secret projection

Public `GetLog`, `ListLogs`, `GetAuditEntry`, and `ListAuditEntries` results pass
through a detached sensitive-field projection at the controller read boundary.
This covers local and forwarded gRPC reads, live and checkpoint reads, and the
HTTP routes that consume the same controller. The projection descends into
serialized audit orders and signed request batches, where protobuf field
annotations cannot reach nested bytes. Unknown protobuf fields are removed
because their confidentiality is not known. URLs and DSNs use registered
format-specific redactors.

System logs may carry a signed response envelope with another serialized log.
Its payload is projected as well. If it changes, the response signature bytes
are removed for the same reason as the signed audit batch.

The persisted audit chain, system logs, accepted orders, hashes, checker input,
and restore data retain their original bytes. A projected credential-bearing
audit order is no longer a hash preimage. When a signed batch changes, its
signature bytes are omitted because they cannot verify the projected payload.
Entries without sensitive fields or unknown protobuf fields keep their original
serialized order and signed payload bytes. Unknown fields are removed even
when no annotated credential is present; this can also rewrite evidence bytes
and cause signature omission. Clients needing cryptographic verification must
use an authorized evidence path.

This is a response-only RC behavior change. Protobuf field numbers, wire types,
persisted formats, Raft entries, snapshot and backup formats, and service
protocol revision remain unchanged. Rolling upgrades require no data migration;
older servers may still return unredacted historical responses until replaced.
