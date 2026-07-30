# Secret Redaction on Historical Reads

The audit chain and system log store accepted business intent. Some accepted
orders necessarily contain credentials used to configure event sinks or mirror
sources. Those persisted records remain authoritative and unchanged: mutating
them would invalidate the audit hash chain, idempotency evidence, or signed
client payloads.

The historical audit and system-log APIs therefore return a deep-cloned,
secret-safe projection from the controller boundary. The projected controller
methods are `GetAuditEntry`, `ListAuditEntriesFrom`, `GetLog`, and `ListLogs`.
This covers HTTP and gRPC reads served locally, checkpoint audit reads, and
cold-storage log reads. For a follower-routed request, the serving node applies
the projection before its gRPC response leaves that node.

## Covered fields

The projection replaces non-empty credentials with `***REDACTED***` in:

- NATS sink URLs (the URL may contain user/password or token userinfo);
- ClickHouse sink DSNs;
- Kafka SASL passwords;
- HTTP sink HMAC secrets;
- Databricks PATs and OAuth client secrets;
- PostgreSQL mirror DSNs;
- HTTP mirror OAuth client secrets.

The same transformation is applied inside:

- `AddedEventsSinkLog.config` and `CreatedLedgerLog.mirror_source`;
- `AuditItem.serialized_order` for `AddEventsSink` and `CreateLedger` orders;
- the `SignedApplyBatch.payload` retained on an audit entry.

Projection always operates on deep clones. Stored records and objects owned by
the controller are never mutated.

## Integrity semantics

`AuditEntry.hash` continues to identify and protect the authoritative persisted
entry. When credentials are replaced, the response is deliberately not the
original hash preimage. Likewise, when a signed `ApplyBatch` contains replaced
credentials, the read projection keeps the signer key ID and a parseable
redacted payload but omits the Ed25519 signature bytes so clients cannot mistake
the projection for independently verifiable signed evidence. Entries without
credentials retain their exact serialized order bytes, signed payload, and
signature, so their evidence remains independently verifiable.

Integrity checking and rebuild paths read the authoritative store directly and
are not projected. Operators who need offline verification of raw audit
evidence must use a separately controlled administrative export rather than a
read-scoped API.

## Failure behavior

If a stored order or signed batch cannot be decoded, the read fails loudly. It
must never fall back to returning opaque bytes that might contain a secret.
This fail-closed behavior also surfaces corrupted or schema-incompatible audit
data instead of silently weakening redaction.

## Related live read boundaries

This mechanism is intentionally limited to historical audit and system-log
records. Live event-sink configuration reads are protected separately by
[PR #1651](https://github.com/formancehq/ledger/pull/1651), and live
`LedgerInfo.mirror_source` reads by
[PR #1653](https://github.com/formancehq/ledger/pull/1653). Those adapter-level
projections cover both HTTP and gRPC without coupling their live response
models to the historical audit projection in this package.
