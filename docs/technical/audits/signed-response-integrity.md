# Signed response integrity: evidence and ownership

The [manifest](signed-response-integrity.json) covers the authenticity of logs
returned to a client when response signing and verification are enabled. It does
not presume that every client enables verification. Bind each claim to an exact
commit and a registered response path.

## Evidence contract

Trace `server_bucket.go` response decoration, `ResponseSigner.SignLog`, the
`SignedLog` envelope, Discovery's response key, and the actual `ledgerctl`
verification call sites. Identify the committed log, the bytes in the envelope,
the visible log, and the trusted public key separately. `SignLog` clones a log,
clears `response_signature`, then marshals and signs the clone.
`VerifyResponseSignature` authenticates only the envelope payload bytes.
`VerifyResponseLog` then decodes those authenticated bytes and compares the
result with the visible log after clearing its envelope. Inspect each consumer
to confirm it invokes that binding check before using visible fields. Do not
substitute client reserialization for the decode-and-compare contract.

Use a non-empty committed log and independently mutate visible fields, payload,
signature, key and presence. A verifier accepting signed bytes says nothing by
itself about the separately presented log. For each hypothesis record the
configuration, caller, exact mutation, accepted or rejected result, and the
client action after verification. Check whether a path is reachable before
calling a missing signature a product defect. A disabled verification flag is
an explicit trust choice.

The command-level regression oracle includes transaction set-metadata and
delete-metadata acknowledgements and numscript save's returned name/version.
Exercise these consumers through the actual Apply RPC with a non-empty log;
assert rejection before success output or returned script identity is printed.
Keep valid signatures and verification-disabled responses as controls alongside
missing, corrupt, wrong-key, and modified visible logs. A verification failure
after Apply does not roll back a mutation.

## Ownership boundaries

`authentication-authorization-boundaries` owns client request signatures and admission.
`integrity-verifier-soundness` owns persisted audit-chain verification.
`api-boundary-contracts` owns generic response shape and wire conversion.
This domain owns server response-log signing and the optional client check that
purports to authenticate those logs. Hand off a shared symptom to the domain
whose correction is needed; do not duplicate a finding.

Key rotation, cross-node discovery consistency and unsigned operations are
questions until the current product contract establishes their guarantees.
Missing test coverage alone is not a defect. Every audit finding still requires
the independent challenge described in `ai-audit-challenge.md`.

## Atomic HTTP Apply

Atomic HTTP Apply reaches the existing service response signer and preserves returned responseSignature envelopes through its option-aware public log projection, except for addEventsSink. That log uses a detached redacted configuration and omits its signature because the signed protobuf payload contains reusable credentials. The committed log and service signature remain unchanged. Visible JSON is not a lossless protobuf codec: for other logs, authenticate exact SignedLog payload bytes, decode them, and compare their public projection rather than reconstructing a Log from ambiguous JSON types. Post-commit signing/encoding failures do not roll back business effects.

Can atomic HTTP Apply bypass the service signer, strip or alter an envelope outside the addEventsSink exception, expose credentials through a sink log or signed payload, or render other visible log fields different from the authenticated payload projection, including creation id/metadata and bigint number/string selection?

Use the registered atomic HTTP route with an enabled real service ResponseSigner and nonempty ledger/configuration logs. Verify the original envelope bytes against its trusted key and compare the authenticated public projection with non-sink logs in both bigint modes. For addEventsSink, assert redaction in the visible log and absence of the signature payload, while checking that the original service log is unchanged. Include missing/invalid signing controls and checked-encoding failure without assuming rollback.

See [the HTTP contract](../architecture/subsystems/api/http-api.md#atomic-http-apply).
API boundary owns conversion/output fidelity; authentication owns scopes and
caller capture; idempotency owns durable atomicity and retry identity; signed
response integrity owns the returned cryptographic proof. Deduplicate by the
mechanism that must change, not by the number of affected operations.
