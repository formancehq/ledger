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
