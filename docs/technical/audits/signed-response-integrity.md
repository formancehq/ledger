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
clears `response_signature`, then marshals and signs the clone. The current
`VerifyResponseSignature` verifies the envelope payload with Ed25519; inspect
the consumer before claiming that this also authenticates the visible log.
The architecture page describes reserialization by clients; reconcile that
description with current client code before treating it as implemented behavior.

Use a non-empty committed log and independently mutate visible fields, payload,
signature, key and presence. A verifier accepting signed bytes says nothing by
itself about the separately presented log. For each hypothesis record the
configuration, caller, exact mutation, accepted or rejected result, and the
client action after verification. Check whether a path is reachable before
calling a missing signature a product defect. A disabled verification flag is
an explicit trust choice.

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
