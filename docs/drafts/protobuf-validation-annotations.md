# Generated protobuf validation annotations

Status: historical design proposal with implementation progress recorded below.
The current contract lives in the technical protobuf and admission documentation.

## Input-rule inventory update

The implemented direct-field generator now distinguishes explicit presence
(`required_for_input`) from an annotated scalar's non-zero value
(`non_zero_for_input`). Admission invokes the generated checks for posting
amounts, prepared-query update targets and names, sink names, and mirror IAM
regions, preserving the existing errors. The HTTP sink handler retains its
existing `INVALID_REQUEST` response. Raft apply and replay do not invoke the
generated methods.

| Existing check | Classification | Decision |
| --- | --- | --- |
| Prepared-query creation `query` and sink `config` | Message presence | Generated checks run at the existing converted-order gates to preserve validation precedence. |
| Prepared-query update/delete names | Non-zero string | Generated checks run before the existing name-shape validator at the same admission position. |
| Prepared-query name, sink name, mirror IAM region | Non-zero string | Generated check replaces the empty-value branch at the existing boundary. |
| Sink `type` | Oneof selection | Remains manual; a direct scalar rule cannot express oneof selection. |
| Ledger-scoped order names | Non-zero string plus value shape | Generated check replaces the empty-name branch at admission; the domain validator retains character, length and reserved-name checks. |
| Ledger names on existing gRPC read guards | Non-zero string | Generated checks replace eleven empty-name branches at their existing adapter positions and retain `ErrLedgerNameRequired`. |
| Account names, metadata keys, signing key IDs | Value shape and context | Existing validators also enforce characters, length or scope and retain their error order. |
| Posting amount, update target | Explicit presence | Generated check accepts a present zero value. |
| Sink batch size and query target enum | Valid zero/default | Must not use a non-zero rule. |
| Cluster drain epochs and leadership transferee | Non-zero scalar plus lifecycle rules | Generated checks replace the existing zero guards at the same RPC positions; epoch length and cluster preconditions remain manual. |
| Cluster state target, bootstrap fields and other IDs | Protocol and lifecycle invariants | Remain at their transport and lifecycle boundaries; zero is valid for the cluster-state target. |
| Persisted order and audit fields | Committed/replay contract | Do not run input-only methods during apply or replay. |

## Goal

Declare structural input requirements once in protobuf and generate Go validation
methods. EN-2945 is the first motivating case: every newly submitted `Posting`
must have a present `amount`, including when the amount is zero. The generator
should support other presence requirements without adding handwritten checks
for each field.

## Proposed first rule

Add a custom `google.protobuf.FieldOptions` extension to `misc/proto/common.proto`:

```proto
extend google.protobuf.FieldOptions {
  bool required_for_input = 71006;
}

message Posting {
  Uint256 amount = 3 [(common.required_for_input) = true];
}
```

The number is provisional until checked against every repository extension and
the protobuf global extension registry. The plugin must reject an annotation on
a field without explicit presence, such as a plain proto3 scalar, and explain
that the field must first use `optional` or a message type. A present empty
`Uint256` passes; a nil message fails. The annotation changes no service field
number, wire type, or persisted message bytes.

## Generator contract

Introduce `tools/protoc-gen-ledger-validate` as a pinned local Go module, built
and invoked by `just generate-proto`. For each annotated message, generate a
`ValidateInput() error` method in its existing Go package. The method checks
annotated fields in field-number order, returns a typed field-path error, and
does not mutate the message. Generation fails for unsupported field shapes,
duplicate or invalid rule declarations, and naming collisions. The generator
emits no method for messages without rules.

The first implementation validates direct fields only. Recursive validation
through nested messages, repeated fields, maps, and oneofs needs a separate
explicit rule because blindly traversing all uses of a shared message would
apply input rules to historical data. Callers must invoke generated methods at
the admission boundary. HTTP and gRPC entry points reach the same admission
gate, including atomic Apply and bulk. The adapter maps the typed field-path
error to the existing `VALIDATION` response without changing revision 40.

Generated validation is not a protobuf decoder constraint. Replay, restore,
snapshot reading, the checker, and Raft apply must continue to decode and apply
previously committed records without invoking `ValidateInput`. In particular,
historical nil posting amounts retain their current zero interpretation. A
follow-up replacement of EN-2945's handwritten check must prove that zero and
all error surfaces remain identical before the old check is removed.

## Implementation stages

Initial inventory candidates:

| Existing check | Candidate for generated presence rule? | Reason |
| --- | --- | --- |
| Explicit posting amount | Yes | Message presence is observable and independent of state. |
| Empty transaction content | No, not as a field rule | Requires a choice among postings, inline script, and reference. |
| Transaction reference length | Later | Value constraint, not presence; currently checked in both admission and FSM. |
| Metadata size limits | No | Committed policy and merged Numscript output are checked in the FSM. |

This is a starting inventory; stage 1 must inspect the remaining admission
validators before choosing the next annotation type.

1. Inventory structural admission checks and classify each by input shape,
   state dependence, error surface, and whether its message is persisted.
2. Implement the extension reader, direct-field generator, and negative
   generation fixtures using the existing protoc plugin conventions.
3. Wire generation into `just generate-proto`; document the generated API in
   `docs/technical/contributing/protobuf.md` and the admission boundary docs.
4. Migrate `Posting.amount` in a separate focused change, with real HTTP,
   atomic Apply, and gRPC regression tests, including explicit zero and an
   old committed nil-amount replay fixture.
5. Expand rules only after reviewing the inventory. State-dependent or audited
   FSM validation remains in the FSM; generator output does not replace it.

## Acceptance gates

- Annotated absent message fails, present empty message passes, and unannotated
  fields retain current behavior.
- Invalid annotations fail `just generate-proto` with the source field named.
- Regeneration is deterministic and leaves no unexplained generated diff.
- `bash scripts/agent-check`, targeted generator tests, HTTP/gRPC regression
  tests, and the exact-base `agent-check-pr` gates pass or report independent
  baseline failures.
- RC mixed-version review confirms that validation is confined to admission;
  historical committed entries decode and apply unchanged.
