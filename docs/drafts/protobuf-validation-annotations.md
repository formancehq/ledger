# Generated protobuf validation annotations

Status: design kickoff. This document is a proposal, not the current validation contract.

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
