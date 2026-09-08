# 0003 — JSON toolchain: evaluate v2 marshal for request-scoped options

**Status:** Proposed (2026-09-08). EN-1795 records a supported, measured JSON
serialization path that preserves Ledger's public JSON contract. The preferred
`encoding/json/v2` direction remains gated on compatibility and performance;
this ADR does not implement or approve the migration. Refreshed against
`release/v3.0` at `47ae5b0482d5c58ad70b1bebb72da23818d5d1a7` and Sonic v1.15.3.

## Context and requirement

EN-1779 (opt-in string `Uint256` amounts) exposes the remaining need: clients
such as JavaScript consumers must be able to request lossless decimal strings
for amounts above 2^53 while the default wire remains numeric. A request-scoped
encoding option must reach every nested amount without process-global state.
EN-1795 explores a shared serialization mechanism for that capability; shipping
EN-1779 itself is separate and must not depend on this migration.

The previous revision of this ADR was based on Sonic v1.15.0, which selected
its `encoding/json` fallback on Go 1.27. The target now pins v1.15.3, whose
native codec supports Go 1.27 on amd64/arm64. The fallback performance cliff
is therefore no longer a current migration rationale. Docker and Nix select
Go 1.27; the module still declares `go 1.26.0`.

The JSON entry points at the refreshed revision are:

- **HTTP:** `internal/adapter/json` delegates `Marshal` to Sonic
  `ConfigDefault` and `MarshalWrite` to `ConfigStd.NewEncoder(w).Encode(v)`.
  Its claim to provide the v2 API surface omits the central `opts ...Options`
  parameter. Native `ConfigDefault` does not sort maps or escape HTML/JS;
  native `ConfigStd` enables sorting and escaping and appends a newline.
  These flags do not guarantee normalization inside opaque custom marshalers.
- **Checked routes:** `writeOKChecked` in `internal/adapter/http/response.go`
  buffers transaction-list, single-log and audit-entry responses before
  committing success headers. A nested marshal failure must remain a clean
  500 (invariant #7), even if direct streaming would allocate less.
- **CLI:** `cmd/ledgerctl/cmdutil/output.go` dispatches on `json.Marshaler`
  before `protojson`; migration must preserve that choice and CLI formatting.
- **Contract tests:** `internal/adapter/http/encoder_contract_test.go` enforces
  that protojson routes have no custom `MarshalJSON` and that the transaction
  and log routes retain their custom representation.

`MarshalJSON() ([]byte, error)` has no options parameter. An outer v2 encoder
cannot propagate `WithMarshalers` into a nested value already serialized by an
opaque v1 method. `Transaction.MarshalJSON` builds an auxiliary shape, then
calls the Sonic adapter; `Posting.MarshalJSON` does the same before reaching
`Uint256.MarshalJSON`. A call-site amount override requires a continuous
option-aware path, not merely replacing the outer adapter. Unrelated paths
can migrate separately, but each affected path must work end to end.

## Decision

1. **Prefer v2 for option propagation, subject to measurement.** Evaluate an
   option-aware marshal path using the standard library. Do not infer a
   performance win from the removed v1.15.0 fallback or historical Go 1.26
   measurements. Keep native Sonic v1.15.3 as the current baseline and as a
   credible alternative until the gates below pass.
2. **Select compatibility options per entry point.** The native baseline
   supersedes the previous buffered prescription (`Deterministic` and
   `EscapeForJS`). For ordinary values, start with:

   - **Buffered `ConfigDefault`:** `encoding/json.DefaultOptionsV1()` with
     `encoding/json/v2.Deterministic(false)`,
     `encoding/json/jsontext.EscapeForHTML(false)` and
     `encoding/json/jsontext.EscapeForJS(false)`. Retain v1 omission and
     nil-as-null behavior; bare `DefaultOptionsV2()` is not equivalent.
   - **Streaming `ConfigStd`:** `encoding/json.DefaultOptionsV1()` for the
     ordinary-value sorting, escaping and legacy framing. Append `\n`
     separately only after a successful `MarshalWrite`: v2 has no
     trailing-newline compatibility option.

   These are candidate option sets, not proof of universal byte equivalence.
   Neither unsorted encoder promises the same map iteration order on two
   calls. Compare map-containing output modulo member order where the current
   contract has no order guarantee, and assert escaping, framing and numeric
   spelling separately. Preserve bytes where they are stable/promised; do not
   add a sorting promise or call nondeterministic output byte-stable.
   Existing nested custom marshalers can retain their own order and escaping;
   verify the complete response, not just a plain map probe, before converting
   a path. Error handling and invalid input behavior also require their own
   compatibility tests.
3. **Retain Sonic decode pending a measured alternative.** v1.15.3 already
   supplies a supported native path. Compare byte and reader decode separately
   on representative payloads before replacing it; no dependency bump, removal
   or build-tag override is part of this ADR.
4. **Preserve checked-response buffering.** A v2 migration must retain the
   pre-header error boundary. Direct streaming is eligible only where a route
   already streams. A post-header writer failure cannot be rolled back.
5. **Align toolchains explicitly.** Raise the module's Go 1.26 minimum only
   when direct v2 imports require it, with Nix/Docker/CI support aligned.
6. **Keep one external representation.** EN-1792 (generated representation)
   and EN-1752 (HTTP DTOs) remain separate architectural options. Resolve their
   overlap before introducing a generator or a competing representation layer.

## Alternatives and consequences

- **Keep Sonic and the existing representation:** supported today and avoids
  a broad migration. It does not itself carry per-call v2 options through the
  custom methods; EN-1779 can deliver its own mode-carrying representation.
  This is the fallback if the proposed migration cannot meet its gates.
- **Convert affected paths to v2:** provides a shared request-scoped option
  mechanism but requires contiguous custom-marshaler conversion, CLI dispatch
  updates and full compatibility validation. An outer encoder swap is not an
  implementation of that mechanism.
- **Generated representation or HTTP DTOs:** credible alternatives under
  EN-1792 / EN-1752, not dependencies silently added by this decision.

Once an affected path propagates options, `WithMarshalers` can implement an
amount override without global state. This does not deliver HTTP negotiation,
input compatibility, schemas or SDK support for EN-1779. The preference for
v2 is a capability decision; current measurements do not establish a general
marshal speedup or a cluster TPS improvement.

## Scoped implementation sequence

1. Inventory HTTP/CLI/event/mirror JSON entry points and custom/generated
   marshaller contracts at the implementation revision. Re-verify which bytes
   are hashed/persisted before claiming that audited protobuf bytes,
   signatures/idempotency and persisted semantics are unaffected.
2. Repeat the linked compatibility probes and benchmarks against the exact
   implementation baseline on supported amd64 and arm64 environments. Record
   runtime backend, dependency, toolchain, payload, ns/op, B/op and allocs/op;
   measure marshal, streaming write, byte decode and reader decode separately.
3. Convert each affected money/log path to an option-aware v2 path end to end.
   Prove a greater-than-2^53 amount override at nested levels and map values,
   followed by a default call that remains numeric (no option leakage).
4. Update CLI dispatch and `encoder_contract_test.go` with any
   `MarshalJSONTo` conversion so messages cannot fall through to `protojson`.
5. Publish the full per-entry-point golden/round-trip matrix: field names,
   map ordering promises, uint256 encoding, oneofs/discriminators, timestamps,
   nil/empty collections, zero-value omission, colors, escaping and newline.
   Specify decode strictness, case matching, duplicate/unknown fields and
   trailing-data behavior explicitly.
6. Inject late nested marshal errors and writer errors. Checked routes must
   return a clean 500 before success headers on serialization failure.
7. Keep the current path and report the unresolved choice if compatibility or
   performance gates cannot be met. This document validates a plan, not a
   completed JSON migration.

## Refreshed evidence

The reproducible probe, exact environment and repeated measurements are in
[the Sonic 1.15.3 evidence](evidence/sonic-1.15.3.md). They exercise the current
custom Ledger types rather than claiming to benchmark an end-to-end v2
conversion that has not been implemented. The full migration matrix and
cross-platform performance gate remain implementation work.
