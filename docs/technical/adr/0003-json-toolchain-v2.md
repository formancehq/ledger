# 0003 — JSON toolchain: stdlib `encoding/json/v2` for marshal, keep Sonic only where decode wins

**Status:** Proposed (2026-09-08). Direction accepted as the plan for EN-1795;
implementation is gated on the compatibility matrix and representative
benchmarks below and remains unmerged. Revisit if Go 1.27/1.28 changes v2
defaults, if a supported Sonic release lands that keeps the decode win, or if
EN-1792 / EN-1752 change the representation/DTO layer first.

## Context

Docker and Nix already select Go 1.27, but `github.com/bytedance/sonic`
v1.15.0 (our pin) carries the build constraint `!go1.27` on both amd64 and
arm64. On Go 1.27 Sonic therefore compiles its thin `encoding/json` wrapper
*fallback* rather than failing the build — a silent performance cliff that
lands exactly when the standard library's `encoding/json/v2` becomes the
baseline.

The JSON entry points at this revision are:

- **HTTP:** `internal/adapter/json` documents itself as "the same API surface as
  `encoding/json/v2`" but delegates to Sonic — `Marshal` via `ConfigDefault`
  (`EscapeHTML=false`, `SortMapKeys=false`), `MarshalWrite` via `ConfigStd`
  (`EscapeHTML=true`, `SortMapKeys=true`, newline-terminated stream). It omits
  the `opts ...Options` parameter that is v2's central design point.
- **Checked routes:** `writeOKChecked` (`internal/adapter/http/response.go`)
  serves transaction-list, single-log and audit-entry responses. It buffers
  `json.Marshal` *before* committing success headers so a nested marshal
  failure surfaces as a clean 500 (invariant #7), unlike the streaming
  `writeJSONResponse`/`writeOK` path.
- **CLI:** `cmd/ledgerctl/cmdutil/output.go` dispatches on `json.Marshaler`
  before falling through to `protojson` — a dispatch that must stay in sync
  with any marshaller surface change.
- **Contract enforcement:** `internal/adapter/http/encoder_contract_test.go`
  pins the split in both directions (protojson routes must have no custom
  `MarshalJSON`; Sonic routes must have one).

The structural blocker is the v1 marshaller boundary: `MarshalJSON() ([]byte,
error)` has no options parameter, so a v2 `MarshalJSONTo(enc *jsontext.Encoder)`
override is silently lost whenever any nested level still exposes the opaque v1
method. The money/log path is hand-written at every level
(`Transaction` → `Posting` → `Uint256`, plus `Log`, `Timestamp`, `Metadata`, …),
so it can only be converted *end to end*, never incrementally.

## Decision

1. **Prefer the standard library for marshal — per entry point, not one
   option set.** Adopt `encoding/json/v2` for marshal while preserving the two
   distinct Sonic configurations in use today, so each entry point stays
   byte-stable against its current output:

   - **Streaming** (`json.MarshalWrite`, Sonic `ConfigStd`) already emits
     sorted map keys, HTML-escaped strings and a trailing `\n`.
     `encoding/json.DefaultOptionsV1()` reproduces those sorted/escaped
     semantics (`encoding/json/v2.Deterministic`,
     `encoding/json/jsontext.EscapeForHTML`/`EscapeForJS`, plus legacy
     `omitempty` and nil-as-null framing). It has no trailing-newline option
     and `encoding/json/v2.MarshalWrite` writes only the JSON value, so
     streaming callers must append the `\n` delimiter separately after a
     successful write to stay byte-stable.

   - **Checked buffering** (`json.Marshal`, `writeOKChecked`, Sonic
     `ConfigDefault`) does not sort map keys and does not HTML-escape strings,
     so `DefaultOptionsV1()` must not be applied there: its `Deterministic`
     and `EscapeForHTML`/`EscapeForJS` options would reorder `MetadataMap`
     keys and escape any metadata/string value containing `<`, `>` or `&`,
     changing bytes instead of preserving them. Buffered callers must start
     from the v2 defaults (`encoding/json/v2.DefaultOptionsV2()` — no
     `Deterministic`, no escaping) and opt in only the framing options needed
     to match `ConfigDefault`, keeping buffered output ConfigDefault-stable.
2. **Do not bump or drop Sonic unconditionally.** Retain Sonic for decode only
   if, at implementation time, a supported pin plus representative Go 1.27
   benchmarks on amd64/arm64 still justify its decode advantage. There is no
   unconditional dependency bump and no unsupported build-tag override.
3. **Preserve the checked-response contract.** Transaction-list, single-log and
   audit routes keep buffering (or an equivalent preflight) so nested marshal
   failures return a clean 500 before any success headers. `MarshalWrite`
   streaming is eligible only where the existing route already streams.
4. **No silent toolchain drop.** The module's `go 1.26.0` minimum (and its
   Nix/Docker/CI alignment) is raised only when a direct v2 import actually
   requires it — not speculatively.
5. **No new generator or alternate DTO here.** EN-1792 (generated
   representation) and EN-1752 (HTTP DTO) are separate options and must be
   reconciled; this decision must not silently absorb either.

## Scoped implementation sequence

1. Inventory HTTP/CLI/event/mirror JSON entry points and custom/generated
   `MarshalJSON` contracts at this revision; re-verify which JSON is
   hashed/persisted (audit/idempotency bind protobuf binary, not JSON).
2. Record Go 1.27+ encoder/decoder baselines on representative payloads —
   marshal, streaming write and unmarshal separately, amd64 and arm64, with
   exact toolchain and ns/B/allocs.
3. Prove compatibility of nested marshaller paths before switching; convert
   each affected path (`MarshalJSON` → `MarshalJSONTo`) end to end so call-site
   options are never absorbed at an opaque v1 boundary.
4. Update CLI dispatch (`cmdutil/output.go`) and `encoder_contract_test.go` for
   `MarshalJSONTo`/v2 so messages do not fall through to `protojson`.
5. Fail-fast tests: inject late nested marshal failures and writer failures;
   checked routes must 500 cleanly before headers.
6. Publish the per-entry-point golden/round-trip matrix (field names, uint256
   encoding, oneofs/discriminators, timestamps, nil/empty collections,
   zero-value omission, colors, escaping, trailing newline) and explicit decode
   strictness/duplicate/unknown/trailing-data behavior.
7. **Keep the existing path and report the unresolved choice** if the
   compatibility or performance gates cannot be met. This ADR validates the
   plan, not the migration.

## Consequences

- `EN-1779` (opt-in string `Uint256` amounts) collapses to a per-call v2
  `WithMarshalers` option once the conversion lands — the motivation for doing
  this work rather than a smaller fix.
- The historical measurements below are local diagnostics from a different
  toolchain/revision. They raise the question but do **not** settle it; their
  causal explanations are not reproduced here and must not be repeated as
  established fact in the migration decision.

### Historical evidence (non-normative)

| Benchmark (100-tx list-shaped payload, darwin/arm64, Go 1.26.5) | ns/op | B/op | allocs/op |
|---|--:|--:|--:|
| Marshal `encoding/json` v1 | 49,152 | 44,732 | 701 |
| Marshal Sonic | 69,609 | 40,646 | 302 |
| MarshalWrite Sonic | 77,568 | 61,066 | 304 |
| Unmarshal v1 | 183,515 | 77,016 | 2,318 |
| Unmarshal Sonic | 52,117 | 94,736 | 906 |

The only durable signal from these numbers is that Sonic is on the wrong path
for marshal here, while its decode advantage is real enough to require a
Go 1.27 baseline before dropping it. Nothing above is a cluster TPS forecast.
