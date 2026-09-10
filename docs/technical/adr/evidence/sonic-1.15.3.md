# Sonic 1.15.3 compatibility and performance evidence

Measured on 2026-09-08 after rebasing PR #1918 on `release/v3.0`
`47ae5b0482d5c58ad70b1bebb72da23818d5d1a7`; local rebased HEAD was
`9f3d430d8289010f35af765d184a1c208d0b6100` before the documentation update.
No production serialization code was changed for these measurements.

## Reproduction and scope

The opt-in [fixture](json_toolchain_test.go) is run explicitly by filename from
the repository root. Its build tag keeps it out of ordinary package tests.

```sh
nix develop --command go test docs/technical/adr/evidence/json_toolchain_test.go -run TestCompatibility -v
nix develop --command go test docs/technical/adr/evidence/json_toolchain_test.go -run '^$' -bench BenchmarkTransactionList -benchmem -count=3 -benchtime=1s
```

The Nix shell returned `go version go1.27.1 darwin/arm64`. Its subsequent
benchmark invocation stalled on a busy Nix evaluation-cache SQLite database;
the actual measurements used that exact pinned binary directly:

```sh
/nix/store/k2v0ib6vkjlqns04bgrg9v9ghk1440vh-go-1.27.1/bin/go test docs/technical/adr/evidence/json_toolchain_test.go -run TestCompatibility -v
/nix/store/k2v0ib6vkjlqns04bgrg9v9ghk1440vh-go-1.27.1/bin/go test docs/technical/adr/evidence/json_toolchain_test.go -run '^$' -bench BenchmarkTransactionList -benchmem -count=3 -benchtime=1s
```

- CPU: Apple M5 Pro, darwin/arm64; default benchmark parallelism suffix: `-15`.
- `GOEXPERIMENT` was empty; no unsupported build tags or toolchain overrides.
- `flake.lock` SHA-256:
  `468523dde3f4fc9090e3a915201e664612ab79ee1a4ddf45701894ed881377fd`.
- Sonic: the repository pin `v1.15.3`, **`sonic.APIKind == 1`**
  (`UseSonicJSON`), asserted by the fixture. Its native build constraint accepts
  Go 1.27 on amd64/arm64 and excludes Go 1.28.
- The machine was shared with other development tasks. Timing variance is
  substantial; this is reproducible diagnostic evidence, not an isolated
  performance study or a release performance gate.

Marshal/write use 100 real `commonpb.Transaction` messages (25,383-byte native
Sonic JSON), each with a posting, empty color, a uint256 amount above 2^53,
timestamp, reference and two metadata keys. Existing nested `MarshalJSON`
methods remain active and call the production adapter/Sonic even under the
stdlib outer encoders. This measures an **outer-library substitution**, not an
end-to-end v2 marshaller implementation. Writers target `io.Discard`; stdlib
v2 writes append the current stream newline.

Decode uses the same JSON bytes and the same typed wire DTO for all decoders,
including `json.Number` for exact amounts. It intentionally has no custom
decode methods. Directly decoding the response into `commonpb.Transaction`
was rejected by all three decoders because the flattened metadata string is
not the protobuf `MetadataValue` representation; those failed runs are excluded
from the performance results. The DTO is benchmark scaffolding, not a proposal
to add a production DTO layer.

## Compatibility observations

The test asserts native Sonic selection, plain streaming byte equality, raw
escaping/framing plus semantic equality for buffered candidates, and nested
option dispatch. Unsorted object member order is sampled and logged rather
than asserted to vary, because Go iteration order is not a portable guarantee.

| Probe | Sonic 1.15.3 observation | v2 candidate / implication |
|---|---|---|
| Plain buffered `ConfigDefault.Marshal` | Three key orders in 64 samples; literal `<>&` and U+2028/U+2029; nil map/slice are `null`; zero int/false tagged `omitempty` are omitted; no newline | `DefaultOptionsV1()` plus `Deterministic(false)`, `EscapeForHTML(false)`, `EscapeForJS(false)` matches these observed semantics and escape bytes; independent unsorted iterations cannot promise identical member order |
| Plain `ConfigStd.NewEncoder.Encode` | Sorted keys, escaped HTML and U+2028/U+2029, nil-as-null, legacy omission, trailing newline | Exact byte equality to `v2.Marshal(..., DefaultOptionsV1())` plus `\n` for this fixture |
| Bare v2 | Nil slice/map become `[]`/`{}` and zero int/false with `omitempty` are emitted | Bare defaults are not a compatible replacement |
| Nested `commonpb.Transaction`, outer Sonic `ConfigStd` | Metadata has three different key orders in 64 samples; HTML/JS still escaped | Outer sort option does not sort opaque JSON emitted by the inner `ConfigDefault.Marshal` |
| Same nested transaction, outer v2 with V1 options | Same semantic fields and HTML/JS escaping, with an observed unsorted metadata order | V1 options do not repair opaque nested map ordering either; this is not proof of byte equality between two unsorted calls |
| `WithMarshalers` for `*commonpb.Uint256` | Direct v2 call emits `"9007199254740993"`; nested transaction retains numeric `9007199254740993`, override callback called zero times | Per-call amount policy cannot traverse existing v1 `MarshalJSON` boundaries |

The nested fixture also includes `color:""`, timestamp
`2026-09-08T00:00:00Z`, and the public transaction field names. This does **not**
cover every endpoint, oneof, log/audit shape, timestamp boundary, malformed
input, duplicate/unknown key policy, trailing input, writer failure, or checked
HTTP response failure. Those remain migration acceptance gates. No amd64
runtime measurement or end-to-end throughput claim is made.

## Raw benchmark observations

Each row below is one run, not an average. `stdlib-v2-v1-options` uses
`encoding/json.DefaultOptionsV1()` for all three operations; the native-default
variant changes only outer marshal options as described above. Sonic marshal
and unmarshal use `ConfigDefault`; Sonic write uses `ConfigStd`.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| stdlib-v1/Marshal | 2028451 | 161393 | 1804 |
| stdlib-v1/Marshal | 1083837 | 161392 | 1804 |
| stdlib-v1/Marshal | 1739996 | 161284 | 1804 |
| stdlib-v1/Write | 1376385 | 136257 | 1803 |
| stdlib-v1/Write | 2325107 | 131135 | 1803 |
| stdlib-v1/Write | 3261914 | 129886 | 1803 |
| stdlib-v1/Unmarshal | 1227443 | 72251 | 709 |
| stdlib-v1/Unmarshal | 915611 | 72245 | 709 |
| stdlib-v1/Unmarshal | 685689 | 72267 | 709 |
| sonic-native/Marshal | 958339 | 168472 | 1805 |
| sonic-native/Marshal | 893241 | 171765 | 1805 |
| sonic-native/Marshal | 915815 | 172155 | 1805 |
| sonic-native/Write | 1506114 | 178410 | 1807 |
| sonic-native/Write | 818916 | 187162 | 1808 |
| sonic-native/Write | 948950 | 186782 | 1808 |
| sonic-native/Unmarshal | 216185 | 85262 | 406 |
| sonic-native/Unmarshal | 189622 | 84132 | 406 |
| sonic-native/Unmarshal | 207706 | 84616 | 406 |
| stdlib-v2-v1-options/Marshal | 750922 | 166012 | 1805 |
| stdlib-v2-v1-options/Marshal | 782024 | 164364 | 1805 |
| stdlib-v2-v1-options/Marshal | 873148 | 167215 | 1805 |
| stdlib-v2-v1-options/Write | 854090 | 135010 | 1803 |
| stdlib-v2-v1-options/Write | 731598 | 133775 | 1803 |
| stdlib-v2-v1-options/Write | 831680 | 132025 | 1803 |
| stdlib-v2-v1-options/Unmarshal | 1071364 | 72251 | 709 |
| stdlib-v2-v1-options/Unmarshal | 408021 | 72274 | 709 |
| stdlib-v2-v1-options/Unmarshal | 589216 | 72262 | 709 |
| stdlib-v2-native-default-options/Marshal | 1443630 | 162116 | 1804 |
| stdlib-v2-native-default-options/Marshal | 700395 | 164061 | 1805 |
| stdlib-v2-native-default-options/Marshal | 592749 | 164636 | 1805 |

All 30 benchmark samples completed successfully. The observed native Sonic
wire-DTO decode timings are lower than the v1/v2 samples in this run. Encoding
results fluctuate enough that they do not establish a stable ranking. Every
outer encoder still pays the existing nested custom-marshaller costs, so these
results cannot estimate the gain from an end-to-end v2 conversion. A decision
to migrate requires isolated repeated measurements on representative production
request/response shapes, including amd64, and the complete compatibility gates.
