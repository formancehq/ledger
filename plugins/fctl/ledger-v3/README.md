# `ledger-v3` plugin — preparation

One of **two separate** Ledger plugins. It shares no catalogue, manifest or
command surface with `../ledger-v2`. See `../README.md` for the boundary rule.

| | |
| --- | --- |
| Product major | **3**, and no other |
| Transport | gRPC `ledger.BucketService` |
| Contract | `misc/proto/bucket.proto` at `bb0297cce39ccd1eb45eba695e9aa10374d5865d` (`release/v3.0`) |
| Generated stubs | `github.com/formancehq/ledger/v3/internal/proto/servicepb` — **internal** |
| Inventory basis | `cmd/ledgerctl` at the same pin |
| Coverage denominator | **54** product commands of 108 executable |
| Signing capability | `sign.ledger.apply-batch`, optional, major 3 only |

## Documents

| File | Contents |
| --- | --- |
| `inventory.json` | All 108 executable commands with classification, aliases, source file and RPCs |
| `mapping.md` | Enumeration method, the 54 included commands by family, the 39-RPC surface, and the verbatim-verified RFC 0009 bindings |
| `exclusions.md` | 13 host/local, 34 operator, 7 signing/event-sink, 5 open decisions, and the `openapi/v3.yaml` anti-confusion boundary |
| `auth-scopes-risks.md` | Two-level scope model, 4 divergences, 7 risks, 5 blockers |
| `manifest.json` | Logical manifest and machine-checked invariants |

## Two blockers worth reading first

**Module path is constrained.** The generated stubs are in an `internal`
package, so only a module path under `github.com/formancehq/ledger/v3/` can
import them. Verified empirically at the pin: module
`github.com/formancehq/ledger/v3/plugins/fctl` compiles typed references to
`servicepb.ApplyBatch`, `signaturepb.SignedApplyBatch` and
`servicepb.BucketServiceClient`, while the identical code under module
`github.com/formancehq/fctl-ledger-v3-plugin` fails with `use of internal
package … not allowed`. Either the plugin adopts that exact module path, or
Ledger promotes the v3 stubs to a public package.

**This is not the branch.** The v3 source series is `release/v3.0`, module
`github.com/formancehq/ledger/v3`. This preparation is committed on a branch off
`origin/main`, module `github.com/formancehq/ledger`, which contains no
`cmd/ledgerctl`, `internal/proto` or `misc/proto`. v3 *implementation* cannot
land here.

## Status

Source-first preparation. No component, install record or dual-host artifact is
declared: runtime gates 4B/4C/4D are not released.

The RFC 0009 bindings are verified verbatim at the pin — `ledger.ApplyBatch`,
`/ledger.BucketService/Apply`, the unsigned/signed `oneof`, and
`idempotency_key` inside the signed bytes. One correction is recorded: the
envelope is `signature.SignedApplyBatch`, not `ledger.SignedApplyBatch`.
