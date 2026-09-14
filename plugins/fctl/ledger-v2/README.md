# `ledger-v2` plugin

One of **two separate** Ledger plugins. It shares no catalogue, manifest or
command surface with `../ledger-v3`. See `../README.md` for the boundary rule.

| | |
| --- | --- |
| Product major | **2**, and no other |
| Transport | generated Ledger v2 client over host-owned `producthttp` |
| API contract | `github.com/formancehq/ledger/pkg/client` and `openapi/v2.yaml` at `8cc679c9440aaf90d4b8646a013e5bd7421443d6` |
| Host SDK contract | `github.com/formancehq/fctl-v2-poc/pkg/plugin` at `545521bfa222250af6b4419b194c7967cded0379` |
| Inventory basis | old-fctl `cmd/ledger` at `693c58e27865f83332e6c3199d61fed81b742f41` |
| Coverage denominator | **22** included of 23 baseline commands |
| Signing capability | **none** — `sign.ledger.apply-batch` is major-3 only |

## Surface rule

`ledger-v2` keeps the **historical user surface**. Where a baseline command was
implemented on `Ledger.V1`, it is converted to its V2 API equivalent rather
than dropped: 14 commands already spoke V2, 8 are converted, and only
`ledger server-infos` leaves the plugin because the `/_/info` probe is
host-owned.

Conversion keeps the command phrase, aliases and user intent while changing the
wire call. The one grammar adaptation is `ledger send`: the historical optional
leading source is `--source` because the portable catalogue forbids an optional
positional before required positionals. Each conversion's request-shaping
obligation is recorded per command, so "converted" never stands in for
"assumed equivalent".

## Documents

| File | Contents |
| --- | --- |
| `inventory.json` | All 23 baseline commands with the baseline SDK call, the bound V2 operation, operationId, method, path, scope, pagination, idempotency, mutation, destructiveness and per-command conversion notes |
| `mapping.md` | The 22 included commands mapped to operationId, SDK method, route and scope; the 8 conversions with their evidence; client-behaviour gate evidence |
| `exclusions.md` | The single host-owned exclusion, the 23 out-of-scope v2 API operations, and barred capabilities |
| `auth-scopes-risks.md` | OAuth2 model, scope facts, divergences, risks, open evidence gaps and resolved prerequisites |
| `manifest.json` | Logical manifest and machine-checked invariants |

## Status

The 22-command catalogue, generated-client adapter, portable descriptor and
reconstructible lifecycle are implemented. The generated client owns request
DTO construction and response decoding; its auth and retry layers are disabled,
so credentials, policy and traffic remain host-owned. Paginated commands execute one page
by default; host-selected `--all` follows opaque cursors within the canonical
100-page, 10,000-item and 4 MiB ceilings. Every request after the first carries
only the opaque cursor. Raw log export accepts at most 4 MiB per response; the
boundary is tested with actual binary payloads at exactly 4 MiB and 4 MiB plus
one byte.

The pinned Speakeasy output has two request-shaping defects: nil optional GET
bodies are serialized as byte-exact `null`, and `V2ListLedgers` injects
`includeDeleted=false` beside a continuation cursor. A transport erratum removes
only byte-exact `null` on GET and, only for a non-empty cursor, canonicalizes the
query to that cursor alone. Other bodies and queries are preserved. Contract
tests lock these boundaries.

Set the explicit fctl source root, then run the SDK contract, unit and
lifecycle tests from the plugin module:

```sh
cd plugins/fctl/ledger-v2
export FCTL_SDK_ROOT=/path/to/fctl-v2-poc
just test
```

The committed `fctl-sdk.lock.json` binds the SDK module, repository, commit,
Nix content hash and canonical WIT hash. `with-fctl-sdk.sh` requires
`FCTL_SDK_ROOT`, validates those values, and creates a temporary `go.work`
that replaces only the pinned SDK module for the wrapped command. A
content-addressed source without Git metadata is accepted; a Git checkout must
also match the locked commit and origin URL. Adversarial tests cover malformed
locks, non-portable paths, content, module, WIT, revision and remote
mismatches, no-`.git` sources, and workspace cleanup on success, failure and
signals.

`just tidy-check` verifies module metadata without modifying `go.mod` or
`go.sum`; `just tidy` applies the same operation through the projected SDK.
Both commands use a temporary modfile, remove the SDK replacement before
publishing metadata, preserve unrelated replacements, and clean temporary
files on success, failure and signals.

Build the component with the product shell, which carries the exact component
authoring pins copied from fctl SDK revision `545521bf`:

```sh
export FCTL_SDK_ROOT=/path/to/fctl-v2-poc
nix develop --impure --no-write-lock-file .# --command \
  just -f plugins/fctl/ledger-v2/Justfile build-component
```

The shell pins `componentize-go` 0.4.1, patched `wasi-virt` revision
`448f6df8`, `wasm-tools` 1.239.0 and Binaryen `wasm-opt` 124. The Go compiler
wrapper applies `wasm-opt -Oz --all-features` to the stripped core module before
component adaptation. The build refuses any other tool version.

The build runs two lanes from the same staged source path, compares the Wasm,
extracted WIT, import list and checksums byte-for-byte, validates the component,
enforces the exact five-import WASI allowlist, and enforces the 16 MiB admission
limit. Only the explicit entrypoint, generated bindings and `main.go` enter
staging; a fail-closed guard rejects any `.claude-flow` path without deleting
the source data. `build/` and `dist/` are intentionally untracked. The fctl
SDK is supplied only through the content-verified wrapper; no workstation path
is committed. The relative generated-client replacement in `go.mod` must be
replaced by a published module version before release.

One compatibility claim remains open: whether the 8 converted commands
reproduce the historical output exactly depends on the undocumented `expand`
and filter-body contracts (`auth-scopes-risks.md` D5, D6, B4). The operations,
scopes and request shapes themselves are verified from source.
