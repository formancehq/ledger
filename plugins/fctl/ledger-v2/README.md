# `ledger-v2` plugin

One of **two separate** Ledger plugins. It shares no catalogue, manifest or
command surface with `../ledger-v3`. See `../README.md` for the boundary rule.

| | |
| --- | --- |
| Product major | **2**, and no other |
| Transport | generated Ledger v2 client over host-owned `producthttp` |
| API contract | `github.com/formancehq/ledger/pkg/client` and `openapi/v2.yaml` at `8cc679c9440aaf90d4b8646a013e5bd7421443d6` |
| Host SDK contract | `github.com/formancehq/fctl-v2-poc/pkg/plugin` at `e9b1395f46f3100b381dbe00f5213de28e6df0e1` |
| Inventory basis | old-fctl `cmd/ledger` at `693c58e27865f83332e6c3199d61fed81b742f41` |
| Coverage denominator | **22** included of 23 baseline commands |
| Signing capability | **none** — `sign.ledger.apply-batch` is major-3 only |

## Surface rule

`ledger-v2` publishes the 22 historical command phrases that belong to the
product API. Where a baseline command used `Ledger.V1`, it is converted to its
V2 API equivalent: 14 commands already spoke V2, 8 are converted, and
`ledger server-infos` leaves the plugin because the `/_/info` probe is
host-owned.

This is command coverage, not byte-for-byte CLI parity. Portable execution
requires an explicit `--ledger`; `ledger send` moves the optional leading source
to `--source`; schema output formatting is host-owned; and schema insertion
reads a host-owned file artifact. Its single `application/octet-stream` media
contract is deliberately format-neutral, so both bounded JSON and YAML keep the
same CLI without requiring a host-owned media selector; the plugin detects and
validates the document after reading it. The old direct-URL schema source is not
implemented because the current input-artifact ABI admits files and stdin, not
URLs. `last`, corrected `last-N`, and the historical `lastN` spelling are
resolved through a bounded read before the transaction operation.

## Table render hints

12 of the 22 commands publish a `render.table` column list; 10 publish none.
The split is evidence-driven, not stylistic: a column is declared only where the
result the adapter actually emits proves the field exists.

| Result | Commands | Columns |
| --- | --- | --- |
| `V2Ledger` | `ledger list` | Name, Bucket, Added At |
| `V2Stats` | `ledger stats` | Accounts, Transactions |
| `V2Account` | `ledger accounts list`, `ledger accounts show` | Address, Insertion Date, Updated At |
| `V2Transaction` | `ledger send`, `ledger transactions list`, `show`, `num`, `revert` | ID, Timestamp, Reference, Reverted |
| `V2VolumesWithBalance` | `ledger volumes list` | Account, Asset, Input, Output, Balance |
| `V2Schema` | `ledger schemas get`, `ledger schemas list` | Version, Created At |

The 10 without a hint are the nine commands whose operation returns no content,
so the plugin publishes the empty object — `ledger create`, `set-metadata`,
`delete-metadata`, `import`, `accounts set-metadata`, `accounts
delete-metadata`, `transactions set-metadata`, `transactions delete-metadata`
and `schemas insert` — plus `ledger export`, whose result is an opaque base64
payload with no fields to name. Declaring a column for any of them would name a
field the plugin never emits.

Every column projects a scalar leaf of a document the command already emits, so
a hint cannot widen what the plugin discloses. Dynamic key/value blobs
(`metadata`, `features`), volume aggregations (`volumes`, `postCommitVolumes`
and their siblings), posting arrays and schema charts are excluded: they are
composites a table would have to serialise back into one cell.
For every hinted result, the raw and public output schemas name these scalar
properties explicitly while continuing to admit the complete generated-client
DTO. Contract tests resolve each field through both that schema and an adapter
result, and require the declared scalar type to accept the emitted value.

`TableColumn.Field` is a dot-separated path, so a nested scalar leaf such as
`status.phase` is expressible. **No Ledger v2 column uses one.** Every nested
value in these results is reached through a dynamic key — an asset code, a
metadata key, an account address — or through an array, and neither can be named
by a static path. `render_hints_test.go` traverses dotted paths regardless, so
the contract is enforced rather than merely assumed.

**These hints change no observable output at the pinned SDK revision.** The
host's `renderTable` derives its columns from the result document itself and
does not read `Command.Render`; its own comment says columns come "never from a
product-supplied layout". The hints are carried across the component codec and
validated for completeness, and they become the layout only if a host chooses to
honour them. Treat the table above as a published declaration, not as a receipt
for what `fctl` prints today.

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
one byte. Import accepts a host-owned artifact up to 64 MiB and sends sequential
batches of at most 100 logs and 4 MiB within the 256-request portable ceiling.
That permits at most 25,600 input lines, or 25,500 when one request is reserved
for the latest-log resume probe. An over-budget artifact is rejected before the
first import write. This is a bounded compatibility path; old-fctl did not
impose the portable 64 MiB artifact or 256-request ceilings.

The pinned Speakeasy output has two request-shaping defects. Nil optional GET
bodies are serialized as byte-exact `null`. And the generated client injects its
own defaults beside a continuation cursor on two of the five paginated
operations, not one: `V2ListLedgers` adds `includeDeleted=false`, and
`V2ListSchemas` adds `order=desc`, `pageSize=15` and `sort=created_at`.
`V2ListAccounts`, `V2ListTransactions` and `V2GetVolumesWithBalances` send the
cursor alone.

A transport erratum removes only byte-exact `null` on GET, and — for any GET
with a non-empty `cursor` — canonicalizes the query to that cursor alone. The
query rule is deliberately broader than the two known defects. It is safe only
because every parameter it can drop is a client-side default: the adapter sets
page size, sort and filters on the first page only, so a continuation request
carries no caller intent beyond the cursor. A contract test pins exactly what
each paginated operation puts on the wire beside a cursor, so a regenerated
client that combines a cursor with a real parameter fails a test rather than
having that parameter silently dropped. Other bodies and queries are preserved.

### Failure detail at the two boundaries

A product failure carries different detail depending on how the plugin is
embedded, and the difference is not cosmetic.

| | In process (`sdk.Host`) | Portable component (delivered) |
| --- | --- | --- |
| Failure code | `product_http_error` (in-range non-2xx), `product_response_failed` (out of range) | same |
| `Retryable` | set from the status: true at 5xx | **dropped** |
| `Details.httpStatus` | the originating status | **dropped** |
| `Message` | `product request returned HTTP N` | **dropped** |

`portable.Frame` carries only `FailureCode`, so the guest rebuilds the wire
failure as `Failure{Code: frame.FailureCode}` before it reaches a component
host — even though the wire codec itself can carry message, details and the
retryable flag. `adapter_v2_edges_test.go` pins the in-process column;
`component/portable_failure_test.go` drives the real lifecycle and pins the
portable one. Read either test only for the boundary it names.

Neither boundary carries the product's own error body. `producthttp`
short-circuits an in-range non-2xx before the body reaches the generated client,
so Ledger's `ErrorResponse.errorCode` — the 20-value `V2ErrorsEnum` — is
unreachable by this plugin, and `productHTTPFailure` builds details containing
the status and nothing else. That is the generic bridge behaving as designed: a
dynamic product body must not become plugin-authored diagnostic text. Both tests
assert that no fragment of the body leaks. The consequence is recorded as
divergence D7 in `auth-scopes-risks.md`: only the status survives, and because
all 22 bound operations declare exactly one `default` error response in
`openapi/v2.yaml`, the status does not identify the code. Recovering the code is a host-SDK contract decision,
not something the plugin can do on its own.

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
authoring pins copied from fctl SDK revision `e9b1395f`:

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
the source data. It also publishes regular, read-only
`browser-input/component.wasm` and `browser-input/imports.txt` copies for the
external browser harness. Their presence is not a Firefox acceptance receipt.
`build/` and `dist/` are intentionally untracked. The fctl
SDK is supplied only through the content-verified wrapper; no workstation path
is committed. The relative generated-client replacement in `go.mod` must be
replaced by a published module version before release.

Exact historical presentation remains open for the 8 converted commands and
requires the external dual-host/live-server evidence tracked in
`auth-scopes-risks.md`. The local gates prove the bounded adapter contracts; they
do not prove Firefox acceptance, release packaging, or byte-for-byte old-fctl
output.
