# Ledger v3 command plugin

This module exposes the 48 runnable Ledger v3 product command paths present in the current
`release/v3.0` command surface to `fctl`. It is a
command-provider plugin: target selection, credentials, request signing,
transport and rendering remain owned by the host.

The runnable product command tree under `cmd/ledgerctl` is the inventory source
of truth. The catalogue contract test constructs those commands and compares
their paths with the plugin catalogue, excluding only the operator-owned
`ledgers promote` action. This makes additions and removals in the product CLI
fail the plugin test instead of relying on a duplicated command list.

The audit also compares the Ledger CLI semantics that are easy to lose while
transposing a path: mirror-ledger creation flags and source modes,
configuration dry-run, index-inspection defaults, and prepared-query
all-pages continuation. The adapter remains the executable contract; a matching
path count alone is not treated as full behavioral parity.

## Human table rendering

Twenty-six commands publish compact, ordered `RenderHints.Table` columns for
the stable scalar identity or summary fields present in their actual success
result. Dot-separated fields such as `transaction.id`,
`revertTransaction.timestamp`, and `indexes.reference` address nested objects.
The hints do not project or remove output. `RawOutputSchema` and
`PublicOutputSchema` remain byte-identical, and explicit schema properties make
every rendered scalar path machine-checkable. Most are permissive product
contracts. The `ledgers create`, `ledgers get`, and `ledgers list` contracts are
closed around `LedgerInfo`: the adapter clones the product response and removes
the OAuth2 client secret or the entire PostgreSQL DSN before emission. JSON and
YAML therefore remain exhaustive public views without exposing stored mirror
credentials.

The other 22 commands intentionally have no table hint:

- successful empty mutations: `account-types add`, `account-types remove`,
  `account-types set-default-enforcement`, `accounts delete-metadata`,
  `accounts set-metadata`, `indexes create`, `indexes drop`,
  `ledgers delete-metadata`, `ledgers remove-metadata-type`,
  `ledgers set-metadata`, `ledgers set-metadata-type`, `queries create`,
  `queries delete`, `queries update`, `transactions delete-metadata`, and
  `transactions set-metadata`;
- collection-only or variant results without one truthful stable scalar row:
  `accounts aggregate-volumes`, `indexes inspect`, `ledgers get-schema`, and
  `queries execute`;
- success shapes that vary or contain no stable scalar result:
  `ledgers configuration apply` and `numscripts save`.

`render_hints_test.go` freezes that 26/22 partition command by command,
constructs the real typed success output behind every hinted command, traverses
every dotted field to a scalar leaf, and verifies the same path is explicitly
declared in the command's public schema. A new command must deliberately join
one side of the partition with either exact columns or an evidence-based
omission reason.

The six former `chapters` commands are intentionally absent: current
`release/v3.0` removed `ListChapters`, `GetChapterSchedule`, `CloseChapter`,
`ArchiveChapter`, `SetChapterSchedule`, and `DeleteChapterSchedule` from the
authoritative protobuf API. The plugin does not retain non-executable shims for
those removed operations. Prepared-query create, update, and delete operations
use the remaining `BucketService.Apply` request variants.

The plugin binds generated Ledger v3 protobuf messages and method descriptors
through fctl's public `productgrpcmessage` SDK adapter. The portable guest does
not include grpc-go, resolve endpoints or open network connections. Mutations
sent through `BucketService.Apply` declare the Ledger v3 signing contract in
their command descriptors.

## Develop

The SDK is not published as a versioned Go module yet. Set `FCTL_SDK_ROOT` to
an fctl source checkout before testing or building. `fctl-sdk.lock.json` pins
the repository, commit, SDK content hash, and WIT hash; the wrapper validates
them and creates a temporary Go workspace replacement without recording the
checkout path in tracked files. Replace this development contract with a
released SDK version before publishing the plugin.

Run the unit and guest-lifecycle tests in the declared development shell:

```sh
FCTL_SDK_ROOT=/path/to/fctl nix develop --command just test
```

`just test` enforces at least 80% aggregate statement coverage across every Go
package owned by this module, then runs the component-guest lifecycle suite.
Generated protobuf bindings live in the parent Ledger module and are therefore
outside this module's coverage formula. The repository root `test-coverage`
gate reaches this module, while root `pre-commit` also reaches the reproducible
component build.

Build the portable component:

```sh
FCTL_SDK_ROOT=/path/to/fctl nix develop --command just build-component
```

The build performs two independent component builds and compares their bytes,
WIT, import inventory and checksums. On success, the read-only artifacts are
written to `dist/ledger-v3/`. Generated build and distribution directories are
ignored by Git.

## Runtime contract

- `wit/plugin.wit` is the public synchronous lifecycle ABI.
- Lifecycle envelopes carry versioned `componentbridge.v1alpha1` protobuf
  payloads as opaque bytes.
- Each execution is reconstructible and keeps no state after completion,
  cancellation or close.
- Product calls use only operations and granular scopes declared by the
  selected command descriptor.
- Paginated commands preserve opaque cursors. Host-request, page, item and byte
  ceilings bound complete collection traversal.
- Numscript, transaction-script, configuration, and mirror rewrite documents
  arrive through host-owned input artifact handles and are capped at 1 MiB.
  Transaction scripts and mirror rewrite documents are optional sources; their
  descriptors require the additive optional-artifact SDK contract, the
  `Optional` and `Repeated` fields on `sdk.InputArtifactSpec`.
  Operations embedding those documents admit up to 2 MiB on the generated
  protobuf request so the declared artifact maximum remains executable.
- The final component must validate, remain byte-for-byte reproducible and stay
  at or below 16 MiB.

The portable artifact still requires install and execution validation in every
host engine supported by the fctl release process.

## Signed Apply

The plugin never holds a signature. RFC 0009 forbids exposing one to a product
plugin and RFC 0012's payload schema enforces it, so there is no boundary
across which the host could hand the plugin a signature to wrap.

The plugin therefore does the same thing whether or not signing is active: it
builds its `ApplyBatch`, serializes it once, and sends an ordinary Apply
request carrying it in the `unsigned` variant. When the profile has an
activated `sign.ledger.apply-batch` signer, the fctl host lifts those exact
serialized bytes off the wire, signs them, and substitutes
`SignedApplyBatch{key_id, signature, payload}` for the unsigned variant,
carrying `forwarded_caller_snapshot` and `skip_response` through untouched
because they sit outside the signed unit. Nothing is re-serialized on the way,
so the bytes the signature covers are the bytes the server receives.

Each of the 22 mutation commands binds signing to its exact declared
`ledger.v3.Apply.*` operation ID rather than a synthetic shared operation.
The declaration also pins the closed opaque-protobuf recipe: outer fields 1
(`unsigned`) and 2 (`signed`), preserved transport fields 3
(`forwarded_caller_snapshot`) and 4 (`skip_response`), and envelope fields 1
(`key_id`), 2 (`signature`), and 3 (`payload`). The ordinary profile admits a
262,140-byte inner payload and a 263,241-byte final signed message; the artifact
and configuration profile admits 2,097,148 and 2,098,250 bytes respectively.
Those bounds include a 1,024-byte key ID and the fixed 64-byte Ed25519
signature.

The plugin's side of that contract — one unsigned variant per Apply, never a
signed one, exact operation identity, the pinned wire recipe, and its byte
ceilings — is asserted in `signed_apply_test.go`. The same test verifies the
result through Ledger's own generated protobuf descriptors,
`signing.Verify`, and `signing.ExtractBatch`.

With no signer activated the Apply goes unsigned, which is the ordinary path.
After activation every refusal is terminal: the host never retries unsigned.

Analyze uses exactly one host request and the generated-client's bounded
4 MiB/message, 16 MiB aggregate, 1,024-message response envelope. Ledger's gRPC
transport deterministically samples the domain's per-500-item callbacks at
power-of-two callback ordinals. It therefore emits at most 64 progress messages
plus the final result for any scan cardinality, without an ABI change.
