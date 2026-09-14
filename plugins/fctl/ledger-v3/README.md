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

One host/product integration gate remains intentionally fail-closed:

- fctl does not yet connect an accepted Ledger v3 `SignedApplyBatch`
  sign-and-send executor to `Composer`. At the pinned SDK commit,
  `Composer.ExecuteWithContinuation` refuses every command whose descriptor sets
  `RequestSigning`, so `sign.ledger.apply-batch` returns `signing_failed` before
  target validation and before product access; the plugin never falls back to an
  unsigned Apply. The host-side signer broker exists but has no production
  caller, and the frozen plugin ABI carries product calls only as opaque
  `(full-method, message)` bytes, so there is no seam for the plugin to hand the
  host a typed unsigned `ApplyBatch`. Repinning the SDK does not lift this gate;
  fctl must add the executor first.

Analyze uses exactly one host request and the generated-client's bounded
4 MiB/message, 16 MiB aggregate, 1,024-message response envelope. Ledger's gRPC
transport deterministically samples the domain's per-500-item callbacks at
power-of-two callback ordinals. It therefore emits at most 64 progress messages
plus the final result for any scan cardinality, without an ABI change.
