# Product-owned fctl plugin

Ledger owns its HTTP command package in the separate `misc/fctl-plugin/` Go module,
named `github.com/formancehq/ledger/misc/fctl-plugin`.
Its sole direct dependency is the public fctl plugin SDK. It imports neither
Ledger service internals nor the fctl core, UI, profiles, or authentication.

The package implements the SDK's `GetManifest` and `Execute` contract. The
manifest declares command arguments, flags, confirmations, and forms. The same
package can run embedded or through the standalone `fctl-plugin-ledger`
executable. The external entry point calls `transport.Serve` with the factory
that accepts the host-provided HTTP client.

In external mode, the SDK broker returns each HTTP request to the fctl host.
The host applies its authentication, endpoint restrictions, transport, and
diagnostics. Exact JSON bytes and partial bulk responses stay on the public
SDK boundary. The executable has no Ledger service gRPC contract dependency;
the plugin SDK protocol version is distinct from `pkg/grpcprotocol.Version`.

Each plugin release names an exact Ledger service version and a positive
plugin revision. A CLI-only fix increments the revision for that service
version. It does not change the service version in the SDK manifest. The
host's catalogue maps this identity and platform to a digest-addressed native
executable. The host validates the running manifest against the locked one.

GoReleaser builds the six supported native platforms in the Ledger release
pipeline. The publisher reads the resulting GoReleaser artifact list and the
SDK manifest exported by the native executable. It validates each binary's Go
entry point, target platform, and injected service version and plugin revision,
computes its SHA-256 checksum, and uploads
one raw executable layer per OCI artifact through the Nix-pinned ORAS tool.
Paths from the artifact list are opened through `os.Root`; traversal and
symlink escapes cannot select a file outside the build root.

Publication builds retain Go linker metadata. They do not use `-trimpath`,
which removes the recorded `-ldflags` needed to verify foreign-platform
version and revision without executing the binary. The publisher checks every
platform before calling ORAS. A stale manifest or a mixed set of product
versions fails preflight and produces neither an upload nor a catalogue.

The publisher creates private staging files before uploading. It emits a
catalogue only after all six uploads succeed. If an upload fails, earlier
artifacts can remain in the registry, but no complete catalogue is emitted.
The canonical Just recipe replaces the prepared catalogue only on success.
The local layout recipe exercises the same ORAS serialization without any
registry upload. See the [build and publication guide](../../../../../misc/fctl-plugin/README.md).
