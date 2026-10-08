# Public Ledger v3 gRPC client

`github.com/formancehq/ledger/pkg/client/v3/grpc` is the one generated Go
package for the Ledger service contract. The server, `ledgerctl`, and Ledger's
gRPC tests import it too, so linking a client with `pkg/testserver` registers
each Protobuf file descriptor once. The public closure comprises `common`,
`signature`, `audit`, `bucket`, `cluster`, and `restore` (55 RPCs). The seven
persisted-only common messages live in `internal_state.proto`; Raft,
replication, bootstrap, snapshot, and storage protocols remain private.

The nested Go module at `pkg/client/v3` contains generated bindings, the six
source `.proto` files, `proto/ledger-public.protoset`, and `contract.json`.
The Protobuf sources and descriptor are the normative, language-neutral
contract; the Go package is the maintained generated convenience client. A
hand-written method attached to a public message is part of the public API, so
it must describe supported wire/JSON behavior rather than server construction
or persistence behavior.
Caller-attribution trust-boundary violations have no public `ErrorReason`:
admission rejects them before proposing, and the FSM rejects malformed
committed entries before audit or business-state mutation. The server reports
only a generic `Internal` status to callers and retains details in diagnostics.
`contract.json` binds its client version to the descriptor SHA-256, compiled
service protocol revision, and server versions known to be compatible when
the client is published. `misc/release/public-client.json` records the exact
client selected for each server release, including the descriptor digest and
protocol revision that justify that selection. The module
ZIP fetched by a pinned Go version is the immutable contract artifact; a
moving branch or copied `.proto` file is not a supported consumer source.

## Go consumer

Pin a published version of `github.com/formancehq/ledger/pkg/client/v3` in
`go.mod` and import `github.com/formancehq/ledger/pkg/client/v3/grpc`.
Use `grpc.ClientOption()` when opening the connection so every unary and
streaming business RPC declares the version this generated client implements.
Supply transport credentials and authentication appropriate to the deployment;
the version metadata is not an authentication credential. For example:

```go
import (
    ledgergrpc "github.com/formancehq/ledger/pkg/client/v3/grpc"
    gogrpc "google.golang.org/grpc"
)

conn, err := gogrpc.NewClient(endpoint, ledgergrpc.ClientOption(), transportOption)
if err != nil { return err }
defer conn.Close()
client := ledgergrpc.NewBucketServiceClient(conn)
_, err = client.Discovery(ctx, &ledgergrpc.DiscoveryRequest{})
```

The public client has no dependency on `ledger/v3/internal/proto`, server
bootstrap, consensus, storage, SQL, or server schema packages. The nested
module contains the wire types, generated gRPC bindings, and the JSON behavior
needed to exchange those types, including precise integer decoding and audit
serializer errors. Server-side policy tables, SQL and schema adapters, metadata
coercion, transaction builders, and conversion to Ledger's domain types live in
root-owned `internal/proto/publicpolicy`, `internal/protosql`, and
`internal/protohelpers`. Ledger's REST-only bulk envelope and server-only
not-found errors also remain in root-owned packages.

## Regeneration and checks

Use the pinned Nix development shell and `just generate-proto` immediately
after changing a `.proto` file. The entry point generates public and internal
passes separately, refreshes the module manifest and descriptor digest, and
requires no remote generator credential. Run it a second time and require no
tracked change. `bash scripts/check-public-client.sh` does that drift check,
tests the nested module, verifies an external Go module can import it through
a local replacement, and starts a real server with the public package in the
same process. CI runs this check in `Tests-Public-Client`; the normal root
build and tests remain separate gates. The external local replacement proves
the proposed source; fetching a pinned published tag is a release gate. The
surface check also scans every client source file for server-only imports, so a
future generated or build-tagged file cannot silently pull storage, consensus,
schema, or server-toolkit dependencies into the published module.

`pkg/grpcprotocol.Version` remains the server's authoritative revision rule.
The generator copies its current value into the client package and contract
manifest. Bump it only when an existing client or server would interpret a
service request, response, or behavior differently, following
[protocol compatibility](protocol-compatibility.md). Moving generated Go code
and persisted-only messages without changing the service wire or semantics
does not increment it.

## Release order and compatibility

For the initial `v3.0.0-beta.6` pair, the client tag is
`pkg/client/v3.0.0-beta.6`. Publish that immutable Go module before the server
tag. Matching versions and commits are convenient when both change together,
but neither is a compatibility rule.

Before every server tag, set `misc/release/public-client.json` to that server
version and the compatible published client version. A server-only fix can
select the existing client: for example, server `v3.0.0-beta.7` can select
client `v3.0.0-beta.6` when the public descriptor and protocol revision are
unchanged. Updating this server metadata does not mutate or retag the client.
An independent client-helper release can likewise advance the client version
without advancing the server version.

Run the complete Ledger validation and human Ledger/Connectivity review before
tagging. The release gate downloads the selected client module at its tag,
checks the module checksum, its embedded manifest and descriptor, the server's
compiled protocol revision and descriptor, the root `go.mod` selection, and
every downloaded module file against an archive rebuilt from the client tag
commit using Go module packaging rules. This also rejects altered or extra Go
source while allowing files Go deliberately excludes from module ZIPs. It
records both tag target commits, the module checksum, the descriptor digest,
and the compatibility basis in `client-release-provenance.json`. It rejects an
absent tag, contract drift, or mismatched compatibility metadata. No tag is
created by regeneration
or CI. Consumers pin the client tag and can verify the descriptor bytes against
its immutable `contract.json`.
