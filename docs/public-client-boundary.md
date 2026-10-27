# Public client boundary

The Ledger server owns the protobufs under `internal/proto`. They are private
implementation types and must not be imported by external consumers.

The public gRPC client is a separate Go module under `pkg/client/v3`. Its
protobufs are generated from the checked-in schemas under `pkg/client/v3/proto`.
Applications and SDK consumers may import that module; server packages must not.

The public module is generated with only `protoc-gen-go` and
`protoc-gen-go-grpc`. Server-only generators such as `protoc-gen-dethash`,
`protoc-gen-skippable`, and the vtprotobuf/reader generators are intentionally
not run for this module; their generated helpers belong to the server's
internal protobuf packages.

The protobufs under `misc/proto/` are the canonical source. The public `.proto`
files under `pkg/client/v3/proto/` are a generated projection of that source:
they contain the public service closure and use the public Go package path. The
projection is file-level: backend-only declarations must live in an excluded
schema such as `internal_state.proto`; adding a private message to a selected
public file publishes it. The generated Go package is one consumer of the
projection; other consumers (for example TypeScript generators) should use the
projection, never edit the projection by hand.

The shared module `pkg/proto/v3` owns the public generated Go types and their
protobuf descriptors. Backend packages import those types directly; the
`internal/proto/*` packages retain only private generated state and small alias
facades for compatibility. This gives a server and its public client one
descriptor owner in the same process, so files such as `signature.proto` are
registered only once. The standalone `pkg/client/v3` module aliases the shared
types and adds the public gRPC client surface; it does not generate a second
descriptor copy.

Run `just generate-public-client` after changing a schema in `misc/proto/`.
This regenerates the public projection and the Go client. The generated client
is checked for drift by the existing CI dirty check.
Backend-only schemas remain under `misc/proto/` and are generated exclusively
by `just generate-proto`. Server-only generators and helpers remain private;
the shared module contains only generators and helpers that are deliberately
part of the public contract.

The release workflow publishes nested Go-module tags in a second phase. A root
`vX.Y.Z` tag first builds the server release and opens an automated PR that
updates `pkg/client/v3/go.mod` to require `pkg/proto/v3@vX.Y.Z`. Once that PR is
merged, the publication workflow tags `pkg/proto/v3/vX.Y.Z` first and then
`pkg/client/v3/vX.Y.Z` from the merged commit. This keeps the dependency resolvable
without ever creating a stable module tag from an RC commit; the local
`replace` directive remains only for monorepo development.
