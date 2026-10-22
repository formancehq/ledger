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
they contain the public service closure, use the public Go package path, and
exclude backend-only messages. The generated Go package is one consumer of the
projection; other consumers (for example TypeScript generators) should use the
projection or `ledger-public.protoset`, never edit the projection by hand.

The two generated Go packages intentionally contain equivalent wire contracts.
They must not be imported into the same process: protobuf descriptors are
registered globally by file name, so loading both copies would register files
such as `signature.proto` twice. The network serialization boundary performs
the conversion between the private server types and public client types.

Run `just generate-public-client` after changing a schema in `misc/proto/`.
This regenerates the public projection, the Go client and
`ledger-public.protoset`. The CI contract check regenerates all three and fails
if any generated output drifts. Backend-only schemas remain under
`misc/proto/` and are generated exclusively by `just generate-proto`.
