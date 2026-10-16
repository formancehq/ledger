# Public client boundary

The Ledger server owns the protobufs under `internal/proto`. They are private
implementation types and must not be imported by external consumers.

The public gRPC client is a separate Go module under `pkg/client/v3`. Its
protobufs are generated from the checked-in schemas under `pkg/client/v3/proto`.
Applications and SDK consumers may import that module; server packages must not.

The two generated packages intentionally contain equivalent wire contracts.
They must not be imported into the same process: protobuf descriptors are
registered globally by file name, so loading both copies would register files
such as `signature.proto` twice. The network serialization boundary performs
the conversion between the private server types and public client types.

Run `just generate-public-client` after changing a public schema. The CI
contract check regenerates the client and fails if generated output drifts.
