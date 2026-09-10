# API

Client-facing transport layers (`internal/adapter/grpc`, `internal/adapter/http`, `internal/adapter/auth`), plus the two packages that carry a failure across them: `internal/adapter/apierr`, the transport-neutral boundary contract every surface reads an error through, and `internal/adapter/grpcerr`, the gRPC encoder/decoder for it and owner of the single `ErrorKind`-to-status-code table. The gRPC service is the primary contract; the HTTP REST surface is a compatibility layer over the same controller.

## Documents

| Document | Description |
|----------|-------------|
| [grpc-api.md](grpc-api.md) | gRPC service, methods, request/response types, type-owned public error details, client examples, and restore-mode service lifetime. |
| [grpc-connections.md](grpc-connections.md) | gRPC connection mechanics, reconnection, and rolling deployment optimizations. |
| [protocol-compatibility.md](protocol-compatibility.md) | Mandatory service protocol revision, client/server rejection behavior, diagnostics, and revision maintenance (EN-1851). |
| [http-api.md](http-api.md) | HTTP REST API endpoints, single-decoding metadata key paths, response formats, exact metadata number decoding, ledger-log JSON output and discriminators, error handling, and the `apierr`/`grpcerr` error boundary crossed by a forwarded write. |
| [structured-credentials.md](structured-credentials.md) | Structured stored connection configurations and public configuration/log masking. |
| [auth.md](auth.md) | Client JWT authentication (OIDC + Ed25519), scope-based authorization, and the Raft inter-node cluster-secret auth layer. |
| [sensitive-projection.md](sensitive-projection.md) | Detached, descriptor-driven secret projection and its ownership boundary. |

Decoded leader errors retain their exact gRPC status through routing wrappers
and streaming cursors. See [forwarding and cancellation](http-api.md#forwarded-writes-and-the-transport-seam)
for the distinction between a decoded rejection and raw transport cancellation.

## Related

- [Admission](../admission/) — what every write request enters next.
- [Scripting](../scripting/) — numscript library available through the API.
