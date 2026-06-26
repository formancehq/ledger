# API

Client-facing transport layers (`internal/adapter/grpc`, `internal/adapter/http`, `internal/adapter/auth`). The gRPC service is the primary contract; the HTTP REST surface is a compatibility layer over the same controller.

## Documents

| Document | Description |
|----------|-------------|
| [grpc-api.md](grpc-api.md) | gRPC service, methods, request/response types, and client examples. |
| [grpc-connections.md](grpc-connections.md) | gRPC connection mechanics, reconnection, and rolling deployment optimizations. |
| [http-api.md](http-api.md) | HTTP REST API endpoints, response formats, and error handling. |

## Related

- [Admission](../admission/) — what every write request enters next.
- [Scripting](../scripting/) — numscript library available through the API.
