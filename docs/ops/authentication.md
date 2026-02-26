# Authentication & Authorization

The ledger supports optional JWT/OIDC authentication with scope-based authorization. When enabled, all API requests (HTTP and gRPC) must carry a valid Bearer token issued by a trusted OIDC provider.

## Quick Start

```bash
ledger-v3-poc run \
  --auth-enabled \
  --auth-issuer https://auth.example.com \
  --auth-check-scopes \
  --auth-service ledger
```

## CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auth-enabled` | bool | `false` | Enable JWT authentication |
| `--auth-issuer` | string | `""` | OIDC issuer URL (used for discovery and token validation) |
| `--auth-check-scopes` | bool | `false` | Enforce scope-based authorization |
| `--auth-service` | string | `""` | Service name prefix for scopes (e.g., `ledger` for `ledger:read`) |
| `--auth-read-key-set-max-retries` | int | `10` | Maximum retries when fetching the JWKS key set |

When `--auth-enabled` is set:
1. The server performs OIDC discovery at `<issuer>/.well-known/openid-configuration`
2. Downloads the JWKS (JSON Web Key Set) from the discovered `jwks_uri`
3. Validates JWT signatures, issuer, and expiration on every request
4. Optionally checks scopes if `--auth-check-scopes` is enabled

## Scopes

The ledger uses three authorization scopes. When `--auth-service=ledger`, the scopes are:

| Scope | Operations |
|-------|------------|
| `ledger:read` | All read operations (GET HTTP, List*/Get* gRPC RPCs) |
| `ledger:write` | All write operations (Apply: create/revert transactions, save/delete metadata, create/delete ledgers, set maintenance mode, etc.) |
| `ledger:admin` | Cluster operations (GetClusterState, TransferLeadership, Backup, AddLearner, PromoteLearner, RemoveNode, GetDiskUsage, GetNodeTime) |

If `--auth-service=myapp`, the scopes become `myapp:read`, `myapp:write`, `myapp:admin`.

## Unauthenticated Endpoints

The following endpoints are always accessible without authentication:

- **HTTP**: `/health`, `/debug/pprof/*`
- **gRPC**: `grpc.health.v1.Health/*`, `BucketService.Discovery`, gRPC reflection

## gRPC Authentication

gRPC clients must include the JWT token in the `authorization` metadata:

```go
md := metadata.Pairs("authorization", "Bearer <token>")
ctx := metadata.NewOutgoingContext(ctx, md)
```

## HTTP Authentication

HTTP clients must include the JWT token in the `Authorization` header:

```
Authorization: Bearer <token>
```

## Disabling Authentication

By default, authentication is disabled (`--auth-enabled=false`). All requests are accepted without tokens. This is suitable for development, testing, or environments where authentication is handled at the network level (e.g., service mesh).
