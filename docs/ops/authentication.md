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

## Ed25519 Key-Based Authentication

For machine-to-machine deployments without an OIDC provider, the server supports Ed25519 key-based authentication. Clients sign JWT tokens (EdDSA, RFC 8037) with their private key, and the server verifies with configured public keys.

Both OIDC and Ed25519 modes can coexist. When `--auth-ed25519-keys` is configured, a composite key set routes EdDSA tokens to the static key set and others to the OIDC key set.

### Server Setup

1. Generate a keypair:

```bash
ledgerctl auth generate-key ./keys
```

2. Create an `auth-keys.json` config file:

```json
{
  "keys": [
    {
      "keyId": "<key-id-from-step-1>",
      "publicKeyFile": "./keys/pubkey.hex",
      "scopes": ["ledger:read", "ledger:write"]
    }
  ]
}
```

3. Start the server with Ed25519 authentication:

```bash
ledger-v3-poc run --auth-ed25519-keys auth-keys.json --bootstrap --node-id 1 --cluster-id test
```

Setting `--auth-ed25519-keys` automatically enables `--auth-enabled` and `--auth-check-scopes`.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auth-ed25519-keys` | string | `""` | Path to JSON file with Ed25519 public keys and scopes |

### Client Usage

1. Generate a JWT token:

```bash
TOKEN=$(ledgerctl auth generate-token \
  --signing-key ./keys/seed.hex \
  --key-id <key-id> \
  --subject ci-bot \
  --scopes ledger:read,ledger:write \
  --expiration 1h)
```

2. Use the token with ledgerctl:

```bash
ledgerctl --auth-token "$TOKEN" ledgers list
# Or via environment variable:
AUTH_TOKEN="$TOKEN" ledgerctl ledgers list
# Or read from a file:
ledgerctl --auth-token @token.txt ledgers list
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `ledgerctl auth generate-key <dir>` | Generate an Ed25519 keypair |
| `ledgerctl auth generate-token` | Generate a signed EdDSA JWT token |

**generate-token flags:**

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--signing-key` | yes | | Path to Ed25519 seed file |
| `--key-id` | yes | | Key ID matching the server config |
| `--subject` | yes | | JWT subject claim |
| `--scopes` | no | | Comma-separated scopes |
| `--expiration` | no | `1h` | Token validity duration |

### Scope Enforcement

Ed25519 keys have a per-key scope allowlist defined in `auth-keys.json`. A token cannot claim scopes beyond what the key allows, even if the JWT payload contains them. This provides defense-in-depth: the server restricts what each key can do, independent of what the client requests.

### Security Notes

- Keep seed files (`seed.hex`) secret and with `0600` permissions
- Public key files (`pubkey.hex`) can be safely distributed
- Tokens are self-signed (no OIDC issuer); the issuer claim is not checked for EdDSA tokens
- Token expiration is always enforced
- Rotate keys by adding new entries to `auth-keys.json` and removing old ones

## Disabling Authentication

By default, authentication is disabled (`--auth-enabled=false`). All requests are accepted without tokens. This is suitable for development, testing, or environments where authentication is handled at the network level (e.g., service mesh).
