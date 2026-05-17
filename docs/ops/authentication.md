# Authentication & Authorization

The ledger supports optional JWT/OIDC authentication with scope-based authorization. When enabled, all API requests (HTTP and gRPC) must carry a valid Bearer token issued by a trusted OIDC provider.

## Quick Start

```bash
ledger-v3-poc run \
  --auth-enabled \
  --auth-issuer https://auth.example.com \
  --auth-service ledger
```

## CLI Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auth-enabled` | bool | `false` | Enable JWT authentication and scope-based authorization |
| `--auth-issuer` | string | `""` | OIDC issuer URL (used for discovery and token validation) |
| `--auth-service` | string | `""` | Service name prefix for scopes (e.g., `ledger` for `ledger:read`) |

When `--auth-enabled` is set:
1. The server performs OIDC discovery at `<issuer>/.well-known/openid-configuration`
2. Downloads the JWKS (JSON Web Key Set) from the discovered `jwks_uri`
3. Validates JWT signatures, issuer, and expiration on every request
4. Enforces scope-based authorization on all endpoints

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

Setting `--auth-ed25519-keys` automatically enables `--auth-enabled` unless `--auth-enabled=false` is explicitly set.

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

## Credential Storage

`ledgerctl` can store JWT tokens in the OS keychain (macOS Keychain, Linux libsecret, Windows Credential Manager) to avoid passing `--auth-token` on every command.

### Token Resolution Priority

When making API calls, `ledgerctl` resolves the bearer token in this order:

1. `--auth-token` flag (or `AUTH_TOKEN` env var)
2. OS keychain (keyed by `--server` address)
3. No authentication

### Workflows

**Login (generate and store a token):**

```bash
ledgerctl auth login \
  --signing-key ./keys/seed.hex \
  --key-id my-key-id \
  --subject ci-bot \
  --scopes ledger:read,ledger:write

# All subsequent commands use the stored token automatically
ledgerctl ledgers list
```

**Login with a key bundle (Kubernetes agents):**

The `kubectl ledger agents get-key --bundle -` command outputs a JSON key bundle that can be piped directly into `ledgerctl auth login`:

```bash
# Single-pipe workflow
kubectl ledger agents get-key my-agent --bundle - | ledgerctl auth login

# Override subject from the bundle
kubectl ledger agents get-key my-agent --bundle - | ledgerctl auth login --subject ci-bot

# Or save the bundle to a file
kubectl ledger agents get-key my-agent --bundle agent-bundle.json
ledgerctl auth login --bundle agent-bundle.json
```

The bundle JSON format:

```json
{
  "signingKey": "<64-char hex seed>",
  "keyId": "agent-key-id",
  "scopes": ["ledger:read", "ledger:write"],
  "subject": "my-agent"
}
```

Explicit flags (`--subject`, `--key-id`, `--scopes`, `--signing-key`, `--expiration`) always override bundle values.

**Check status:**

```bash
ledgerctl auth status
# Shows: server, token source, subject, scopes, expiry
```

**Remove stored credentials:**

```bash
ledgerctl auth logout
```

**Multi-server usage:**

```bash
# Login to different servers
ledgerctl --server dev:8888 auth login --signing-key ./keys/seed.hex --key-id dev --subject ci
ledgerctl --server prod:8888 auth login --signing-key ./keys/seed.hex --key-id prod --subject ci

# Commands automatically use the correct token
ledgerctl --server dev:8888 ledgers list   # uses dev token
ledgerctl --server prod:8888 ledgers list  # uses prod token
```

### CLI Commands

| Command | Description |
|---------|-------------|
| `ledgerctl auth login` | Generate a token and store it in the OS keychain |
| `ledgerctl auth logout` | Remove a stored token |
| `ledgerctl auth status` | Show current authentication status |
| `ledgerctl auth generate-token --store` | Generate and store a token |

## Disabling Authentication

By default, authentication is disabled (`--auth-enabled=false`). All requests are accepted without tokens. This is suitable for development, testing, or environments where authentication is handled at the network level (e.g., service mesh).
