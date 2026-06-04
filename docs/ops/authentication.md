# Authentication & Authorization

The ledger supports optional JWT/OIDC authentication with scope-based authorization. When enabled, all API requests (HTTP and gRPC) must carry a valid Bearer token issued by a trusted OIDC provider.

## Quick Start

```bash
ledger run \
  --node-id 1 \
  --cluster-id prod-ledger \
  --bootstrap \
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

## Configuration Invariants

The server enforces the following rules at startup:

- `--auth-enabled` requires at least one of `--auth-issuer` (OIDC) or
  `--auth-ed25519-keys` (Ed25519 key file). The server refuses to start
  without a credential source.
- Setting auth-related flags (`--auth-issuer`, `--auth-ed25519-keys`,
  `--auth-scope-mapping-file`) without `--auth-enabled` is rejected to
  prevent operators from believing authentication is active when it is not.

## Unauthenticated Endpoints

The following endpoints are always accessible without authentication:

- **HTTP**: `/health`, `/livez`, `/readyz`
- **gRPC**: `grpc.health.v1.Health/*`, `BucketService.Discovery`, gRPC reflection

<<<<<<< HEAD
## Anonymous Scopes (writes-only mode)

By default, every request must authenticate. To open up a subset of operations to unauthenticated callers — typically: **all reads public, writes still authenticated** — declare *anonymous scopes*.

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--auth-anonymous-scopes` | string | `""` | Comma-separated granular scopes (or `*:read` / `*:write` wildcards) granted to requests without a bearer token |

The writes-only configuration is:

```bash
ledger run --auth-enabled --auth-issuer https://auth.example.com \
  --auth-anonymous-scopes "*:read"
```

Semantics:

| Request | Token | Outcome |
|---------|-------|---------|
| Read endpoint | Absent | **200** — anonymous covers the required `*:read` scope |
| Read endpoint | Invalid (bad signature, expired, malformed) | **401** — a broken token is a client error, never silently ignored |
| Read endpoint | Valid with `*:read` | 200 — the token's scopes apply (no anonymous merging) |
| Write endpoint | Absent | **401** — anonymous does not cover writes |
| Write endpoint | Valid with `*:write` | 200 |
| Write endpoint | Valid without write scope | 403 |

Equivalent scope-mapping JSON (`--auth-scope-mapping-file`):

```json
{ "anonymous": ["*:read"] }
```

Both forms are supported — the CLI flag is shorthand for the JSON entry.

When neither is set, the behavior is identical to before (every request must authenticate, no regression possible).

Note: `/debug/pprof/*` endpoints require the `ops:read` scope when authentication is enabled.

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
ledger run --auth-ed25519-keys auth-keys.json --bootstrap --node-id 1 --cluster-id test
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
| `--god` | no | `false` | Include god-mode claim (grants all scopes; key must allow it) |

### Scope Enforcement

Ed25519 keys have a per-key scope allowlist defined in `auth-keys.json`. A token cannot claim scopes beyond what the key allows, even if the JWT payload contains them. This provides defense-in-depth: the server restricts what each key can do, independent of what the client requests.

### God Mode

A key can be configured with `"god": true` in `auth-keys.json` to allow it to emit tokens that bypass all scope checks. When a JWT contains the custom claim `"god": true` and is signed by a god-enabled key, the token is granted all granular scopes regardless of the `scopes` claim.

```json
{
  "keys": [
    {
      "keyId": "admin-key",
      "publicKeyFile": "./keys/admin-pubkey.hex",
      "scopes": [],
      "god": true
    }
  ]
}
```

Generate a god-mode token:

```bash
TOKEN=$(ledgerctl auth generate-token \
  --signing-key ./keys/seed.hex \
  --key-id admin-key \
  --subject admin \
  --god)
```

For OIDC tokens, the god claim is trusted if present in the JWT issued by the configured OIDC provider. For Ed25519 tokens, only keys with `"god": true` in the server config are allowed to claim god mode; tokens signed by non-god keys that contain the claim are rejected.

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
