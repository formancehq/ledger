# Authentication and Authorization

## Overview

Authentication on the API surface is **JWT bearer tokens** in the standard `Authorization: Bearer <token>` form, validated either against an OIDC provider's JWKS or a static Ed25519 keyset for development. Authorization is **scope-based** — every API method declares the granular scope(s) it requires, and the token must carry an expanded set of scopes that covers them.

Two distinct auth layers coexist:

1. **Client auth** — JWT, validated on every request, gates the public gRPC/HTTP API.
2. **Inter-node auth (Raft)** — shared cluster secret, not JWT, gates the Raft transport between nodes.

This page covers both. The cryptographic request-signing layer (Ed25519, used to sign batch bodies for tamper-evidence and audit) is a separate concern, documented in [admission / signing.md](../admission/signing.md).

## Client authentication

### Token formats

`internal/adapter/auth/grpc_auth.go:validateToken (lines 186-241)` accepts two formats:

| Format | Use case |
|--------|---------|
| **OIDC** (RS256, ES256, PS256) | Production. Tokens issued by a configured OIDC provider; signing keys discovered via JWKS. |
| **Ed25519** (EdDSA, self-signed) | Dev / CI / scripts. Static keyset loaded from a JSON file at boot. |

Both formats coexist via a **composite keyset** (`internal/bootstrap/module.go:1626`). EdDSA tokens skip issuer verification (they're self-signed); OIDC tokens go through full issuer + JWKS verification.

### Wire shape

| Transport | Where the token lives |
|-----------|----------------------|
| gRPC | `metadata.FromIncomingContext(ctx)` → `authorization: Bearer <token>` |
| HTTP | `r.Header.Get("Authorization")` |

If no bearer is present, the request is treated as **anonymous** and given whatever scopes are configured via `--auth-anonymous-scopes` (CSV, default empty). A method requiring a scope not in the anonymous set returns `Unauthenticated` (gRPC) / `401` (HTTP).

### Token validation

`validateToken()` runs on every request — there is **no token cache**. Steps:

1. Decode the token (`grpc_auth.go:189`).
2. Parse claims (`oidc.AccessTokenClaims`).
3. Verify signature against the composite keyset (OIDC JWKS + Ed25519 statics).
4. Verify expiration.
5. For OIDC, verify issuer.

The lack of cache is deliberate at this stage — JWKS lookups are local to the in-memory keyset (the OIDC discovery is done once at boot, see below).

### Scopes

`internal/adapter/auth/scopes.go:21-36` defines 14 granular scopes:

```
ledger:LedgerRead       ledger:LedgerWrite
ledger:TransactionRead  ledger:TransactionWrite
ledger:AccountRead      ledger:MetadataWrite
ledger:AuditRead        ledger:AuditWrite
ledger:OpsRead          ledger:OpsWrite
ledger:QueryRead        ledger:QueryWrite
ledger:ClusterRead      ledger:ClusterWrite
```

Tokens may carry either **granular** scopes (used as-is) or **virtual** scopes (for example `ledger:read`, `ledger:write`, `ledger:admin`) that expand into a granular set via a configurable mapping (`scopes.go:56-89`). Granular scopes are always in the Ledger application namespace with the `ledger:<Resource><Action>` shape, matching the Membership scope convention. The mapping can be a JSON file, an environment variable, or a built-in default. Once expanded, only the granular form is consulted at authorization time.

### Authorization enforcement

There is **no global gRPC interceptor**. Each RPC calls `auth.Authenticate(ctx, cfg, scopeRequired...)` explicitly — `server_bucket.go:106` and similar. The rationale (`server.go:692`): granular scopes vary per RPC, and per-method declaration keeps the contract visible at the call site.

HTTP follows the same model: a `RequireScope` middleware (`http_middleware.go:100-126`) wraps each protected route.

### Error mapping

| Situation | gRPC | HTTP |
|-----------|------|------|
| No bearer token, anonymous scopes insufficient | `Unauthenticated` | 401 |
| Bearer token invalid (bad signature, expired, wrong issuer) | `Unauthenticated` | 401 |
| Bearer token valid, scopes insufficient | `PermissionDenied` | 403 |

Failures are structured-logged with reason, key ID, remote address, and an OTel span via `logAuthFailure()` (`grpc_auth.go:257-292`) — so an operator can correlate a 403 to a specific span without parsing logs.

## Anonymous access

`--auth-anonymous-scopes` controls what an unauthenticated request can do. Examples:

| Flag value | Effect |
|------------|--------|
| `""` (default) | Every request must be authenticated. |
| `"*:read"` | Public reads, authenticated writes. |
| `"ledger:LedgerRead,ledger:TransactionRead"` | Specific anonymous reads, nothing else. |

This is the right setting to relax for embedded / dev deployments without disabling auth altogether.

## Dev-mode bypass

`--auth-enabled` (default `false`) is the master switch. When auth is disabled, `Authenticate()` is a no-op and every request is admitted with full scopes. **There is no separate `--unsafe-disable-auth` flag** — the default is "off" because the system is designed for explicit opt-in.

## Inter-node authentication (Raft)

Raft transport uses a **shared cluster secret**, not JWT. `internal/adapter/grpc/raft_auth.go:27-75`:

- Each Raft RPC carries `authorization: Bearer <clusterSecret>` in metadata.
- Comparison uses `crypto/subtle.ConstantTimeCompare` to avoid timing attacks.
- If `--cluster-secret` is empty, the legacy "no auth" mode is used (not recommended).

There is a **fast path** when the cluster secret is presented through the client surface (`grpc_auth.go:91-100`): the request bypasses JWT validation, gets every granular scope, is marked as cluster-internal in the context, and — if the request is a leader forwarding a follower's work — carries the forwarded `CallerSnapshot` so the audit chain still attributes the operation to the original caller. If a request carries a forwarded snapshot but the connection is **not** cluster-internal (cluster secret unset or mismatched), the leader **rejects** it with `PermissionDenied` rather than silently dropping the identity (`server_bucket.go:adoptForwardedSnapshotIfTrusted`) — a misconfiguration surfaces as a failed write instead of an unattributed audit entry.

## Caller identity in the audit chain

Every proposal carries a `CallerSnapshot` (proto `common.proto`):

```proto
message CallerSnapshot {
  CallerIdentity identity = 1;        // subject + source
  repeated string scopes = 2;         // granular scopes at admission time
  bool god = 3;                       // god-mode flag
}

message CallerIdentity {
  string subject = 1;
  oneof source {
    string issuer = 4;                // OIDC token issuer URL
    string key_id = 5;                // Ed25519 signing key ID
    string system_component = 6;      // system/internal actor; subject empty
  }
}
```

It is built by `ResolveCallerSnapshot()` (`internal/adapter/auth/caller_snapshot.go`), in precedence order:

1. **System actor** — a background action marked with `WithSystemActor(ctx, component)` resolves to a snapshot whose source is `system_component` (e.g. `chapter-archiver`, `mirror`, `events-sink`, `cluster-config`, `idempotency-eviction`, `backup`). This is what keeps system-generated entries attributable instead of blank.
2. **Forwarded snapshot** — a request forwarded by a follower uses the snapshot the follower captured, verbatim.
3. **Local claims** — otherwise `buildCallerSnapshot()` reads the context (subject, source, scopes, god). Returns `nil` only for an unauthenticated user request.

So an audit entry attributes a write by, in order: the user subject when present, otherwise the source (Ed25519 key id / OIDC issuer) — an Ed25519 token minted without a `sub` claim is still attributable by its key id — otherwise the system component. `nil` means an unauthenticated user write.

The snapshot enters the audit-chain hash via `BuildHashedHeaderPayload` (see [audit-chain.md](../checker/audit-chain.md)). **It is not re-evaluated downstream** — the FSM, the checker, and any later observer see exactly what admission resolved. A token expiring between admission and FSM apply does not retroactively invalidate the proposal.

### Attribution observability

Admission emits signals when a user write is committed with weak attribution while auth is enabled (`admission.observeCallerSnapshot`):

- `admission.audit.missing_caller` (+ error log) — a user write resolved to a `nil` caller. Since writes without a valid token are already rejected at the scope check, this is anomalous (e.g. anonymous scopes misconfigured to allow writes).
- `admission.audit.caller_subject_empty` (+ info log) — the caller has a user source (key id / issuer) but an empty subject; the entry is still attributable by source.

System actions carry a `system_component` source and are exempt from both.

## OIDC discovery

`OIDCDiscoveryTimeout` (config `internal/bootstrap/config.go:33-39`, default 10 s) bounds OIDC discovery + JWKS fetches at boot:

- `oidc.Discover(ctx, issuer, DiscoveryEndpoint)` runs with `context.WithTimeout` (`module.go:1606`).
- The HTTP client used for discovery + JWKS is decorated with the same timeout (`TimeoutHTTPClient`, `module.go:1579-1584`).
- A value of `0` reverts to legacy unbounded behaviour.

This bound is what prevents a slow IdP from stalling node startup indefinitely.

## What is not (yet) here

- **Token cache.** Every request re-validates from scratch. The signature check is cheap (in-memory keyset); for OIDC tokens the JWKS is in memory after discovery, so there is no per-request network call.
- **Refresh tokens.** Tokens are stateless; clients refresh against the provider directly.
- **Service accounts** as a first-class concept. Ed25519 self-signed tokens with a `god: true` claim are the practical approximation for scripts and CI today.
- **Per-method audit logging of denials.** Authorization decisions are logged but not audit-chain-bound. Only successful proposals carry the `CallerSnapshot` into the chain — denied calls never reached admission and have no `AuditEntry`.

## Where to look in the code

| Concern | File |
|---------|------|
| JWT validation, scope enforcement | `internal/adapter/auth/grpc_auth.go` |
| HTTP auth middleware | `internal/adapter/auth/http_middleware.go` |
| Scope definitions and mapping | `internal/adapter/auth/scopes.go` |
| Ed25519 static keyset | `internal/adapter/auth/ed25519_keys.go` |
| Caller-snapshot resolution | `internal/adapter/auth/caller_snapshot.go` |
| System-actor snapshot + component names | `internal/pkg/commands/system_caller.go` |
| Attribution observability | `internal/application/admission/admission.go` (`observeCallerSnapshot`) |
| Raft cluster-secret interceptor | `internal/adapter/grpc/raft_auth.go` |
| OIDC discovery + composite keyset wiring | `internal/bootstrap/module.go` (around lines 1587-1652) |
