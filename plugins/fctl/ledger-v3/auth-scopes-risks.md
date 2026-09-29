# Ledger v3 plugin — auth, scopes, risks, blockers

## Authentication model (fact)

Ledger v3 authenticates gRPC callers by bearer JWT, or by Ed25519 signing key
identity, or as a trusted cluster peer. `CallerIdentity`
(`misc/proto/common.proto:1726`) carries exactly one source:

```protobuf
oneof source {
  string issuer = 2;             // OIDC token issuer URL
  string key_id = 3;             // Ed25519 signing key ID
  string system_component = 4;   // system/internal actor
}
```

The plugin sees none of this. Credentials, transport and target resolution stay
host-owned; the plugin issues typed requests through `productgrpc`.

`CallerSnapshot` (`common.proto:1743`) records `identity`, `scopes` and `god`
at admission time, **for audit only** — the proto states scopes must not be
re-evaluated downstream.

## Scope model (fact) — two levels, and it is configurable

This is the single most important auth difference from Ledger v2.

**Level 1 — virtual scopes** carried in JWTs: `ledger:read`, `ledger:write`,
`ledger:admin`. Note `ledger:admin` has no v2 equivalent.

**Level 2 — 14 granular scopes** actually enforced
(`internal/adapter/auth/scopes.go:24`):

`ledger:LedgerRead`, `ledger:LedgerWrite`, `ledger:TransactionRead`,
`ledger:TransactionWrite`, `ledger:AccountRead`, `ledger:MetadataWrite`,
`ledger:AuditRead`, `ledger:AuditWrite`, `ledger:OpsRead`, `ledger:OpsWrite`,
`ledger:QueryRead`, `ledger:QueryWrite`, `ledger:ClusterRead`,
`ledger:ClusterWrite`.

The default mapping (`scopes.go:66`, `DefaultMapping`) is:

| Virtual | Expands to granular |
| --- | --- |
| `ledger:read` | `LedgerRead`, `TransactionRead`, `AccountRead`, `AuditRead`, `OpsRead`, `QueryRead` |
| `ledger:write` | `LedgerWrite`, `TransactionWrite`, `MetadataWrite`, `AuditWrite`, `OpsWrite`, `QueryWrite` |
| `ledger:admin` | `ClusterRead`, `ClusterWrite` |

Per-request granular requirements come from
`internal/adapter/auth/request_scope.go`, which decides per `Request` oneof
variant — for example `CreateLedger`/`DeleteLedger`/`PromoteLedger`/
`CreateIndex`/`DropIndex`/`SaveNumscript` → `ledger:LedgerWrite`;
`SetMetadataFieldType` → `ledger:MetadataWrite`; signing and event-sink
variants → `ledger:OpsWrite`.

That file fails closed: an unclassified variant returns `ledger:OpsWrite`, the
most restrictive write scope, and `TestRequiredScopeForRequest_ProtoExhaustive`
fails CI until a new oneof field gets an explicit decision. Good product
hygiene, and it means the source is a trustworthy scope oracle **for the
default mapping only**.

### Blocker B1 — effective scopes are server-configuration-dependent

`DefaultMapping` is a default, not a contract:

- `internal/adapter/auth/scope_mapping_file.go` loads an operator-supplied
  mapping, e.g. `{"ledger:read": ["ledger:LedgerRead", …]}`.
- `ScopeMappingAnonymousKey = "anonymous"` grants granular scopes to
  **unauthenticated** requests when present.
- Wildcards `*:read` and `*:write` are accepted inside mapping value lists.
- `ExpandScopes` also passes a granular scope through by identity if a token
  presents one directly.

So the virtual scope required to run a given v3 command cannot be derived
statically from source: it depends on the deployment's mapping file.

**Consequence for RFC 0014** ("exact per-operation authorization scopes"): for
Ledger v3, fctl can declare the exact *granular* scope per operation — that is
source-derivable and recorded — but it **cannot** statically declare the exact
*virtual* JWT scope. Any descriptor claiming `ledger:write` as the exact
required scope is only correct under `DefaultMapping`.

This is a blocker on RFC 0014 closure for v3, not on the inventory. It needs an
explicit decision: declare granular scopes, or declare virtual scopes with a
stated `DefaultMapping` assumption.

## Divergences found

### D1 — v2 and v3 scope vocabularies are not the same

Ledger v2 declares flat `ledger:read` / `ledger:write` per operation in
OpenAPI. Ledger v3 enforces 14 granular scopes behind three virtual ones and
adds `ledger:admin`. A shared scope table across the two plugins would be
wrong. This is independent structural support for keeping the plugins separate.

### D2 — gRPC and HTTP scope groups had to be reconciled by hand in the product

`request_scope.go` comments record that `CreateIndex`/`DropIndex` must return
`ledger:LedgerWrite` because the HTTP routes
`POST/DELETE /v3/{ledgerName}/indexes[/{canonicalId}]` sit in the
`ledger:LedgerWrite` group — otherwise "the same operation demands
ledger:OpsWrite over gRPC (default fallthrough) and ledger:LedgerWrite over
HTTP". `SaveNumscript` carries the same note referencing `handler.go:176`.

So the pinned v3 tree exposes **both** a gRPC service and a v3 HTTP API, and
their scope groups are aligned only by deliberate maintenance. The plugin binds
gRPC only. Do not infer HTTP-route scopes from it, and do not assume the two
stay aligned.

### D3 — `SignedApplyBatch` is in the `signature` proto package

Recorded in `mapping.md`. Its full name is `signature.SignedApplyBatch`, not
`ledger.SignedApplyBatch`. Only `ApplyBatch` is `ledger.ApplyBatch`.

### D4 — interactive prompting in the source CLI

`ledgerctl accounts list` prompts the user to select a ledger when `--ledger`
is omitted and several exist. fctl must be non-interactive with machine-readable
stdout. The selection rule is open decision O3 in `exclusions.md`.

## Risks

| # | Risk | Severity |
| --- | --- | --- |
| R1 | The v3 generated stubs are **internal** (`…/v3/internal/proto/servicepb`). Reaching them constrains the plugin's module path — see B2. | high |
| R2 | `signing require` can make signatures mandatory server-side, locking out every unsigned client including fctl profiles with no activated signer. Deferred in bucket C, but the command exists at the pin. | high |
| R3 | `cmdutil.DrainAllPages` drains every page and is reached indirectly by `account-types get`/`list` via `GetAllLedgersInfo`. Reusing it would import unbounded page draining, explicitly barred. | medium |
| R4 | 12 of 39 RPCs are server-streaming and paginate through the `x-next-cursor` **trailer**, available only after clean EOF. A partial read yields no cursor, so a naive early break silently loses pagination. | medium |
| R5 | `ApplyRequest.forwarded_caller_snapshot` is honoured only for trusted cluster peers and ignored for direct clients. The plugin must never set it; doing so would look like a privilege-escalation attempt. | medium |
| R6 | `ApplyRequest.skip_response` strips log payloads and returns only sequence numbers. Using it to reduce payload size would break the host's canonical structured result. | low |
| R7 | The pin `bb0297cc` is on `release/v3.0` and is **not** an ancestor of `origin/main`. It will not pick up main's fixes, and `release/v3.0` has advanced to `b9d8ccbb`. The pin is a source pin, never a product version, and never replaces the live `/_info` major. | medium |

## Blockers

| # | Blocker | Blocks |
| --- | --- | --- |
| B1 | Effective virtual scopes are server-configuration-dependent (above). | RFC 0014 closure for v3 |
| B2 | **Module-path constraint.** The v3 plugin can only reach the generated stubs if its own module path is under `github.com/formancehq/ledger/v3/`. Verified empirically at the pin: a module declared `github.com/formancehq/ledger/v3/plugins/fctl` compiles typed references to `servicepb.ApplyBatch`, `signaturepb.SignedApplyBatch` and `servicepb.BucketServiceClient`; the identical code in a module declared `github.com/formancehq/fctl-ledger-v3-plugin` fails with `use of internal package github.com/formancehq/ledger/v3/internal/proto/servicepb not allowed`. Task 7 requires that a module-path mismatch pauses implementation for an explicit source or contract revision. Either the plugin adopts that exact module path, or Ledger promotes the v3 stubs to a public package. | v3 adapter implementation |
| B3 | The v3 plugin's source branch (`release/v3.0`) is not the branch this preparation is committed on (`origin/main`). The two series are different modules; v3 *implementation* cannot land here. | v3 code landing |
| B4 | Runtime gates 4B/4C/4D are not released. No portable component, install record, or dual-host artifact may be produced. | all v3 implementation |
| B5 | Bucket C (signing/event-sink) and open decisions O1–O5 must be settled before the v3 catalogue is frozen. | catalogue freeze |

## Explicitly closed

The RFC 0009 bindings are **verified verbatim** at the pin: `ledger.ApplyBatch`,
`/ledger.BucketService/Apply`, the unsigned/signed `oneof`, and
`idempotency_key` inside the signed `ApplyBatch` bytes. Signed execution may be
claimed against these bindings once the runtime gates open.
