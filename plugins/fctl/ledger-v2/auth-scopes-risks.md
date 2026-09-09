# Ledger v2 plugin — auth, scopes, risks, blockers

## Authentication model (fact)

`openapi/v2.yaml` at `8cc679c9` declares exactly one security scheme:

```yaml
securitySchemes:
  Authorization:
    type: oauth2
    flows:
      clientCredentials:
        tokenUrl: "/oauth/token"
        refreshUrl: "/oauth/token"
        scopes: {}
```

OAuth2 client credentials. The plugin never sees client ID, client secret,
endpoint, access token, or `Authorization` bytes: it consumes a host-resolved
`auth.stack` capability and issues requests through the injected
`producthttp.Client`.

## Scopes (fact)

Ledger v2 uses a **flat two-scope** model, declared per operation:

| Scope | Operations declaring it (of 45) |
| --- | --- |
| `ledger:read` | 18 |
| `ledger:write` | 16 |
| *(none declared)* | 11 |

All 15 operations bound by this plugin declare a scope. Exact per-operation
values are in `inventory.json` and `mapping.md`.

## Divergences found (spec vs. spec, spec vs. server)

### D1 — `securitySchemes.scopes` is empty while operations require scopes

Operations declare `- Authorization: [ledger:read]` / `[ledger:write]`, but the
scheme's `scopes` map is `{}`. Per OpenAPI 3, scopes referenced in a security
requirement must be declared in the scheme. Consequences:

- Any tool deriving the scope vocabulary from `securitySchemes` gets an empty
  set and silently concludes "no scopes required".
- RFC 0014 ("exact per-operation authorization scopes") must read
  per-operation `security:` blocks, **not** the scheme, for Ledger v2.

**Status:** confirmed defect in the pinned spec. Product-side fix is out of
scope for this preparation. The recorded scopes in `inventory.json` come from
the per-operation blocks and are authoritative for the plugin.

### D2 — 11 operations declare no security at all

`v2ListExporters`, `v2CreateExporter`, `v2GetExporterState`,
`v2DeleteExporter`, `v2ListPipelines`, `v2CreatePipeline`,
`v2GetPipelineState`, `v2DeletePipeline`, `v2ResetPipeline`,
`v2StartPipeline`, `v2StopPipeline`.

Four of these are destructive or state-changing (`DELETE`, `reset`, `stop`).
Whether the server enforces a scope the spec does not declare is **unverified
here** — it needs a live-server check, which this source-first preparation does
not perform.

**Impact on this plugin:** none. All 11 are outside the 14-command denominator
(`exclusions.md` §B). They must not be adopted before D2 is resolved.

### D3 — `v2ExportLogs` is a read that requires `ledger:write`

`POST /v2/{ledger}/logs/export` declares `ledger:write` although exporting does
not mutate ledger state. A caller holding only `ledger:read` cannot export.
Recorded as a product decision, not a plugin bug: the plugin declares the scope
the operation actually requires. Do not "correct" it to `ledger:read`.

### D4 — Idempotency-Key coverage is asymmetric

Of the included mutations:

| Has `Idempotency-Key` | Lacks it |
| --- | --- |
| `v2InsertSchema`, `v2DeleteAccountMetadata`, `v2DeleteTransactionMetadata`, `v2RevertTransaction` | `v2CreateLedger`, `v2UpdateLedgerMetadata`, `v2DeleteLedgerMetadata`, `v2ImportLogs`, `v2ExportLogs` |

`v2CreateLedger` and `v2ImportLogs` are the material cases: both are
non-idempotent by nature and both lack a retry-safety header. A transport retry
on either can double-apply.

**Consequence:** the v2 plugin must mark `ledger create` and `ledger import` as
non-retryable and must not let the host's advisory retryability turn a
timeout into a second send.

## Risks

| # | Risk | Severity |
| --- | --- | --- |
| R1 | `ledger import` replays logs into an existing ledger, is destructive, and has no idempotency key. | high |
| R2 | Retry-on-timeout for `ledger create` can create a duplicate ledger. | medium |
| R3 | Baseline pin `693c58e2` is a historical CLI. Any behaviour reconstructed from it must be re-read from source, never from memory of the old CLI. | medium |
| R4 | The generated client ships its own auth and retry options. Both must be left disabled while injecting `producthttp.Client` through `WithClient`; enabling either would move auth ownership out of the host. | medium |
| R5 | 5 of 14 commands are cursor-paginated. Reusing Ledger's own drain-all helper would import unbounded page draining, which the programme plan bars. | medium |

## Blockers

| # | Blocker | Blocks |
| --- | --- | --- |
| B1 | Runtime gates 4B/4C/4D are not released. No portable component, install record, or dual-host artifact may be produced. | all v2 implementation |
| B2 | The `producthttp` bridge and frozen SDK catalogue are Lane A/Core deliverables consumed by this plugin; they are not re-specified here. | v2 adapter implementation |
| B3 | D1 must be resolved before RFC 0014 can claim exact per-operation scopes are machine-derived for Ledger v2. Until then the scope table here is source-of-truth by manual extraction. | RFC 0014 closure |

## Explicitly not a blocker

The Task 7 client-behaviour gate is **closed**: `WithClient`, `HTTPClient`, the
module path, and all 15 `V2.*` methods are verified present at `8cc679c9`
(`mapping.md`). Implementation is not paused on source or contract revision for
the v2 side.
