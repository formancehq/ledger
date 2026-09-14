# Ledger v2 plugin — auth, scopes, risks and evidence gaps

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

All 22 operations bound by this plugin declare a scope: 10 `ledger:read` and 12
`ledger:write`. Exact per-operation values are in `inventory.json` and
`mapping.md`.

The eight conversions add no new scope. Each converted command's V2 operation
declares the scope its V1 predecessor implied: `ledger:read` for
`ledger stats`, `ledger accounts show`, `ledger transactions list` and
`ledger transactions show`; `ledger:write` for `ledger send`,
`ledger transactions num`, `ledger accounts set-metadata` and
`ledger transactions set-metadata`.

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

`v2UpdateExporter` (`PUT /v2/_/exporters/{exporterID}`) is **not** in this set:
it declares `ledger:write`. The exporter group is therefore internally
inconsistent — four of its five operations declare nothing while the update
declares a write scope. That inconsistency is itself evidence that D2 is a
defect rather than a deliberate "public endpoint" design.

**Impact on this plugin:** none. All 11, and `v2UpdateExporter` with them, are
outside the 22-command denominator (`exclusions.md` §B). They must not be
adopted before D2 is resolved.

### D3 — `v2ExportLogs` is a read that requires `ledger:write`

`POST /v2/{ledger}/logs/export` declares `ledger:write` although exporting does
not mutate ledger state. A caller holding only `ledger:read` cannot export.
Recorded as a product decision, not a plugin bug: the plugin declares the scope
the operation actually requires. Do not "correct" it to `ledger:read`.

### D4 — Idempotency-Key coverage is asymmetric

Of the 12 bound write-scope operations:

| Has `Idempotency-Key` (7) | Lacks it (5) |
| --- | --- |
| `v2CreateTransaction`, `v2AddMetadataToAccount`, `v2AddMetadataOnTransaction`, `v2InsertSchema`, `v2DeleteAccountMetadata`, `v2DeleteTransactionMetadata`, `v2RevertTransaction` | `v2CreateLedger`, `v2UpdateLedgerMetadata`, `v2DeleteLedgerMetadata`, `v2ImportLogs`, `v2ExportLogs` |

`v2CreateLedger` and `v2ImportLogs` are the material cases: both are
non-idempotent by nature and both lack a retry-safety header. A transport retry
on either can double-apply.

**Consequence:** the v2 plugin must mark `ledger create` and `ledger import` as
non-retryable and must not let the host's advisory retryability turn a
timeout into a second send.

The conversions improve this picture rather than worsening it: `ledger send`,
`ledger transactions num` and both `set-metadata` commands gain an
`Idempotency-Key` their V1 predecessors had no equivalent for. The plugin must
actually send it — inheriting the header slot and leaving it empty would make
`ledger send` a non-idempotent transaction create.

### D5 — the v2 list operations require a JSON body on a GET

`v2ListTransactions` and `v2ListAccounts` are `GET` operations with
`requestBody.required: true`, whose schema is an untyped
`{type: object, additionalProperties: true}` — the filter query DSL. Two
consequences:

- Converting `ledger transactions list` cannot be a query-parameter remap. The
  V1 flat filters must be translated into `$and`/`$match` expressions in that
  body (`mapping.md`). old-fctl already does this for
  `ledger accounts list`, so the shape is evidenced, not invented.
- The filter vocabulary is **not** in the spec. It cannot be validated against
  `openapi/v2.yaml`, so a malformed filter surfaces only as a server error.

**Status:** recorded, not resolved. It bounds how faithfully the converted
filters can be checked before a live-server test.

### D6 — `expand` is declared with a malformed schema

`v2GetAccount` and `v2GetTransaction` declare
`expand: {in: query, schema: {type: string, items: {type: string}}}` — a
`string` carrying an `items` keyword, which is meaningless in OpenAPI 3. The
accepted values are documented nowhere in the spec.

This matters because `V2Account.volumes` and `V2Transaction.postCommitVolumes`
are optional and only returned when `expand` names them, while the historical
`ledger accounts show` and `ledger transactions show` both print volumes
unconditionally. The conversion therefore depends on an undocumented parameter
contract.

**Status:** recorded. The expand values must be confirmed against the server
before the two converted show commands are considered output-faithful.

## Risks

| # | Risk | Severity |
| --- | --- | --- |
| R1 | `ledger import` replays logs into an existing ledger, is destructive, and has no idempotency key. | high |
| R2 | Retry-on-timeout for `ledger create` can create a duplicate ledger. | medium |
| R3 | Baseline pin `693c58e2` is a historical CLI. Any behaviour reconstructed from it must be re-read from source, never from memory of the old CLI. | medium |
| R4 | The generated client ships its own auth and retry options. The adapter explicitly supplies an empty security source and configures no retries while injecting `producthttp.Client` through `WithClient`; enabling either would move auth ownership out of the host. | medium |
| R5 | 5 of 22 commands are cursor-paginated (6 bound operations with the secondary `v2ListLogs`). Reusing Ledger's own drain-all helper would import unbounded page draining, which the host contract bars. | medium |
| R6 | The 8 conversions change the wire call under an unchanged command phrase. A response-shape difference (D5, D6) is a silent output regression for users of the historical CLI, not a visible error. | high |
| R7 | `ledger send` and `ledger transactions num` bind the same `v2CreateTransaction`. Sharing one adapter path risks letting a `postings` body reach the script command or the reverse; the spec makes the two fields mutually exclusive. | medium |

## Open evidence gaps

| # | Blocker | Blocks |
| --- | --- | --- |
| B3 | D1 must be resolved before RFC 0014 can claim exact per-operation scopes are machine-derived for Ledger v2. Until then the scope table here is source-of-truth by manual extraction. | RFC 0014 closure |
| B4 | D5 and D6 leave the converted filter and `expand` contracts unvalidatable against the spec. A live-server check is required before the 8 conversions may be claimed output-faithful to the historical CLI. | parity claim for the 8 converted commands |

B4 blocks a *claim*, not execution: the operations, scopes and request
shapes are all verified from source. What is unverified is whether the converted
output matches byte-for-byte what old-fctl printed.

## Resolved prerequisites

- The public `producthttp` bridge and frozen SDK catalogue are available and
  consumed directly by the generated-client adapter.
- The portable descriptor and reconstructible lifecycle are implemented. A
  release still requires a deterministic component build and dual-host
  acceptance receipt.

## Explicitly not a blocker

The generated-client behaviour gate is **closed**: `WithClient`, `HTTPClient`, the
module path, the 21 distinct primary `V2.*` methods required by the 22 included
commands, and the secondary `v2ListLogs` call are verified present at `8cc679c9`
(`mapping.md`). Implementation is not paused on source or contract revision for
the v2 side.

The absence of a V2 equivalent is also not a blocker: every baseline command
except the host-owned `/_/info` probe has one, which is why the denominator is
22 of 23 rather than 14.
