# Ledger v2 plugin — mapping

**Plugin:** `ledger-v2` · **Product major:** 2 (only) · **Signing capability:** none

**Inventory basis:** old-fctl `cmd/ledger` at `693c58e27865f83332e6c3199d61fed81b742f41`
**Client basis:** Ledger `origin/main` at `8cc679c9440aaf90d4b8646a013e5bd7421443d6`,
module `github.com/formancehq/ledger/pkg/client`, spec `openapi/v2.yaml`

## Denominator

`ledger-v2` keeps the historical user surface. A baseline command implemented on
`Ledger.V1` is **converted** to its V2 API equivalent, not dropped — dropping it
would silently remove a published command from the fctl surface. Only the
host-owned `/_/info` probe leaves the plugin.

| Quantity | Count | Evidence |
| --- | --- | --- |
| Historical baseline executable commands | 23 | 23 non-grouping command files under `cmd/ledger` |
| Excluded as host-owned | 1 | `ledger server-infos` — the `/_/info` probe is host-owned (see `exclusions.md`) |
| **Included coverage denominator** | **22** | every other baseline command, each bound to a V2 operation |

Of the 22, **14** already called `Ledger.V2` at the baseline and **8** are
converted from a `Ledger.V1` call. The historical V1-only set was **9**
commands: 8 converted plus `ledger server-infos`.

The 22 commands bind **21** distinct `operationId`s — `ledger send` and
`ledger transactions num` are the same `v2CreateTransaction` operation
distinguished only by their request body. With the secondary `v2ListLogs` call
that is **22 of the 45** operations `openapi/v2.yaml` declares at the pin; the
remaining **23** are enumerated in `exclusions.md` §B.

## Client behaviour

The pinned generated client exposes the injectable HTTP client required by the
host-owned transport:

- `pkg/client/formance.go:25` — `type HTTPClient interface { Do(*http.Request) (*http.Response, error) }`
- `pkg/client/formance.go:90` — `func WithClient(client HTTPClient) SDKOption`
- `pkg/client/go.mod` — `module github.com/formancehq/ledger/pkg/client`

All required `V2.*` methods are present in `pkg/client/v2.go`: 21 distinct
primary methods for the 22 included commands, plus the secondary `v2ListLogs`
resume probe. No client-surface mismatch remains.

The adapter passes `producthttp.Client` through `WithClient`, supplies an empty
security source so ambient generated-client credentials cannot be used, and
does not configure generated retries. DTO construction and response decoding
remain generated-client owned.

Generator erratum: the pinned Speakeasy output serializes nil GET request
bodies as byte-exact JSON `null`; `V2ListLedgers` also injects its
`includeDeleted=false` default when a continuation cursor is present, although
the API requires cursor-only continuation. The adapter's bounded transport shim
removes only byte-exact `null` on GET continuation. Required first-page filter
operations always send an explicit JSON body, including `{"$and":[]}` when no
filter is selected. When and only when a non-empty cursor is present, the shim
replaces the query with the canonically encoded cursor alone. Empty cursors,
non-`null` bodies and non-continuation queries are unchanged. Exact contract
tests guard every exception.

## Included commands (22)

`operationId` is the `openapi/v2.yaml` identifier; `sdk_method` is the generated
Go receiver method. The generated client renames `v2Xxx` to `V2.Xxx`.

| fctl command | operationId | SDK method | HTTP | Scope | Page | Idem-Key |
| --- | --- | --- | --- | --- | --- | --- |
| `ledger list` | `v2ListLedgers` | `V2.ListLedgers` | `GET /v2` | `ledger:read` | yes | no |
| `ledger create` | `v2CreateLedger` | `V2.CreateLedger` | `POST /v2/{ledger}` | `ledger:write` | – | **no** |
| `ledger set-metadata` | `v2UpdateLedgerMetadata` | `V2.UpdateLedgerMetadata` | `PUT /v2/{ledger}/metadata` | `ledger:write` | – | no |
| `ledger delete-metadata` | `v2DeleteLedgerMetadata` | `V2.DeleteLedgerMetadata` | `DELETE /v2/{ledger}/metadata/{key}` | `ledger:write` | – | no |
| `ledger export` | `v2ExportLogs` | `V2.ExportLogs` | `POST /v2/{ledger}/logs/export` | `ledger:write` | – | no |
| `ledger import` | `v2ImportLogs` | `V2.ImportLogs` | `POST /v2/{ledger}/logs/import` | `ledger:write` | – | **no** |
| `ledger stats` † | `v2ReadStats` | `V2.ReadStats` | `GET /v2/{ledger}/stats` | `ledger:read` | – | no |
| `ledger send` † | `v2CreateTransaction` | `V2.CreateTransaction` | `POST /v2/{ledger}/transactions` | `ledger:write` | – | yes |
| `ledger accounts list` | `v2ListAccounts` | `V2.ListAccounts` | `GET /v2/{ledger}/accounts` | `ledger:read` | yes | no |
| `ledger accounts show` † | `v2GetAccount` | `V2.GetAccount` | `GET /v2/{ledger}/accounts/{address}` | `ledger:read` | – | no |
| `ledger accounts set-metadata` † | `v2AddMetadataToAccount` | `V2.AddMetadataToAccount` | `POST /v2/{ledger}/accounts/{address}/metadata` | `ledger:write` | – | yes |
| `ledger accounts delete-metadata` | `v2DeleteAccountMetadata` | `V2.DeleteAccountMetadata` | `DELETE /v2/{ledger}/accounts/{address}/metadata/{key}` | `ledger:write` | – | yes |
| `ledger transactions list` † | `v2ListTransactions` | `V2.ListTransactions` | `GET /v2/{ledger}/transactions` | `ledger:read` | yes | no |
| `ledger transactions show` † | `v2GetTransaction` | `V2.GetTransaction` | `GET /v2/{ledger}/transactions/{id}` | `ledger:read` | – | no |
| `ledger transactions num` † | `v2CreateTransaction` | `V2.CreateTransaction` | `POST /v2/{ledger}/transactions` | `ledger:write` | – | yes |
| `ledger transactions set-metadata` † | `v2AddMetadataOnTransaction` | `V2.AddMetadataOnTransaction` | `POST /v2/{ledger}/transactions/{id}/metadata` | `ledger:write` | – | yes |
| `ledger transactions delete-metadata` | `v2DeleteTransactionMetadata` | `V2.DeleteTransactionMetadata` | `DELETE /v2/{ledger}/transactions/{id}/metadata/{key}` | `ledger:write` | – | yes |
| `ledger transactions revert` | `v2RevertTransaction` | `V2.RevertTransaction` | `POST /v2/{ledger}/transactions/{id}/revert` | `ledger:write` | – | yes |
| `ledger volumes list` | `v2GetVolumesWithBalances` | `V2.GetVolumesWithBalances` | `GET /v2/{ledger}/volumes` | `ledger:read` | yes | no |
| `ledger schemas list` | `v2ListSchemas` | `V2.ListSchemas` | `GET /v2/{ledger}/schemas` | `ledger:read` | yes | no |
| `ledger schemas get` | `v2GetSchema` | `V2.GetSchema` | `GET /v2/{ledger}/schemas/{version}` | `ledger:read` | – | no |
| `ledger schemas insert` | `v2InsertSchema` | `V2.InsertSchema` | `POST /v2/{ledger}/schemas/{version}` | `ledger:write` | – | yes |

† converted from a `Ledger.V1` call at the baseline — see the next section.

Secondary call: `ledger import` also invokes `V2.ListLogs`
(`GET /v2/{ledger}/logs`, `ledger:read`) to inspect existing logs before
importing. It is not a separate command.

`ledger transactions revert` is the one baseline command calling **both**
`V1.RevertTransaction` and `V2.RevertTransaction`. The v2 plugin binds the V2
call only, which is why it is not counted as a conversion.

## Conversions (8)

Each row's baseline call is the sole `Ledger.V1.*` call in the old-fctl source
file at `693c58e2`; each V2 operation is the one `openapi/v2.yaml` declares at
`8cc679c9`. `inventory.json` records the same evidence per command in
`baseline_sdk_method` and `conversion_note`.

| fctl command | Baseline call | V2 operation | Request-shaping obligation |
| --- | --- | --- | --- |
| `ledger transactions list` | `V1.ListTransactions` | `v2ListTransactions` | The V1 flat filters (`account`, `source`, `destination`, `reference`, `start`, `end`, `metadata`) have **no** v2 query-parameter equivalent. `v2ListTransactions` takes a **required** JSON filter body, so they must be translated into its `$and`/`$match` form. |
| `ledger transactions show` | `V1.GetTransaction` | `v2GetTransaction` | `txid` becomes the `id` path parameter. The historical renderer prints pre- and post-commit volumes, which `V2Transaction` returns only when `expand` names them. |
| `ledger accounts show` | `V1.GetAccountLedger` | `v2GetAccount` | `V1.GetAccountLedger` returned volumes unconditionally. `V2Account.volumes` is optional, so the conversion must send `expand=volumes` to keep the historical volumes table. |
| `ledger send` | `V1.CreateTransaction` | `v2CreateTransaction` | `PostTransaction.postings` maps to `V2PostTransaction.postings`; `reference` and `metadata` carry over. The historical optional leading source is exposed as `--source` because portable command grammar cannot place an optional positional before required ones. |
| `ledger transactions num` | `V1.CreateTransaction` | `v2CreateTransaction` | `PostTransaction.script{plain,vars}` maps to `V2PostTransaction.script{plain,vars}`; `timestamp` carries over. |
| `ledger accounts set-metadata` | `V1.AddMetadataToAccount` | `v2AddMetadataToAccount` | Same address parameter and metadata map body; the v2 operation adds `dryRun` and an `Idempotency-Key` header. |
| `ledger transactions set-metadata` | `V1.AddMetadataOnTransaction` | `v2AddMetadataOnTransaction` | Same metadata map body, with `txid` becoming `id`; the v2 operation adds `dryRun` and an `Idempotency-Key` header. |
| `ledger stats` | `V1.ReadStats` | `v2ReadStats` | `StatsResponse.data` maps to `V2StatsResponse.data` with the same `accounts` and `transactions` fields. `transactions` widens from `int64` to `bigint`, so the renderer must not narrow it. |

The filter translation is not speculative: old-fctl already does it at the
baseline. `cmd/ledger/accounts/list.go` builds a `{"$and": [{"$match": {…}}]}`
body for `V2.ListAccounts` from its `--metadata` flags. The same shape applies
to `v2ListTransactions`.

`v2CreateTransaction` accepts `postings` and `script` as **mutually exclusive**
body fields, which is what lets `ledger send` and `ledger transactions num`
remain two distinct commands over one operation.

`ledger schemas insert` consumes one host-owned, 64 KiB
`application/octet-stream` artifact. That media type is intentionally
format-neutral: the adapter performs bounded JSON/YAML detection after the host
has resolved the file, so accepting both formats does not add a required
`--source-media-type` selector and never turns the plugin into a URL fetcher.

## Destructive operations (5)

`ledger delete-metadata`, `ledger accounts delete-metadata`,
`ledger transactions delete-metadata`, `ledger transactions revert`,
`ledger import`.

The conversions add no destructive command: they are 4 reads and 4
non-destructive mutations.

`ledger import` is classified destructive because it replays logs into an
existing ledger. It carries **no** `Idempotency-Key` header in the spec — see
`auth-scopes-risks.md`.

## Mutations (12 commands, 11 operations)

`ledger send` and `ledger transactions num` share `v2CreateTransaction`.
`ledger export` is **not** a mutation despite requiring `ledger:write` — see
divergence D3.

## Pagination

Six bound reads are cursor-paginated (`cursor` + `pageSize` query parameters):
`ledger list`, `ledger accounts list`, `ledger transactions list`,
`ledger volumes list`, `ledger schemas list`, and the secondary `V2.ListLogs`
call.

The plugin surfaces one page by default. When the host selects bounded `--all`,
it follows the opaque cursor under the host-provided page, item and byte
ceilings; every request after the first contains only `cursor`. It does not use
Ledger's unbounded page-draining helper.

## Streaming

None. Every included v2 operation is a single request/response JSON exchange.
`export`/`import` transfer a body, not a stream.

## CLI grammar

Included commands keep their historical `fctl ledger …` phrases and aliases as
recorded in `inventory.json`. No command is renamed. `ledger send` alone adapts
its optional leading source positional to `--source`, because the portable
catalogue forbids an optional positional before required positionals.
