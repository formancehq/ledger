# Ledger v2 plugin — mapping

**Plugin:** `ledger-v2` · **Product major:** 2 (only) · **Signing capability:** none

**Inventory basis:** old-fctl `cmd/ledger` at `693c58e27865f83332e6c3199d61fed81b742f41`
**Client basis:** Ledger `origin/main` at `8cc679c9440aaf90d4b8646a013e5bd7421443d6`,
module `github.com/formancehq/ledger/pkg/client`, spec `openapi/v2.yaml`

## Denominator

| Quantity | Count | Evidence |
| --- | --- | --- |
| Historical baseline executable commands | 23 | 23 non-grouping command files under `cmd/ledger` |
| Excluded as V1-only | 9 | sole SDK call is `V1.*` (see `exclusions.md`) |
| **Included coverage denominator** | **14** | remaining commands with a `V2.*` call |

## Client-behaviour gate — closed

Task 7 requires the refreshed `origin/main` generated client to still expose an
injectable HTTP client. Verified at the pinned commit:

- `pkg/client/formance.go:25` — `type HTTPClient interface { Do(*http.Request) (*http.Response, error) }`
- `pkg/client/formance.go:90` — `func WithClient(client HTTPClient) SDKOption`
- `pkg/client/go.mod` — `module github.com/formancehq/ledger/pkg/client`

All 15 `V2.*` methods required by the 14 included commands are present in
`pkg/client/v2.go`. No mismatch: implementation is not paused on this gate.

## Included commands (14)

`operationId` is the `openapi/v2.yaml` identifier; `sdk_method` is the generated
Go receiver method. The generated client renames `v2Xxx` to `V2.Xxx`.

| fctl command | operationId | SDK method | HTTP | Scope | Page | Idem-Key |
| --- | --- | --- | --- | --- | --- | --- |
| `ledger list` | `v2ListLedgers` | `V2.ListLedgers` | `GET /v2` | `ledger:read` | yes | no |
| `ledger create` | `v2CreateLedger` | `V2.CreateLedger` | `POST /v2/{ledger}` | `ledger:write` | – | **no** |
| `ledger set-metadata` | `v2UpdateLedgerMetadata` | `V2.UpdateLedgerMetadata` | `PUT /v2/{ledger}/metadata` | `ledger:write` | – | no |
| `ledger delete-metadata` | `v2DeleteLedgerMetadata` | `V2.DeleteLedgerMetadata` | `DELETE /v2/{ledger}/metadata/{key}` | `ledger:write` | – | no |
| `ledger export` | `v2ExportLogs` | `V2.ExportLogs` | `POST /v2/{ledger}/logs/export` | `ledger:write` | – | no |
| `ledger import` | `v2ImportLogs` | `V2.ImportLogs` | `POST /v2/{ledger}/logs/import` | `ledger:write` | – | no |
| `ledger accounts list` | `v2ListAccounts` | `V2.ListAccounts` | `GET /v2/{ledger}/accounts` | `ledger:read` | yes | no |
| `ledger accounts delete-metadata` | `v2DeleteAccountMetadata` | `V2.DeleteAccountMetadata` | `DELETE /v2/{ledger}/accounts/{address}/metadata/{key}` | `ledger:write` | – | yes |
| `ledger transactions revert` | `v2RevertTransaction` | `V2.RevertTransaction` | `POST /v2/{ledger}/transactions/{id}/revert` | `ledger:write` | – | yes |
| `ledger transactions delete-metadata` | `v2DeleteTransactionMetadata` | `V2.DeleteTransactionMetadata` | `DELETE /v2/{ledger}/transactions/{id}/metadata/{key}` | `ledger:write` | – | yes |
| `ledger volumes list` | `v2GetVolumesWithBalances` | `V2.GetVolumesWithBalances` | `GET /v2/{ledger}/volumes` | `ledger:read` | yes | no |
| `ledger schemas list` | `v2ListSchemas` | `V2.ListSchemas` | `GET /v2/{ledger}/schemas` | `ledger:read` | yes | no |
| `ledger schemas get` | `v2GetSchema` | `V2.GetSchema` | `GET /v2/{ledger}/schemas/{version}` | `ledger:read` | – | no |
| `ledger schemas insert` | `v2InsertSchema` | `V2.InsertSchema` | `POST /v2/{ledger}/schemas/{version}` | `ledger:write` | – | yes |

Secondary call: `ledger import` also invokes `V2.ListLogs`
(`GET /v2/{ledger}/logs`, `ledger:read`) to inspect existing logs before
importing. It is not a separate command.

`ledger transactions revert` is the one baseline command calling **both**
`V1.RevertTransaction` and `V2.RevertTransaction`. It is retained because a
faithful V2 path exists; the v2 plugin binds the V2 call only.

## Destructive operations (5)

`ledger delete-metadata`, `ledger accounts delete-metadata`,
`ledger transactions delete-metadata`, `ledger transactions revert`,
`ledger import`.

`ledger import` is classified destructive because it replays logs into an
existing ledger. It carries **no** `Idempotency-Key` header in the spec — see
`auth-scopes-risks.md`.

## Pagination

Five included reads are cursor-paginated (`cursor` + `pageSize` query
parameters): `ledger list`, `ledger accounts list`, `ledger volumes list`,
`ledger schemas list`, and the secondary `V2.ListLogs` call.

The plugin must surface pages through the host's opaque cursor envelope. It must
not drain pages itself — the programme plan forbids copying Ledger's unbounded
page draining into generic behaviour.

## Streaming

None. Every included v2 operation is a single request/response JSON exchange.
`export`/`import` transfer a body, not a stream.

## CLI grammar

Included commands keep their historical `fctl ledger …` phrases and aliases as
recorded in `inventory.json`. No renaming is proposed: the existing wording
already reads as intent rather than as an API-shaped CRUD tree.
