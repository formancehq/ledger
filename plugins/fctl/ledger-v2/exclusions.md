# Ledger v2 plugin — exclusions

Every exclusion below is a named, evidence-backed decision. None is a silent
omission. `23 = 14 included + 9 excluded`.

## A. V1-only baseline commands (9) — excluded from the v2 plugin

The programme decision is explicit: commands whose audited implementation is
exclusively `Ledger.V1` are historical evidence, not published by the v2
provider. Evidence is the sole SDK call in each old-fctl source file at
`693c58e2`.

| Command | Sole SDK call |
| --- | --- |
| `ledger send` | `V1.CreateTransaction` |
| `ledger server-infos` | `V1.GetInfo` |
| `ledger stats` | `V1.ReadStats` |
| `ledger accounts show` | `V1.GetAccountLedger` |
| `ledger accounts set-metadata` | `V1.AddMetadataToAccount` |
| `ledger transactions list` | `V1.ListTransactions` |
| `ledger transactions show` | `V1.GetTransaction` |
| `ledger transactions set-metadata` | `V1.AddMetadataOnTransaction` |
| `ledger transactions num` | `V1.CreateTransaction` |

`ledger server-infos` is doubly excluded: besides being V1-only, the `/_info`
probe is host-owned in fctl-v2 and must never be a product command.

### Open decision — not resolved here

The v2 API *does* offer equivalents for most of these
(`v2CreateTransaction`, `v2GetAccount`, `v2AddMetadataToAccount`,
`v2ListTransactions`, `v2GetTransaction`, `v2AddMetadataOnTransaction`,
`v2ReadStats`). Re-basing any of the nine onto its V2 operation would be a
**new** command, not old-fctl parity, and would change the frozen 14-command
denominator. That requires an explicit scope decision and is deliberately left
open.

## B. v2 API operations outside the denominator (30)

`openapi/v2.yaml` at the pinned `origin/main` declares **45** operations. The
14-command denominator binds **15** of them (14 primary + `v2ListLogs` as a
secondary call). The remaining 30 are outside this plugin's frozen scope
because no old-fctl command exercised them.

Recorded so the gap is visible, not to expand scope:

- **Transactions/accounts reads and writes with no baseline command:**
  `v2CreateTransaction`, `v2GetTransaction`, `v2ListTransactions`,
  `v2CountTransactions`, `v2AddMetadataOnTransaction`, `v2GetAccount`,
  `v2CountAccounts`, `v2AddMetadataToAccount`, `v2CreateBulk`
- **Ledger reads:** `v2GetLedger`, `v2GetLedgerInfo`, `v2ReadStats`,
  `v2GetBalancesAggregated`, `v2RunQuery`
- **Info/metrics:** `v2GetInfo`, `getMetrics` — host-owned or operator concerns
- **Bucket lifecycle:** `v2DeleteBucket`, `v2RestoreBucket` — operator concerns
- **Exporters (4):** `v2ListExporters`, `v2CreateExporter`,
  `v2GetExporterState`, `v2DeleteExporter`
- **Pipelines (7):** `v2ListPipelines`, `v2CreatePipeline`,
  `v2GetPipelineState`, `v2DeletePipeline`, `v2ResetPipeline`,
  `v2StartPipeline`, `v2StopPipeline`

The 11 exporter and pipeline operations additionally declare **no** `security:`
block in the spec — see `auth-scopes-risks.md`. They must not be adopted before
that is resolved.

## C. Capabilities the v2 plugin must never declare

| Excluded | Reason |
| --- | --- |
| `sign.ledger.apply-batch` | RFC 0009 binds it to product major 3 only. Ledger v2 cannot declare or consume it. The v2 catalogue and runtime must not be able to name it. |
| Response-signature verification | Not in RFC 0009 scope for either plugin. |
| Endpoint / auth / target resolution | Host-owned. |
| Direct server, TLS, raw-token, active-ledger fallback, self-update, product-consistency flags | Explicitly barred from generic fctl behaviour by the programme plan. |

## D. Not shared with `ledger-v3`

No catalogue entry, manifest field, command path, or operation identifier is
shared with the `ledger-v3` plugin. The two source series are different Go
modules on disjoint branches (see `../README.md`), so sharing is not merely
discouraged — it is not constructible.
