# Ledger v2 plugin — exclusions

Every exclusion below is a named, evidence-backed decision. None is a silent
omission. `23 = 22 included + 1 excluded`.

## A. The single excluded baseline command (1)

| Command | Baseline call | V2 equivalent | Reason |
| --- | --- | --- | --- |
| `ledger server-infos` | `V1.GetInfo` | `v2GetInfo` — `GET /_/info`, `ledger:read`, exposed as `Ledger.GetInfo` in the generated client | The `/_/info` probe is **host-owned** in fctl-v2 and must never be a product command. |

The exclusion is about ownership, not availability: a faithful V2 equivalent
exists and is recorded in `inventory.json` so the decision cannot later be
mistaken for a coverage gap.

Note the receiver. The generated client puts `v2GetInfo` on `Ledger`, not on
`V2` (`pkg/client/ledger.go:40`), so it is the one bound-adjacent operation that
is not a `V2.*` method.

## The 9 historical V1-only commands are converted, not dropped

At the baseline pin, nine commands were implemented exclusively on `Ledger.V1`.
The programme decision is to **keep the historical user surface** by binding
each to its V2 API equivalent. Eight are converted and published; the ninth is
`ledger server-infos` above.

| Command | Baseline call | Status |
| --- | --- | --- |
| `ledger send` | `V1.CreateTransaction` | converted → `v2CreateTransaction` |
| `ledger stats` | `V1.ReadStats` | converted → `v2ReadStats` |
| `ledger accounts show` | `V1.GetAccountLedger` | converted → `v2GetAccount` |
| `ledger accounts set-metadata` | `V1.AddMetadataToAccount` | converted → `v2AddMetadataToAccount` |
| `ledger transactions list` | `V1.ListTransactions` | converted → `v2ListTransactions` |
| `ledger transactions show` | `V1.GetTransaction` | converted → `v2GetTransaction` |
| `ledger transactions set-metadata` | `V1.AddMetadataOnTransaction` | converted → `v2AddMetadataOnTransaction` |
| `ledger transactions num` | `V1.CreateTransaction` | converted → `v2CreateTransaction` |
| `ledger server-infos` | `V1.GetInfo` | **excluded** — host-owned probe |

Conversion binds the V2 operation and keeps the command phrase, aliases and
arguments. It is not a new command: the per-command request-shaping obligations
are recorded in `mapping.md` and in each row's `conversion_note`. What the
plugin must never do is publish a command that still binds a `V1.*` call — the
audit enforces that.

## B. v2 API operations outside the denominator (23)

`openapi/v2.yaml` at the pinned `origin/main` declares **45** operations. The
22-command denominator binds **22** of them (21 distinct primary operations —
`ledger send` and `ledger transactions num` share `v2CreateTransaction` — plus
`v2ListLogs` as a secondary call). The remaining 23 are outside this plugin's
scope because no baseline command exercised them.

Recorded so the gap is visible, not to expand scope. The list is exhaustive:
`22 + 23 = 45`.

- **Transactions/accounts operations with no baseline command (3):**
  `v2CountTransactions`, `v2CountAccounts`, `v2CreateBulk`
- **Ledger reads (4):** `v2GetLedger`, `v2GetLedgerInfo`,
  `v2GetBalancesAggregated`, `v2RunQuery`
- **Info/metrics (2):** `v2GetInfo`, `getMetrics` — host-owned or operator
  concerns. `v2GetInfo` is the operation behind the excluded
  `ledger server-infos` in §A.
- **Bucket lifecycle (2):** `v2DeleteBucket`, `v2RestoreBucket` — operator
  concerns
- **Exporters (5):** `v2ListExporters`, `v2CreateExporter`, `v2UpdateExporter`,
  `v2GetExporterState`, `v2DeleteExporter`
- **Pipelines (7):** `v2ListPipelines`, `v2CreatePipeline`,
  `v2GetPipelineState`, `v2DeletePipeline`, `v2ResetPipeline`,
  `v2StartPipeline`, `v2StopPipeline`

Of these, **11** declare **no** `security:` block in the spec: the four
exporter operations other than `v2UpdateExporter`, plus all seven pipeline
operations. `v2UpdateExporter` does declare `ledger:write`. See
`auth-scopes-risks.md` D2. None of the 11 may be adopted before that is
resolved.

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
