# `ledger-v2` plugin — preparation

One of **two separate** Ledger plugins. It shares no catalogue, manifest or
command surface with `../ledger-v3`. See `../README.md` for the boundary rule.

| | |
| --- | --- |
| Product major | **2**, and no other |
| Transport | HTTP through the generated Speakeasy client |
| Client | `github.com/formancehq/ledger/pkg/client` at `8cc679c9440aaf90d4b8646a013e5bd7421443d6` |
| Inventory basis | old-fctl `cmd/ledger` at `693c58e27865f83332e6c3199d61fed81b742f41` |
| Coverage denominator | **22** included of 23 baseline commands |
| Signing capability | **none** — `sign.ledger.apply-batch` is major-3 only |

## Surface rule

`ledger-v2` keeps the **historical user surface**. Where a baseline command was
implemented on `Ledger.V1`, it is converted to its V2 API equivalent rather
than dropped: 14 commands already spoke V2, 8 are converted, and only
`ledger server-infos` leaves the plugin because the `/_/info` probe is
host-owned.

Conversion keeps the command phrase, aliases and arguments and changes the wire
call. Each conversion's request-shaping obligation is recorded per command, so
"converted" never stands in for "assumed equivalent".

## Documents

| File | Contents |
| --- | --- |
| `inventory.json` | All 23 baseline commands with the baseline SDK call, the bound V2 operation, operationId, method, path, scope, pagination, idempotency, mutation, destructiveness and per-command conversion notes |
| `mapping.md` | The 22 included commands mapped to operationId, SDK method, route and scope; the 8 conversions with their evidence; client-behaviour gate evidence |
| `exclusions.md` | The single host-owned exclusion, the 23 out-of-scope v2 API operations, and barred capabilities |
| `auth-scopes-risks.md` | OAuth2 model, scope facts, 6 divergences, 7 risks, 4 blockers |
| `manifest.json` | Logical manifest and machine-checked invariants |

## Status

Source-first preparation. No component, install record or dual-host artifact is
declared: runtime gates 4B/4C/4D are not released.

The Task 7 client-behaviour gate is **closed** — `WithClient`, `HTTPClient`,
the module path and all 22 required `V2.*` methods are verified present at the
pin. Implementation is not paused on a source or contract revision for this
plugin.

One claim remains open: whether the 8 converted commands reproduce the
historical output exactly depends on the undocumented `expand` and filter-body
contracts (`auth-scopes-risks.md` D5, D6, B4). The operations, scopes and
request shapes themselves are verified from source.
