# `ledger-v2` plugin — preparation

One of **two separate** Ledger plugins. It shares no catalogue, manifest or
command surface with `../ledger-v3`. See `../README.md` for the boundary rule.

| | |
| --- | --- |
| Product major | **2**, and no other |
| Transport | HTTP through the generated Speakeasy client |
| Client | `github.com/formancehq/ledger/pkg/client` at `8cc679c9440aaf90d4b8646a013e5bd7421443d6` |
| Inventory basis | old-fctl `cmd/ledger` at `693c58e27865f83332e6c3199d61fed81b742f41` |
| Coverage denominator | **14** included of 23 baseline commands |
| Signing capability | **none** — `sign.ledger.apply-batch` is major-3 only |

## Documents

| File | Contents |
| --- | --- |
| `inventory.json` | All 23 baseline commands with V1/V2 backing, operationId, method, path, scope, pagination, idempotency, mutation and destructiveness |
| `mapping.md` | The 14 included commands mapped to operationId, SDK method, route and scope; client-behaviour gate evidence |
| `exclusions.md` | The 9 V1-only exclusions, the 30 out-of-scope v2 API operations, and barred capabilities |
| `auth-scopes-risks.md` | OAuth2 model, scope facts, 4 divergences, 5 risks, 3 blockers |
| `manifest.json` | Logical manifest and machine-checked invariants |

## Status

Source-first preparation. No component, install record or dual-host artifact is
declared: runtime gates 4B/4C/4D are not released.

The Task 7 client-behaviour gate is **closed** — `WithClient`, `HTTPClient`,
the module path and all 15 required `V2.*` methods are verified present at the
pin. Implementation is not paused on a source or contract revision for this
plugin.
