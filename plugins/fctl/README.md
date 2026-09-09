# fctl plugin preparation — Ledger

Source-first preparation for the fctl-v2 programme, Task 7 ("Deliver separate
Ledger v2 and Ledger v3 plugins"). This directory contains **no runtime
component, no WASM artifact, no install record and no Proto binding**. It is an
audited inventory and mapping that a later, gate-released task turns into code.

## Governing decision (2026-09-10)

Ledger ships **two separate plugins**: `ledger-v2` and `ledger-v3`.

This is a decision, not an optimisation. The preparation below keeps them
apart at every level:

| Boundary | Rule |
| --- | --- |
| Catalogue | Separate. Never merged, never unioned. |
| Manifest | Separate `manifest.json` per plugin. |
| Command surface | Separate. A command belongs to exactly one plugin. |
| Product major | `ledger-v2` declares major 2 only; `ledger-v3` declares major 3 only. |
| Signing capability | `ledger-v3` only. `ledger-v2` must not name `sign.ledger.apply-batch`. |
| Shared code | Only `internal/audit`, an internal audit tool. It carries no contract and is not packaged into either plugin. |

Both plugins still contribute to the single public `fctl ledger` experience.
Two plugins does not mean two CLIs.

### Why the split is structural, not stylistic

Verified at the pins below: the two source series are disjoint Go modules and
neither contains the other's contract.

| | `ledger-v2` source | `ledger-v3` source |
| --- | --- | --- |
| Branch | `origin/main` | `release/v3.0` |
| Module path | `github.com/formancehq/ledger` | `github.com/formancehq/ledger/v3` |
| Transport | HTTP + generated Speakeasy `pkg/client` | gRPC `BucketService` |
| `cmd/ledgerctl` | absent | present |
| `internal/proto`, `misc/proto` | absent | present |
| `pkg/client` | present | absent |

A single plugin cannot span both: it would need two module paths at once.

## Pinned revisions

| Input | Revision | Evidence |
| --- | --- | --- |
| Ledger `origin/main` (v2 source) | `8cc679c9440aaf90d4b8646a013e5bd7421443d6` (2026-09-06) | refreshed 2026-09-10 |
| Ledger `release/v3.0` pin (v3 source) | `bb0297cce39ccd1eb45eba695e9aa10374d5865d` (2026-08-13) | ancestor of `origin/release/v3.0` = `b9d8ccbbb5da4f9158b0f47b81612a518e4fed77`; **not** an ancestor of `origin/main` |
| Historical fctl baseline | `693c58e27865f83332e6c3199d61fed81b742f41` (2026-06-23) | old-fctl Ledger command tree |
| fctl-v2 host contract | `8de8c45` | `docs/superpowers/plans/2026-08-26-fctl-complete-program.md` |

The v3 pin closes the plan's provisional `335bd03c08de` prefix gap: that prefix
is superseded by the refreshed full `origin/main` commit above.

## Verified counts

| Plugin | Denominator | Basis |
| --- | --- | --- |
| `ledger-v2` | **14** included of **23** baseline commands (**9** V1-only excluded) | old-fctl `cmd/ledger` at `693c58e2` |
| `ledger-v3` | **54** included of **108** executable commands | `cmd/ledgerctl` at `bb0297cc` |

Each count is reproduced by `internal/audit`. See each plugin's `mapping.md`
and `exclusions.md` for the per-command evidence.

## Layout

```
plugins/fctl/
├── ledger-v2/          inventory, mapping, exclusions, auth/scopes/risks, manifest
├── ledger-v3/          inventory, mapping, exclusions, auth/scopes/risks, manifest
└── internal/audit/     deterministic audit tool + tests (shared, contract-free)
```

## Running the audit

```sh
nix develop -c just fctl-plugin-audit
```

Exit code 0 means every recorded invariant holds. Findings go to stdout as one
JSON document; diagnostics go to stderr.

## Gates still closed

This preparation deliberately stops short of implementation. See
`ledger-v2/auth-scopes-risks.md` and `ledger-v3/auth-scopes-risks.md` for the
exact blockers, and the programme plan for runtime gates 4B/4C/4D.
