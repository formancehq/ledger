# fctl plugins — Ledger

Ledger v2 and Ledger v3 are separate plugins behind one public `fctl ledger`
surface. Ledger v2 has an implemented portable catalogue and generated-client
adapter. Ledger v3 remains a source-audited catalogue until its component is
implemented. Neither manifest claims a release artifact or install record.

## Governing decision (2026-09-10)

Ledger ships **two separate plugins**: `ledger-v2` and `ledger-v3`.

This is a decision, not an optimisation. The implementation keeps them apart
at every level:

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
| fctl-v2 host contract | `545521bfa222250af6b4419b194c7967cded0379` | Ledger v2 `fctl-sdk.lock.json` |

The v3 pin closes the plan's provisional `335bd03c08de` prefix gap: that prefix
is superseded by the refreshed full `origin/main` commit above.

## Verified counts

| Plugin | Denominator | Basis |
| --- | --- | --- |
| `ledger-v2` | **22** included of **23** baseline commands (**1** excluded as host-owned) | old-fctl `cmd/ledger` at `693c58e2` |
| `ledger-v3` | **54** included of **108** executable commands | `cmd/ledgerctl` at `bb0297cc` |

`ledger-v2` keeps the historical user surface: the 9 commands the baseline
implemented on `Ledger.V1` are **converted** to their V2 API equivalents, except
`ledger server-infos`, whose `/_/info` probe is host-owned. See
`ledger-v2/mapping.md` for the 8 conversions and their evidence.

### What the audit does and does not derive

`internal/audit` recomputes the tallies **from the checked-in inventories** and
fails if a document disagrees with them or with itself: the v2
included/excluded/converted counts and distinct-operation count, the v3
four-bucket classification tally and its totality, and every recorded `counts`
block.

It does **not** re-derive the upstream figures. The 23 baseline commands, the 45
`openapi/v2.yaml` operations and the 108 `cmd/ledgerctl` commands come from
manual extraction at the pins, recorded as reviewed constants in
`internal/audit/audit.go` with their evidence in each plugin's `mapping.md`.
Re-verifying those against upstream source needs the pinned checkouts, which the
audit deliberately does not depend on.

See each plugin's `mapping.md` and `exclusions.md` for the per-command evidence.

## Layout

```
plugins/fctl/
├── ledger-v2/          implemented catalogue, adapter, component source and audit documents
├── ledger-v3/          inventory, mapping, exclusions, auth/scopes/risks and manifest
└── internal/audit/     deterministic audit tool + tests (shared, contract-free)
```

## Running the audit

```sh
nix develop -c just fctl-plugin-audit
```

Exit code 0 means every recorded invariant holds. Findings go to stdout as one
JSON document; diagnostics go to stderr.

## Runtime evidence

Implementation status and release evidence are distinct. Ledger v2 source is
implemented, but its manifest deliberately leaves `runtime_artifact` unset
until a built component, installation and host acceptance are verified. Ledger
v3 remains at preparation status. See each plugin's `auth-scopes-risks.md` for
the remaining product-specific evidence gaps.
