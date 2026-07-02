# Architecture Documentation

Architecture documentation is organised by **subsystem** — each `subsystems/<name>/` directory mirrors a code boundary (`internal/application/<name>`, `internal/infra/<name>`, `internal/storage/<name>`) and is self-contained with its own README.

Cross-cutting concerns (overall design, data flows shared across subsystems, the data model, the audit-and-checker invariants, low-level wire formats) live at the root.

## Start here

| Document | When to read it |
|----------|-----------------|
| [overview.md](overview.md) | First read — system components, main interactions, and high-level design. |
| [data-flows.md](data-flows.md) | Detailed sequence diagrams for write / read / synchronization flows. |
| [data-model.md](data-model.md) | Ledger system, transaction management, and data organisation. |

## Subsystems

| Subsystem | Code | Covers |
|-----------|------|--------|
| [admission](subsystems/admission/) | `internal/application/admission` | Request gateway, validation, signing, idempotency. |
| [api](subsystems/api/) | `internal/adapter/grpc`, `internal/adapter/http`, `internal/adapter/auth` | gRPC and HTTP transport surfaces. |
| [attributes](subsystems/attributes/) | `internal/infra/attributes`, `internal/infra/cache`, `internal/infra/bloom` | In-memory attribute caches, bloom filters, key hashing. |
| [chapters](subsystems/chapters/) | `internal/infra/coldstorage`, `internal/infra/receipt`, `internal/application/backup` | Chapter lifecycle, archival, receipt-based reverts. |
| [checker](subsystems/checker/) | `internal/application/check`, `internal/domain/replay` | Audit hash chain and integrity verification of every persisted projection. |
| [consensus](subsystems/consensus/) | `internal/infra/node`, `internal/infra/transport` | Raft replication, global log, hybrid logical clock. |
| [events-mirror](subsystems/events-mirror/) | `internal/application/events`, `internal/application/mirror` | Event sinks (NATS / Kafka / ClickHouse / Databricks / HTTP) and mirror ingest. |
| [fsm](subsystems/fsm/) | `internal/infra/state`, `internal/infra/plan`, `internal/infra/preload` | Deterministic apply path, cache layering, preload contract. |
| [indexer](subsystems/indexer/) | `internal/application/indexbuilder`, `internal/storage/readstore` (key layout) | Inverted-index builder, schema rewrite, atomic switch. |
| [read-path](subsystems/read-path/) | `internal/application/ctrl` (reads), `internal/query`, `internal/storage/readstore` | Query pipeline, prepared queries, query checkpoints, typed metadata. |
| [scripting](subsystems/scripting/) | numscript runtime + library | Reusable numscript programs and their lifecycle. |
| [storage](subsystems/storage/) | `internal/storage/dal`, `wal`, `spool`, `pebblecfg` | Pebble main store, WAL, snapshots, spool. |

## Cross-cutting

| Document | Why |
|----------|-----|
| [data-flows.md](data-flows.md) | Sequence diagrams that span multiple subsystems. |
| [data-model.md](data-model.md) | Ledgers, transactions, postings — the core domain shape. |
| [primitives/uint256-wire-format.md](primitives/uint256-wire-format.md) | Fixed-size monetary-amount wire format used everywhere on the boundary. |

## Conventions

Whenever you introduce a new technical mechanism, subsystem, or non-obvious invariant, add a dedicated page under the matching subsystem directory and link it from that subsystem's README (and update this top-level table if a brand-new subsystem is being added). See [AGENTS.md / Documentation Maintenance](../../../AGENTS.md#documentation-maintenance) for the rule.
