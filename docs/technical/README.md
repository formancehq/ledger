# Technical Documentation

Technical reference for the Ledger v3 distributed ledger system.

## Reading Order

1. **[Architecture Overview](architecture/overview.md)** -- canonical high-level system shape and subsystem boundaries
2. **[Architecture Deep Dives](architecture/)** -- documents grouped by subsystem/topic
3. **[Contributing](contributing/getting-started.md)** -- set up your dev environment, conventions, and testing
4. **[Architecture Decision Records](adr/)** -- significant technical decisions (including "we chose not to do X")

AI agents should not preload this entire tree. Start from [`AGENTS.md`](../../AGENTS.md) and use [`agent-context.md`](agent-context.md) to select the minimum authoritative documentation for the task.

`architecture/overview.md` is the single canonical architecture overview. The older `architecture-overview.md` path is retained only as a compatibility redirect and must not contain an independent description.

## I want to...

| Goal | Document |
|------|----------|
| Route AI-agent context for a code change | [agent-context.md](agent-context.md) |
| Understand the system architecture | [architecture/overview.md](architecture/overview.md) |
| Learn how Raft consensus works here | [architecture/subsystems/consensus/raft-consensus.md](architecture/subsystems/consensus/raft-consensus.md) |
| Understand the deterministic FSM | [architecture/subsystems/fsm/deterministic-fsm.md](architecture/subsystems/fsm/deterministic-fsm.md) |
| Learn about storage internals | [architecture/subsystems/storage/storage.md](architecture/subsystems/storage/storage.md) |
| Understand ledgers and transactions | [architecture/data-model.md](architecture/data-model.md) |
| See data flow diagrams | [architecture/data-flows.md](architecture/data-flows.md) |
| Understand the audit trail (source of truth) | [architecture/subsystems/checker/audit-chain.md](architecture/subsystems/checker/audit-chain.md) |
| Add a new persisted projection (checker duties) | [architecture/subsystems/checker/checker.md](architecture/subsystems/checker/checker.md) |
| Learn the gRPC API | [architecture/subsystems/api/grpc-api.md](architecture/subsystems/api/grpc-api.md) |
| Learn the HTTP API | [architecture/subsystems/api/http-api.md](architecture/subsystems/api/http-api.md) |
| Set up the dev environment | [contributing/getting-started.md](contributing/getting-started.md) |
| Understand code conventions | [contributing/conventions.md](contributing/conventions.md) |
| Write and run tests | [contributing/testing.md](contributing/testing.md) |
| Run a bounded AI review/fix loop | [contributing/ai-review-loop.md](contributing/ai-review-loop.md) |
| Work with Protocol Buffers | [contributing/protobuf.md](contributing/protobuf.md) |
| Understand Numscript language | [contributing/numscript.md](contributing/numscript.md) |
| Track v2 API parity | [contributing/api-comparison.md](contributing/api-comparison.md) |

## Directory Layout

```text
technical/
  agent-context.md             -- context routing for AI agents
  agent-reference-legacy.md    -- temporary migration snapshot; non-canonical
  architecture-overview.md     -- compatibility redirect only
  architecture/
    README.md                   -- architecture navigation and subsystem map
    overview.md                 -- canonical high-level architecture overview
    data-flows.md               -- cross-subsystem sequences
    data-model.md               -- core domain model
    audit-vs-technical-state.md -- persistence/integrity classification
    primitives/                 -- cross-cutting wire/data primitives
    subsystems/                 -- authoritative subsystem deep dives
  contributing/                 -- getting started, conventions, testing, protobuf
  adr/                          -- architecture decision records
```

The temporary `agent-reference-legacy.md` snapshot preserves the former monolithic agent instructions during migration. It is not part of the normal reading order and is not authoritative over current subsystem documentation.
