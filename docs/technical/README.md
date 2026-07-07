# Technical Documentation

Technical reference for the Ledger v3 distributed ledger system.

## Reading Order

1. **[Architecture Overview](architecture/overview.md)** -- system components, Raft consensus, and how they fit together
2. **[Architecture Deep Dives](architecture/)** -- 24 documents grouped by topic (core, storage, data model, API)
3. **[Contributing](contributing/getting-started.md)** -- set up your dev environment, conventions, and testing
4. **[Architecture Decision Records](adr/)** -- significant technical decisions (including "we chose not to do X")

## I want to...

| Goal | Document |
|------|----------|
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
| Work with Protocol Buffers | [contributing/protobuf.md](contributing/protobuf.md) |
| Understand Numscript language | [contributing/numscript.md](contributing/numscript.md) |
| Track v2 API parity | [contributing/api-comparison.md](contributing/api-comparison.md) |

## Directory Layout

```
technical/
  architecture/
    core/          -- Raft, FSM, global log, HLC
    storage/       -- Pebble engine, drivers, attributes, spool
    data-model/    -- ledgers, chapters, events, idempotency, metadata
    api/           -- gRPC, HTTP, Numscript library
  contributing/    -- getting started, conventions, testing, protobuf
```
