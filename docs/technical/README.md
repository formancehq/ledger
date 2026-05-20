# Technical Documentation

Technical reference for the Ledger v3 distributed ledger system.

## Reading Order

1. **[Architecture Overview](architecture/core/architecture.md)** -- system components, Raft consensus, and how they fit together
2. **[Architecture Deep Dives](architecture/)** -- 24 documents grouped by topic (core, storage, data model, API)
3. **[Contributing](contributing/getting-started.md)** -- set up your dev environment, conventions, and testing

## I want to...

| Goal | Document |
|------|----------|
| Understand the system architecture | [architecture/core/architecture.md](architecture/core/architecture.md) |
| Learn how Raft consensus works here | [architecture/core/raft-consensus.md](architecture/core/raft-consensus.md) |
| Understand the deterministic FSM | [architecture/core/deterministic-fsm.md](architecture/core/deterministic-fsm.md) |
| Learn about storage internals | [architecture/storage/storage.md](architecture/storage/storage.md) |
| Understand ledgers and transactions | [architecture/data-model/buckets-ledgers.md](architecture/data-model/buckets-ledgers.md) |
| See data flow diagrams | [architecture/data-model/data-flows.md](architecture/data-model/data-flows.md) |
| Learn the gRPC API | [architecture/api/grpc-api.md](architecture/api/grpc-api.md) |
| Learn the HTTP API | [architecture/api/api.md](architecture/api/api.md) |
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
    data-model/    -- ledgers, periods, events, idempotency, metadata
    api/           -- gRPC, HTTP, Numscript library
  contributing/    -- getting started, conventions, testing, protobuf
```
