# Drafts & RFCs

This directory contains draft documents, RFCs (Request for Comments), and exploratory design documents.

## Documents

### [Problems Solved from v2](../sales/v2-vs-v3.md)
Summary of key problems and limitations from Ledger v2 that have been addressed in this v3 POC.

### [System Limitations](../sales/limitations.md)
Current system limitations based on ID types: maximum ledgers, transactions, and logs.

### [Numscript RFCs](./numscript/)
Draft RFCs for Numscript evolution:
- [Static Inputs RFC](./numscript/numscript-static-inputs-rfc.md) — Static input declaration contract
- [WASM Compilation RFC](./numscript/numscript-wasm-compilation-rfc.md) — WASM compilation pipeline
- [Typed Metadata RFC](./numscript/numscript-typed-metadata-rfc.md) — Typed metadata integration

### [Advanced Concepts](./advanced-concepts.md)
Exploratory documentation on advanced features and concepts.

### [Advanced Read Queries](./advanced-read-queries.md)
Design draft for advanced read capabilities: ListAccounts with prefix filtering, AggregateBalances, ListLogs per ledger, ledger stats, point-in-time reads, and transactions-by-account index. Leverages Pebble's sorted key layout for efficient range scans.

### [Unified Hot-Path Apply Simulation RFC](./apply-simulation-rfc.md)
Investigation spike (EN-1545) proposing a single `simulate(order, StateReader, WriteSink)` interpreter run in two modes (admission dry-run, FSM apply) to eliminate admission's hand-derived shadow of the FSM apply path and the redundant double numscript evaluation. Includes a go/no-go recommendation and a phased migration plan. Depends on EN-1406.

### [Chart of Accounts](./chart-of-accounts.md)
RFC for declarative account address validation. Defines a per-ledger chart of accounts with fixed and variable segments, regex constraints, default metadata, and configurable enforcement modes (strict/audit). Replaces the v2 magic JSON format with an idiomatic, protobuf-typeable structure.

## Graduated to Documentation

- **Typed Metadata** — moved to [architecture/typed-metadata.md](../technical/architecture/subsystems/read-path/typed-metadata.md)
- **Data Retention & Cold Storage** — moved to [architecture/chapters.md](../technical/architecture/subsystems/chapters/lifecycle.md)
- **Numscript Library** — moved to [architecture/numscript-library.md](../technical/architecture/subsystems/scripting/numscript-library.md)

---

**Note**: Documents in this directory are work-in-progress or exploratory. They may not reflect the current implementation.
