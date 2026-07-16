# Drafts & RFCs

This directory contains draft documents, RFCs (Request for Comments), and exploratory design documents.

## Documents

### [Problems Solved from v2](../sales/v2-vs-v3.md)
Summary of key problems and limitations from Ledger v2 that have been addressed in this v3 POC.

### [System Limitations](../sales/limitations.md)
Current system limitations based on ID types: maximum ledgers, transactions, and logs.

### [Numscript RFCs](./numscript/)
Draft RFCs for Numscript evolution:
- [WASM Compilation RFC](./numscript/numscript-wasm-compilation-rfc.md) — WASM compilation pipeline
- [Typed Metadata RFC](./numscript/numscript-typed-metadata-rfc.md) — Typed metadata integration

Dependency resolution (formerly the "Static Inputs RFC") is now implemented — see
[Numscript → Volume Preloading (Dependency Resolution)](../technical/contributing/numscript.md#volume-preloading-dependency-resolution).

### [Advanced Concepts](./advanced-concepts.md)
Exploratory documentation on advanced features and concepts.

### [Advanced Read Queries](./advanced-read-queries.md)
Design draft for advanced read capabilities: ListAccounts with prefix filtering, AggregateBalances, ListLogs per ledger, ledger stats, point-in-time reads, and transactions-by-account index. Leverages Pebble's sorted key layout for efficient range scans.

### [Safe Reuse Between Admission Planning and FSM Apply (RFC)](./apply-simulation-rfc.md)
Investigation spike (EN-1545), currently **BLOCKED** on EN-1406 / EN-1532 / EN-1288. Neutrally compares bounded options for reducing the divergence between admission's planning pass and the FSM apply path — (A) current design + targeted options, (B) shared typed non-reading helpers, (C) transcript/sidecar with an explicit preflight manifest, (D) a universal `simulate(order, StateReader, WriteSink)` interpreter (evaluated as the **no-go** control). Per the authoritative EN-1545 decision, the universal simulator is not adopted; no-go is an explicitly valid outcome. No single recommendation is asserted pending re-validation once the blocking dependencies settle.

### [Chart of Accounts](./chart-of-accounts.md)
RFC for declarative account address validation. Defines a per-ledger chart of accounts with fixed and variable segments, regex constraints, default metadata, and configurable enforcement modes (strict/audit). Replaces the v2 magic JSON format with an idiomatic, protobuf-typeable structure.

## Graduated to Documentation

- **Typed Metadata** — moved to [architecture/typed-metadata.md](../technical/architecture/subsystems/read-path/typed-metadata.md)
- **Data Retention & Cold Storage** — moved to [architecture/chapters.md](../technical/architecture/subsystems/chapters/lifecycle.md)
- **Numscript Library** — moved to [architecture/numscript-library.md](../technical/architecture/subsystems/scripting/numscript-library.md)

---

**Note**: Documents in this directory are work-in-progress or exploratory. They may not reflect the current implementation.
