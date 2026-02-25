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

## Graduated to Documentation

- **Typed Metadata** — moved to [architecture/typed-metadata.md](../dev/architecture/typed-metadata.md)
- **Data Retention & Cold Storage** — moved to [architecture/periods.md](../dev/architecture/periods.md)

---

**Note**: Documents in this directory are work-in-progress or exploratory. They may not reflect the current implementation.
