# Drafts & RFCs

This directory contains draft documents, RFCs (Request for Comments), and exploratory design documents.

## Documents

### [Problems Solved from v2](../sales/v2-vs-v3.md)
Summary of key problems and limitations from Ledger v2 that have been addressed in this v3 POC.

### [System Limitations](../sales/limitations.md)
Current system limitations based on ID types: maximum ledgers, transactions, and logs.

### [Numscript Static Inputs RFC](./numscript-static-inputs-rfc.md)
RFC for static inputs in Numscript transactions.

### [Advanced Concepts](./advanced-concepts.md)
Exploratory documentation on advanced features and concepts.

### [Data Retention & Cold Storage](./data-retention-cold-storage.md)
Design draft for period-based data retention, cold storage archival (S3/filesystem), and receipt-based cross-period transaction reverts.

### [Advanced Read Queries](./advanced-read-queries.md)
Design draft for advanced read capabilities: ListAccounts with prefix filtering, AggregateBalances, ListLogs per ledger, ledger stats, point-in-time reads, and transactions-by-account index. Leverages Pebble's sorted key layout for efficient range scans.

---

**Note**: Documents in this directory are work-in-progress or exploratory. They may not reflect the current implementation.
