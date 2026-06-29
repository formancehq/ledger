# Chapters

Chapters partition a ledger's transaction history into discrete sealed segments. Once sealed, a chapter can be archived to cold storage (S3 or filesystem) — logs and audit entries get exported and purged from Pebble while attributes stay hot. Receipt-based reverts allow undoing archived transactions without rehydrating cold data.

## Documents

| Document | Description |
|----------|-------------|
| [lifecycle.md](lifecycle.md) | Chapter lifecycle (OPEN, CLOSING, CLOSED, ARCHIVING, ARCHIVED), sealing hash, and crash recovery. |
| [cold-storage.md](cold-storage.md) | Export of sealed chapter logs to durable cold storage (filesystem, S3) with a two-phase archive flow. |
| [receipts.md](receipts.md) | HMAC-signed JWT receipts that allow reverting archived transactions without re-reading cold storage. |
| [backup.md](backup.md) | Full-database backup (Pebble checkpoint + incremental segments) to S3, Azure, or filesystem; restore for disaster recovery. |

## Related

- [Storage](../storage/) — the Pebble zones that hold logs / audit entries before archival.
- [Checker & audit](../checker/) — hash chain over audit entries underpins seal verification.
