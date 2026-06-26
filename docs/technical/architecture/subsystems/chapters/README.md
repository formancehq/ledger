# Chapters

Chapters partition a ledger's transaction history into discrete sealed segments. Once sealed, a chapter can be archived to cold storage (S3 or filesystem) — logs and audit entries get exported and purged from Pebble while attributes stay hot. Receipt-based reverts allow undoing archived transactions without rehydrating cold data.

## Documents

| Document | Description |
|----------|-------------|
| [lifecycle.md](lifecycle.md) | Chapter lifecycle (OPEN, CLOSING, CLOSED, ARCHIVING, ARCHIVED), sealing hash, and crash recovery. |

## Related

- [Storage](../storage/) — the Pebble zones that hold logs / audit entries before archival.
- [Checker & audit](../checker/) — hash chain over audit entries underpins seal verification.
