# System Limitations

This document describes the current limitations of the system based on the data types used for identifiers.

## ID Types and Maximum Values

| Resource | ID Type | Maximum Value | Practical Limit |
|----------|---------|---------------|-----------------|
| Ledgers | `uint32` | 4,294,967,295 (~4.3 billion) | ~4.3 billion ledgers |
| Transactions per ledger | `uint64` | 18,446,744,073,709,551,615 (~18 quintillion) | Virtually unlimited |
| Logs per ledger | `uint64` | 18,446,744,073,709,551,615 (~18 quintillion) | Virtually unlimited |
| Global sequence | `uint64` | 18,446,744,073,709,551,615 (~18 quintillion) | Virtually unlimited |

## Detailed Limitations

### Ledger Count

- **Type**: `uint32`
- **Maximum**: ~4.3 billion ledgers
- **Storage key format**: `[prefix][ledger_id:4bytes]`

The ledger ID is stored as a 4-byte big-endian integer in the storage keys. This limits the system to approximately 4.3 billion unique ledgers.

### Transactions per Ledger

- **Type**: `uint64`
- **Maximum**: ~18 quintillion transactions per ledger

Each ledger maintains its own transaction ID counter (`next_transaction_id`). Transaction IDs are sequential within each ledger, starting from 1.

At a rate of 1 million transactions per second, it would take approximately **584,942 years** to exhaust the transaction ID space for a single ledger.

### Logs per Ledger

- **Type**: `uint64`
- **Maximum**: ~18 quintillion logs per ledger

Each ledger maintains its own log ID counter (`next_log_id`). Logs include:
- Transaction creations
- Account metadata updates
- Transaction reversions

Multiple logs can be created for a single transaction (e.g., a transaction with metadata creates multiple log entries).

### Global Sequence

- **Type**: `uint64`
- **Maximum**: ~18 quintillion

The global sequence is a system-wide counter that provides ordering across all operations on all ledgers. Every log entry receives a unique global sequence number.

## Storage Considerations

### Key Size

Storage keys use fixed-size binary encoding:
- Ledger ID: 4 bytes (`uint32`)
- Transaction ID: 8 bytes (`uint64`)
- Log ID: 8 bytes (`uint64`)
- Sequence: 8 bytes (`uint64`)

### Practical Limits

While the theoretical limits are very high, practical limits depend on:

1. **Disk space**: Each log entry and balance update consumes storage
2. **Memory**: The FSM state keeps ledger metadata in memory
3. **Network bandwidth**: Raft replication requires transmitting all logs
4. **Cluster size**: Performance degrades with very large clusters

## Protobuf Definitions

The ID types are defined in the following proto files:

- `misc/proto/raftcmd.proto`: FSM state and commands
  - `State.next_ledger_id`: `uint32`
  - `LedgerState.next_log_id`: `uint64`
  - `LedgerState.next_transaction_id`: `uint64`
  - `State.next_sequence`: `uint64`

- `misc/proto/common.proto`: Common data structures
  - `LedgerInfo.id`: `uint32`
  - `Transaction.id`: `uint64`
  - `Log.sequence`: `uint64`

## Recommendations

1. **Ledger cleanup**: Implement ledger archival/deletion for unused ledgers to reclaim IDs
2. **Monitoring**: Monitor sequence numbers to detect approaching limits (extremely unlikely)
3. **Sharding**: For extremely large deployments, consider sharding across multiple clusters
