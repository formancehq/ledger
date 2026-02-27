# Spool Technical Documentation

## Overview

The **Spool** is a durable buffer that sits between Raft consensus and FSM application. It stores committed Raft entries that haven't been applied to the FSM yet, providing durability guarantees and efficient recovery.

## Interface

```go
type Spool interface {
    // AppendCommittedEntries appends committed Raft entries to the spool
    AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error
    
    // End returns the current write position (watermark) for replay bounds
    End() (*Position, error)
    
    // ReplayUntil replays entries from the cached position to 'end'
    // Only entries with Index > lastApplied are passed to applyFn
    ReplayUntil(ctx context.Context, end Position, lastApplied uint64, applyFn func(raftpb.Entry) error) error
    
    // Prune removes segments where all entries have been applied
    Prune(lastApplied uint64) error
}

type Position struct {
    SegID  uint64  // Segment ID
    Offset int64   // Byte offset within the segment
}
```

## DefaultSpool Implementation

The `DefaultSpool` is the default implementation used in the service. It provides:

- **Segment-based storage**: Entries are stored in sequentially numbered segment files
- **Buffered writes**: Uses Go's `bufio.Writer` for performance
- **CRC32 checksums**: Validates data integrity on read
- **Read caching**: Avoids re-parsing already-read entries
- **Trailer metadata**: Quick segment skip during replay

### Directory Structure

```
spool/
├── spool-00000000000000000001.log   # First segment
├── spool-00000000000000000002.log   # Second segment
└── spool-00000000000000000003.log   # Current segment (active)
```

### Configuration

```go
type DefaultSpoolConfig struct {
    Dir             string        // Directory for segment files
    SegmentMaxBytes int64         // Max bytes per segment (default: 256MB)
    WriteBufBytes   int           // Write buffer size (default: 1MB)
    SyncEvery       int           // Sync to disk every N entries (default: 1024)
    SyncMaxDelay    time.Duration // Max delay between syncs (default: 200ms)
}
```

## Record Format

Each entry is stored as a record with a fixed header:

```
┌────────────────────────────────────────────────────────────────┐
│                         Record Header (16 bytes)                │
├────────────┬────────────┬────────────┬─────────────────────────┤
│  Magic     │  Length    │   CRC32    │   Reserved              │
│  (4 bytes) │  (4 bytes) │  (4 bytes) │   (4 bytes)             │
├────────────┴────────────┴────────────┴─────────────────────────┤
│                         Payload (variable)                      │
│                    Protobuf-encoded raftpb.Entry                │
└────────────────────────────────────────────────────────────────┘

Magic:  0x53504F4C ("SPOL")
Length: Payload length in bytes
CRC32:  IEEE CRC32 checksum of payload
```

### Segment Trailer

Each segment has an optional trailer written when the segment is closed:

```
┌────────────────────────────────────────────────────────────────┐
│                      Segment Trailer (24 bytes)                 │
├────────────┬────────────┬────────────┬─────────────────────────┤
│  Magic     │  MinIndex  │  MaxIndex  │   CRC32                 │
│  (4 bytes) │  (8 bytes) │  (8 bytes) │   (4 bytes)             │
└────────────────────────────────────────────────────────────────┘

Magic:    0x54504F53 ("SPOT")
MinIndex: Minimum Raft index in this segment
MaxIndex: Maximum Raft index in this segment
CRC32:    IEEE CRC32 checksum of trailer data
```

The trailer enables quick segment skipping during replay: if `MaxIndex <= lastApplied`, the entire segment can be skipped without parsing records.

## Write Path

### AppendCommittedEntries

```
┌─────────────────────────────────────────────────────────────────┐
│                     AppendCommittedEntries                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  for each entry:                                                 │
│    1. Check if segment needs rotation                           │
│       (size + record > SegmentMaxBytes)                         │
│                                                                  │
│    2. Serialize entry to protobuf                               │
│                                                                  │
│    3. Write record header + payload to buffer                   │
│                                                                  │
│    4. Update segment min/max index                              │
│                                                                  │
│    5. If pendingN >= SyncEvery OR                               │
│       time.Since(pendingSince) >= SyncMaxDelay:                 │
│         - Flush buffer                                          │
│         - fsync() to disk                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Segment Rotation

When a segment reaches `SegmentMaxBytes`:

1. Flush the write buffer
2. Write the trailer with min/max index
3. fsync() to ensure durability
4. Close the current segment file
5. Increment segment ID
6. Open a new segment file

## Read Path

### ReplayUntil

```
┌─────────────────────────────────────────────────────────────────┐
│                         ReplayUntil                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Check for rewind (lastApplied < cached rLastApplied)        │
│     → Reset cache to first segment if rewind detected           │
│                                                                  │
│  2. Start from cached position (rSegID, rOffset)                │
│                                                                  │
│  3. For each segment from start to end.SegID:                   │
│                                                                  │
│     a. If offset=0, read trailer to check MaxIndex              │
│        → Skip segment if MaxIndex <= lastApplied                │
│                                                                  │
│     b. Seek to offset, read records sequentially                │
│                                                                  │
│     c. For each record:                                         │
│        - Validate magic number                                  │
│        - Validate CRC32 checksum                                │
│        - If entry.Index > lastApplied: call applyFn()           │
│        - Update read cache position                             │
│                                                                  │
│     d. Stop when reaching end.Offset on end.SegID               │
│                                                                  │
│  4. Read cache now points to end position                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Read Cache

The read cache (`rSegID`, `rOffset`, `rLastApplied`) is an in-memory optimization:

- **Purpose**: Avoid re-parsing entries on subsequent replay calls
- **Location**: Stored in RAM only (not persisted)
- **Reset**: Automatically resets if `lastApplied` decreases (rewind scenario)
- **Behavior**: Advances as records are successfully processed

## Pruning

The `Prune(lastApplied)` method removes old segments:

```go
func (s *DefaultSpool) Prune(lastApplied uint64) error {
    for _, id := range segments {
        // Read trailer
        _, maxI, ok := readTrailer(f)
        if ok && maxI <= lastApplied {
            // All entries in this segment have been applied
            os.Remove(segmentPath(id))
        }
    }
}
```

> **Note**: The current segment (being written to) is never pruned.

## Durability Guarantees

### Sync Strategy

The DefaultSpool uses a **batched sync** strategy:

1. Writes are buffered in memory (`WriteBufBytes` default: 1MB)
2. Sync occurs when:
   - `SyncEvery` entries have been written (default: 1024)
   - `SyncMaxDelay` has elapsed since last sync (default: 200ms)
   - A segment rotation occurs
   - `End()` is called (flushes buffer)

### Failure Scenarios

| Scenario | Behavior |
|----------|----------|
| Crash before sync | Unsynced entries are lost; Raft will re-send |
| Crash during sync | Partial writes detected by CRC; truncated on recovery |
| Crash during replay | Safe; replay resumes from cached position |
| Corrupt record | `ErrCorrupt` returned; manual intervention may be needed |

## Performance Considerations

### Tuning Parameters

| Parameter | Default | Impact |
|-----------|---------|--------|
| `SegmentMaxBytes` | 256MB | Larger = fewer rotations, but longer recovery scans |
| `WriteBufBytes` | 1MB | Larger = better write batching, higher memory usage |
| `SyncEvery` | 1024 | Lower = more durable, higher disk I/O |
| `SyncMaxDelay` | 200ms | Lower = more durable, higher latency |

### Recommendations

1. **High durability**: Set `SyncEvery=1` (sync every entry)
2. **High throughput**: Increase `SyncEvery` and `SyncMaxDelay`
3. **Fast recovery**: Keep `SegmentMaxBytes` small (e.g., 64MB)
4. **Fast storage**: Use NVMe/SSD for the spool directory

## Integration with Raft

The Spool is used by the **Applier** component (owned by the Node). The Applier runs as a dedicated goroutine, decoupled from the WAL-writing `processReadies` goroutine, so that WAL writes and FSM application can overlap across consecutive Ready cycles:

```
┌─────────────────────────────────────────────────────────────────┐
│                          Applier                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  On committed entries (received via Submit() channel):          │
│    1. If syncing/snapshotting: spool.AppendCommittedEntries()   │
│    2. If normal: applyEntriesToFSM(entries)                     │
│    3. On gating termination (unspoolAndResume):                 │
│       - end := spool.End()                                      │
│       - spool.ReplayUntil(end, lastApplied, fsm.Apply)          │
│       - spool.Prune(lastApplied)                                │
│    4. If snapshot threshold reached:                            │
│       - Create snapshot (maintenance task)                      │
│       - wal.Compact()                                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Error Handling

### ErrCorrupt

Returned when a record fails validation:
- Invalid magic number
- CRC32 checksum mismatch

**Recovery**: Manual inspection required. The corrupt segment may need to be truncated or removed.

### Segment Not Found

If a segment referenced by the read cache has been pruned:
- The cache automatically advances to the next available segment
- Replay continues from the new position

## Next Steps

- [Data Flows](./data-flows.md) - How the Spool fits in the synchronization flow
- [Storage](./storage.md) - Persistent storage architecture
- [Raft Consensus](./raft-consensus.md) - Raft protocol details
