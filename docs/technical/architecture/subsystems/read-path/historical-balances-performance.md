# Historical Balance Performance

Historical balances are off the Raft/FSM hot path. Their cost is paid by one asynchronous builder per replica and by historical reads.

## Expected cost

| Operation | Complexity and main cost |
|---|---|
| Commit notification | O(1), non-blocking coalesced signal after Pebble commit |
| Builder batch | O(postings × 2 temporalities), grouped contiguous source-log scans, plus sorted Pebble batch writes |
| Filtered read | O(segments × selected account/asset identities) seeks and additions, with one reusable catalog iterator and one reusable data iterator per segment |
| Unfiltered read | O(segments × all account identities for the ledger) |
| Compaction | O(entries in selected segments), asynchronous |
| Enable/disable | Full replay of the configured ledger set; asynchronous and fail-closed |
| Explicit structural verification | O(all catalog and data rows); never run on ordinary store open |

Removing ledger-level aggregate rows halves the logical temporal rows produced per posting compared with the account-plus-ledger design: each effect is written once for effective temporality and once for insertion temporality. It also removes duplicate compaction and storage amplification. The trade-off is intentional: an unfiltered ledger read must scan and aggregate account rows instead of seeking one pre-aggregated ledger row.

Ledger names make keys longer than numeric IDs, but the repeated `(prefix, segment, temporality, ledgerName)` portion is well suited to Pebble block prefix compression. Names also avoid a separate ID-to-name coupling in the peer projection and align with the rest of the read/index builder surface.

Post-commit wake-ups reduce normal tail latency from “up to one polling interval” to scheduler plus processing latency. The FSM emits this wake for every committed batch, even when its log watermark does not advance. One wake drains every complete source batch already visible, so sustained ingestion is not capped by `batchSize / 200 ms`. The hot source resolves consecutive referenced logs with one ordered scan per contiguous range instead of one Pebble point lookup per log; isolated old references remain isolated ranges rather than forcing a scan across the gap. The 200 ms interval remains only as a retry/recovery ticker.

Logical compaction is also woken by store mutations and runs before a boot or repair can mark the projection READY. The periodic maintenance interval is a correctness fallback, not the sole compaction cadence. Immutable publication deletes the immediately superseded unleased manifest in the same Pebble batch; periodic garbage collection reclaims released leases and crash orphans after scanning candidates outside the publication/view lock.

Removing projection checksums, semantic digests, cold upload/hydration, and periodic certifier replay eliminates whole-store hashing and remote-I/O work. Audit replay and structural validation remain the correctness boundary. The local disk footprint is therefore unbounded by a projection cold tier and must be capacity-planned or rebuilt on demand.

## Benchmarks

The store benchmark varies history age, selected-account cardinality, and logical segment count:

```bash
GOROOT= go test ./internal/storage/balancehistorystore \
  -run '^$' -bench 'BenchmarkHistoryRead' -benchmem
```

When reporting results, include CPU, filesystem, Pebble cache state, account cardinality, segment count, selected temporality, and whether the read is filtered. Never compare a warmed filtered read with an unfiltered cold-cache read as the same workload.

The acceptance checks are:

- no synchronous FSM read or projection write;
- notification handling remains non-blocking;
- builder lag converges under sustained writes;
- segment count remains bounded by background compaction;
- boot and repair do not expose an uncompacted segment fan-out;
- filtered read latency scales with selected identities, not ledger age;
- unfiltered read/storage trade-off is measured explicitly;
- configuration rebuild never exposes partial results.
