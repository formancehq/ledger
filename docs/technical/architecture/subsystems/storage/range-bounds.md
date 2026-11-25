# Range bounds on sequence-keyed scans

Pebble's `IterOptions.UpperBound` and `Batch.DeleteRange` end key are **exclusive**. Every prefix scan in the main store therefore needs a bound that is strictly greater than every key under the prefix — and, for a sequence-keyed prefix, that bound is *not* the prefix followed by a run of `0xFF` bytes.

## The defect this rule replaces

Keys under `[zone][sub]` carry an eight-byte big-endian sequence:

```
[ZoneHistory(1)][SubHistoryLog(1)][sequence BE(8)]
```

The store used to bound those scans with `dal.MaxUint64Bytes`, an eight-byte `0xFF` run appended to the prefix. That value is byte-identical to the key of the row whose sequence is `math.MaxUint64`, so an exclusive bound built from it excluded **exactly that row** — silently, with no error and no short read.

Every sequence-keyed reader inherited the hole: `dal.ReadLastEntry` (and through it `query.ReadLastSequence` / `ReadLastAuditEntry` / `ReadLastAppliedProposal`, so recovery, the index builder and query alignment), `query.ReadLogsSince` / `ReadLogsSinceRaw` (event emission and index building), `query.ReadAuditEntries`, `query.ReadAppliedProposals`, and the checker's own log scan. A `Log` row planted at the top of the key space was invisible to the one pass built to see planted rows.

A second shape of the same wrap: a bound built as `PutUint64(seq + 1)` overflows to `[zone][sub][0x00 x8]`, which sorts **below** the lower bound. Pebble reads the inverted range as empty, so the scan returns nothing rather than failing.

## The rule

Use the **prefix successor**:

```go
// Every key under [zone][sub].
upperBound := dal.ZonePrefixUpperBound(dal.ZoneHistory, dal.SubHistoryLog)

// Every key under an arbitrary prefix (ledger-scoped keys, index prefixes).
upperBound := dal.PrefixUpperBound(prefix)
```

`PrefixUpperBound` increments the last byte that is not `0xFF`, carrying through a trailing `0xFF` run and truncating there. It returns `nil` — which Pebble reads as "no upper bound", the correct answer — when the prefix is empty or all `0xFF`. The returned slice is freshly allocated and never aliases the input, because callers keep the prefix as their lower bound.

A two-byte successor sorts strictly below every key in the next sub-prefix, all of which carry a suffix, so the bound admits the whole prefix and nothing beyond it.

**Never** append a `0xFF` run to a prefix to bound a scan. `dal.MaxUint64Bytes` was deleted so the pattern cannot be reintroduced by reaching for a named constant; a literal `0xFF` run in a bound is a review finding.

## Overflow guards on `+1`

Where a bound or a counter is derived by adding one to a stored sequence, `math.MaxUint64` cannot be handled by arithmetic and must be handled explicitly:

| Site | Handling |
|------|----------|
| `query.ReadAuditItems` | Falls back to the prefix successor; a wrapped bound would report the entry as carrying no items, a shape no writer produces. |
| `query.readAuditPageFromZone` | Already branched on `hi == ^uint64(0)`; that branch now uses the prefix successor instead of the `0xFF` run it reached for. |
| `state.LoadFSMStateFromStore` | **Refuses to boot.** `lastSeq + 1` would wrap to 0 and the FSM would start allocating at a sequence the checker reports as impossible, on top of whatever row is there. Same for the audit head, which would rewrite the chain from its beginning. |

## Reachability

The FSM cannot allocate `math.MaxUint64`. `FSMState.NextSequenceID` and `NextAuditSequenceID` are seeded at 1 and advanced by a plain `+1` (`internal/infra/state/write_set.go`, `internal/infra/state/fsmstate.go`), so reaching the top of the space would take 2^64 committed proposals.

It is reachable through storage corruption or a tampered checkpoint. The
checkpoint restore path copies Pebble files and validates their object hashes;
it does not replay individual exported rows.

That is the threat model these bounds serve: not a live cluster mis-serving data, but a corrupted or hostile store that the checker must be able to see and that recovery must refuse to build on.

## Related

- [Checker](../checker/checker.md) — the stored log bounds pass, which reports a row at either end of the audited interval.
- [Checkpoint restore contract](../backup/incremental-restore-contract.md) — which persisted effects must survive a backup.
