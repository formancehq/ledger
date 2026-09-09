# Iterator Seek Contract

How `Seek` behaves across the read-store iterator algebra
(`internal/storage/readstore/iterator*.go`, `combinator_*.go`), and the
exhaustion-proof cache (`seekFloor`/`seekCeil`) that keeps re-seeks cheap.
Introduced by EN-1597, where iterators that latched on exhaustion silently
dropped rows under nested boolean filters. Restated once for both directions
by EN-1966.

## Direction is a type parameter, not a second contract

There is one iterator interface, `Iterator[D Direction]`, with one
positioning method. `Direction` is a sealed pair — `Asc` and `Desc` — and its
only content is a comparator that orders two entities **along the direction of
travel**. `EntityIterator` and `ReverseIterator` are aliases for
`Iterator[Asc]` and `Iterator[Desc]`.

This matters for correctness, not only for tidiness:

- **The ascending and descending algebras are the same code.** `AndIterator`,
  `OrIterator`, `NotIterator`, `FilterIterator` and `SliceIterator` are single
  direction-parameterized implementations. A new filter kind or a change to
  boolean composition cannot be correct in one direction and missing in the
  other, because there is only one place to change.
- **The directions stay distinct types.** `Iterator[Asc]` is not assignable to
  `Iterator[Desc]`, so an ascending consumer cannot be handed a descending
  iterator. This is the guard the separate `Seek`/`SeekLE` method names used
  to provide.

Only leaves whose *physical* traversal differs — a Pebble cursor walked with
`First`/`Next` versus `Last`/`Prev`, or an event group resolved in the
opposite order — have a per-direction implementation.

## The absolute-seek contract

`Iterator[D].Seek(target)` positions at the first entity **at or after
`target` in `D`'s order**: the smallest entity `>= target` ascending, the
largest entity `<= target` descending. It is an **absolute reposition**:

1. **Computed from `target` alone.** The result never depends on the
   iterator's current position, distance travelled, or exhaustion state.
2. **Idempotent and non-consuming.** Repeating the seek with the same target
   yields the same entity; a seek must not consume `Current()` (a destructive
   consume makes a repeated seek return the *next* row, silently dropping an
   intersection — the `AddressTxIterator` bug class).
3. **Well-defined after exhaustion.** A false `Next`/`Seek` does not latch
   the iterator; a later seek to a target further back repositions normally.
4. **A failed seek leaves the iterator un-positioned but re-seekable.**
   `Next` returns false until the next successful seek.

Composite iterators rely on this freely: `AndIterator.Seek` seeks **every**
child to the target (a child left at a stale position past the target would
become the convergence candidate and skip valid intersections behind it), then
`converge` leapfrogs children along the direction of travel; `OrIterator`
re-seeks all children per seek; `NotIterator` re-seeks its excluded child on
every `Seek` — including after the child reported done — and catches it up
with `Next()` as the universe advances. Any latch or consuming seek in a leaf
turns these algebra steps into silent row drops.

`AndIterator` positions **every** child on its first `Next`, rather than
advancing the first child and letting `converge` seek the rest. The lazy form
works ascending only by accident: an unpositioned child returns an empty
`Current()`, which sorts below any candidate and so reads as "behind, seek it
forward". Descending, empty reads as "past the end", `converge` adopts it as
the candidate and the intersection collapses to nothing.

One leaf is exempt by construction: `RangeIterator` emits rows in
`(value, entity)` order across index-value buckets, so an entity-space
`Seek` is undefined on the raw scan. It only supports forward draining;
every construction site materializes it into a sorted `SliceIterator` before
composing, and a direct `Seek` call fails the query with an invariant
error.

### Bounded entity-ordered leaves

A leaf whose keys place the entity at a fixed suffix and are physically
ordered by it — `BoundedEntityIterator` — can honour the absolute-seek
contract *without* materializing. Its Pebble iterator is bounded by the
half-open `[lower, upper)` range resolved from the compile-time bounds, so
both the first `Next` and every absolute `Seek` observe those bounds:
`Seek(target)` builds `prefix + target`, Pebble clamps the probe into
`[lower, upper)`, and the emitted entity is the first in-range one `>= target`.
The floor cache works exactly as on the prefix leaves, and reaching the range's
last entity (a `MaxUint64` log ID) must not wrap the following `Next` back to
the smallest ID.

An absent upper bound is closed with the **successor of the namespace
prefix** (`IncrementBytes(prefix)`), not `prefix` plus eight `0xff` bytes: the
latter would exclude a `MaxUint64` log ID because the upper bound is exclusive.
The log compiler resolves a singleton lower bound at `MaxUint64` as a point
read. The shared leaf also supports `[MaxUint64, unbounded)` directly without
computing an exclusive successor for that ID.

`NewLedgerLogRangeIterator` and `NewPebbleTxRangeIterator` are semantic
wrappers around this shared implementation. The constructor takes the reader,
prefix, lower/upper suffixes and entity length; the prefix length determines
the entity offset. The keyspace must contain one key per entity: ledger logs
use `[0x09][ledger 64B][logID_BE]`, and canonical transaction attributes use
`[0xF1][T][ledger 64B][0x02][txID_BE]`. Transaction updates replace the value at
the same canonical key; there are no by-log suffix entries in this namespace.
Both leaves therefore advance with Pebble `Next`, without computing an ID
successor or wrapping after `MaxUint64`. Multi-key entity indexes require a
different, deduplicating iterator.

The shared constructor rejects non-positive entity widths and non-nil bounds
whose length differs from that width (an empty slice is not an absent bound).
Every stored key reached by `Next` or `Seek` must have exactly that suffix
length. A shorter or longer suffix latches an invariant error in `Err`, clears
any exhaustion proof, and stops all subsequent positioning calls. Corruption
must fail the query; it must never be skipped, truncated, or mistaken for clean
exhaustion that could hide later valid rows. Seek probes retain the interface's
lexicographic semantics; the exact-width bound checks apply at construction.

The contract is enforced by unit tests per leaf (`iterator_floor_test.go`,
`iterator_address_test.go`, `iterator_and_seek_test.go`), by the
direction-parity suite over the shared combinators
(`combinator_direction_test.go`, which asserts a descending traversal is the
exact reverse of the ascending one and that `Seek` is absolute in both
directions), and end-to-end by the contradiction specs in
`tests/e2e/business/filter_nested_not_reposition_test.go`.

## The materialized union (`AddressTxIterator`)

`iterator_address.go`. An address match on the `TRANSACTIONS` target has no
entity-ordered index to scan: it walks the matching account addresses and, per
account, scans that account's `account→tx` bucket. Each per-account scan is
ascending, but the accounts are visited in address order, so the transaction
IDs arrive out of order across accounts.

The iterator satisfies the absolute-seek contract by materializing the whole
union once, on first use, and keeping it for the iterator's lifetime; `Next`
and `Seek` are then cursor moves over a stable sorted slice. `Seek`
binary-searches that slice, which makes it computed from `target` alone,
idempotent, and well-defined after exhaustion for free.

The observable requirement is on the *exposed* slice, not on how it is built:

- Before any positioning call returns, the slice is **sorted and unique**.
  `ensureMaterialized` is the single gate in front of both `Next` and
  `Seek`, and it returns only after the sort.
- The order **during** materialization is unspecified. IDs are appended as
  they are scanned, deduplicated through a `uint64` set, and the completed
  slice is sorted once (`slices.SortFunc` with `bytes.Compare`, which is the
  numeric order of the 8-byte big-endian IDs — see `ReadStoreComparer`).
  Nothing outside `materialize` may observe the intermediate order.

Sorting per insertion instead — a binary search plus a tail shift, the
pre-EN-1965 `insertSorted` — is the same output at O(U²) element movement for
U unique IDs, because interleaved account histories make almost every new ID
land near the front. Appending and sorting once is O(U log U). Both variants
materialize in full, so neither claims O(pageSize) memory, and IDs stay
immutable 8-byte copies rather than retained Pebble key buffers.

`iterator_address_bench_test.go` holds the workloads that keep this honest:
interleaved multi-account histories, an already-ascending single account (a
one-shot sort can only lose there, so any small-history regression is visible
rather than implicit), and duplicate-heavy unions that report scanned rows
alongside unique IDs.

## The exhaustion-proof cache (`seekFloor`/`seekCeil`)

`iterator_floor.go`. Without the latch, an exhausted child would be re-seeked
by its composite parent once per merge step — a fresh Pebble seek plus
allocations, O(rows) times per query. The floor restores the O(1) fast path
without reintroducing the latch:

- a **cleanly** failed ascending `Seek(t)` proves *no entity >= t exists in
  the view*; the floor records `t` and every later seek at or above it
  returns false in one comparison. `seekCeil` mirrors this for a descending
  `Seek` (*no entity <= t*).
- a seek below the floor (above the ceil) is not covered and repositions
  normally — the contract above is preserved.

Two preconditions make the proof permanent, and both are load-bearing:

1. **The view is snapshot-fixed.** Every leaf holds a `pebble.Iterator`, whose
   view is fixed at creation; iterators are created per query. A proof can
   therefore never go stale, and the bound is never cleared. Handing these
   iterators a live, mutating view would silently violate this.
2. **Only clean exhaustion proves anything.** `Seek` also returns false on
   I/O error (`Err()`), and an I/O-failed seek proves nothing about the view's
   contents. `seekFloor.fail`/`seekCeil.fail` take the iterator's storage
   error and drop the proof when it is non-nil. (Pebble's error is sticky and
   pagination propagates `Err()` unconditionally, so the query still fails
   loudly either way — the guard keeps the cache sound on its own terms
   rather than by leaning on that second-order property.)
