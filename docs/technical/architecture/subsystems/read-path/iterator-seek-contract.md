# Iterator Seek Contract

How `SeekGE`/`SeekLE` behave across the read-store iterator algebra
(`internal/storage/readstore/iterator_*.go`), and the exhaustion-proof cache
(`seekFloor`/`seekCeil`) that keeps re-seeks cheap. Introduced by EN-1597,
where iterators that latched on exhaustion silently dropped rows under nested
boolean filters.

## The absolute-seek contract

`EntityIterator.SeekGE(target)` (and its descending mirror,
`ReverseIterator.SeekLE`) is an **absolute reposition**:

1. **Computed from `target` alone.** The result never depends on the
   iterator's current position, direction of travel, or exhaustion state.
2. **Idempotent and non-consuming.** Repeating the seek with the same target
   yields the same entity; a seek must not consume `Current()` (a destructive
   consume makes a repeated seek return the *next* row, silently dropping an
   intersection — the `AddressTxIterator` bug class).
3. **Well-defined after exhaustion.** A false `Next`/`SeekGE` does not latch
   the iterator; a later seek to a smaller (forward) or larger (reverse)
   target repositions normally.
4. **A failed seek leaves the iterator un-positioned but re-seekable.**
   `Next` returns false until the next successful seek.

Composite iterators rely on this freely: `AndIterator.SeekGE` seeks **every**
child to the target (a child left at a stale position past the target would
become the convergence candidate and skip valid intersections below it), then
`converge` leapfrogs children forward; `OrIterator`/`ReverseOrIterator`
re-seek all children per seek; `NotIterator` re-seeks its excluded child on
every `SeekGE` — including after the child reported done — and catches it up
with `Next()` as the universe advances. Any latch or consuming seek in a leaf
turns these algebra steps into silent row drops.

One leaf is exempt by construction: `RangeIterator` emits rows in
`(value, entity)` order across index-value buckets, so an entity-space
`SeekGE` is undefined on the raw scan. It only supports forward draining;
every construction site materializes it into a sorted `SliceIterator` before
composing, and a direct `SeekGE` call fails the query with an invariant
error.

The contract is enforced by unit tests per leaf (`iterator_floor_test.go`,
`iterator_address_test.go`, `iterator_and_seek_test.go`) and end-to-end by the
contradiction specs in
`tests/e2e/business/filter_nested_not_reposition_test.go`.

## The exhaustion-proof cache (`seekFloor`/`seekCeil`)

`iterator_floor.go`. Without the latch, an exhausted child would be re-seeked
by its composite parent once per merge step — a fresh Pebble seek plus
allocations, O(rows) times per query. The floor restores the O(1) fast path
without reintroducing the latch:

- a **cleanly** failed `SeekGE(t)` proves *no entity >= t exists in the view*;
  the floor records `t` and every later seek at or above it returns false in
  one comparison. `seekCeil` mirrors this for `SeekLE` (*no entity <= t*).
- a seek below the floor (above the ceil) is not covered and repositions
  normally — the contract above is preserved.

Two preconditions make the proof permanent, and both are load-bearing:

1. **The view is snapshot-fixed.** Every leaf holds a `pebble.Iterator`, whose
   view is fixed at creation; iterators are created per query. A proof can
   therefore never go stale, and the bound is never cleared. Handing these
   iterators a live, mutating view would silently violate this.
2. **Only clean exhaustion proves anything.** `SeekGE` also returns false on
   I/O error (`Err()`), and an I/O-failed seek proves nothing about the view's
   contents. `seekFloor.fail`/`seekCeil.fail` take the iterator's storage
   error and drop the proof when it is non-nil. (Pebble's error is sticky and
   pagination propagates `Err()` unconditionally, so the query still fails
   loudly either way — the guard keeps the cache sound on its own terms
   rather than by leaning on that second-order property.)
