package readstore

import "bytes"

// Direction is the traversal order of an Iterator. It is a sealed set: the
// unexported method means Asc and Desc are the only implementations, so a
// direction is always one of the two the read path understands.
//
// A Direction is a zero-size value carried as a type parameter, so the
// comparator it supplies is resolved when the composite is constructed and the
// iterator itself pays no per-row dispatch.
type Direction interface {
	// compare orders a and b ALONG THE DIRECTION OF TRAVEL: negative when a
	// comes first, positive when b does. Ascending it is byte order;
	// descending it is byte order reversed.
	//
	// Every generic combinator is written against this one comparison, which
	// is what makes the ascending and descending algebras the same code
	// rather than two implementations kept in step by hand (EN-1966).
	compare(a, b []byte) int
}

// Asc traverses entities in ascending byte order.
type Asc struct{}

// Desc traverses entities in descending byte order.
type Desc struct{}

func (Asc) compare(a, b []byte) int  { return bytes.Compare(a, b) }
func (Desc) compare(a, b []byte) int { return bytes.Compare(b, a) }

// comparator returns D's ordering function. Callers hold the result for the
// iterator's lifetime rather than re-deriving it per row.
func comparator[D Direction]() func(a, b []byte) int {
	var d D

	return d.compare
}

// Iterator iterates over entity IDs (account addresses or transaction IDs as
// raw bytes) in D's order.
//
// Direction is a type parameter and not a runtime flag on purpose. It keeps
// the ascending and descending contracts distinct types, so an ascending
// consumer cannot be handed a descending iterator — the guard the separate
// SeekGE/SeekLE method names used to provide before the two algebras were
// merged (EN-1966).
type Iterator[D Direction] interface {
	// Next advances to the next entity. Returns false when exhausted OR when
	// the underlying storage reported an error. Callers MUST call Err() after
	// the loop terminates to distinguish clean exhaustion from a Pebble I/O
	// failure (block checksum, blob-file read error, transient I/O). Without
	// the check a corrupted SST returns a shorter, plausible-looking page
	// rather than an error (#320).
	Next() bool

	// Current returns the current entity ID. The returned slice is only
	// valid until the next call to Next or Seek.
	Current() []byte

	// Seek positions the iterator at the first entity at or after target in
	// D's order: the smallest entity >= target ascending, the largest entity
	// <= target descending. Returns false if no such entity exists OR on I/O
	// error — see Err().
	//
	// Seek is an ABSOLUTE reposition (see
	// docs/technical/architecture/subsystems/read-path/iterator-seek-contract.md):
	//   - the result is computed from target alone, never from the iterator's
	//     position, distance travelled, or exhaustion state;
	//   - it is idempotent: repeating Seek with the same target yields the
	//     same entity, and must not consume it;
	//   - it is well-defined after exhaustion (a false Next/Seek) — a later
	//     seek to a target further back repositions normally;
	//   - a failed seek leaves the iterator un-positioned (Next returns false)
	//     but still re-seekable.
	//
	// Composite iterators (AND/OR/NOT) re-seek children freely under this
	// contract; a latch or a consuming seek silently drops rows (EN-1597).
	Seek(target []byte) bool

	// Err returns the first storage error encountered during iteration, or
	// nil for clean exhaustion. Callers MUST consult Err after Next/Seek
	// returns false.
	Err() error

	// Close releases resources held by this iterator.
	Close()

	// Direction is a compile-time witness, never called. It exists because
	// Go decides interface satisfaction STRUCTURALLY: with D absent from
	// every method signature, Iterator[Asc] and Iterator[Desc] would have
	// identical method sets and be freely interchangeable, so
	// PaginateReverse(anAscendingIterator) would compile and silently return
	// a page in the wrong order. Naming D in a result type is what makes the
	// two interfaces distinct types, which is the guard the separate
	// SeekGE/SeekLE method names used to provide.
	//
	// Implementations return the zero value: `func (*T) Direction() (d Asc)
	// { return }`, or `func (t *T[D]) Direction() (d D) { return }` for a
	// direction-parameterized one.
	Direction() D
}

// EntityIterator is the ascending read-path iterator.
type EntityIterator = Iterator[Asc]

// ReverseIterator is the descending read-path iterator.
type ReverseIterator = Iterator[Desc]
