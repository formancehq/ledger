package oracle

import (
	"crypto/sha256"
	"encoding/binary"
	"iter"
	"math/bits"

	"github.com/benbjohnson/immutable"
	"github.com/holiman/uint256"
)

// Persistent, fingerprinted collections backing the model's state.
//
// The serialization search (candidateBases in the driver) forks a state per
// hypothesized commit order, so both forking and the dedup identity must be
// cheap. Map and List are immutable: a mutation returns a new value sharing
// structure with the receiver, so a fork is O(1) and a mutation O(log n).
// Each collection also maintains a 128-bit multiset fingerprint of its entries
// — the sum of a sha256-derived term per entry, mod 2^128 — so a whole state's
// dedup key is read off in O(1) instead of re-rendering every entry.
//
// Soundness: two collections holding the same entries always have the same
// fingerprint (the sum is order-independent); differing collections collide
// only if sha256-derived 128-bit terms happen to cancel, which is infeasible
// for the model's non-adversarial inputs. Terms must be pure functions of
// (key, value): anything reachable from a stored value must be replaced, never
// mutated in place, or the running sum goes stale.

// Digest is a 128-bit multiset fingerprint. The zero Digest is the fingerprint
// of an empty collection.
type Digest struct{ hi, lo uint64 }

func (d Digest) add(t Digest) Digest {
	lo, carry := bits.Add64(d.lo, t.lo, 0)
	hi, _ := bits.Add64(d.hi, t.hi, carry)

	return Digest{hi: hi, lo: lo}
}

func (d Digest) sub(t Digest) Digest {
	lo, borrow := bits.Sub64(d.lo, t.lo, 0)
	hi, _ := bits.Sub64(d.hi, t.hi, borrow)

	return Digest{hi: hi, lo: lo}
}

// termBuilder encodes one entry into an injective byte string — every field is
// length- or count-prefixed, so distinct field sequences can never render the
// same bytes — and hashes it to the entry's 128-bit term.
type termBuilder struct{ buf []byte }

// newTerm starts a term with a domain tag, keeping entries of different
// collections (volumes vs metadata vs ...) in disjoint term spaces.
func newTerm(tag string) termBuilder {
	t := termBuilder{buf: make([]byte, 0, 128)}
	t.str(tag)

	return t
}

func (t *termBuilder) str(ss ...string) {
	for _, s := range ss {
		t.buf = binary.BigEndian.AppendUint32(t.buf, uint32(len(s)))
		t.buf = append(t.buf, s...)
	}
}

func (t *termBuilder) u64(vs ...uint64) {
	for _, v := range vs {
		t.buf = binary.BigEndian.AppendUint64(t.buf, v)
	}
}

func (t *termBuilder) boolean(b bool) {
	if b {
		t.buf = append(t.buf, 1)
	} else {
		t.buf = append(t.buf, 0)
	}
}

func (t *termBuilder) u256(v *uint256.Int) {
	b := v.Bytes32()
	t.buf = append(t.buf, b[:]...)
}

func (t *termBuilder) digest(d Digest) {
	t.u64(d.hi, d.lo)
}

func (t *termBuilder) sum() Digest {
	s := sha256.Sum256(t.buf)

	return Digest{
		hi: binary.BigEndian.Uint64(s[0:8]),
		lo: binary.BigEndian.Uint64(s[8:16]),
	}
}

// Map is an immutable key-sorted map carrying the multiset fingerprint of its
// entries. The zero Map is unusable — construct with NewMap.
type Map[K, V any] struct {
	tree *immutable.SortedMap[K, V]
	fp   Digest
	term func(K, V) Digest
}

func NewMap[K, V any](cmp immutable.Comparer[K], term func(K, V) Digest) Map[K, V] {
	return Map[K, V]{tree: immutable.NewSortedMap[K, V](cmp), term: term}
}

func (m Map[K, V]) Get(k K) (V, bool) { return m.tree.Get(k) }

func (m Map[K, V]) Has(k K) bool {
	_, ok := m.tree.Get(k)

	return ok
}

func (m Map[K, V]) Len() int { return m.tree.Len() }

// Set replaces k's entry; an existing entry's term leaves the fingerprint as
// the new one enters.
func (m Map[K, V]) Set(k K, v V) Map[K, V] {
	if old, ok := m.tree.Get(k); ok {
		m.fp = m.fp.sub(m.term(k, old))
	}
	m.fp = m.fp.add(m.term(k, v))
	m.tree = m.tree.Set(k, v)

	return m
}

// Delete removes k's entry; deleting an absent key is a no-op.
func (m Map[K, V]) Delete(k K) Map[K, V] {
	old, ok := m.tree.Get(k)
	if !ok {
		return m
	}
	m.fp = m.fp.sub(m.term(k, old))
	m.tree = m.tree.Delete(k)

	return m
}

func (m Map[K, V]) Fingerprint() Digest { return m.fp }

// All iterates entries in key order — deterministic, unlike a Go map, so
// callers may draw Antithesis-reproducible decisions while ranging.
func (m Map[K, V]) All() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		itr := m.tree.Iterator()
		for !itr.Done() {
			k, v, _ := itr.Next()
			if !yield(k, v) {
				return
			}
		}
	}
}

// From iterates entries in key order starting at the first key >= k. Seeking
// to a randomly drawn key gives an O(log n) near-uniform random pick — the
// generator's alternative to collecting every entry.
func (m Map[K, V]) From(k K) iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		itr := m.tree.Iterator()
		itr.Seek(k)
		for !itr.Done() {
			k, v, _ := itr.Next()
			if !yield(k, v) {
				return
			}
		}
	}
}

// List is an immutable dense list carrying the multiset fingerprint of its
// (index, value) entries. The zero List is unusable — construct with NewList.
type List[V any] struct {
	list *immutable.List[V]
	fp   Digest
	term func(int, V) Digest
}

func NewList[V any](term func(int, V) Digest) List[V] {
	return List[V]{list: immutable.NewList[V](), term: term}
}

func (l List[V]) Get(i int) V { return l.list.Get(i) }

func (l List[V]) Len() int { return l.list.Len() }

func (l List[V]) Append(v V) List[V] {
	l.fp = l.fp.add(l.term(l.list.Len(), v))
	l.list = l.list.Append(v)

	return l
}

func (l List[V]) Set(i int, v V) List[V] {
	l.fp = l.fp.sub(l.term(i, l.list.Get(i))).add(l.term(i, v))
	l.list = l.list.Set(i, v)

	return l
}

// PopFront returns the list without its first entry. Terms are keyed on the
// index, so dropping the head shifts every remaining entry and the sum is
// re-folded rather than adjusted — it stays a pure function of the resulting
// contents, so a list reached by different routes fingerprints alike.
func (l List[V]) PopFront() List[V] {
	l.list = l.list.Slice(1, l.list.Len())

	l.fp = Digest{}
	for i, v := range l.All() {
		l.fp = l.fp.add(l.term(i, v))
	}

	return l
}

func (l List[V]) Fingerprint() Digest { return l.fp }

// All iterates entries in index order.
func (l List[V]) All() iter.Seq2[int, V] {
	return func(yield func(int, V) bool) {
		itr := l.list.Iterator()
		for !itr.Done() {
			i, v := itr.Next()
			if !yield(i, v) {
				return
			}
		}
	}
}

// Comparers for the model's key types.

type stringComparer struct{}

func (stringComparer) Compare(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

type volumeKeyComparer struct{}

func (volumeKeyComparer) Compare(a, b VolumeKey) int { return CompareVolumeKey(a, b) }

type metaKeyComparer struct{}

func (metaKeyComparer) Compare(a, b MetaKey) int { return CompareMetaKey(a, b) }
