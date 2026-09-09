package readstore

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Forward/reverse conformance suite (EN-1966).
//
// Every iterator that gained a descending twin is registered here as a PAIR,
// and the suite drives both halves over the same fixture. The registry is the
// point: a reverse leaf added later is checked by construction, instead of
// depending on whoever writes it remembering to hand-write the same four
// tests.
//
// The gate-parity case (TestIteratorPairs_GateParity) is the one that earns
// the suite. A whole-set parity test cannot catch a missing visibility gate:
// it compares the two directions against the SAME pinned view, so a gate
// absent from both halves still agrees and the test stays green. Only
// comparing each direction against the independently known expected set at a
// pin separates them.

// iterPair builds both directions over one fixture. want is the expected
// ascending entity list; the reverse half must yield exactly its reverse.
type iterPair struct {
	name    string
	build   func(t *testing.T) (EntityIterator, ReverseIterator)
	want    []string
	entityB func(string) []byte // entity string -> seek target bytes
	// render turns a raw entity back into its want-list spelling. Fixed-width
	// numeric entities are stored big-endian, so a raw string compare against
	// a decimal want list would compare the wrong things.
	render func([]byte) string
}

func stringEntity(s string) []byte { return []byte(s) }

func renderString(b []byte) string { return string(b) }

func renderTx(b []byte) string { return strconv.FormatUint(binary.BigEndian.Uint64(b), 10) }

// txEntity encodes a decimal transaction id the way the tx leaves do.
func txEntity(s string) []byte {
	var id uint64

	_, err := fmt.Sscanf(s, "%d", &id)
	if err != nil {
		panic(err)
	}

	return EncodeTxID(nil, id)
}

// seedPrefixRows writes value-less rows under a metadata index prefix so both
// prefix iterators scan the same keyspace.
func seedPrefixRows(t *testing.T, entities ...string) (*Store, []byte, int) {
	t.Helper()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	prefix := append([]byte(nil), MetadataIndexPrefixV(kb, "l", NamespaceAccount, "k", 1)...)
	prefix = append(prefix, []byte("v")...)

	for _, e := range entities {
		key := append(append([]byte(nil), prefix...), []byte(e)...)
		require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
	}

	return s, prefix, len(prefix)
}

func conformancePairs() []iterPair {
	return []iterPair{
		{
			name: "PrefixIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				s, prefix, off := seedPrefixRows(t, "a", "b", "c", "d")

				fwd, err := NewPrefixIterator(s.DB(), prefix, off, 0)
				require.NoError(t, err)
				t.Cleanup(fwd.Close)

				rev, err := NewReversePrefixIterator(s.DB(), prefix, off, 0)
				require.NoError(t, err)
				t.Cleanup(rev.Close)

				return fwd, rev
			},
			want:    []string{"a", "b", "c", "d"},
			entityB: stringEntity,
			render:  renderString,
		},
		{
			name: "EventResolveIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				s, prefix := eventFixture(t, "v",
					ev{"a", 5, MetadataEventAdd},
					ev{"b", 6, MetadataEventAdd},
					ev{"c", 7, MetadataEventAdd},
					// d was added then removed below the pin: dead either way.
					ev{"d", 8, MetadataEventAdd},
					ev{"d", 9, MetadataEventDel},
				)

				fwd, err := NewEventResolveIterator(s.DB(), prefix, 100)
				require.NoError(t, err)
				t.Cleanup(fwd.Close)

				rev, err := NewReverseEventResolveIterator(s.DB(), prefix, 100)
				require.NoError(t, err)
				t.Cleanup(rev.Close)

				return fwd, rev
			},
			want:    []string{"a", "b", "c"},
			entityB: stringEntity,
			render:  renderString,
		},
		{
			name: "AddressTxIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				s := newTestStore(t)
				kb := dal.NewKeyBuilder()

				for _, id := range []uint64{2, 4, 6} {
					require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", "acc:1", id), nil, pebble.NoSync))
				}

				fwd := NewAddressTxIterator(s.DB(), dal.NewKeyBuilder(), "l",
					newAliasingIter("acc:1"), PrefixAccountTx)
				t.Cleanup(fwd.Close)

				rev := NewReverseAddressTxIterator(s.DB(), dal.NewKeyBuilder(), "l",
					newAliasingIter("acc:1"), PrefixAccountTx)
				t.Cleanup(rev.Close)

				return fwd, rev
			},
			want:    []string{"2", "4", "6"},
			entityB: txEntity,
			render:  renderTx,
		},
		{
			name: "SliceIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				entities := [][]byte{[]byte("a"), []byte("b"), []byte("c")}

				return newAliasingIter("a", "b", "c"), NewReverseSliceIterator(entities)
			},
			want:    []string{"a", "b", "c"},
			entityB: stringEntity,
			render:  renderString,
		},
		{
			name: "BitsetIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				bs := &bitset.Bitset{}
				// Spread across word boundaries: 1, 63, 64, 200.
				for _, b := range []uint64{1, 63, 64, 200} {
					bs.Set(b)
				}

				return NewBitsetIterator(bs), NewReverseBitsetIterator(bs)
			},
			want:    []string{"1", "63", "64", "200"},
			entityB: txEntity,
			render:  renderTx,
		},
		{
			name: "OrIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				return NewOrIterator(newAliasingIter("a", "c"), newAliasingIter("b", "c")),
					NewReverseOrIterator(newReverseAliasingIter("a", "c"), newReverseAliasingIter("b", "c"))
			},
			want:    []string{"a", "b", "c"},
			entityB: stringEntity,
			render:  renderString,
		},
		{
			name: "AndIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				return NewAndIterator(newAliasingIter("a", "b", "c", "d"), newAliasingIter("b", "c", "e")),
					NewReverseAndIterator(newReverseAliasingIter("a", "b", "c", "d"), newReverseAliasingIter("b", "c", "e"))
			},
			want:    []string{"b", "c"},
			entityB: stringEntity,
			render:  renderString,
		},
		{
			name: "NotIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				return NewNotIterator(newAliasingIter("a", "b", "c", "d"), newAliasingIter("b", "d")),
					NewReverseNotIterator(newReverseAliasingIter("a", "b", "c", "d"), newReverseAliasingIter("b", "d"))
			},
			want:    []string{"a", "c"},
			entityB: stringEntity,
			render:  renderString,
		},
		{
			name: "FilterIterator",
			build: func(t *testing.T) (EntityIterator, ReverseIterator) {
				keep := func(e []byte) (bool, error) { return string(e) != "b", nil }

				return NewFilterIterator(newAliasingIter("a", "b", "c"), keep),
					NewFilterReverseIterator(newReverseAliasingIter("a", "b", "c"), keep)
			},
			want:    []string{"a", "c"},
			entityB: stringEntity,
			render:  renderString,
		},
	}
}

// drainRendered walks an iterator to exhaustion and renders each entity. One
// implementation for both directions: draining is the same loop either way,
// over the direction-agnostic nextable that collectPage also uses. It differs
// from drain (combinator_direction_test.go) only in returning the entities
// individually through a render hook, which fixed-width numeric entities need
// — a raw string compare against a decimal want list compares the wrong
// bytes.
func drainRendered(it nextable, render func([]byte) string) []string {
	var out []string
	for it.Next() {
		out = append(out, render(it.Current()))
	}

	return out
}

// TestIteratorPairs_SetParity: the reverse drain is the forward drain
// reversed, and both equal the declared expectation. The declared expectation
// matters — comparing the two drains to each other alone would pass for a pair
// that agrees on the wrong answer.
func TestIteratorPairs_SetParity(t *testing.T) {
	t.Parallel()

	for _, p := range conformancePairs() {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			fwd, rev := p.build(t)

			gotFwd := drainRendered(fwd, p.render)
			require.NoError(t, fwd.Err())
			require.Equal(t, p.want, gotFwd, "ascending drain")

			wantRev := slices.Clone(p.want)
			slices.Reverse(wantRev)

			gotRev := drainRendered(rev, p.render)
			require.NoError(t, rev.Err())
			require.Equal(t, wantRev, gotRev, "descending drain must be the ascending drain reversed")
		})
	}
}

// TestIteratorPairs_ReverseSeekContract drives the absolute-seek contract from
// docs/technical/architecture/subsystems/read-path/iterator-seek-contract.md
// against every reverse half: idempotent, non-consuming at the same target,
// well-defined after exhaustion, and re-seekable after a failed seek.
func TestIteratorPairs_ReverseSeekContract(t *testing.T) {
	t.Parallel()

	for _, p := range conformancePairs() {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			require.GreaterOrEqual(t, len(p.want), 2, "fixture needs at least two entities")

			top := p.want[len(p.want)-1]
			bottom := p.want[0]

			_, rev := p.build(t)

			// Idempotent and non-consuming: two seeks to the same target
			// yield the same entity, and the entity is not skipped.
			require.True(t, rev.Seek(p.entityB(top)))
			require.Equal(t, top, p.render(rev.Current()))
			require.True(t, rev.Seek(p.entityB(top)), "repeat seek")
			require.Equal(t, top, p.render(rev.Current()), "seek must not consume")

			// Well-defined after exhaustion: drain, then re-seek.
			for rev.Next() {
			}

			require.NoError(t, rev.Err())
			require.True(t, rev.Seek(p.entityB(top)), "re-seek after exhaustion")
			require.Equal(t, top, p.render(rev.Current()))

			// Re-seekable after a failed seek: a seek below every entity
			// fails, and a later valid seek still repositions.
			_, rev2 := p.build(t)

			below := belowAll(p.entityB(bottom))
			require.False(t, rev2.Seek(below), "seek below the lowest entity must fail")
			require.NoError(t, rev2.Err(), "a clean empty seek is not an error")
			require.True(t, rev2.Seek(p.entityB(top)), "re-seek after a failed seek")
			require.Equal(t, top, p.render(rev2.Current()))
		})
	}
}

// belowAll returns a target strictly below every entity of the same width.
func belowAll(lowest []byte) []byte {
	out := make([]byte, len(lowest))

	// All-zero sorts at or below any entity; for a fixed-width numeric entity
	// that is id 0, which no fixture uses.
	return out
}

// gatedPair is a pair whose visibility depends on a pin. Both halves must make
// the SAME admission decision for a row above the pin.
type gatedPair struct {
	name string
	// build returns both directions gated at pin.
	build func(t *testing.T, pin uint64) (EntityIterator, ReverseIterator)
	// wantAtPin is the ascending expected set at the pin the test uses.
	wantAtPin []string
	pin       uint64
}

func gatedPairs() []gatedPair {
	return []gatedPair{
		{
			name: "StampGatedPrefixIterator",
			build: func(t *testing.T, pin uint64) (EntityIterator, ReverseIterator) {
				s := newTestStore(t)
				kb := dal.NewKeyBuilder()
				prefix := append([]byte(nil), MetadataIndexPrefixV(kb, "l", NamespaceAccount, "k", 1)...)
				prefix = append(prefix, []byte("v")...)

				// a,b are folded at or below the pin; z is folded above it and
				// MUST be invisible in both directions.
				for _, row := range []struct {
					entity string
					stamp  uint64
				}{{"a", 5}, {"b", 10}, {"z", 999}} {
					key := append(append([]byte(nil), prefix...), []byte(row.entity)...)
					require.NoError(t, s.DB().Set(key, EncodeTxID(nil, row.stamp), pebble.NoSync))
				}

				fwd, err := NewStampGatedPrefixIterator(s.DB(), prefix, len(prefix), 0, pin)
				require.NoError(t, err)
				t.Cleanup(fwd.Close)

				rev, err := NewStampGatedReversePrefixIterator(s.DB(), prefix, len(prefix), 0, pin)
				require.NoError(t, err)
				t.Cleanup(rev.Close)

				return fwd, rev
			},
			wantAtPin: []string{"a", "b"},
			pin:       10,
		},
		{
			name: "EventResolveAtPin",
			build: func(t *testing.T, pin uint64) (EntityIterator, ReverseIterator) {
				s, prefix := eventFixture(t, "v",
					ev{"a", 5, MetadataEventAdd},
					// b is ADDed above the pin: invisible at pin=10.
					ev{"b", 50, MetadataEventAdd},
					// c is ADDed below and DELeted above: still visible at pin=10.
					ev{"c", 6, MetadataEventAdd},
					ev{"c", 60, MetadataEventDel},
					// d is ADDed then DELeted below the pin: invisible.
					ev{"d", 3, MetadataEventAdd},
					ev{"d", 4, MetadataEventDel},
				)

				fwd, err := NewEventResolveIterator(s.DB(), prefix, pin)
				require.NoError(t, err)
				t.Cleanup(fwd.Close)

				rev, err := NewReverseEventResolveIterator(s.DB(), prefix, pin)
				require.NoError(t, err)
				t.Cleanup(rev.Close)

				return fwd, rev
			},
			wantAtPin: []string{"a", "c"},
			pin:       10,
		},
	}
}

// TestIteratorPairs_GateParity is the case a whole-set parity test cannot
// cover. Both directions are compared against the INDEPENDENTLY declared set
// at the pin, so a gate missing from one direction — or from both — fails
// here. Removing the gate from ReversePrefixIterator must break this test;
// that is the check that keeps the registry honest.
func TestIteratorPairs_GateParity(t *testing.T) {
	t.Parallel()

	for _, p := range gatedPairs() {
		t.Run(p.name, func(t *testing.T) {
			t.Parallel()

			fwd, rev := p.build(t, p.pin)

			gotFwd := drainRendered(fwd, renderString)
			require.NoError(t, fwd.Err())
			require.Equal(t, p.wantAtPin, gotFwd,
				"ascending must hide every row above the pin")

			wantRev := slices.Clone(p.wantAtPin)
			slices.Reverse(wantRev)

			gotRev := drainRendered(rev, renderString)
			require.NoError(t, rev.Err())
			require.Equal(t, wantRev, gotRev,
				"descending must hide exactly the rows ascending hides — a gate on only one side is a direction-dependent visibility bug")
		})
	}
}

// TestReverseComposites_PropagateChildError: a storage fault under a reverse
// composite must surface through Err() rather than read as clean exhaustion
// (#320), for every composite that owns children.
func TestReverseComposites_PropagateChildError(t *testing.T) {
	t.Parallel()

	newFailing := func() *failingReverseIter {
		return &failingReverseIter{rows: [][]byte{[]byte("b"), []byte("a")}, failAt: 2}
	}

	cases := []struct {
		name string
		iter ReverseIterator
	}{
		{"ReverseOr", NewReverseOrIterator(newFailing(), newReverseAliasingIter("a", "b"))},
		{"ReverseAnd", NewReverseAndIterator(newFailing(), newReverseAliasingIter("a", "b"))},
		{"ReverseNot", NewReverseNotIterator(newFailing(), newReverseAliasingIter("a"))},
		{"FilterReverse", NewFilterReverseIterator(newFailing(), func([]byte) (bool, error) { return true, nil })},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			for c.iter.Next() {
			}

			require.Error(t, c.iter.Err(),
				"a child I/O fault must surface, not read as exhaustion")
		})
	}
}

// TestReverseNotIterator_SeekRepositionsConsumedChild is the descending
// EN-1597 regression, and it needs its own test: the paged parity oracle
// rebuilds the iterator tree for every page, so it never re-seeks a child that
// forward iteration already consumed, and a latched childDone survives it
// green.
//
// The shape that bites in production is `reverted=false` — NOT(universe,
// reversion bitset) — nested under an AND that re-seeks it. Draining the NOT
// exhausts the finite bitset child; a later absolute Seek must re-seek that
// child, or the NOT can no longer tell that the entity at target is excluded
// and leaks it into the difference.
func TestReverseNotIterator_SeekRepositionsConsumedChild(t *testing.T) {
	t.Parallel()

	universe := newReverseAliasingIter("a", "b", "c", "d")
	excluded := newReverseAliasingIter("b", "d")

	it := NewReverseNotIterator(universe, excluded)
	defer it.Close()

	// Forward pass: difference is {c, a}; the child is consumed on the way.
	require.Equal(t, []string{"c", "a"}, drainRendered(it, renderString))
	require.NoError(t, it.Err())

	// Absolute re-seek onto an EXCLUDED entity. With the child left latched
	// as done, the NOT would emit "d"; correct behaviour re-seeks the child,
	// sees the exclusion, and falls through to "c".
	require.True(t, it.Seek([]byte("d")), "reposition after exhaustion")
	require.Equal(t, "c", string(it.Current()),
		"a consumed child must be re-seeked: \"d\" is excluded and must not leak into the difference")

	require.True(t, it.Seek([]byte("b")), "second backward seek onto an excluded entity")
	require.Equal(t, "a", string(it.Current()))
	require.NoError(t, it.Err())
}

// TestReverseAndIterator_OverNotWithConsumedChild drives the same rule through
// a composite, which is how it is reached in a compiled plan: the AND's
// converge loop seeks the NOT repeatedly, and each seek must reach the NOT's
// own child.
func TestReverseAndIterator_OverNotWithConsumedChild(t *testing.T) {
	t.Parallel()

	// not = {a, c} (universe {a..d} minus {b, d}); other = {c, d}.
	// The intersection is therefore exactly {c}.
	not := NewReverseNotIterator(
		newReverseAliasingIter("a", "b", "c", "d"),
		newReverseAliasingIter("b", "d"),
	)
	other := newReverseAliasingIter("c", "d")

	it := NewReverseAndIterator(not, other)
	defer it.Close()

	require.Equal(t, []string{"c"}, drainRendered(it, renderString),
		"AND over a NOT must not admit an entity the NOT excludes")
	require.NoError(t, it.Err())
}
