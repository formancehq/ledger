package readstore

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// aliasingIter mimics the lifetime contract of the real leaf iterators
// (PrefixIterator / RangeIterator): Current() returns a slice that
// aliases a single backing buffer rewritten on each Next(). That is the
// exact source of the OrIterator dedup bug — without an owned copy in
// findMin/findMax, it.current ends up pointing at memory the winning
// child's next positioning call just overwrote (#319).
type aliasingIter struct {
	keys [][]byte
	idx  int
	buf  []byte // single buffer reused across Next() — the aliasing source
}

func newAliasingIter(keys ...string) *aliasingIter {
	out := &aliasingIter{idx: -1}
	for _, k := range keys {
		out.keys = append(out.keys, []byte(k))
	}

	return out
}

func (it *aliasingIter) Next() bool {
	it.idx++
	if it.idx >= len(it.keys) {
		return false
	}

	it.buf = append(it.buf[:0], it.keys[it.idx]...)

	return true
}

func (it *aliasingIter) Current() []byte { return it.buf }

func (it *aliasingIter) SeekGE(target []byte) bool {
	for i, k := range it.keys {
		if string(k) >= string(target) {
			it.idx = i
			it.buf = append(it.buf[:0], k...)

			return true
		}
	}

	it.idx = len(it.keys)

	return false
}

func (it *aliasingIter) Close() {}

// Err satisfies the EntityIterator contract introduced by this PR. The
// test driver never injects an iterator error, so always returning nil
// keeps the OR-iterator alias-safety invariant the only thing under test.
func (it *aliasingIter) Err() error { return nil }

// TestOrIterator_DedupSurvivesAliasedChildBuffers pins the fix for #319.
// Pre-fix, OrIterator.findMin stored an alias into the winning child's
// Pebble-key buffer. On the next Next() the dedup loop advanced that
// child, the buffer was overwritten, and the subsequent bytes.Equal
// comparisons ran against the NEW key — so duplicate-holding siblings
// were not advanced and the merge emitted the same entity twice.
//
// We reproduce with two children that both yield "alpha". With the
// fix the union must be exactly {alpha, bravo, charlie}; without the
// fix "alpha" is emitted twice.
func TestOrIterator_DedupSurvivesAliasedChildBuffers(t *testing.T) {
	t.Parallel()

	left := newAliasingIter("alpha", "bravo")
	right := newAliasingIter("alpha", "charlie")

	it := NewOrIterator(left, right)
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}

	require.Equal(t, []string{"alpha", "bravo", "charlie"}, got,
		"OR merge must deduplicate even when child Current() aliases reused buffers (#319)")
}

// reverseAliasingIter is the reverse-order analogue of aliasingIter.
type reverseAliasingIter struct {
	keys [][]byte
	idx  int
	buf  []byte
}

func newReverseAliasingIter(keys ...string) *reverseAliasingIter {
	out := &reverseAliasingIter{idx: len(keys)}
	for _, k := range keys {
		out.keys = append(out.keys, []byte(k))
	}

	return out
}

func (it *reverseAliasingIter) Next() bool {
	it.idx--
	if it.idx < 0 {
		return false
	}

	it.buf = append(it.buf[:0], it.keys[it.idx]...)

	return true
}

func (it *reverseAliasingIter) Current() []byte { return it.buf }

func (it *reverseAliasingIter) SeekLE(target []byte) bool {
	for i, v := range slices.Backward(it.keys) {
		if string(v) <= string(target) {
			it.idx = i
			it.buf = append(it.buf[:0], v...)

			return true
		}
	}

	it.idx = -1

	return false
}

func (it *reverseAliasingIter) Close() {}

// Err satisfies the ReverseIterator contract introduced by this PR. See
// aliasingIter.Err for the rationale.
func (it *reverseAliasingIter) Err() error { return nil }

// TestReverseOrIterator_DedupSurvivesAliasedChildBuffers is the
// descending-order twin of TestOrIterator_DedupSurvivesAliasedChildBuffers
// (same root cause, same fix).
func TestReverseOrIterator_DedupSurvivesAliasedChildBuffers(t *testing.T) {
	t.Parallel()

	// Keys must be passed in ascending order so the reverse iterator
	// walks them high-to-low.
	left := newReverseAliasingIter("alpha", "charlie")
	right := newReverseAliasingIter("bravo", "charlie")

	it := NewReverseOrIterator(left, right)
	defer it.Close()

	var got []string
	for it.Next() {
		got = append(got, string(it.Current()))
	}

	require.Equal(t, []string{"charlie", "bravo", "alpha"}, got,
		"reverse OR merge must deduplicate even when child Current() aliases reused buffers (#319)")
}
