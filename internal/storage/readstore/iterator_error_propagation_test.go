package readstore

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// failingIter simulates the production-time race where the underlying
// Pebble iterator surfaces an I/O error (block checksum failure, blob
// read error, transient I/O) mid-iteration. Next() returns false after
// the configured number of rows and Err() then returns the sentinel.
// This mirrors how the real leaf iterators behave once the Err()
// contract is honored (#320).
type failingIter struct {
	rows [][]byte
	idx  int
	// failAt is the call number of Next() that triggers the error and
	// returns false. 0 means "fail immediately, before yielding any row".
	failAt int
	err    error
	calls  int
}

func (it *failingIter) Next() bool {
	it.calls++

	if it.calls == it.failAt {
		it.err = errors.New("simulated pebble I/O error")

		return false
	}

	if it.idx >= len(it.rows) {
		return false
	}

	it.idx++

	return true
}

func (it *failingIter) Current() []byte {
	if it.idx == 0 || it.idx > len(it.rows) {
		return nil
	}

	return it.rows[it.idx-1]
}

func (it *failingIter) SeekGE(_ []byte) bool { return it.Next() }

func (it *failingIter) Err() error { return it.err }

func (it *failingIter) Close() {}

var _ EntityIterator = (*failingIter)(nil)

// TestPaginateForward_PropagatesIteratorError pins the fix for #320.
// Before this PR PaginateForward returned (items, hasMore) and silently
// treated any iterator stoppage as clean exhaustion. A mid-page I/O
// error therefore produced a short, plausible-looking page with
// hasMore=false. Now Paginate surfaces iter.Err() so the API can
// return a 5xx instead of a truncated balance/transaction list.
func TestPaginateForward_PropagatesIteratorError(t *testing.T) {
	t.Parallel()

	it := &failingIter{
		rows:   [][]byte{{0x01}, {0x02}},
		failAt: 2, // after yielding row 1, fail on the next Next()
	}

	items, hasMore, err := PaginateForward(it, 10, nil)
	require.Error(t, err, "Pebble iterator error must surface through PaginateForward (#320)")
	require.False(t, hasMore)

	// We may still get the rows yielded before the failure — what matters
	// is that the error is reported so callers do not treat the truncated
	// page as a complete result.
	require.LessOrEqual(t, len(items), 1)
}

// failingReverseIter is the descending-order analogue.
type failingReverseIter struct {
	rows   [][]byte
	idx    int
	failAt int
	err    error
	calls  int
}

func (it *failingReverseIter) Next() bool {
	it.calls++

	if it.calls == it.failAt {
		it.err = errors.New("simulated pebble I/O error")

		return false
	}

	if it.idx >= len(it.rows) {
		return false
	}

	it.idx++

	return true
}

func (it *failingReverseIter) Current() []byte {
	if it.idx == 0 || it.idx > len(it.rows) {
		return nil
	}

	return it.rows[it.idx-1]
}

func (it *failingReverseIter) SeekLE(_ []byte) bool { return it.Next() }

func (it *failingReverseIter) Err() error { return it.err }

var _ ReverseIterator = (*failingReverseIter)(nil)

func TestPaginateReverse_PropagatesIteratorError(t *testing.T) {
	t.Parallel()

	it := &failingReverseIter{
		rows:   [][]byte{{0x02}, {0x01}},
		failAt: 2,
	}

	items, hasMore, err := PaginateReverse(it, 10, nil)
	require.Error(t, err)
	require.False(t, hasMore)
	require.LessOrEqual(t, len(items), 1)
}

// TestPaginateForward_NoErrorOnCleanExhaustion is the negative control:
// a clean exhaustion (no I/O error) must continue to return err=nil.
func TestPaginateForward_NoErrorOnCleanExhaustion(t *testing.T) {
	t.Parallel()

	it := &failingIter{rows: [][]byte{{0x01}, {0x02}}}

	items, hasMore, err := PaginateForward(it, 10, nil)
	require.NoError(t, err)
	require.False(t, hasMore)
	require.Len(t, items, 2)
}
