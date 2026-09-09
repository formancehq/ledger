package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func txIDBytes(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)

	return b
}

// newAddressTxFixture writes account→tx rows in the any-role bucket and
// returns an AddressTxIterator over the given addresses.
func newAddressTxFixture(t *testing.T, txsByAccount map[string][]uint64, addrs ...string) *AddressTxIterator {
	t.Helper()

	return newAddressTxFixtureForPrefix(t, PrefixAccountTx, txsByAccount, addrs...)
}

// newAddressTxFixtureForPrefix is newAddressTxFixture over an explicit
// account→tx bucket, so tests can cover every address role reached from
// addressRolePrefix (source, destination, any).
func newAddressTxFixtureForPrefix(
	tb testing.TB,
	prefix byte,
	txsByAccount map[string][]uint64,
	addrs ...string,
) *AddressTxIterator {
	tb.Helper()

	s := newTestStore(tb)
	kb := dal.NewKeyBuilder()

	for account, txs := range txsByAccount {
		for _, id := range txs {
			require.NoError(tb, s.DB().Set(AccountTxKey(kb, prefix, "l", account, id), nil, pebble.NoSync))
		}
	}

	return NewAddressTxIterator(s.DB(), dal.NewKeyBuilder(), "l", newAliasingIter(addrs...), prefix)
}

// drainIDs consumes the iterator and returns the decoded transaction IDs.
func drainIDs(tb testing.TB, it *AddressTxIterator) []uint64 {
	tb.Helper()

	var got []uint64
	for it.Next() {
		got = append(got, binary.BigEndian.Uint64(it.Current()))
	}

	require.NoError(tb, it.Err())

	return got
}

// Seek on AddressTxIterator must be an absolute reposition over the
// materialized union: repeatable at the same target, seekable backwards, and
// well-defined after exhaustion (EN-1597, paul-nicolas review of PR #1635).
// The prior implementation consumed the matched entry (`pendingTxns[idx+1:]`)
// and latched on exhaustion, so a repeated or backward seek dropped rows.
func TestAddressTxIterator_SeekIsAbsolute(t *testing.T) {
	t.Parallel()

	it := newAddressTxFixture(t, map[string][]uint64{
		"acc:1": {1, 3},
		"acc:2": {2, 3}, // tx 3 shared — union must deduplicate
	}, "acc:1", "acc:2")
	defer it.Close()

	// Full forward pass: the deduplicated union in order.
	var got []uint64
	for it.Next() {
		got = append(got, binary.BigEndian.Uint64(it.Current()))
	}
	require.Equal(t, []uint64{1, 2, 3}, got)
	require.NoError(t, it.Err())

	// Reposition after exhaustion.
	require.True(t, it.Seek(txIDBytes(2)))
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))

	// Same target again: same row (a conforming seek does not consume).
	require.True(t, it.Seek(txIDBytes(2)))
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))

	// Next continues from the seeked position.
	require.True(t, it.Next())
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))

	// Backward seek below everything.
	require.True(t, it.Seek(txIDBytes(0)))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))

	// Seek past the end fails, then a lower seek succeeds again.
	require.False(t, it.Seek(txIDBytes(99)))
	require.False(t, it.Next())
	require.True(t, it.Seek(txIDBytes(3)))
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

// An AND over an AddressTxIterator must not drop intersections when converge
// re-seeks the child onto its own current position (the destructive-consume
// regression from the unconditional all-children re-seek in AndIterator.Seek).
func TestAndIterator_AddressTxChildKeepsIntersection(t *testing.T) {
	t.Parallel()

	addrTx := newAddressTxFixture(t, map[string][]uint64{
		"acc:1": {1, 2, 3},
	}, "acc:1")

	other := newAliasingIter(
		string(txIDBytes(1)), string(txIDBytes(2)), string(txIDBytes(3)),
	)

	it := NewAndIterator(addrTx, other)
	defer it.Close()

	require.True(t, it.Seek(txIDBytes(1)))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))

	// Re-seek to the same target: the intersection must still start at 1.
	require.True(t, it.Seek(txIDBytes(1)))
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))

	var rest []uint64
	for it.Next() {
		rest = append(rest, binary.BigEndian.Uint64(it.Current()))
	}
	require.Equal(t, []uint64{2, 3}, rest)
	require.NoError(t, it.Err())
}

// A NOT whose child is an AddressTxIterator must keep excluding after the
// child was driven to exhaustion by a forward pass — the child's Seek must
// reposition, not latch. Pre-fix, the exhausted (and consumed) child stopped
// excluding, leaking every excluded row into the difference.
func TestNotIterator_AddressTxChildExcludesAfterExhaustion(t *testing.T) {
	t.Parallel()

	addrTx := newAddressTxFixture(t, map[string][]uint64{
		"acc:1": {1, 2, 3},
	}, "acc:1")

	universe := newAliasingIter(
		string(txIDBytes(1)), string(txIDBytes(2)), string(txIDBytes(3)), string(txIDBytes(4)),
	)

	it := NewNotIterator(universe, addrTx)
	defer it.Close()

	// Forward pass: 1-3 are excluded; reaching 4 drives the child past its
	// last entry, exhausting it.
	require.True(t, it.Next())
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	// The absolute re-seek back to 1 must still exclude 1-3 and land on 4.
	require.True(t, it.Seek(txIDBytes(1)))
	require.Equal(t, uint64(4), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}

func TestAddressTxIterator_EmptyUnion(t *testing.T) {
	t.Parallel()

	it := newAddressTxFixture(t, nil, "acc:1")
	defer it.Close()

	require.False(t, it.Next())
	require.False(t, it.Seek(txIDBytes(0)))
	require.False(t, it.Next())
	require.NoError(t, it.Err())
}

// The union is appended in whatever order the address iterator and the per-
// account Pebble scans produce, then sorted once. Whatever that append order
// is, the exposed slice must be sorted and unique before the first positioning
// call — the observable requirement that replaced per-insertion insertSorted
// (EN-1965).
func TestAddressTxIterator_UnionIsSortedAndUnique(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		txsByAccount map[string][]uint64
		addrs        []string
		want         []uint64
	}{
		{
			// One account: the Pebble scan already appends ascending.
			name:         "single account ascending",
			txsByAccount: map[string][]uint64{"acc:1": {1, 2, 3, 4}},
			addrs:        []string{"acc:1"},
			want:         []uint64{1, 2, 3, 4},
		},
		{
			// Interleaved histories: every account appends IDs that belong
			// before IDs already appended. This is the workload whose tail
			// shifts were quadratic.
			name: "interleaved histories",
			txsByAccount: map[string][]uint64{
				"acc:1": {1, 4, 7},
				"acc:2": {2, 5, 8},
				"acc:3": {3, 6, 9},
			},
			addrs: []string{"acc:1", "acc:2", "acc:3"},
			want:  []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// Strictly descending append order: each later account holds only
			// IDs below every ID appended so far.
			name: "descending append order",
			txsByAccount: map[string][]uint64{
				"acc:1": {7, 8, 9},
				"acc:2": {4, 5, 6},
				"acc:3": {1, 2, 3},
			},
			addrs: []string{"acc:1", "acc:2", "acc:3"},
			want:  []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// Every ID is shared by all three accounts: the dedup map must
			// still collapse them to one entry each.
			name: "duplicate heavy",
			txsByAccount: map[string][]uint64{
				"acc:1": {1, 2, 3},
				"acc:2": {1, 2, 3},
				"acc:3": {1, 2, 3},
			},
			addrs: []string{"acc:1", "acc:2", "acc:3"},
			want:  []uint64{1, 2, 3},
		},
		{
			// Partially overlapping histories: the dedup map must collapse the
			// shared IDs while keeping the ID only one account holds.
			name: "partially overlapping histories",
			txsByAccount: map[string][]uint64{
				"acc:1": {1, 2, 3},
				"acc:2": {1, 2, 3},
				"acc:3": {2, 3, 4},
			},
			addrs: []string{"acc:1", "acc:2", "acc:3"},
			want:  []uint64{1, 2, 3, 4},
		},
		{
			// A matched address with no transaction rows contributes nothing
			// and must not disturb the other accounts.
			name: "matched address with no rows",
			txsByAccount: map[string][]uint64{
				"acc:1": {2},
				"acc:3": {1},
			},
			addrs: []string{"acc:1", "acc:2", "acc:3"},
			want:  []uint64{1, 2},
		},
		{
			// Only the matched addresses contribute; acc:2 is indexed but not
			// selected.
			name: "unmatched account excluded",
			txsByAccount: map[string][]uint64{
				"acc:1": {1, 3},
				"acc:2": {2},
			},
			addrs: []string{"acc:1"},
			want:  []uint64{1, 3},
		},
		{
			name:         "no matched addresses",
			txsByAccount: map[string][]uint64{"acc:1": {1, 2}},
			addrs:        nil,
			want:         nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			it := newAddressTxFixture(t, tc.txsByAccount, tc.addrs...)
			defer it.Close()

			require.Equal(t, tc.want, drainIDs(t, it))

			// A seek before the first entry sees the same sorted union, so no
			// consumer can reach an unsorted slice through either entry point.
			if len(tc.want) == 0 {
				require.False(t, it.Seek(txIDBytes(0)))

				return
			}

			require.True(t, it.Seek(txIDBytes(0)))
			require.Equal(t, tc.want[0], binary.BigEndian.Uint64(it.Current()))
			require.NoError(t, it.Err())
		})
	}
}

// Sorting once must hold in every account→tx bucket, not only the any-role one
// the other tests use: compileAddressPrefix picks the bucket from the query's
// address role (addressRolePrefix), and the iterator must never mix buckets.
func TestAddressTxIterator_UnionIsSortedPerAddressRole(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		prefix byte
	}{
		{name: "any role", prefix: PrefixAccountTx},
		{name: "source", prefix: PrefixSourceAccountTx},
		{name: "destination", prefix: PrefixDestinationAccountTx},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			it := newAddressTxFixtureForPrefix(t, tc.prefix, map[string][]uint64{
				"acc:1": {5, 6},
				"acc:2": {3, 4},
				"acc:3": {1, 2},
			}, "acc:1", "acc:2", "acc:3")
			defer it.Close()

			require.Equal(t, []uint64{1, 2, 3, 4, 5, 6}, drainIDs(t, it))
		})
	}
}

// The rows written for one role must stay invisible to an iterator scanning
// another role's bucket, so a per-role empty union really is empty.
func TestAddressTxIterator_AddressRoleBucketsAreIsolated(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixSourceAccountTx, "l", "acc:1", 1), nil, pebble.NoSync))

	it := NewAddressTxIterator(
		s.DB(), dal.NewKeyBuilder(), "l", newAliasingIter("acc:1"), PrefixDestinationAccountTx,
	)
	defer it.Close()

	require.Empty(t, drainIDs(t, it))
	require.False(t, it.Seek(txIDBytes(0)))
	require.NoError(t, it.Err())
}
