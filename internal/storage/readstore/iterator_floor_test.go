package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestSeekFloor(t *testing.T) {
	t.Parallel()

	var f seekFloor

	require.False(t, f.covers([]byte("a")), "unset floor covers nothing")

	f.fail([]byte("m"))
	require.True(t, f.covers([]byte("m")), "failure bound is inclusive")
	require.True(t, f.covers([]byte("z")))
	require.False(t, f.covers([]byte("a")), "targets below the bound must re-seek")

	f.fail([]byte("d"))
	require.True(t, f.covers([]byte("d")), "a lower failure tightens the bound")
	require.False(t, f.covers([]byte("c")))
}

func TestSeekCeil(t *testing.T) {
	t.Parallel()

	var c seekCeil

	require.False(t, c.covers([]byte("a")), "unset ceil covers nothing")

	c.fail([]byte("m"))
	require.True(t, c.covers([]byte("m")), "failure bound is inclusive")
	require.True(t, c.covers([]byte("a")))
	require.False(t, c.covers([]byte("z")), "targets above the bound must re-seek")

	c.fail([]byte("t"))
	require.True(t, c.covers([]byte("t")), "a higher failure tightens the bound")
	require.False(t, c.covers([]byte("u")))
}

// A floored leaf must still honor the absolute-seek contract: failed seeks at
// or above the proven bound return false (without re-seeking Pebble), while a
// seek below it repositions normally — including after exhaustion.
func TestPrefixIterator_SeekFloorKeepsRepositioning(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	kb := dal.NewKeyBuilder()

	for _, id := range []uint64{1, 2, 3} {
		require.NoError(t, s.DB().Set(AccountTxKey(kb, PrefixAccountTx, "l", "acc:1", id), nil, pebble.NoSync))
	}

	prefix := AccountTxPrefix(dal.NewKeyBuilder(), PrefixAccountTx, "l", "acc:1")

	it, err := NewPrefixIterator(s.DB(), prefix, len(prefix), 8)
	require.NoError(t, err)
	defer it.Close()

	require.False(t, it.SeekGE(txIDBytes(5)), "no entity >= 5")
	require.False(t, it.Next(), "failed seek leaves the iterator exhausted")
	require.False(t, it.SeekGE(txIDBytes(7)), "covered by the floor")

	require.True(t, it.SeekGE(txIDBytes(2)), "below the floor: real reposition")
	require.Equal(t, uint64(2), binary.BigEndian.Uint64(it.Current()))
	require.True(t, it.Next())
	require.Equal(t, uint64(3), binary.BigEndian.Uint64(it.Current()))
	require.False(t, it.Next())

	require.True(t, it.SeekGE(txIDBytes(0)), "reposition after Next-exhaustion")
	require.Equal(t, uint64(1), binary.BigEndian.Uint64(it.Current()))
	require.NoError(t, it.Err())
}
