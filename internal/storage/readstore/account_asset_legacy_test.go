package readstore

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func legacyAccountAssetKey(kb *dal.KeyBuilder, account string) []byte {
	return kb.Reset().PutByte(PrefixAccountByAsset).PutLedgerNameFixed("ledger").
		PutStringNull("USD").PutByte(2).PutString(account).Consume()
}

// TODO: Retire these RC.5 compatibility fixtures with the dual decoder and
// GC conversion, after upgrades, restores, and retained checkpoints cannot
// reopen plain account-by-asset rows.
func TestAccountAssetLegacyRowsResolveAndMigrate(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	prefix := AccountByAssetPrefix(kb, "ledger", "USD", 2)
	for _, row := range []struct {
		account string
		seq     uint64
	}{{"a", 10}, {"ab", 7}} {
		require.NoError(t, s.DB().Set(legacyAccountAssetKey(kb, row.account), binary.BigEndian.AppendUint64(nil, row.seq), pebble.NoSync))
	}
	// A late backfill discovers a's earlier touch after its RC.5 row exists.
	require.NoError(t, s.DB().Set(AccountByAssetEventKey(kb, "ledger", "USD", 2, "a", 5), nil, pebble.NoSync))
	require.NoError(t, s.DB().Set(AccountByAssetEventKey(kb, "ledger", "USD", 2, "c", 12), nil, pebble.NoSync))
	read := func(store *Store, pin uint64) (forward, reverse []string) {
		t.Helper()
		fwdIter, err := NewEventResolveIterator(store.DB(), prefix, pin)
		require.NoError(t, err)
		defer fwdIter.Close()
		for fwdIter.Next() {
			forward = append(forward, string(fwdIter.Current()))
		}
		require.NoError(t, fwdIter.Err())
		revIter, err := NewReverseEventResolveIterator(store.DB(), prefix, pin)
		require.NoError(t, err)
		defer revIter.Close()
		for revIter.Next() {
			reverse = append(reverse, string(revIter.Current()))
		}
		require.NoError(t, revIter.Err())

		return forward, reverse
	}
	for _, tc := range []struct {
		pin  uint64
		want []string
		rev  []string
	}{{4, nil, nil}, {5, []string{"a"}, []string{"a"}}, {7, []string{"a", "ab"}, []string{"ab", "a"}}, {12, []string{"a", "ab", "c"}, []string{"c", "ab", "a"}}} {
		fwd, rev := read(s, tc.pin)
		require.Equal(t, tc.want, fwd)
		require.Equal(t, tc.rev, rev)
	}
	fwdIter, err := NewEventResolveIterator(s.DB(), prefix, 7)
	require.NoError(t, err)
	require.True(t, fwdIter.Seek([]byte("a")))
	require.Equal(t, "a", string(fwdIter.Current()))
	require.True(t, fwdIter.Seek([]byte("ab")))
	require.Equal(t, "ab", string(fwdIter.Current()))
	require.NoError(t, fwdIter.Err())
	fwdIter.Close()
	revIter, err := NewReverseEventResolveIterator(s.DB(), prefix, 7)
	require.NoError(t, err)
	require.True(t, revIter.Seek([]byte("ab")))
	require.Equal(t, "ab", string(revIter.Current()))
	require.True(t, revIter.Seek([]byte("a")))
	require.Equal(t, "a", string(revIter.Current()))
	require.NoError(t, revIter.Err())
	revIter.Close()

	// Frozen checkpoints cannot be swept by live GC, so both readers must keep
	// accepting their legacy rows after the live store has converted its copy.
	checkpoint := filepath.Join(t.TempDir(), "readindex")
	require.NoError(t, s.CreateCheckpoint(checkpoint))
	frozen, err := OpenReadOnly(checkpoint, s.logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, frozen.Close()) }()
	var next []byte
	converted := 0
	for range 8 {
		count, resume, gcErr := GCEventZone(s.DB(), PrefixAccountByAsset, next, 8, 1)
		require.NoError(t, gcErr)
		converted += count
		next = resume
		if next == nil {
			break
		}
	}
	require.Nil(t, next, "bounded GC must finish the zone")
	require.Equal(t, 2, converted)
	for _, account := range []string{"a", "ab"} {
		_, closer, getErr := s.DB().Get(legacyAccountAssetKey(kb, account))
		require.ErrorIs(t, getErr, pebble.ErrNotFound)
		if closer != nil {
			require.NoError(t, closer.Close())
		}
	}
	gotFwd, gotRev := read(s, 7)
	require.Equal(t, []string{"a", "ab"}, gotFwd)
	require.Equal(t, []string{"ab", "a"}, gotRev)
	gotFwd, gotRev = read(frozen, 7)
	require.Equal(t, []string{"a", "ab"}, gotFwd)
	require.Equal(t, []string{"ab", "a"}, gotRev)

	// Reopening a checkpoint taken after conversion proves the replacement
	// survives a fresh Pebble handle, while the earlier checkpoint stays dual.
	convertedCheckpoint := filepath.Join(t.TempDir(), "converted-readindex")
	require.NoError(t, s.CreateCheckpoint(convertedCheckpoint))
	reopened, err := OpenReadOnly(convertedCheckpoint, s.logger)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	gotFwd, gotRev = read(reopened, 7)
	require.Equal(t, []string{"a", "ab"}, gotFwd)
	require.Equal(t, []string{"ab", "a"}, gotRev)
}

func TestAccountAssetMalformedLegacyRowFailsWithoutConversion(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	key := append([]byte(nil), legacyAccountAssetKey(kb, "broken")...)
	require.NoError(t, s.DB().Set(key, []byte{5}, pebble.NoSync))
	prefix := AccountByAssetPrefix(kb, "ledger", "USD", 2)
	fwd, err := NewEventResolveIterator(s.DB(), prefix, 10)
	require.NoError(t, err)
	require.False(t, fwd.Next())
	require.ErrorContains(t, fwd.Err(), "malformed")
	fwd.Close()
	rev, err := NewReverseEventResolveIterator(s.DB(), prefix, 10)
	require.NoError(t, err)
	require.False(t, rev.Next())
	require.ErrorContains(t, rev.Err(), "malformed")
	rev.Close()
	changed, _, err := GCEventZone(s.DB(), PrefixAccountByAsset, nil, 11, 100)
	require.NoError(t, err)
	require.Zero(t, changed)
	_, closer, err := s.DB().Get(key)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
}
