package readstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAccountAssetEventsPinAndLeaseGC(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	kb := dal.NewKeyBuilder()
	prefix := AccountByAssetPrefix(kb, "ledger", "USD", 2)
	for _, seq := range []uint64{10, 5} { // backfill arrives after the live fold
		require.NoError(t, store.DB().Set(AccountByAssetEventKey(kb, "ledger", "USD", 2, "accounts:a", seq), nil, pebble.NoSync))
	}
	read := func(pin uint64) []string {
		t.Helper()
		it, err := NewEventResolveIterator(store.DB(), prefix, pin)
		require.NoError(t, err)
		defer it.Close()
		var got []string
		for it.Next() {
			got = append(got, string(it.Current()))
		}
		require.NoError(t, it.Err())

		return got
	}
	require.Empty(t, read(4))
	require.Equal(t, []string{"accounts:a"}, read(5))
	lease, ok := store.Leases().Pin(7)
	require.True(t, ok)
	watermark := store.Leases().BeginGC(11)
	require.Equal(t, uint64(7), watermark)
	pruned, _, err := GCEventZone(store.DB(), PrefixAccountByAsset, nil, watermark, 100)
	require.NoError(t, err)
	require.Zero(t, pruned)
	require.Equal(t, []string{"accounts:a"}, read(7))
	lease.Release()
	watermark = store.Leases().BeginGC(11)
	require.Equal(t, uint64(11), watermark)
	pruned, _, err = GCEventZone(store.DB(), PrefixAccountByAsset, nil, watermark, 100)
	require.NoError(t, err)
	require.Equal(t, 1, pruned)
	require.Equal(t, []string{"accounts:a"}, read(11))
}
