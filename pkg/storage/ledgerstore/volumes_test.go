package ledgerstore_test

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/stretchr/testify/require"
)

func TestGetAssetsVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := core.Now()

	tx1 := core.NewTransaction().
		WithID(0).
		WithPostings(
			core.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithTimestamp(now.Add(-3 * time.Hour))
	tx2 := core.NewTransaction().
		WithID(1).
		WithPostings(
			core.NewPosting("world", "bob", "USD", big.NewInt(100)),
		).
		WithTimestamp(now.Add(-2 * time.Hour))
	tx3 := core.NewTransaction().
		WithID(2).
		WithPostings(
			core.NewPosting("world", "users:marley", "USD", big.NewInt(100)),
		).
		WithTimestamp(now.Add(-time.Hour))

	require.NoError(t, insertTransactions(context.Background(), store, *tx1, *tx2, *tx3))

	assetVolumesForWorld, err := store.GetAssetsVolumes(context.Background(), "world")
	require.NoError(t, err, "get asset volumes should not fail")
	require.Equal(t, core.VolumesByAssets{
		"USD": core.NewEmptyVolumes().WithOutputInt64(300),
	}, assetVolumesForWorld, "asset volumes should be equal")

	assetVolumesForBob, err := store.GetAssetsVolumes(context.Background(), "bob")
	require.NoError(t, err, "get asset volumes should not fail")
	require.Equal(t, core.VolumesByAssets{
		"USD": core.NewEmptyVolumes().WithInputInt64(100),
	}, assetVolumesForBob, "asset volumes should be equal")
}
