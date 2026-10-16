package backup

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestReplayAccountCreatedAtAfterPurgeAndRecreation(t *testing.T) {
	store := newRebuildTestStore(t)
	attrs := attributes.New()
	batch := store.OpenWriteSession()
	writer := &attributeReplayWriter{
		store: store, batch: batch, volume: attrs.Volume, metadata: attrs.Metadata,
		accountInventory: attrs.AccountInventory,
		pendingVolumes:   make(map[string]*raftcmdpb.VolumePair),
		pendingMetadata:  make(map[string]*commonpb.MetadataValue),
	}
	volume := domain.NewVolumeKey("ledger", "account:1", "USD", "")
	writer.accountDate = 100
	require.NoError(t, writer.AddVolumeDelta(volume.Bytes(), big.NewInt(5), big.NewInt(0)))
	writer.accountDate = 200
	require.NoError(t, writer.AddVolumeDelta(volume.Bytes(), big.NewInt(2), big.NewInt(0)))
	writer.accountDate = 300
	require.NoError(t, writer.DeleteVolume(volume.Bytes()))
	writer.accountDate = 400
	require.NoError(t, writer.AddVolumeDelta(volume.Bytes(), big.NewInt(3), big.NewInt(0)))
	require.NoError(t, batch.Commit())
	require.NoError(t, rebuildAccountInventories(store))

	got, err := attrs.AccountInventory.Get(store, volume.AccountKey.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(400), got.GetCreatedAt())
	require.Equal(t, uint32(1), got.GetVolumeCount())
	// Rebuilding again must preserve the same creation date.
	require.NoError(t, rebuildAccountInventories(store))
	got, err = attrs.AccountInventory.Get(store, volume.AccountKey.Bytes())
	require.NoError(t, err)
	require.Equal(t, uint64(400), got.GetCreatedAt())
}
