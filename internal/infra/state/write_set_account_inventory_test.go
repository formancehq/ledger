package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAccountCreatedAtSurvivesValueChangesAndRenewsAfterDeletion(t *testing.T) {
	fsm, store, _ := newTestMachine(t)
	ws := NewWriteSet(fsm)
	key := domain.NewVolumeKey("ledger", "account:1", "USD", "")
	newVolume := attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{Key: key, Old: kv.None[*raftcmdpb.VolumePair]()}
	write := func(date, index uint64, updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], deletions []attributes.Deletion[domain.VolumeKey]) {
		fsm.State.LastAppliedIndex = index
		ws.SetDate(&commonpb.Timestamp{Data: date})
		batch := store.OpenWriteSession()
		require.NoError(t, ws.writeAccountInventories(batch, 0, updates, nil, deletions, nil, nil))
		require.NoError(t, batch.Commit())
	}
	read := func() *raftcmdpb.AccountInventoryState {
		state, _, err := fsm.Registry.AccountInventories.GetKey(key.AccountKey)
		require.NoError(t, err)

		return state
	}

	write(100, 10, []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{newVolume}, nil)
	require.Equal(t, uint64(100), read().GetCreatedAt())
	require.Equal(t, uint64(10), read().GetBirthIndex())
	write(200, 11, []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{{Key: key, Old: kv.Some(&raftcmdpb.VolumePair{})}}, nil)
	require.Equal(t, uint64(100), read().GetCreatedAt())
	write(300, 12, nil, []attributes.Deletion[domain.VolumeKey]{{Key: key}})
	write(400, 13, []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{newVolume}, nil)
	require.Equal(t, uint64(400), read().GetCreatedAt())
	require.Equal(t, uint64(13), read().GetBirthIndex())
}
