package state

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// TestWriteSetQueryCheckpointMerge pins that staged creates/deletes are written
// to Pebble on Merge and are readable back via the recovery read path. The FSM
// apply path stages unconditionally; the live-checkpoint limit is enforced at
// admission, not here.
func TestWriteSetQueryCheckpointMerge(t *testing.T) {
	t.Parallel()
	buf, machine, dataStore := newTestBuffer(t)

	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 1})
	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 2})

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	rh, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh.Close() })

	ids, err := query.ReadLiveQueryCheckpointIDs(rh)
	require.NoError(t, err)
	require.Equal(t, map[uint64]struct{}{1: {}, 2: {}}, ids, "recovery read must see the committed rows")

	// Proposal 2: deleting one removes its row.
	buf2 := NewWriteSet(machine)
	buf2.Reset(&commonpb.Timestamp{Data: 1700000001})
	buf2.DeleteQueryCheckpoint(1)

	batch2 := dataStore.OpenWriteSession()
	require.NoError(t, buf2.Merge(batch2, nil))
	require.NoError(t, batch2.Commit())

	rh2, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh2.Close() })

	ids2, err := query.ReadLiveQueryCheckpointIDs(rh2)
	require.NoError(t, err)
	require.Equal(t, map[uint64]struct{}{2: {}}, ids2)
}

// TestWriteSetQueryCheckpointRollback pins that a proposal aborted via Reset (no
// Merge) writes nothing to the store.
func TestWriteSetQueryCheckpointRollback(t *testing.T) {
	t.Parallel()
	buf, _, dataStore := newTestBuffer(t)

	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 9})
	buf.Reset(&commonpb.Timestamp{Data: 1700000002})

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	rh, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh.Close() })

	ids, err := query.ReadLiveQueryCheckpointIDs(rh)
	require.NoError(t, err)
	require.Empty(t, ids)
}

// TestIsCheckpointLimitReached pins the scheduler's reason classification: only
// the typed CHECKPOINT_LIMIT_REACHED business rejection is treated as the
// expected cap steady-state; every other error (and nil) is not.
func TestIsCheckpointLimitReached(t *testing.T) {
	t.Parallel()

	require.True(t, isCheckpointLimitReached(&domain.ErrCheckpointLimitReached{Limit: 10}))
	require.True(t, isCheckpointLimitReached(&domain.BusinessError{Err: &domain.ErrCheckpointLimitReached{Limit: 10}}))
	require.True(t, isCheckpointLimitReached(fmt.Errorf("propose: %w", &domain.ErrCheckpointLimitReached{Limit: 10})))

	require.False(t, isCheckpointLimitReached(&domain.ErrCheckpointNotFound{CheckpointID: 3}))
	require.False(t, isCheckpointLimitReached(errors.New("transient")))
	require.False(t, isCheckpointLimitReached(nil))
}
