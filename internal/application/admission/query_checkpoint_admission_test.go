package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// checkQueryCheckpointLimit rejects a create once the live count reaches the
// configured per-node limit; below it, creation is admitted.
func TestCheckQueryCheckpointLimit(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store, WithQueryCheckpointLimit(2))

	saveCheckpoint := func(id uint64) {
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: id}))
		require.NoError(t, batch.Commit())
	}

	// Empty store: 0 < 2, admitted.
	require.NoError(t, admission.checkQueryCheckpointLimit())

	// One live: 1 < 2, still admitted.
	saveCheckpoint(1)
	require.NoError(t, admission.checkQueryCheckpointLimit())

	// At the cap: 2 >= 2, rejected with the typed error carrying the limit.
	saveCheckpoint(2)
	err := admission.checkQueryCheckpointLimit()
	var limitErr *domain.ErrCheckpointLimitReached
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, uint64(2), limitErr.Limit)
}

// checkQueryCheckpointExists rejects a zero id and a non-live id, and admits a
// delete for a live checkpoint.
func TestCheckQueryCheckpointExists(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)

	// id 0 is invalid regardless of store contents.
	require.ErrorIs(t, admission.checkQueryCheckpointExists(0), domain.ErrCheckpointIDRequired)

	// A non-live id is not found.
	err := admission.checkQueryCheckpointExists(5)
	var notFoundErr *domain.ErrCheckpointNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(5), notFoundErr.CheckpointID)

	// A live id is admitted.
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: 5}))
	require.NoError(t, batch.Commit())
	require.NoError(t, admission.checkQueryCheckpointExists(5))
}
