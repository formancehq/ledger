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
	require.NoError(t, admission.checkQueryCheckpointLimit(newBulkOverlay()))

	// One live: 1 < 2, still admitted.
	saveCheckpoint(1)
	require.NoError(t, admission.checkQueryCheckpointLimit(newBulkOverlay()))

	// At the cap: 2 >= 2, rejected with the typed error carrying the limit.
	saveCheckpoint(2)
	err := admission.checkQueryCheckpointLimit(newBulkOverlay())
	var limitErr *domain.ErrCheckpointLimitReached
	require.ErrorAs(t, err, &limitErr)
	require.Equal(t, uint64(2), limitErr.Limit)
}

// A single bulk that folds same-batch effects: a second create in one bulk
// counts the first against the limit, and a delete earlier in the bulk frees a
// slot for a later create even at the committed cap.
func TestCheckQueryCheckpointLimit_BulkOverlay(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store, WithQueryCheckpointLimit(2))

	saveCheckpoint := func(id uint64) {
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: id}))
		require.NoError(t, batch.Commit())
	}

	// One committed live checkpoint (limit 2). Two creates in one bulk: the first
	// is admitted (1 < 2) and recorded; the second sees 1+1 = 2 and is rejected.
	saveCheckpoint(1)
	overlay := newBulkOverlay()
	require.NoError(t, admission.checkQueryCheckpointLimit(overlay))

	err := admission.checkQueryCheckpointLimit(overlay)
	var limitErr *domain.ErrCheckpointLimitReached
	require.ErrorAs(t, err, &limitErr)

	// Fresh bulk at the committed cap (2 live): a delete earlier in the bulk
	// frees a slot, so the following create is admitted atomically.
	saveCheckpoint(2)
	batchOverlay := newBulkOverlay()
	require.NoError(t, admission.checkQueryCheckpointExists(1, batchOverlay))
	require.NoError(t, admission.checkQueryCheckpointLimit(batchOverlay),
		"a same-bulk delete must free a slot for a later create")
}

// checkQueryCheckpointExists rejects a zero id and a non-live id, admits a
// delete for a live checkpoint, and rejects a repeated delete within one bulk.
func TestCheckQueryCheckpointExists(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	overlay := newBulkOverlay()

	// id 0 is invalid regardless of store contents.
	require.ErrorIs(t, admission.checkQueryCheckpointExists(0, overlay), domain.ErrCheckpointIDRequired)

	// A non-live id is not found.
	err := admission.checkQueryCheckpointExists(5, overlay)
	var notFoundErr *domain.ErrCheckpointNotFound
	require.ErrorAs(t, err, &notFoundErr)
	require.Equal(t, uint64(5), notFoundErr.CheckpointID)

	// A live id is admitted.
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: 5}))
	require.NoError(t, batch.Commit())
	require.NoError(t, admission.checkQueryCheckpointExists(5, overlay))

	// Deleting the same id again in the same bulk is rejected (already staged).
	require.ErrorAs(t, admission.checkQueryCheckpointExists(5, overlay), &notFoundErr)
}
