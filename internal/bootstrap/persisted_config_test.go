package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeletePersistedConfig(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	// Save a config first
	batch := store.NewBatch()
	err := SavePersistedConfig(batch, &PersistedConfig{
		NodeID:    1,
		ClusterID: "test-cluster",
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Verify it was saved
	cfg, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	// Delete it
	batch = store.NewBatch()
	err = DeletePersistedConfig(batch)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Verify it was deleted
	cfg, err = LoadPersistedConfig(store)
	require.NoError(t, err)
	assert.Nil(t, cfg, "Persisted config should be nil after deletion")
}

func TestDeletePersistedConfig_WhenNotPresent(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	// Delete when nothing is persisted — should not error
	batch := store.NewBatch()
	err := DeletePersistedConfig(batch)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}
